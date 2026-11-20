"""标的维表同步服务。

盘前 9:10 调用 tf.exchanges.get_instruments("SH"/"SZ"/"BJ", type="stock")
获取全量标的元数据，flatten ext 字段，写入 instruments.parquet。

单个交易所超时、5xx、空响应或结构无效时，只保留该市场上次有效行，
不用残表覆盖整张维表。成功的交易所仍整表替换，从而允许改名和退市。

Starter+ 盘后可用 quotes.get(universes) 顺便补充 name。
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from app.market_time import cn_today
from app.services.fs_utils import atomic_write_parquet
from app.tickflow import client as tf_client
from app.tickflow.client import get_client

logger = logging.getLogger(__name__)

_EXCHANGES = ["SH", "SZ", "BJ"]
_EXCHANGE_ATTEMPTS = 3
# 仅在失败后的重试之间等待。最后一次失败不再睡。
_RETRY_BACKOFF_SECONDS = (0.5, 1.5)


def _flatten_instruments(items: list[dict], *, current_limits: bool = True) -> list[dict]:
    """把 SDK 返回的 Instrument 列表 flatten 成扁平行。"""
    rows = []
    for item in items:
        row = {
            "symbol": item.get("symbol"),
            "name": item.get("name"),
            "code": item.get("code"),
            "exchange": item.get("exchange"),
            "region": item.get("region"),
            "type": item.get("type"),
        }
        ext = item.get("ext") or {}
        row["listing_date"] = ext.get("listing_date")
        row["total_shares"] = ext.get("total_shares")
        row["float_shares"] = ext.get("float_shares")
        row["tick_size"] = ext.get("tick_size")
        row["limit_up"] = ext.get("limit_up") if current_limits else None
        row["limit_down"] = ext.get("limit_down") if current_limits else None
        rows.append(row)
    return rows


def _fetch_instruments_via_provider() -> list[dict] | None:
    """若当前日K数据源不是 tickflow 且该 provider 提供 get_instruments, 用它拉标的维表。

    返回 flatten 行列表; 未命中(仍应走 tickflow)时返回 None。
    标的维表跟随日K数据源(二者天然耦合, 无独立偏好项)。
    """
    from app.services import preferences

    provider_name = preferences.get_daily_data_provider()
    if provider_name == "tickflow":
        return None
    from app.data_providers import custom as custom_sources

    if not custom_sources.is_custom_provider(provider_name):
        return None
    provider = custom_sources.get_provider(provider_name)
    if not hasattr(provider, "get_instruments"):
        return None
    try:
        items = provider.get_instruments("stock") or []
    except Exception as e:  # noqa: BLE001
        logger.warning("provider %s get_instruments 失败: %s", provider_name, e)
        return None
    rows = _flatten_instruments(items)
    logger.info("instruments via %s: %d stocks", provider_name, len(rows))
    return rows


@dataclass(frozen=True)
class InstrumentSyncResult:
    """一次个股维表同步的结果。`rows` 是本次实际写入的行数，未写盘时为 0。

    status:
      ok      — 本次快照完整写盘
      partial — 写了盘，但至少一个交易所保留的是上次有效行
      failed  — 没有写盘
    """

    rows: int
    written: bool
    status: str
    failed_exchanges: tuple[str, ...] = ()
    preserved_exchanges: tuple[str, ...] = ()
    updated_exchanges: tuple[str, ...] = ()
    message: str = ""

    def as_dict(self) -> dict:
        return {
            "instruments_rows": self.rows,
            "written": self.written,
            "status": self.status,
            "failed_exchanges": list(self.failed_exchanges),
            "preserved_exchanges": list(self.preserved_exchanges),
            "updated_exchanges": list(self.updated_exchanges),
            "message": self.message,
        }


def sync_instruments(data_dir: Path) -> int:
    """全量同步标的维表，返回本次写入行数。未写盘时返回 0。

    失败细节见 `sync_instruments_result`。只看返回的整数会把「部分写入」
    和「完整成功」混在一起，调度与管道应使用结果对象。
    """
    return sync_instruments_result(data_dir).rows


def sync_instruments_result(data_dir: Path) -> InstrumentSyncResult:
    """同步个股维表。TickFlow 按交易所合并；自定义源仍以其返回作为全量快照。"""
    provider_rows = _fetch_instruments_via_provider()
    if provider_rows is not None:
        return _sync_provider_snapshot(data_dir, provider_rows)
    return _sync_tickflow_markets(data_dir)


def _retry_sleep(seconds: float) -> None:
    time.sleep(seconds)


def _sync_provider_snapshot(data_dir: Path, rows: list[dict]) -> InstrumentSyncResult:
    """自定义源的 get_instruments 是一份全量快照，不按交易所猜测缺了谁。

    空快照不写盘。源本身没有逐交易所的失败信号，缺一个市场不能当成上游故障。
    """
    if not rows:
        message = "个股维表同步失败(数据源返回空), 未覆盖已有维表"
        logger.error(message)
        return InstrumentSyncResult(rows=0, written=False, status="failed", message=message)
    height = _commit_frame(pl.DataFrame(rows), data_dir, stamp_all=True)
    message = f"个股维表同步完成,{height} 只标的"
    logger.info(message)
    present = tuple(
        ex for ex in _EXCHANGES
        if any(_canonical_market(row.get("symbol"), row.get("exchange")) == ex for row in rows)
    )
    return InstrumentSyncResult(
        rows=height, written=True, status="ok",
        updated_exchanges=present, message=message,
    )


def _sync_tickflow_markets(data_dir: Path) -> InstrumentSyncResult:
    tf = get_client()
    # 免费接口的限价来自历史行情，不能盖上同步日期当作当日权威价。
    current_limits = tf_client.current_mode() == "api_key"
    fresh: list[dict] = []
    failed: list[str] = []
    reasons: list[str] = []
    for ex in _EXCHANGES:
        rows, reason = _fetch_exchange_rows(tf, ex, current_limits=current_limits)
        if rows is None:
            failed.append(ex)
            reasons.append(f"{ex}: {reason}")
            continue
        fresh.extend(rows)

    path = data_dir / "instruments" / "instruments.parquet"
    prior, prior_state = _load_prior(path)
    updated = tuple(ex for ex in _EXCHANGES if ex not in failed)
    if failed and prior is not None:
        _warn_sharp_shrink(prior, fresh, updated)

    if not updated:
        message = _failure_message(reasons, prior_state)
        logger.error(message)
        return InstrumentSyncResult(
            rows=0, written=False, status="failed",
            failed_exchanges=tuple(failed), message=message,
        )

    merged = _merge_markets(prior, fresh, updated)
    if merged is None:
        message = _failure_message(reasons, prior_state)
        logger.error(message)
        return InstrumentSyncResult(
            rows=0, written=False, status="failed",
            failed_exchanges=tuple(failed), message=message,
        )
    if not _failed_markets_retained(prior, merged, failed):
        message = (
            "个股维表同步失败(" + "; ".join(reasons) + "), "
            "合并结果未能保留失败市场的已有名称, 未覆盖维表"
        )
        logger.error(message)
        return InstrumentSyncResult(
            rows=0, written=False, status="failed",
            failed_exchanges=tuple(failed), message=message,
        )

    height = _commit_frame(merged, data_dir, stamp_all=not failed)
    preserved = tuple(
        ex for ex in failed
        if prior is not None and _symbols_for_market(prior, ex)
    )
    if failed:
        message = _partial_message(reasons, updated, preserved, height, prior_state)
        logger.error(message)
        status = "partial"
    else:
        message = f"个股维表同步完成,{height} 只标的"
        logger.info(message)
        status = "ok"
    return InstrumentSyncResult(
        rows=height, written=True, status=status,
        failed_exchanges=tuple(failed), preserved_exchanges=preserved,
        updated_exchanges=updated, message=message,
    )


def _fetch_exchange_rows(tf, ex: str, *, current_limits: bool) -> tuple[list[dict] | None, str]:
    """拉取一个交易所。空列表和非列表会重试；结构无效的非空响应不重试。"""
    last_reason = "unknown"
    for attempt in range(_EXCHANGE_ATTEMPTS):
        try:
            items = tf.exchanges.get_instruments(ex, instrument_type="stock")
        except Exception as e:  # noqa: BLE001
            last_reason = _short_reason(f"{type(e).__name__}: {e}")
            logger.warning(
                "get_instruments(%s) attempt %d/%d failed: %s",
                ex, attempt + 1, _EXCHANGE_ATTEMPTS, last_reason,
            )
        else:
            rows, reason = _validate_exchange_payload(ex, items, current_limits=current_limits)
            if rows is not None:
                logger.info("instruments %s: %d stocks", ex, len(rows))
                return rows, ""
            last_reason = reason
            retry = reason == "empty" or reason.startswith("not-a-list")
            logger.warning(
                "get_instruments(%s) attempt %d/%d unusable: %s",
                ex, attempt + 1, _EXCHANGE_ATTEMPTS, reason,
            )
            if not retry:
                return None, reason
        if attempt + 1 < _EXCHANGE_ATTEMPTS:
            _retry_sleep(_RETRY_BACKOFF_SECONDS[min(attempt, len(_RETRY_BACKOFF_SECONDS) - 1)])
    logger.error(
        "get_instruments(%s) failed after %d attempts: %s",
        ex, _EXCHANGE_ATTEMPTS, last_reason,
    )
    return None, last_reason


def _validate_exchange_payload(
    ex: str, items, *, current_limits: bool,
) -> tuple[list[dict] | None, str]:
    """一个交易所有效，当且仅当至少留下一行属于该市场的标的。"""
    if items is None or items == []:
        return None, "empty"
    if not isinstance(items, list):
        return None, "not-a-list"
    dicts = [item for item in items if isinstance(item, dict)]
    if not dicts:
        return None, "invalid"
    kept: dict[str, dict] = {}
    for row in _flatten_instruments(dicts, current_limits=current_limits):
        symbol = row.get("symbol")
        if not isinstance(symbol, str) or not symbol.strip():
            continue
        symbol = _normalize_symbol(symbol)
        market = _canonical_market(symbol, row.get("exchange"))
        if market != ex:
            continue
        name = row.get("name")
        if isinstance(name, str):
            row["name"] = name.strip() or None
        row["symbol"] = symbol
        row["exchange"] = ex
        kept[symbol] = row
    if not kept:
        return None, "invalid"
    return list(kept.values()), ""


def _normalize_symbol(symbol: str) -> str:
    text = symbol.strip()
    code, dot, suffix = text.rpartition(".")
    if dot and suffix.upper() in _EXCHANGES:
        return f"{code}.{suffix.upper()}"
    return text


def _canonical_market(symbol: object, exchange: object) -> str | None:
    suffix = None
    if isinstance(symbol, str) and "." in symbol:
        tail = symbol.rsplit(".", 1)[-1].upper()
        if tail in _EXCHANGES:
            suffix = tail
    declared = None
    if isinstance(exchange, str):
        text = exchange.strip().upper()
        if text in _EXCHANGES:
            declared = text
    if suffix and declared and suffix != declared:
        return None
    return suffix or declared


def _load_prior(path: Path) -> tuple[pl.DataFrame | None, str]:
    if not path.exists():
        return None, "missing"
    try:
        df = pl.read_parquet(path)
    except Exception as e:  # noqa: BLE001
        logger.error("instruments 历史维表无法读取, 失败市场无法保留名称: %s", e)
        return None, "unusable"
    if df.is_empty() or "symbol" not in df.columns:
        logger.error("instruments 历史维表没有可用 symbol, 失败市场无法保留名称")
        return None, "unusable"
    return df, "ok"


def _with_market(df: pl.DataFrame) -> pl.DataFrame:
    symbol = pl.col("symbol").cast(pl.Utf8, strict=False)
    suffix = (
        pl.when(symbol.str.contains(r"\."))
        .then(symbol.str.split(".").list.last().str.to_uppercase())
        .otherwise(pl.lit(None, dtype=pl.Utf8))
    )
    suffix = pl.when(suffix.is_in(list(_EXCHANGES))).then(suffix).otherwise(pl.lit(None, dtype=pl.Utf8))
    if "exchange" in df.columns:
        declared = pl.col("exchange").cast(pl.Utf8, strict=False).str.strip_chars().str.to_uppercase()
        declared = (
            pl.when(declared.is_in(list(_EXCHANGES)))
            .then(declared)
            .otherwise(pl.lit(None, dtype=pl.Utf8))
        )
        market = pl.coalesce(suffix, declared)
    else:
        market = suffix
    return df.with_columns(market.alias("_market"))


def _symbols_for_market(df: pl.DataFrame | None, market: str) -> set[str]:
    if df is None or df.is_empty() or "symbol" not in df.columns:
        return set()
    tagged = _with_market(df)
    values = tagged.filter(pl.col("_market") == market)["symbol"].drop_nulls().to_list()
    return {str(v) for v in values}


def _warn_sharp_shrink(prior: pl.DataFrame, fresh: list[dict], updated: tuple[str, ...]) -> None:
    """有效快照变短仍会替换该市场。这里只留下日志，不用固定行数拦住退市。"""
    fresh_df = pl.DataFrame(fresh) if fresh else None
    for ex in updated:
        old_n = len(_symbols_for_market(prior, ex))
        new_n = len(_symbols_for_market(fresh_df, ex)) if fresh_df is not None else 0
        if old_n >= 20 and new_n * 2 < old_n:
            logger.warning(
                "instruments %s 从 %d 行降到 %d 行, 仍按有效快照替换该市场",
                ex, old_n, new_n,
            )


def _merge_markets(
    prior: pl.DataFrame | None, fresh: list[dict], updated: tuple[str, ...],
) -> pl.DataFrame | None:
    frames: list[pl.DataFrame] = []
    if prior is not None and len(updated) < len(_EXCHANGES):
        preserved = _with_market(prior).filter(~pl.col("_market").is_in(list(updated))).drop("_market")
        if preserved.height:
            frames.append(preserved)
    if fresh:
        frames.append(pl.DataFrame(fresh).with_columns(pl.lit(cn_today()).alias("as_of")))
    if not frames:
        return None
    merged = frames[0] if len(frames) == 1 else pl.concat(frames, how="diagonal_relaxed")
    if "symbol" not in merged.columns or merged.is_empty():
        return None
    return merged.unique(subset=["symbol"], keep="last")


def _failed_markets_retained(
    prior: pl.DataFrame | None, merged: pl.DataFrame, failed: list[str],
) -> bool:
    if prior is None:
        return True
    for ex in failed:
        old = _symbols_for_market(prior, ex)
        if old and not old <= _symbols_for_market(merged, ex):
            return False
    return True


def _commit_frame(df: pl.DataFrame, data_dir: Path, *, stamp_all: bool) -> int:
    df = _preserve_instrument_fields(df, data_dir)
    if stamp_all or "as_of" not in df.columns:
        df = df.with_columns(pl.lit(cn_today()).alias("as_of"))
    out = data_dir / "instruments" / "instruments.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    if df.is_empty() or "symbol" not in df.columns:
        raise ValueError("refusing to write an empty instruments table")
    atomic_write_parquet(df, out)
    return int(df.height)


def _short_reason(text: str) -> str:
    compact = " ".join(text.split())
    return compact[:180]


def _failure_message(reasons: list[str], prior_state: str) -> str:
    detail = "; ".join(reasons) if reasons else "无可用数据"
    if prior_state == "ok":
        return f"个股维表同步失败({detail}), 未覆盖已有维表"
    if prior_state == "unusable":
        return f"个股维表同步失败({detail}), 历史维表无法读取且本次无可用数据, 未写入"
    return f"个股维表同步失败({detail}), 未写入维表"


def _partial_message(
    reasons: list[str], updated: tuple[str, ...], preserved: tuple[str, ...],
    rows: int, prior_state: str,
) -> str:
    kept = ",".join(preserved) if preserved else "无"
    refreshed = ",".join(updated) if updated else "无"
    extra = ""
    if prior_state == "unusable":
        extra = "；历史维表无法读取,失败市场的名称未能保留"
    elif prior_state == "missing":
        extra = "；无历史维表,仅写入本次成功的市场"
    return (
        f"个股维表部分失败({'; '.join(reasons)}): "
        f"已更新 {refreshed}, 保留 {kept} 的上次有效名称, 共 {rows} 行, 未用残表覆盖失败市场"
        f"{extra}"
    )


# 股本与静态交易规则字段: 部分行情源的标的维表只有代码/名称/市场/类型。
# 这些字段缺失会让 enriched 的 turnover_rate / 市值为空, 进而使
# 依赖换手率与流通市值的策略筛不出任何标的 —— 因此绝不能用 null 覆盖已有值。
# 涨跌停价随交易日变化，不从历史维表继承。
_SHARE_FIELDS = (
    "listing_date", "total_shares", "float_shares", "tick_size",
)


def _preserve_instrument_fields(df: pl.DataFrame, data_dir: Path) -> pl.DataFrame:
    """用现有维表补全 provider 未提供的名称、股本和交易规则字段。

    新增标的仍保留 null (没有历史值可用, 不伪造); 只在同一 symbol 上把
    provider 的空值替换为既有值, 并在日志里给出补全规模。
    """
    if "name" in df.columns:
        df = df.with_columns(pl.col("name").cast(pl.Utf8).str.strip_chars().replace("", None))
    available = [c for c in ("name", *_SHARE_FIELDS) if c in df.columns]
    if not available:
        return df
    if all(df[c].null_count() == 0 for c in available):
        return df  # provider 已提供全部字段, 无需补全

    path = data_dir / "instruments" / "instruments.parquet"
    if not path.exists():
        logger.warning(
            "instruments provider 缺少标的信息且无历史维表可补全: %s",
            "/".join(c for c in available if df[c].null_count()),
        )
        return df
    try:
        existing = pl.read_parquet(path)
    except Exception as e:  # noqa: BLE001
        logger.warning("instruments 历史维表读取失败, 无法补全股本字段: %s", e)
        return df
    if "symbol" not in existing.columns:
        return df

    keep = ["symbol", *[c for c in available if c in existing.columns]]
    if len(keep) <= 1:
        return df
    merged = df.join(existing.select(keep), on="symbol", how="left", suffix="_prev")
    filled: list[str] = []
    for col in keep[1:]:
        prev = f"{col}_prev"
        if prev not in merged.columns:
            continue
        # 逐行 coalesce: provider 有值用 provider, 为空才回落历史值。这样
        # 「部分标的缺字段」也能补齐, 而不只处理整列为空的源。
        if merged[col].null_count() and merged[prev].is_not_null().any():
            merged = merged.with_columns(
                pl.coalesce(pl.col(col), pl.col(prev)).alias(col)
            )
            filled.append(col)
    merged = merged.drop([f"{c}_prev" for c in keep[1:] if f"{c}_prev" in merged.columns])
    if filled:
        restored = int(merged.select(
            [pl.col(c).is_not_null().sum() for c in filled]
        ).row(0)[0])
        logger.info(
            "instruments provider 缺 %s, 已逐行从历史维表补全 (首列非空 %d/%d)",
            "/".join(filled), restored, merged.height,
        )
    return merged


def enrich_names_from_quotes(
    data_dir: Path,
    quotes_data: list[dict],
) -> int:
    """从 quotes 响应中提取 name，更新 instruments 维表（兜底补充）。

    盘后 quotes.get(universes) 返回的数据中包含 ext.name，
    用来补充 instruments 中可能缺失的 name。
    """
    if not quotes_data:
        return 0

    # 构建 symbol → name 映射
    name_map: dict[str, str] = {}
    for q in quotes_data:
        symbol = q.get("symbol", "")
        ext = q.get("ext") or {}
        name = ext.get("name") or q.get("name", "")
        if symbol and name:
            name_map[symbol] = name

    if not name_map:
        return 0

    inst_path = data_dir / "instruments" / "instruments.parquet"
    if not inst_path.exists():
        return 0

    df = pl.read_parquet(inst_path)

    # 只更新空 name 的行
    updates = pl.DataFrame({
        "symbol": list(name_map.keys()),
        "_new_name": list(name_map.values()),
    })
    df = df.join(updates, on="symbol", how="left")
    df = df.with_columns(
        pl.when(pl.col("name").is_null() | (pl.col("name") == ""))
        .then(pl.col("_new_name"))
        .otherwise(pl.col("name"))
        .alias("name"),
    ).drop("_new_name")

    atomic_write_parquet(df, inst_path)
    logger.info("instruments name enriched from quotes: %d names", len(name_map))
    return len(name_map)
