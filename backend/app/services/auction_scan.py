"""全市场集合竞价终态采集。

只请求扶摇文档中的 /api/a-share/auction/snapshot。不使用盘中行情快照，
也不使用短线风向标名单。有效档只在全部门闩通过后原子替换。
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
from datetime import date, datetime, time
from pathlib import Path

import polars as pl

from app.market_time import CN_TZ, cn_now
from app.plugins.fuyao.client import FuyaoError
from app.services.preferences import get_auction_scan_settings, set_auction_scan_settings

logger = logging.getLogger(__name__)

_SYMBOL = re.compile(r"^[0-9]{6}\.(SH|SZ|BJ)$")
_BATCH = 100
_GAP_S = 0.15
_RETRY_WAITS = (1.0, 3.0)
_WINDOW_START = time(9, 26)
_WINDOW_END = time(10, 30)
_ITEM_FIELDS = (
    "thscode", "ticker", "name", "auction_price", "auction_pct", "auction_volume",
    "auction_amount", "auction_unmatched", "auction_turnover_pct",
    "auction_yesterday_ratio_pct", "auction_volume_ratio", "pre_close_price",
    "open_price", "last_price", "float_market_cap",
)
_NUMERIC = {name for name in _ITEM_FIELDS if name not in {"thscode", "ticker", "name"}}
_LOCK = threading.Lock()

_TRANSPORT = {503, 5001, 5002, 5003}
_RATE_LIMIT = {429, 4001}
_STOP_NOW = {2001, 2003}


def get_settings() -> dict:
    return get_auction_scan_settings()


def save_settings(*, enabled: bool, min_pct: float, min_volume_ratio: float) -> dict:
    return set_auction_scan_settings(
        enabled=enabled, min_pct=min_pct, min_volume_ratio=min_volume_ratio,
    )


def live_client():
    from app.plugins.fuyao.client import FuyaoClient
    from app.plugins.fuyao.provider import get_api_key

    key = get_api_key()
    if not key:
        return None
    return FuyaoClient(api_key=key, timeout=20.0)


def run_scheduled() -> dict:
    return run_scan(_root(), mode="auto", now=cn_now(), client=live_client())


def run_catchup() -> dict:
    return run_scan(_root(), mode="catchup", now=cn_now(), client=live_client())


def run_manual() -> dict:
    return run_scan(_root(), mode="manual", now=cn_now(), client=live_client())


def run_scan(data_dir: Path, *, mode: str, now: datetime, client=None, sleeper=None) -> dict:
    if not _LOCK.acquire(blocking=False):
        return {"status": "already_running", "message": "已有竞价采集在进行"}
    try:
        return _run(data_dir, mode=mode, now=now, client=client, sleeper=sleeper or _sleep)
    finally:
        _LOCK.release()


def view_scan(data_dir: Path, *, today: date, on_date: date | None = None) -> dict:
    settings = get_settings()
    target = on_date or today
    final = _read_json(_final_path(data_dir, target))
    valid = final if _is_valid_record(final, target) else None
    unverified = None
    if target == today:
        raw_unverified = _read_json(_unverified_path(data_dir, today))
        if raw_unverified and raw_unverified.get("kind") == "unverified":
            unverified = {
                "label": "未核验采集",
                "started_at": raw_unverified.get("started_at"),
                "rows": raw_unverified.get("rows") or [],
                "coverage": raw_unverified.get("coverage"),
            }
    rows = _hits(valid["rows"], settings) if valid else []
    if on_date is None:
        state = "valid_final" if valid else "no_valid_today"
        is_today = True
    else:
        state = "valid_final" if valid else "no_valid_today"
        is_today = on_date == today
    return {
        "display_date": target.isoformat(),
        "is_today": is_today,
        "state": state,
        "rows": rows,
        "coverage": None if valid is None else valid.get("coverage"),
        "collected_at": None if valid is None else valid.get("started_at"),
        "running": not _LOCK.acquire(blocking=False) or _release_running(),
        "now": cn_now().isoformat(),
        "last_run": _read_json(_last_run_path(data_dir)),
        "unverified": unverified,
        "settings": settings,
        "message": None,
        "settings_href": "/settings?tab=data-sources",
    }


def _release_running() -> bool:
    _LOCK.release()
    return False


def _run(data_dir: Path, *, mode: str, now: datetime, client, sleeper) -> dict:
    moment = now.astimezone(CN_TZ)
    trade_date = moment.date()
    settings = get_settings()
    in_window = _WINDOW_START <= moment.time() <= _WINDOW_END
    if mode in {"auto", "catchup"} and not settings["enabled"]:
        return {"status": "skipped_disabled"}
    if mode in {"auto", "catchup"} and not in_window:
        return {"status": "skipped_outside_window"}
    if mode in {"auto", "catchup"} and _is_valid_record(_read_json(_final_path(data_dir, trade_date)), trade_date):
        return {"status": "skipped_exists"}
    if client is None:
        summary = _summary("not_configured", trade_date, moment, message="未配置 fuyao API Key")
        _write_last_run(data_dir, summary)
        return summary

    try:
        calendar = client.trading_days()
    except FuyaoError as exc:
        summary = _summary(
            "calendar_unavailable", trade_date, moment,
            message="交易日历不可用", error_code=_code(exc),
        )
        _write_last_run(data_dir, summary)
        logger.warning("auction scan calendar failed code=%s", _code(exc))
        return summary
    if not _calendar_usable(calendar):
        summary = _summary("calendar_unavailable", trade_date, moment, message="交易日历不可用")
        _write_last_run(data_dir, summary)
        return summary
    if trade_date not in _calendar_dates(calendar):
        summary = _summary("not_trading_day", trade_date, moment, message="不是交易日")
        _write_last_run(data_dir, summary)
        return summary

    universe = _universe(data_dir)
    if universe is None or not universe["eligible"]:
        summary = _summary(
            "universe_unavailable", trade_date, moment,
            message="个股维表不可用",
            coverage={"eligible": 0, "rejected_codes": [] if universe is None else universe["rejected"]},
        )
        _write_last_run(data_dir, summary)
        return summary

    outcome = _collect(client, universe["eligible"], trade_date, sleeper)
    coverage = {
        "eligible": len(universe["eligible"]),
        "returned": len(outcome["rows"]),
        "missing_codes": outcome["missing"],
        "rejected_codes": universe["rejected"],
        "unexpected_codes": outcome["unexpected"],
        "failed_batches": outcome["failed_batches"],
    }
    if outcome["stop_code"] in _STOP_NOW:
        summary = _summary(
            "failed", trade_date, moment, coverage=coverage, error_code=outcome["stop_code"],
            message="当前 Key 无权调用集合竞价快照" if outcome["stop_code"] == 2003 else "扶摇认证失败",
        )
        _write_last_run(data_dir, summary)
        return summary
    if outcome["status"] != "complete":
        summary = _summary(
            outcome["status"], trade_date, moment, coverage=coverage,
            error_code=outcome.get("error_code"), message=outcome.get("message") or "竞价采集未完成",
        )
        _write_last_run(data_dir, summary)
        return summary

    record = {
        "kind": "valid_final" if in_window else "unverified",
        "trade_date": trade_date.isoformat(),
        "started_at": moment.isoformat(),
        "rows": outcome["rows"],
        "coverage": coverage,
        "batch_timestamps": outcome["timestamps"],
    }
    if in_window:
        _atomic_json(_final_path(data_dir, trade_date), record)
        summary = _summary("valid_final", trade_date, moment, coverage=coverage, message="已写入当日有效终态")
    else:
        record["label"] = "未核验采集"
        _atomic_json(_unverified_path(data_dir, trade_date), record)
        summary = _summary("unverified", trade_date, moment, coverage=coverage, message="未核验采集")
    _write_last_run(data_dir, summary)
    return summary


def _collect(client, symbols: list[str], trade_date: date, sleeper) -> dict:
    rows: list[dict] = []
    missing: list[str] = []
    unexpected: list[str] = []
    failed: list[dict] = []
    timestamps: list[int] = []
    accepted = False
    for index, batch in enumerate(_chunks(symbols, _BATCH)):
        if index:
            sleeper(_GAP_S)
        try:
            data, code = _fetch_batch(client, batch, sleeper)
        except _StopError as stop:
            failed.append({"index": index, "code": stop.code})
            missing.extend(batch)
            return {
                "status": "failed", "rows": rows, "missing": missing,
                "unexpected": unexpected, "failed_batches": failed,
                "timestamps": timestamps, "stop_code": stop.code, "error_code": stop.code,
                "message": "请求被限流，已停止后续批次" if stop.code in _RATE_LIMIT else "竞价采集失败",
            }
        if data is None:
            failed.append({"index": index, "code": code})
            missing.extend(batch)
            continue
        accepted = True
        gate = _gate_batch(data, batch, trade_date)
        timestamps.append(gate["timestamp"])
        unexpected.extend(gate["unexpected"])
        if gate["error"] == "missing":
            rows.extend(gate["rows"])
            missing.extend(gate["missing"])
            continue
        if gate["error"]:
            failed.append({"index": index, "code": gate["error"]})
            missing.extend([code for code in batch if code not in {row["thscode"] for row in gate["rows"]}])
            if gate["error"] in {"phase", "status", "timestamp", "shape"}:
                return {
                    "status": "rejected", "rows": [], "missing": missing,
                    "unexpected": unexpected, "failed_batches": failed,
                    "timestamps": timestamps, "stop_code": None, "error_code": gate["error"],
                    "message": "竞价响应未通过终态校验",
                }
            continue
        rows.extend(gate["rows"])
        missing.extend(gate["missing"])
    if not accepted and failed:
        return {
            "status": "failed", "rows": [], "missing": missing, "unexpected": unexpected,
            "failed_batches": failed, "timestamps": timestamps, "stop_code": None,
            "error_code": failed[0]["code"], "message": "竞价采集失败",
        }
    if failed or missing:
        return {
            "status": "partial", "rows": rows, "missing": missing, "unexpected": unexpected,
            "failed_batches": failed, "timestamps": timestamps, "stop_code": None,
            "error_code": None, "message": "竞价采集不完整",
        }
    return {
        "status": "complete", "rows": rows, "missing": [], "unexpected": unexpected,
        "failed_batches": [], "timestamps": timestamps, "stop_code": None,
        "error_code": None, "message": None,
    }


def _fetch_batch(client, batch: list[str], sleeper):
    attempt = 0
    while True:
        try:
            return client.auction_snapshot(batch, stage="final"), None
        except FuyaoError as exc:
            code = _code(exc)
            logger.warning("auction scan batch failed code=%s", code)
            if code in _STOP_NOW:
                raise _StopError(code) from exc
            retryable = code in _TRANSPORT or code in _RATE_LIMIT
            if not retryable or attempt >= len(_RETRY_WAITS):
                if code in _RATE_LIMIT:
                    raise _StopError(code) from exc
                return None, code
            sleeper(_RETRY_WAITS[attempt])
            attempt += 1


class _StopError(Exception):
    def __init__(self, code):
        self.code = code


def _gate_batch(data: dict, batch: list[str], trade_date: date) -> dict:
    empty = {"rows": [], "missing": list(batch), "unexpected": [], "timestamp": 0, "error": "shape"}
    if not isinstance(data, dict):
        return empty
    if data.get("auction_phase") != "final":
        return {**empty, "error": "phase"}
    if data.get("data_status") != "ready":
        return {**empty, "error": "status"}
    stamp = data.get("timestamp")
    if not _timestamp_matches(stamp, trade_date):
        return {**empty, "error": "timestamp", "timestamp": stamp or 0}
    items = data.get("item")
    if not isinstance(items, list):
        return {**empty, "error": "shape", "timestamp": stamp}
    if data.get("total") is not None and data.get("total") != len(items):
        return {**empty, "error": "total", "timestamp": stamp}
    wanted = set(batch)
    grouped: dict[str, dict] = {}
    unexpected: list[str] = []
    for item in items:
        if not isinstance(item, dict) or not item.get("thscode"):
            continue
        code = str(item["thscode"])
        if code not in wanted:
            unexpected.append(code)
            continue
        normalized = _normalize(item)
        previous = grouped.get(code)
        if previous is not None and previous != normalized:
            return {**empty, "error": "conflict", "timestamp": stamp, "unexpected": unexpected}
        grouped[code] = normalized
    missing = [code for code in batch if code not in grouped]
    if missing:
        return {
            "rows": [grouped[code] for code in batch if code in grouped],
            "missing": missing, "unexpected": unexpected, "timestamp": stamp, "error": "missing",
        }
    return {
        "rows": [grouped[code] for code in batch],
        "missing": [], "unexpected": unexpected, "timestamp": stamp, "error": None,
    }


def _normalize(item: dict) -> dict:
    out = {}
    for name in _ITEM_FIELDS:
        if name in _NUMERIC:
            out[name] = _number(item.get(name)) if name in item else None
        elif name in item and item[name] is not None:
            out[name] = item[name]
        else:
            out[name] = None
    return out


def _number(value):
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _timestamp_matches(stamp, trade_date: date) -> bool:
    if isinstance(stamp, bool) or not isinstance(stamp, (int, float)):
        return False
    try:
        moment = datetime.fromtimestamp(float(stamp) / 1000.0, tz=CN_TZ)
    except (OverflowError, OSError, ValueError):
        return False
    return moment.date() == trade_date


def _universe(data_dir: Path) -> dict | None:
    path = data_dir / "instruments" / "instruments.parquet"
    if not path.exists():
        return None
    try:
        frame = pl.read_parquet(path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("auction scan instruments unreadable: %s", type(exc).__name__)
        return None
    if "symbol" not in frame.columns:
        return None
    eligible: list[str] = []
    rejected: list[str] = []
    seen: set[str] = set()
    exchanges = frame["exchange"].to_list() if "exchange" in frame.columns else [None] * frame.height
    for symbol, exchange in zip(frame["symbol"].to_list(), exchanges, strict=True):
        text = "" if symbol is None else str(symbol)
        if not _SYMBOL.match(text):
            if text:
                rejected.append(text)
            continue
        if exchange not in (None, ""):
            declared = str(exchange).upper()
            if declared != text.rsplit(".", 1)[-1]:
                rejected.append(text)
                continue
        if text in seen:
            continue
        seen.add(text)
        eligible.append(text)
    eligible.sort()
    return {"eligible": eligible, "rejected": rejected}


def _hits(rows: list[dict], settings: dict) -> list[dict]:
    min_pct = settings["min_pct"]
    min_ratio = settings["min_volume_ratio"]
    hits = []
    for row in rows:
        pct = row.get("auction_pct")
        ratio = row.get("auction_volume_ratio")
        if pct is None or ratio is None:
            continue
        if pct >= min_pct and ratio >= min_ratio:
            hits.append(row)
    return hits


def _calendar_dates(rows) -> set[date]:
    found: set[date] = set()
    if not isinstance(rows, list):
        return found
    for row in rows:
        if not isinstance(row, dict):
            continue
        raw = row.get("date")
        if isinstance(raw, str) and re.fullmatch(r"\d{8}", raw):
            found.add(datetime.strptime(raw, "%Y%m%d").date())
            continue
        if isinstance(raw, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
            found.add(date.fromisoformat(raw))
            continue
        stamp = row.get("date_ms")
        if isinstance(stamp, (int, float)) and not isinstance(stamp, bool):
            try:
                found.add(datetime.fromtimestamp(float(stamp) / 1000.0, tz=CN_TZ).date())
            except (OverflowError, OSError, ValueError):
                continue
    return found


def _calendar_usable(rows) -> bool:
    if not isinstance(rows, list) or not rows:
        return False
    return bool(_calendar_dates(rows))


def _chunks(symbols: list[str], size: int):
    for start in range(0, len(symbols), size):
        yield symbols[start:start + size]


def _summary(status: str, trade_date: date, moment: datetime, **extra) -> dict:
    body = {"status": status, "trade_date": trade_date.isoformat(), "started_at": moment.isoformat()}
    body.update(extra)
    return body


def _code(exc: FuyaoError):
    if exc.code not in (None, ""):
        return int(exc.code) if str(exc.code).isdigit() else exc.code
    match = re.search(r"HTTP (\d+)", str(exc))
    if match:
        return int(match.group(1))
    return None


def _is_valid_record(record: dict | None, trade_date: date) -> bool:
    return bool(
        isinstance(record, dict)
        and record.get("kind") == "valid_final"
        and record.get("trade_date") == trade_date.isoformat()
        and isinstance(record.get("rows"), list)
    )


def _read_json(path: Path) -> dict | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, UnicodeError):
        return None
    return data if isinstance(data, dict) else None


def _atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    try:
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def _write_last_run(data_dir: Path, summary: dict) -> None:
    _atomic_json(_last_run_path(data_dir), summary)


def _final_path(data_dir: Path, day: date) -> Path:
    return data_dir / "auction_scan" / "final" / f"date={day.isoformat()}.json"


def _unverified_path(data_dir: Path, day: date) -> Path:
    return data_dir / "auction_scan" / "unverified" / f"date={day.isoformat()}.json"


def _last_run_path(data_dir: Path) -> Path:
    return data_dir / "auction_scan" / "last_run.json"


def _root() -> Path:
    from app.config import settings
    return Path(settings.data_dir)


def _sleep(seconds: float) -> None:
    import time as time_mod
    time_mod.sleep(seconds)
