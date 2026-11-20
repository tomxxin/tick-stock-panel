"""个股维表按交易所失败时不得抹掉该市场已有名称。

生产现象: 某一交易所 503 / 超时 / 空响应后, 同步仍记成功, 并用其余交易所的残表
覆盖 instruments.parquet。这里锁定的是「失败市场保留上次有效行, 成功市场允许
改名和退市」, 不用固定行数当门槛。
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from app.jobs import daily_pipeline
from app.services import instrument_sync as ims

TODAY = date(2026, 11, 20)
YESTERDAY = date(2026, 11, 19)


class _Client:
    def __init__(self, handlers: dict):
        self.handlers = handlers
        self.calls: dict[str, int] = {}
        self.exchanges = self

    def get_instruments(self, ex, instrument_type="stock"):
        assert instrument_type == "stock"
        self.calls[ex] = self.calls.get(ex, 0) + 1
        spec = self.handlers[ex]
        if isinstance(spec, BaseException):
            raise spec
        if callable(spec):
            return spec(self.calls[ex])
        return spec


def _item(symbol: str, name: str, *, float_shares: float | None = 2.0) -> dict:
    exchange = symbol.rsplit(".", 1)[-1]
    return {
        "symbol": symbol,
        "name": name,
        "code": symbol.split(".")[0],
        "exchange": exchange,
        "region": "CN",
        "type": "stock",
        "ext": {
            "float_shares": float_shares,
            "total_shares": float_shares,
            "tick_size": 0.01,
        },
    }


def _stored(symbol: str, name: str, *, as_of: date = YESTERDAY, float_shares: float = 1.0) -> dict:
    exchange = symbol.rsplit(".", 1)[-1]
    return {
        "symbol": symbol,
        "name": name,
        "code": symbol.split(".")[0],
        "exchange": exchange,
        "region": "CN",
        "type": "stock",
        "listing_date": "2020-01-01",
        "total_shares": float_shares,
        "float_shares": float_shares,
        "tick_size": 0.01,
        "limit_up": None,
        "limit_down": None,
        "as_of": as_of,
    }


def _write(data_dir: Path, rows: list[dict]) -> Path:
    path = data_dir / "instruments" / "instruments.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)
    return path


def _by_symbol(data_dir: Path) -> dict[str, dict]:
    frame = pl.read_parquet(data_dir / "instruments" / "instruments.parquet")
    return {row["symbol"]: row for row in frame.to_dicts()}


@pytest.fixture
def tickflow(monkeypatch):
    sleeps: list[float] = []
    monkeypatch.setattr(ims, "_retry_sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(ims, "_fetch_instruments_via_provider", lambda: None)
    monkeypatch.setattr(ims.tf_client, "current_mode", lambda: "none")
    monkeypatch.setattr(ims, "cn_today", lambda: TODAY)

    def install(handlers: dict) -> _Client:
        client = _Client(handlers)
        monkeypatch.setattr(ims, "get_client", lambda: client)
        return client

    install.sleeps = sleeps
    return install


def _ok_markets() -> dict:
    return {
        "SH": [_item("600000.SH", "浦发银行")],
        "SZ": [_item("000001.SZ", "平安银行")],
        "BJ": [_item("830001.BJ", "北交示例")],
    }


def test_market_exception_keeps_that_market_and_applies_other_updates(tmp_path, tickflow):
    """SZ 超时: 保留 SZ 名称和股本; SH 改名、退市、新上市仍然生效。"""
    _write(tmp_path, [
        _stored("600000.SH", "旧浦发", float_shares=9.0),
        _stored("600001.SH", "待退市"),
        _stored("000001.SZ", "平安银行", float_shares=8.0),
        _stored("830001.BJ", "旧北交"),
    ])
    handlers = _ok_markets()
    handlers["SH"] = [_item("600000.SH", "浦发银行"), _item("600009.SH", "新上市", float_shares=None)]
    handlers["SZ"] = TimeoutError("timed out")
    handlers["BJ"] = [_item("830001.BJ", "北交新名")]
    client = tickflow(handlers)

    result = ims.sync_instruments_result(tmp_path)

    assert result.status == "partial"
    assert result.written is True
    assert result.failed_exchanges == ("SZ",)
    assert result.preserved_exchanges == ("SZ",)
    assert result.updated_exchanges == ("SH", "BJ")
    assert "同步完成" not in result.message
    assert "TimeoutError" in result.message
    assert client.calls["SZ"] == ims._EXCHANGE_ATTEMPTS
    assert tickflow.sleeps  # 超时会重试, 但测试里不真的等待
    rows = _by_symbol(tmp_path)
    assert set(rows) == {"600000.SH", "600009.SH", "000001.SZ", "830001.BJ"}
    assert rows["600000.SH"]["name"] == "浦发银行"
    assert rows["600000.SH"]["float_shares"] == pytest.approx(2.0)
    assert rows["600009.SH"]["name"] == "新上市"
    assert rows["600009.SH"]["float_shares"] is None
    assert rows["000001.SZ"]["name"] == "平安银行"
    assert rows["000001.SZ"]["float_shares"] == pytest.approx(8.0)
    assert rows["000001.SZ"]["as_of"] == YESTERDAY
    assert rows["600000.SH"]["as_of"] == TODAY
    assert rows["830001.BJ"]["name"] == "北交新名"


def test_empty_and_invalid_payloads_do_not_drop_the_market(tmp_path, tickflow):
    _write(tmp_path, [
        _stored("600000.SH", "浦发银行"),
        _stored("000001.SZ", "平安银行"),
        _stored("000002.SZ", "万科A"),
        _stored("830001.BJ", "北交示例"),
    ])
    handlers = _ok_markets()
    handlers["SZ"] = []
    client = tickflow(handlers)
    empty = ims.sync_instruments_result(tmp_path)
    assert empty.status == "partial"
    assert empty.preserved_exchanges == ("SZ",)
    assert client.calls["SZ"] == ims._EXCHANGE_ATTEMPTS
    assert _by_symbol(tmp_path)["000002.SZ"]["name"] == "万科A"

    handlers["SZ"] = {"error": "503"}
    client = tickflow(handlers)
    not_list = ims.sync_instruments_result(tmp_path)
    assert not_list.failed_exchanges == ("SZ",)
    assert client.calls["SZ"] == ims._EXCHANGE_ATTEMPTS
    assert _by_symbol(tmp_path)["000001.SZ"]["name"] == "平安银行"

    handlers["SZ"] = [{"symbol": "600000.SH", "name": "错市场", "exchange": "SH"}]
    client = tickflow(handlers)
    invalid = ims.sync_instruments_result(tmp_path)
    assert invalid.failed_exchanges == ("SZ",)
    assert client.calls["SZ"] == 1  # 非空但结构无效, 不把重试当成修复
    names = _by_symbol(tmp_path)
    assert names["000001.SZ"]["name"] == "平安银行"
    assert names["000002.SZ"]["name"] == "万科A"


def test_all_markets_down_keeps_the_file_byte_for_byte(tmp_path, tickflow):
    path = _write(tmp_path, [
        _stored("600000.SH", "浦发银行"),
        _stored("000001.SZ", "平安银行"),
        _stored("830001.BJ", "北交示例"),
    ])
    before = path.read_bytes()
    tickflow({
        "SH": TimeoutError("timed out"),
        "SZ": RuntimeError("503 Service Unavailable"),
        "BJ": [],
    })

    result = ims.sync_instruments_result(tmp_path)

    assert result.status == "failed"
    assert result.written is False
    assert result.rows == 0
    assert path.read_bytes() == before
    assert "未覆盖已有维表" in result.message
    assert not list(path.parent.glob("*.tmp"))


def test_bootstrap_without_prior_cache_writes_only_successful_markets(tmp_path, tickflow):
    handlers = _ok_markets()
    handlers["SZ"] = TimeoutError("timed out")
    tickflow(handlers)

    result = ims.sync_instruments_result(tmp_path)

    assert result.status == "partial"
    assert result.preserved_exchanges == ()
    assert "无历史维表" in result.message
    assert set(_by_symbol(tmp_path)) == {"600000.SH", "830001.BJ"}


def test_bootstrap_all_markets_down_creates_nothing(tmp_path, tickflow):
    tickflow({ex: TimeoutError("timed out") for ex in ("SH", "SZ", "BJ")})

    result = ims.sync_instruments_result(tmp_path)

    assert result.status == "failed"
    assert result.written is False
    assert not (tmp_path / "instruments" / "instruments.parquet").exists()


def test_corrupt_prior_full_refresh_replaces_it(tmp_path, tickflow):
    path = _write(tmp_path, [_stored("600000.SH", "不会被读到")])
    path.write_bytes(b"not a parquet")
    tickflow(_ok_markets())

    result = ims.sync_instruments_result(tmp_path)

    assert result.status == "ok"
    assert set(_by_symbol(tmp_path)) == {"600000.SH", "000001.SZ", "830001.BJ"}


def test_corrupt_prior_partial_does_not_claim_names_were_kept(tmp_path, tickflow):
    path = _write(tmp_path, [_stored("000001.SZ", "平安银行")])
    path.write_bytes(b"not a parquet")
    handlers = _ok_markets()
    handlers["SZ"] = TimeoutError("timed out")
    tickflow(handlers)

    result = ims.sync_instruments_result(tmp_path)

    assert result.status == "partial"
    assert result.preserved_exchanges == ()
    assert "无法读取" in result.message
    assert "000001.SZ" not in _by_symbol(tmp_path)
    assert "600000.SH" in _by_symbol(tmp_path)


def test_corrupt_prior_all_fail_leaves_the_bytes_untouched(tmp_path, tickflow):
    path = _write(tmp_path, [_stored("000001.SZ", "平安银行")])
    path.write_bytes(b"not a parquet")
    before = path.read_bytes()
    tickflow({ex: TimeoutError("down") for ex in ("SH", "SZ", "BJ")})

    result = ims.sync_instruments_result(tmp_path)

    assert result.status == "failed"
    assert "无法读取" in result.message
    assert path.read_bytes() == before


def test_full_refresh_applies_rename_delisting_and_listing(tmp_path, tickflow):
    _write(tmp_path, [
        _stored("600000.SH", "旧名", float_shares=4.0),
        _stored("600001.SH", "退市股"),
        _stored("000001.SZ", "平安银行"),
        _stored("830001.BJ", "北交示例"),
    ])
    handlers = _ok_markets()
    handlers["SH"] = [
        _item("600000.SH", "新名", float_shares=None),
        _item("600009.SH", "新上市"),
    ]
    tickflow(handlers)

    result = ims.sync_instruments_result(tmp_path)

    assert result.status == "ok"
    assert result.failed_exchanges == ()
    assert result.written is True
    rows = _by_symbol(tmp_path)
    assert set(rows) == {"600000.SH", "600009.SH", "000001.SZ", "830001.BJ"}
    assert rows["600000.SH"]["name"] == "新名"
    assert rows["600000.SH"]["float_shares"] == pytest.approx(4.0)
    assert "600001.SH" not in rows


def test_successful_market_may_shrink_without_a_fixed_minimum(tmp_path, tickflow):
    _write(tmp_path, [
        _stored("600000.SH", "甲"),
        _stored("600001.SH", "乙"),
        _stored("600002.SH", "丙"),
        _stored("000001.SZ", "平安银行"),
        _stored("830001.BJ", "北交示例"),
    ])
    handlers = _ok_markets()
    handlers["SH"] = [_item("600002.SH", "丙保留")]
    tickflow(handlers)

    result = ims.sync_instruments_result(tmp_path)

    assert result.status == "ok"
    rows = _by_symbol(tmp_path)
    assert "600000.SH" not in rows
    assert "600001.SH" not in rows
    assert rows["600002.SH"]["name"] == "丙保留"
    assert rows["000001.SZ"]["name"] == "平安银行"


def test_transient_timeout_then_success_replaces_that_market(tmp_path, tickflow):
    _write(tmp_path, [_stored("000001.SZ", "旧平安"), _stored("600000.SH", "浦发银行"), _stored("830001.BJ", "北交示例")])

    def sz(attempt: int):
        if attempt < 3:
            raise TimeoutError("timed out")
        return [_item("000001.SZ", "平安银行")]

    handlers = _ok_markets()
    handlers["SZ"] = sz
    client = tickflow(handlers)

    result = ims.sync_instruments_result(tmp_path)

    assert result.status == "ok"
    assert client.calls["SZ"] == 3
    assert len(tickflow.sleeps) == 2
    assert _by_symbol(tmp_path)["000001.SZ"]["name"] == "平安银行"
    assert _by_symbol(tmp_path)["000001.SZ"]["as_of"] == TODAY


def test_blank_name_on_a_live_symbol_keeps_the_previous_name(tmp_path, tickflow):
    _write(tmp_path, [_stored("600000.SH", "浦发银行"), _stored("000001.SZ", "平安银行"), _stored("830001.BJ", "北交示例")])
    handlers = _ok_markets()
    handlers["SH"] = [_item("600000.SH", "   ")]
    tickflow(handlers)

    ims.sync_instruments_result(tmp_path)

    assert _by_symbol(tmp_path)["600000.SH"]["name"] == "浦发银行"


def test_provider_empty_snapshot_does_not_wipe(tmp_path, monkeypatch):
    path = _write(tmp_path, [_stored("000001.SZ", "平安银行"), _stored("600000.SH", "浦发银行")])
    before = path.read_bytes()
    monkeypatch.setattr(ims, "_fetch_instruments_via_provider", lambda: [])

    result = ims.sync_instruments_result(tmp_path)

    assert result.status == "failed"
    assert result.written is False
    assert path.read_bytes() == before


def test_provider_snapshot_remains_a_full_replace(tmp_path, monkeypatch):
    """自定义源没有逐交易所的失败信号, 返回的列表仍是整表快照。"""
    _write(tmp_path, [_stored("600000.SH", "浦发银行"), _stored("000001.SZ", "平安银行")])
    monkeypatch.setattr(ims, "_fetch_instruments_via_provider", lambda: [
        {
            "symbol": "600000.SH", "name": "浦发新名", "code": "600000",
            "exchange": "SH", "region": "CN", "type": "stock",
            "listing_date": None, "total_shares": None, "float_shares": None,
            "tick_size": None, "limit_up": None, "limit_down": None,
        },
    ])
    monkeypatch.setattr(ims, "cn_today", lambda: TODAY)

    result = ims.sync_instruments_result(tmp_path)

    assert result.status == "ok"
    assert set(_by_symbol(tmp_path)) == {"600000.SH"}
    assert _by_symbol(tmp_path)["600000.SH"]["name"] == "浦发新名"


def test_interrupted_write_keeps_previous_table(tmp_path, tickflow, monkeypatch):
    path = _write(tmp_path, [
        _stored("600000.SH", "旧浦发"),
        _stored("000001.SZ", "平安银行"),
        _stored("830001.BJ", "北交示例"),
    ])
    before = pl.read_parquet(path)
    handlers = _ok_markets()
    handlers["SH"] = [_item("600000.SH", "不应落盘")]
    handlers["SZ"] = TimeoutError("timed out")
    tickflow(handlers)
    real_write = pl.DataFrame.write_parquet

    def crash(self, file, *args, **kwargs):
        Path(file).write_bytes(b"PAR1truncated")
        raise OSError("simulated storage failure")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", crash)

    with pytest.raises(OSError, match="simulated storage failure"):
        ims.sync_instruments_result(tmp_path)

    assert pl.read_parquet(path).equals(before)
    monkeypatch.setattr(pl.DataFrame, "write_parquet", real_write)


def test_premarket_total_failure_does_not_refresh_cache(tmp_path, tickflow):
    _write(tmp_path, [
        _stored("600000.SH", "浦发银行"),
        _stored("000001.SZ", "平安银行"),
        _stored("830001.BJ", "北交示例"),
    ])
    tickflow({ex: TimeoutError("timed out") for ex in ("SH", "SZ", "BJ")})
    events: list[str] = []
    repo = SimpleNamespace(
        store=SimpleNamespace(data_dir=tmp_path),
        refresh_view=lambda *args, **kwargs: events.append("view"),
        clear_cache=lambda: events.append("clear"),
        refresh_cache=lambda: events.append("cache"),
    )

    with pytest.raises(RuntimeError, match="未覆盖已有维表"):
        daily_pipeline.run_instruments_sync(repo, on_progress=lambda *args, **kwargs: None)

    assert events == []
    assert _by_symbol(tmp_path)["000001.SZ"]["name"] == "平安银行"


def test_premarket_partial_sync_is_a_failure_and_refreshes_cache(tmp_path, tickflow):
    _write(tmp_path, [
        _stored("600000.SH", "浦发银行"),
        _stored("000001.SZ", "平安银行"),
        _stored("830001.BJ", "北交示例"),
    ])
    handlers = _ok_markets()
    handlers["SZ"] = RuntimeError("503 Service Unavailable")
    tickflow(handlers)
    events: list[str] = []
    stages: list[str] = []
    messages: list[str] = []

    repo = SimpleNamespace(
        store=SimpleNamespace(data_dir=tmp_path),
        refresh_view=lambda *args, **kwargs: events.append("view"),
        clear_cache=lambda: events.append("clear"),
        refresh_cache=lambda: events.append("cache"),
    )

    with pytest.raises(RuntimeError, match="部分失败") as exc:
        daily_pipeline.run_instruments_sync(
            repo,
            on_progress=lambda stage, pct, msg, **kwargs: (stages.append(stage), messages.append(msg)),
        )

    assert "同步完成" not in str(exc.value)
    assert any("503" in msg for msg in messages)
    assert "done" not in stages
    assert events == ["view", "clear", "cache"]
    assert _by_symbol(tmp_path)["000001.SZ"]["name"] == "平安银行"


def test_pipeline_partial_sync_is_a_stage_error(tmp_path, monkeypatch):
    from app.services import data_integrity, preferences
    from app.tickflow.capabilities import CapabilitySet

    messages: list[str] = []
    monkeypatch.setattr(daily_pipeline.settings, "data_dir", tmp_path)
    monkeypatch.setattr(preferences, "load", lambda: {
        "pipeline_pull_a_share": False,
        "pipeline_pull_etf": False,
        "pipeline_regime_enabled": False,
        "minute_sync_enabled": False,
        "adj_factor_provider": "tickflow",
    })
    monkeypatch.setattr(
        daily_pipeline.instrument_sync, "sync_instruments_result",
        lambda *_args, **_kwargs: ims.InstrumentSyncResult(
            rows=4, written=True, status="partial",
            failed_exchanges=("SZ",), preserved_exchanges=("SZ",),
            updated_exchanges=("SH", "BJ"),
            message="个股维表部分失败(SZ: RuntimeError: 503): 已更新 SH,BJ, 保留 SZ 的上次有效名称, 共 4 行, 未用残表覆盖失败市场",
        ),
    )
    monkeypatch.setattr(daily_pipeline, "_resolve_universe", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(daily_pipeline, "_invalidate", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(daily_pipeline, "_refresh_views", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(daily_pipeline, "_refresh_single_view", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(data_integrity, "scan_recent_integrity", lambda *_args, **_kwargs: [])
    repo = SimpleNamespace(
        store=SimpleNamespace(data_dir=tmp_path),
        latest_daily_date=lambda: date(2026, 9, 28),
        refresh_view=lambda *_args, **_kwargs: None,
        get_index_symbol_set=lambda: set(),
    )

    with pytest.raises(daily_pipeline.PipelineStageError, match="部分失败") as exc:
        daily_pipeline.run_now(
            repo, CapabilitySet(set()),
            on_progress=lambda stage, pct, msg, **kwargs: messages.append(f"{stage}:{msg}"),
        )

    instrument_msgs = [msg for msg in messages if msg.startswith("sync_instruments:")]
    assert instrument_msgs
    assert any("部分失败" in msg for msg in instrument_msgs)
    assert all("同步完成" not in msg for msg in instrument_msgs)
    assert "SZ" in str(exc.value)


def test_scheduled_failure_keeps_the_exception_text(monkeypatch):
    from app.services import pipeline_jobs

    seen: list[str] = []
    monkeypatch.setattr(pipeline_jobs.job_store, "create", lambda: ("job-1", True))
    monkeypatch.setattr(pipeline_jobs.job_store, "fail", lambda job_id, error: seen.append(error))
    monkeypatch.setattr(pipeline_jobs, "try_acquire_run_slot", lambda job_id: True)
    monkeypatch.setattr(pipeline_jobs, "release_run_slot", lambda job_id: None)
    monkeypatch.setattr(pipeline_jobs, "run_with_capacity", lambda job_id, fn: fn())

    def boom(on_progress=None):
        raise RuntimeError("个股维表部分失败(SZ: TimeoutError: timed out)")

    assert daily_pipeline._run_tracked(boom, "instruments_sync") is False
    assert seen == ["个股维表部分失败(SZ: TimeoutError: timed out)"]


def test_stock_instrument_repair_route_is_registered():
    from app.api.pipeline import router

    paths = {getattr(route, "path", "") for route in router.routes}
    assert "/api/pipeline/sync-instruments" in paths
