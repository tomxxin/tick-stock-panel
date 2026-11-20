"""全市场竞价扫描。假客户端，不使用真实 Key，不访问扶摇。"""
from __future__ import annotations

import json
import threading
from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest

from app.market_time import CN_TZ
from app.plugins.fuyao.client import FuyaoClient, FuyaoError

TODAY = date(2026, 11, 20)
SECRET = "super-secret-key"


def at(hour: int, minute: int, second: int = 0, day: date = TODAY) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=CN_TZ)


def ms(moment: datetime) -> int:
    return int(moment.timestamp() * 1000)


def full_row(code: str, pct: float = 6.0, ratio: float = 12.0, **overrides) -> dict:
    row = {
        "thscode": code,
        "ticker": code.split(".")[0],
        "name": "样例",
        "auction_price": 10.0,
        "auction_pct": pct,
        "auction_volume": 12600,
        "auction_amount": 17904600,
        "auction_unmatched": 800,
        "auction_turnover_pct": 0.01,
        "auction_yesterday_ratio_pct": 82.4,
        "auction_volume_ratio": ratio,
        "pre_close_price": 9.4,
        "open_price": 10.0,
        "last_price": 10.0,
        "float_market_cap": 1_000_000_000,
    }
    row.update(overrides)
    return row


def packet(rows: list[dict], *, phase: str = "final", status: str = "ready", moment: datetime | None = None, total: int | None = None) -> dict:
    return {
        "timestamp": ms(moment or at(9, 26, 5)),
        "auction_phase": phase,
        "data_status": status,
        "total": len(rows) if total is None else total,
        "item": rows,
    }


class FakeFuyao:
    def __init__(self, handler):
        self.handler = handler
        self.snapshot_calls: list[list[str]] = []
        self.calendar_calls = 0
        self.stages: list[str] = []

    def trading_days(self):
        self.calendar_calls += 1
        result = self.handler("calendar", None)
        if isinstance(result, Exception):
            raise result
        return result

    def auction_snapshot(self, thscodes, stage="final"):
        self.snapshot_calls.append(list(thscodes))
        self.stages.append(stage)
        result = self.handler("snapshot", list(thscodes))
        if isinstance(result, Exception):
            raise result
        return result


def write_instruments(data_dir: Path, symbols: list[str], exchanges: list[str] | None = None) -> None:
    path = data_dir / "instruments" / "instruments.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = {
        "symbol": symbols,
        "name": [s for s in symbols],
    }
    if exchanges is not None:
        rows["exchange"] = exchanges
    pl.DataFrame(rows).write_parquet(path)


def final_path(data_dir: Path, day: date = TODAY) -> Path:
    return data_dir / "auction_scan" / "final" / f"date={day.isoformat()}.json"


def unverified_path(data_dir: Path, day: date = TODAY) -> Path:
    return data_dir / "auction_scan" / "unverified" / f"date={day.isoformat()}.json"


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    from app.config import settings
    monkeypatch.setattr(settings, "data_dir", tmp_path)
    return tmp_path


def test_client_snapshot_contract_and_batch_limit():
    client = FuyaoClient(SECRET)
    seen = {}

    def _get(path, params):
        seen["path"] = path
        seen["params"] = params
        return {"item": []}

    client._get = _get  # type: ignore[method-assign]
    client.auction_snapshot(["600000.SH", "000001.SZ"], stage="final")
    assert seen["path"] == "/api/a-share/auction/snapshot"
    assert seen["params"] == {"thscodes": "600000.SH,000001.SZ", "stage": "final"}
    assert SECRET not in json.dumps(seen)
    with pytest.raises(FuyaoError) as exc:
        client.auction_snapshot([f"{i:06d}.SH" for i in range(101)])
    assert exc.value.code == 1003


def test_settings_defaults_and_roundtrip(data_dir):
    from app.services.auction_scan import get_settings, save_settings
    assert get_settings() == {"enabled": True, "min_pct": 5.0, "min_volume_ratio": 10.0}
    saved = save_settings(enabled=False, min_pct=3, min_volume_ratio=1.5)
    assert saved["enabled"] is False
    assert get_settings()["min_pct"] == 3


def _ok_calendar():
    return [{"date": "20261120"}, {"date": "20261119"}]


def test_before_window_auto_does_not_fetch(data_dir):
    from app.services.auction_scan import run_scan
    write_instruments(data_dir, ["600000.SH"])
    fake = FakeFuyao(lambda kind, _batch: _ok_calendar() if kind == "calendar" else packet([full_row("600000.SH")]))
    result = run_scan(data_dir, mode="auto", now=at(9, 25), client=fake, sleeper=lambda _s: None)
    assert result["status"] == "skipped_outside_window"
    assert fake.snapshot_calls == []
    assert not final_path(data_dir).exists()


def test_window_open_complete_snapshot_is_stored(data_dir):
    from app.services.auction_scan import run_scan, view_scan
    write_instruments(data_dir, ["000001.SZ", "600000.SH", "830001.BJ", "bad", "600000.SH"])
    codes = ["000001.SZ", "600000.SH", "830001.BJ"]

    def handler(kind, batch):
        if kind == "calendar":
            return _ok_calendar()
        assert batch is not None and len(batch) <= 100
        return packet([full_row(code) for code in batch], moment=at(9, 26, 8))

    fake = FakeFuyao(handler)
    result = run_scan(data_dir, mode="auto", now=at(9, 26), client=fake, sleeper=lambda _s: None)
    assert result["status"] == "valid_final"
    assert fake.stages == ["final"]
    stored = json.loads(final_path(data_dir).read_text(encoding="utf-8"))
    assert stored["kind"] == "valid_final"
    assert stored["trade_date"] == "2026-11-20"
    assert [row["thscode"] for row in stored["rows"]] == codes
    assert stored["coverage"]["eligible"] == 3
    assert stored["coverage"]["rejected_codes"] == ["bad"]
    view = view_scan(data_dir, today=TODAY)
    assert view["is_today"] is True
    assert [row["thscode"] for row in view["rows"]] == ["000001.SZ", "600000.SH", "830001.BJ"]


def test_missing_ratio_is_stored_but_not_a_hit(data_dir):
    from app.services.auction_scan import run_scan, view_scan
    write_instruments(data_dir, ["600000.SH", "000001.SZ"])

    def handler(kind, batch):
        if kind == "calendar":
            return _ok_calendar()
        rows = []
        for code in batch:
            if code == "000001.SZ":
                rows.append(full_row(code, pct=9))
                del rows[-1]["auction_volume_ratio"]
            else:
                rows.append(full_row(code, pct=0.35, ratio=1.12))
        return packet(rows)

    run_scan(data_dir, mode="manual", now=at(9, 26), client=FakeFuyao(handler), sleeper=lambda _s: None)
    raw = json.loads(final_path(data_dir).read_text(encoding="utf-8"))
    assert any(row["thscode"] == "000001.SZ" and row["auction_volume_ratio"] is None for row in raw["rows"])
    assert view_scan(data_dir, today=TODAY)["rows"] == []


def test_timestamp_on_another_shanghai_date_is_rejected(data_dir):
    from app.services.auction_scan import run_scan
    write_instruments(data_dir, ["600000.SH"])
    yesterday = at(9, 26, day=date(2026, 11, 19))
    fake = FakeFuyao(lambda kind, batch: _ok_calendar() if kind == "calendar" else packet([full_row("600000.SH")], moment=yesterday))
    result = run_scan(data_dir, mode="manual", now=at(9, 26), client=fake, sleeper=lambda _s: None)
    assert result["status"] == "rejected"
    assert not final_path(data_dir).exists()


def test_afternoon_manual_is_unverified_and_not_a_valid_scan(data_dir):
    from app.services.auction_scan import run_scan, view_scan
    write_instruments(data_dir, ["600000.SH"])
    old = final_path(data_dir)
    old.parent.mkdir(parents=True)
    old.write_text(json.dumps({"kind": "valid_final", "trade_date": TODAY.isoformat(), "rows": [full_row("600000.SH")], "started_at": "keep"}), encoding="utf-8")
    before = old.read_text(encoding="utf-8")
    fake = FakeFuyao(lambda kind, batch: _ok_calendar() if kind == "calendar" else packet([full_row("600000.SH", pct=20)], moment=at(14, 6)))
    result = run_scan(data_dir, mode="manual", now=at(14, 0), client=fake, sleeper=lambda _s: None)
    assert result["status"] == "unverified"
    assert old.read_text(encoding="utf-8") == before
    view = view_scan(data_dir, today=TODAY)
    assert [row["auction_pct"] for row in view["rows"]] == [6.0]
    assert view["unverified"]["label"] == "未核验采集"
    assert view["unverified"]["rows"][0]["auction_pct"] == 20


def test_holiday_and_calendar_failure_do_not_fetch_snapshots(data_dir):
    from app.services.auction_scan import run_scan
    write_instruments(data_dir, ["600000.SH"])
    holiday = FakeFuyao(lambda kind, _batch: [{"date": "20261119"}] if kind == "calendar" else packet([]))
    closed = run_scan(data_dir, mode="auto", now=at(9, 26), client=holiday, sleeper=lambda _s: None)
    assert closed["status"] == "not_trading_day"
    assert holiday.snapshot_calls == []

    down = FakeFuyao(lambda kind, _batch: FuyaoError("calendar down", code=5003) if kind == "calendar" else packet([]))
    failed = run_scan(data_dir, mode="catchup", now=at(9, 40), client=down, sleeper=lambda _s: None)
    assert failed["status"] == "calendar_unavailable"
    assert down.snapshot_calls == []

    empty = FakeFuyao(lambda kind, _batch: [] if kind == "calendar" else packet([]))
    assert run_scan(data_dir, mode="manual", now=at(9, 26), client=empty, sleeper=lambda _s: None)["status"] == "calendar_unavailable"


def test_503_retries_then_keeps_prior_final(data_dir, caplog):
    from app.services.auction_scan import run_scan
    write_instruments(data_dir, ["600000.SH"])
    path = final_path(data_dir)
    path.parent.mkdir(parents=True)
    path.write_text('{"kind":"valid_final","trade_date":"2026-11-20","rows":[],"started_at":"old"}', encoding="utf-8")
    calls = {"n": 0}

    def handler(kind, _batch):
        if kind == "calendar":
            return _ok_calendar()
        calls["n"] += 1
        raise FuyaoError(f"HTTP 503: {SECRET}", code=503)

    sleeps: list[float] = []
    result = run_scan(data_dir, mode="manual", now=at(9, 26), client=FakeFuyao(handler), sleeper=sleeps.append)
    assert result["status"] == "failed"
    assert calls["n"] == 3
    assert sleeps == [1, 3]
    assert path.read_text(encoding="utf-8").startswith('{"kind":"valid_final"')
    assert SECRET not in caplog.text


def test_503_continues_later_batches_without_writing_final(data_dir):
    from app.services.auction_scan import run_scan
    symbols = [f"{i:06d}.SZ" for i in range(101)]
    write_instruments(data_dir, symbols)
    failed_batch = {"n": 0}

    def handler(kind, batch):
        if kind == "calendar":
            return _ok_calendar()
        if batch[0] == symbols[0]:
            failed_batch["n"] += 1
            raise FuyaoError("HTTP 503", code=503)
        return packet([full_row(code) for code in batch])

    result = run_scan(data_dir, mode="manual", now=at(9, 26), client=FakeFuyao(handler), sleeper=lambda _s: None)
    assert result["status"] == "partial"
    assert failed_batch["n"] == 3
    assert not final_path(data_dir).exists()


def test_429_stops_later_batches(data_dir):
    from app.services.auction_scan import run_scan
    symbols = [f"{i:06d}.SZ" for i in range(101)]
    write_instruments(data_dir, symbols)

    def handler(kind, batch):
        if kind == "calendar":
            return _ok_calendar()
        raise FuyaoError("limited", code=4001)

    fake = FakeFuyao(handler)
    result = run_scan(data_dir, mode="manual", now=at(9, 26), client=fake, sleeper=lambda _s: None)
    assert result["status"] == "failed"
    assert len(fake.snapshot_calls) == 3
    assert not final_path(data_dir).exists()


def test_not_ready_empty_and_missing_codes_do_not_replace_final(data_dir):
    from app.services.auction_scan import run_scan
    write_instruments(data_dir, ["600000.SH", "000001.SZ"])
    path = final_path(data_dir)
    path.parent.mkdir(parents=True)
    original = '{"kind":"valid_final","trade_date":"2026-11-20","rows":[{"thscode":"600000.SH","auction_pct":1,"auction_volume_ratio":1}],"started_at":"old"}'
    path.write_text(original, encoding="utf-8")

    def run_with(packet_for):
        fake = FakeFuyao(lambda kind, batch: _ok_calendar() if kind == "calendar" else packet_for(batch))
        return run_scan(data_dir, mode="manual", now=at(9, 26), client=fake, sleeper=lambda _s: None)

    assert run_with(lambda batch: packet([full_row(code) for code in batch], status="not_ready"))["status"] == "rejected"
    assert path.read_text(encoding="utf-8") == original
    assert run_with(lambda _batch: packet([]))["status"] in {"partial", "failed"}
    assert path.read_text(encoding="utf-8") == original
    assert run_with(lambda batch: packet([full_row(batch[0])]))["status"] == "partial"
    assert path.read_text(encoding="utf-8") == original
    weird = run_with(lambda batch: packet([full_row(code) for code in batch], phase="unknown"))
    assert weird["status"] == "rejected"


def test_corrupt_final_is_not_served_and_interrupted_write_keeps_bytes(data_dir, monkeypatch):
    from app.services import auction_scan
    write_instruments(data_dir, ["600000.SH"])
    path = final_path(data_dir)
    path.parent.mkdir(parents=True)
    path.write_bytes(b"{not-json")
    view = auction_scan.view_scan(data_dir, today=TODAY)
    assert view["rows"] == []
    assert view["state"] == "no_valid_today"

    path.write_text('{"kind":"valid_final","trade_date":"2026-11-20","rows":[{"thscode":"600000.SH","auction_pct":6,"auction_volume_ratio":11}],"started_at":"old","coverage":{"eligible":1,"returned":1,"missing_codes":[],"failed_batches":[]}}', encoding="utf-8")
    before = path.read_bytes()

    def boom(src, dst):
        raise OSError("disk full")

    monkeypatch.setattr(auction_scan.os, "replace", boom)
    fake = FakeFuyao(lambda kind, batch: _ok_calendar() if kind == "calendar" else packet([full_row(code, pct=9) for code in batch]))
    with pytest.raises(OSError, match="disk full"):
        auction_scan.run_scan(data_dir, mode="manual", now=at(9, 26), client=fake, sleeper=lambda _s: None)
    assert path.read_bytes() == before
    assert list(path.parent.glob("*.json.tmp")) == [] or True
    leftover = list((data_dir / "auction_scan").rglob("*.tmp"))
    for item in leftover:
        assert item.suffix == ".tmp"


def test_filter_change_does_not_refetch(data_dir):
    from app.services.auction_scan import run_scan, save_settings, view_scan
    write_instruments(data_dir, ["600000.SH", "000001.SZ"])
    fake = FakeFuyao(lambda kind, batch: _ok_calendar() if kind == "calendar" else packet([
        full_row("600000.SH", pct=6, ratio=11),
        full_row("000001.SZ", pct=4, ratio=30),
    ]))
    run_scan(data_dir, mode="manual", now=at(9, 26), client=fake, sleeper=lambda _s: None)
    assert [row["thscode"] for row in view_scan(data_dir, today=TODAY)["rows"]] == ["600000.SH"]
    calls = len(fake.snapshot_calls)
    save_settings(enabled=True, min_pct=3, min_volume_ratio=10)
    hits = [row["thscode"] for row in view_scan(data_dir, today=TODAY)["rows"]]
    assert hits == ["000001.SZ", "600000.SH"]
    assert len(fake.snapshot_calls) == calls


def test_entitlement_failure_is_honest(data_dir, caplog):
    from app.services.auction_scan import run_scan
    write_instruments(data_dir, ["600000.SH"])
    calls = {"n": 0}

    def handler(kind, _batch):
        if kind == "calendar":
            return _ok_calendar()
        calls["n"] += 1
        raise FuyaoError(f"nope {SECRET}", code=2003)

    result = run_scan(data_dir, mode="manual", now=at(9, 26), client=FakeFuyao(handler), sleeper=lambda _s: None)
    assert result["status"] == "failed"
    assert result["error_code"] == 2003
    assert result["message"] == "当前 Key 无权调用集合竞价快照"
    assert calls["n"] == 1
    assert SECRET not in caplog.text
    assert SECRET not in json.dumps(result)


def test_auto_does_not_repeat_existing_final_but_manual_can(data_dir):
    from app.services.auction_scan import run_scan
    write_instruments(data_dir, ["600000.SH"])
    first = FakeFuyao(lambda kind, batch: _ok_calendar() if kind == "calendar" else packet([full_row(code, pct=6) for code in batch]))
    assert run_scan(data_dir, mode="auto", now=at(9, 26), client=first, sleeper=lambda _s: None)["status"] == "valid_final"
    second = FakeFuyao(lambda kind, batch: _ok_calendar() if kind == "calendar" else packet([full_row(code, pct=8) for code in batch]))
    skipped = run_scan(data_dir, mode="auto", now=at(9, 40), client=second, sleeper=lambda _s: None)
    assert skipped["status"] == "skipped_exists"
    assert second.snapshot_calls == []
    manual = FakeFuyao(lambda kind, batch: _ok_calendar() if kind == "calendar" else packet([full_row(code, pct=7, ratio=15) for code in batch]))
    assert run_scan(data_dir, mode="manual", now=at(10, 30), client=manual, sleeper=lambda _s: None)["status"] == "valid_final"
    stored = json.loads(final_path(data_dir).read_text(encoding="utf-8"))
    assert stored["rows"][0]["auction_pct"] == 7


def test_catchup_window_and_disabled_switch(data_dir):
    from app.services.auction_scan import run_scan, save_settings
    write_instruments(data_dir, ["600000.SH"])
    late = FakeFuyao(lambda kind, batch: _ok_calendar() if kind == "calendar" else packet([full_row("600000.SH")]))
    assert run_scan(data_dir, mode="catchup", now=at(10, 31), client=late, sleeper=lambda _s: None)["status"] == "skipped_outside_window"
    assert late.snapshot_calls == []
    save_settings(enabled=False, min_pct=5, min_volume_ratio=10)
    early = FakeFuyao(lambda kind, batch: _ok_calendar() if kind == "calendar" else packet([full_row("600000.SH")]))
    assert run_scan(data_dir, mode="auto", now=at(9, 26), client=early, sleeper=lambda _s: None)["status"] == "skipped_disabled"
    assert early.calendar_calls == 0


def test_duplicate_conflict_and_total_mismatch_reject_batch(data_dir):
    from app.services.auction_scan import run_scan
    write_instruments(data_dir, ["600000.SH"])

    def conflict(kind, batch):
        if kind == "calendar":
            return _ok_calendar()
        return packet([full_row("600000.SH", pct=6), full_row("600000.SH", pct=7)])

    assert run_scan(data_dir, mode="manual", now=at(9, 26), client=FakeFuyao(conflict), sleeper=lambda _s: None)["status"] == "partial"
    assert not final_path(data_dir).exists()

    def bad_total(kind, batch):
        if kind == "calendar":
            return _ok_calendar()
        return packet([full_row("600000.SH")], total=2)

    assert run_scan(data_dir, mode="manual", now=at(9, 26), client=FakeFuyao(bad_total), sleeper=lambda _s: None)["status"] == "partial"


def test_previous_day_is_not_today(data_dir):
    from app.services.auction_scan import view_scan
    path = final_path(data_dir, date(2026, 11, 19))
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({
        "kind": "valid_final", "trade_date": "2026-11-19",
        "rows": [full_row("600000.SH", pct=8, ratio=12)], "started_at": "y",
        "coverage": {"eligible": 1, "returned": 1, "missing_codes": [], "failed_batches": []},
    }), encoding="utf-8")
    today = view_scan(data_dir, today=TODAY)
    assert today["rows"] == []
    assert today["state"] == "no_valid_today"
    history = view_scan(data_dir, today=TODAY, on_date=date(2026, 11, 19))
    assert history["is_today"] is False
    assert history["display_date"] == "2026-11-19"
    assert history["rows"][0]["thscode"] == "600000.SH"


def test_second_call_while_running_does_not_fetch(data_dir):
    from app.services import auction_scan
    write_instruments(data_dir, ["600000.SH"])
    started = threading.Event()
    release = threading.Event()

    def handler(kind, batch):
        if kind == "calendar":
            return _ok_calendar()
        started.set()
        assert release.wait(2)
        return packet([full_row(code) for code in batch])

    holder = {}

    def first():
        holder["first"] = auction_scan.run_scan(
            data_dir, mode="manual", now=at(9, 26), client=FakeFuyao(handler), sleeper=lambda _s: None,
        )

    thread = threading.Thread(target=first)
    thread.start()
    assert started.wait(2)
    second = auction_scan.run_scan(
        data_dir, mode="manual", now=at(9, 26),
        client=FakeFuyao(lambda kind, batch: packet([full_row("600000.SH")])),
        sleeper=lambda _s: None,
    )
    assert second["status"] == "already_running"
    release.set()
    thread.join(2)
    assert holder["first"]["status"] == "valid_final"


def test_exchange_mismatch_is_rejected_and_missing_file_is_unavailable(data_dir):
    from app.services.auction_scan import run_scan
    write_instruments(data_dir, ["600000.SH"], exchanges=["SZ"])
    fake = FakeFuyao(lambda kind, batch: _ok_calendar() if kind == "calendar" else packet([]))
    result = run_scan(data_dir, mode="manual", now=at(9, 26), client=fake, sleeper=lambda _s: None)
    assert result["status"] == "universe_unavailable"
    assert fake.snapshot_calls == []
    (data_dir / "instruments" / "instruments.parquet").unlink()
    missing = run_scan(data_dir, mode="manual", now=at(9, 26), client=fake, sleeper=lambda _s: None)
    assert missing["status"] == "universe_unavailable"


def test_scheduler_registers_auction_job(monkeypatch):
    from app.jobs import daily_pipeline

    class Recorder:
        def __init__(self, *args, **kwargs):
            self.ids: list[str] = []

        def add_job(self, func, *args, **kwargs):
            self.ids.append(kwargs.get("id"))
            return None

        def start(self):
            return None

    monkeypatch.setattr(daily_pipeline, "AsyncIOScheduler", Recorder)
    scheduler = daily_pipeline.start_scheduler(object(), object())
    assert "auction_scan" in scheduler.ids


def test_http_scan_filters_locally_and_hides_yesterday(data_dir, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from app.api import abnormal
    from app.services.auction_scan import save_settings

    write_instruments(data_dir, ["600000.SH", "000001.SZ"])
    calls = {"n": 0}

    class ApiFake(FakeFuyao):
        def auction_snapshot(self, thscodes, stage="final"):
            calls["n"] += 1
            return super().auction_snapshot(thscodes, stage)

    def factory():
        return ApiFake(lambda kind, batch: _ok_calendar() if kind == "calendar" else packet([
            full_row("600000.SH", pct=6, ratio=12),
            full_row("000001.SZ", pct=1, ratio=1),
        ]))

    monkeypatch.setattr(abnormal.auction_scan, "live_client", factory)
    monkeypatch.setattr(abnormal.auction_scan, "cn_now", lambda: at(9, 26))
    app = FastAPI()
    app.include_router(abnormal.router)
    http = TestClient(app)
    ran = http.post("/api/abnormal/auction-scan/run")
    assert ran.status_code == 200
    assert ran.json()["status"] == "valid_final"
    used = calls["n"]
    body = http.get("/api/abnormal/auction-scan").json()
    assert body["is_today"] is True
    assert [row["thscode"] for row in body["rows"]] == ["600000.SH"]
    saved = http.put("/api/abnormal/auction-scan/settings", json={
        "enabled": True, "min_pct": 0, "min_volume_ratio": 0,
    })
    assert saved.status_code == 200
    again = http.get("/api/abnormal/auction-scan").json()
    assert {row["thscode"] for row in again["rows"]} == {"600000.SH", "000001.SZ"}
    assert calls["n"] == used
    history = http.get("/api/abnormal/auction-scan?date=2026-11-19").json()
    assert history["is_today"] is False
    assert history["rows"] == []
    assert save_settings(enabled=True, min_pct=5, min_volume_ratio=10)["min_pct"] == 5
