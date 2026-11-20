"""异动监控 API — 竞价/盘中/偏移三类异动。

- /intraday: 盘中量价信号聚合 (enriched 当日信号列, 零新增采集)
- /overview: 偏移异动边缘总览 (交易所异动规则口径的接近度)
"""
from __future__ import annotations

from datetime import date as date_cls

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from app.services import auction_scan
from app.services.abnormal_moves import build_intraday, build_overview

router = APIRouter(prefix="/api/abnormal", tags=["abnormal"])


@router.get("/intraday")
def abnormal_intraday(
    request: Request,
    limit: int = Query(500, ge=1, le=2000),
):
    """盘中异动: 涨停/炸板/跌停翘板/跌停/新高/新低/放量 信号命中行。"""
    repo = request.app.state.repo
    return build_intraday(repo, limit=limit)


@router.get("/overview")
def abnormal_overview(
    request: Request,
    min_closeness: float = Query(0.5, ge=0.0, le=1.0),
    limit: int = Query(200, ge=1, le=1000),
):
    """异动边缘总览: 规则表 + 各窗口实时偏离 + 接近度排序。

    min_closeness: 0.5=观察 / 0.7=边缘 / 1.0=已触发。
    """
    repo = request.app.state.repo
    quote_service = getattr(request.app.state, "quote_service", None)
    return build_overview(repo, quote_service, min_closeness=min_closeness, limit=limit)


class AuctionScanSettingsIn(BaseModel):
    enabled: bool
    min_pct: float
    min_volume_ratio: float


@router.get("/auction-scan")
def auction_scan_view(date: str | None = None) -> dict:
    """今日有效竞价终态。date 只用于明确查看某一天，缺省是上海时间今天。"""
    today = auction_scan.cn_now().date()
    on_date = None
    if date:
        try:
            on_date = date_cls.fromisoformat(date)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="date 必须是 YYYY-MM-DD") from exc
    return auction_scan.view_scan(auction_scan._root(), today=today, on_date=on_date)


@router.post("/auction-scan/run")
def auction_scan_run() -> dict:
    """手动采集。窗口外的结果只记为未核验。"""
    return auction_scan.run_manual()


@router.get("/auction-scan/settings")
def auction_scan_settings() -> dict:
    return auction_scan.get_settings()


@router.put("/auction-scan/settings")
def auction_scan_settings_save(body: AuctionScanSettingsIn) -> dict:
    return auction_scan.save_settings(
        enabled=body.enabled, min_pct=body.min_pct, min_volume_ratio=body.min_volume_ratio,
    )
