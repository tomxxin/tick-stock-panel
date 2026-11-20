import type { AuctionScanPayload, AuctionScanRow } from '@/lib/api'

export interface AuctionScanPresentation {
  todayRows: AuctionScanRow[]
  historyRows: AuctionScanRow[]
  unverifiedRows: AuctionScanRow[]
  unverifiedLabel: string | null
  headline: string
  coverageText: string
}

export function presentAuctionScan(payload: AuctionScanPayload): AuctionScanPresentation {
  const coverage = payload.coverage
  const missing = coverage?.missing_codes?.length ?? 0
  const failed = coverage?.failed_batches?.length ?? 0
  const coverageText = coverage
    ? `合格 ${coverage.eligible} · 返回 ${coverage.returned} · 缺失 ${missing} · 失败批次 ${failed}`
    : '尚无覆盖数字'
  const unverifiedRows = payload.unverified?.rows ?? []
  const unverifiedLabel = payload.unverified ? payload.unverified.label : null
  if (!payload.is_today) {
    return {
      todayRows: [],
      historyRows: payload.state === 'valid_final' ? payload.rows : [],
      unverifiedRows,
      unverifiedLabel,
      headline: payload.state === 'valid_final'
        ? `${payload.display_date} 的历史有效终态`
        : `${payload.display_date} 没有有效终态`,
      coverageText,
    }
  }
  if (payload.running) {
    return emptyToday('正在采集', coverageText, unverifiedRows, unverifiedLabel)
  }
  if (payload.state === 'valid_final') {
    return {
      todayRows: payload.rows,
      historyRows: [],
      unverifiedRows,
      unverifiedLabel,
      headline: `今日有效终态 · ${payload.collected_at ?? payload.display_date}`,
      coverageText,
    }
  }
  const last = payload.last_run?.status
  const code = payload.last_run?.error_code
  if (last === 'not_configured' || code === 2003) {
    return emptyToday('竞价快照未配置或无权调用', coverageText, unverifiedRows, unverifiedLabel)
  }
  if (last === 'calendar_unavailable') return emptyToday('交易日历不可用', coverageText, unverifiedRows, unverifiedLabel)
  if (last === 'not_trading_day') return emptyToday('不是交易日', coverageText, unverifiedRows, unverifiedLabel)
  if (last === 'failed' || last === 'partial' || last === 'rejected') {
    return emptyToday('今日无有效终态 · 最近一次采集未完成', coverageText, unverifiedRows, unverifiedLabel)
  }
  return emptyToday('今日无有效终态', coverageText, unverifiedRows, unverifiedLabel)
}

function emptyToday(
  headline: string,
  coverageText: string,
  unverifiedRows: AuctionScanRow[],
  unverifiedLabel: string | null,
): AuctionScanPresentation {
  return { todayRows: [], historyRows: [], unverifiedRows, unverifiedLabel, headline, coverageText }
}
