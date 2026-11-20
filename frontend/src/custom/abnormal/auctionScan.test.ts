import { describe, expect, it } from 'vitest'
import { presentAuctionScan } from './auctionScan'
import type { AuctionScanPayload } from '@/lib/api'

function payload(overrides: Partial<AuctionScanPayload> = {}): AuctionScanPayload {
  return {
    display_date: '2026-11-20',
    is_today: true,
    state: 'no_valid_today',
    rows: [],
    coverage: { eligible: 3, returned: 2, missing_codes: ['830001.BJ'], failed_batches: [{ index: 1, code: 503 }] },
    ...overrides,
  }
}

describe('presentAuctionScan', () => {
  it('keeps a non-today payload out of the today list', () => {
    const view = presentAuctionScan(payload({
      is_today: false,
      display_date: '2026-11-19',
      state: 'valid_final',
      rows: [{ thscode: '600000.SH', auction_pct: 8, auction_volume_ratio: 12 }],
    }))
    expect(view.todayRows).toEqual([])
    expect(view.historyRows.map(row => row.thscode)).toEqual(['600000.SH'])
    expect(view.headline).toContain('2026-11-19')
    expect(view.headline).not.toContain('今日有效')
  })

  it('does not borrow yesterday rows when today failed', () => {
    const view = presentAuctionScan(payload({
      state: 'no_valid_today',
      rows: [{ thscode: '600000.SH', auction_pct: 8, auction_volume_ratio: 12 }],
      last_run: { status: 'partial', message: '竞价采集不完整' },
    }))
    expect(view.todayRows).toEqual([])
    expect(view.headline).toContain('未完成')
    expect(view.coverageText).toBe('合格 3 · 返回 2 · 缺失 1 · 失败批次 1')
  })

  it('labels an afternoon snapshot as unverified and excludes it from today rows', () => {
    const view = presentAuctionScan(payload({
      state: 'valid_final',
      rows: [{ thscode: '600000.SH', auction_pct: 6, auction_volume_ratio: 11 }],
      unverified: {
        label: '未核验采集',
        rows: [{ thscode: '000001.SZ', auction_pct: 20, auction_volume_ratio: 30 }],
      },
    }))
    expect(view.todayRows.map(row => row.thscode)).toEqual(['600000.SH'])
    expect(view.unverifiedRows.map(row => row.thscode)).toEqual(['000001.SZ'])
    expect(view.unverifiedLabel).toBe('未核验采集')
  })
})
