# 全市场竞价扫描 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在交易日上海时间 09:26 用扶摇公开的集合竞价快照接口采集全市场终态，按日保存完整原始结果，并用可调整的本地筛选展示，失败和不核验的结果不得冒充有效终态。

**Architecture:** `FuyaoClient.auction_snapshot` 只发 `GET /api/a-share/auction/snapshot`。`auction_scan` 服务冻结维表股票池、串行分批、校验日历/窗口/阶段/状态/时间戳/覆盖，再用临时文件原子写入 `data/auction_scan/`。调度、启动补跑和手动按钮都进同一把进程锁。界面只把当天有效档筛进主列表。

**Tech Stack:** Python 3.12、FastAPI、Polars、现有扶摇 httpx 客户端、React 18、TanStack Query、Vitest。

## Global Constraints

- 只调用文档中的 `GET /api/a-share/auction/snapshot`，`stage=final`，每批去重后最多 100 个 `thscodes`。
- 不调用价格快照、短线风向标或集合竞价机会接口来完成本功能。
- 交易日只认扶摇 `trading-days` 的成功结果。失败、空列表或无法解析时不采集，不用星期几推断。
- 自动采集默认开启。周一至周五 09:26 只负责叫醒。窗口是上海时间 09:26:00 到 10:30:00，按运行开始时刻判断。10:31 之后不自动补跑。
- 默认筛选 `auction_pct >= 5`（百分数原值）且 `auction_volume_ratio >= 10`（接口原值）。改筛选不重新请求。
- `auction_volume` 原样保存，不换算股或手。
- 有效终态必须同时满足：交易日、窗口内开始、每批 `auction_phase==final`、`data_status==ready`、时间戳的上海日期等于该交易日、覆盖完整、原子写入。
- 窗口外手动采集即使返回 `final`/`ready` 也只记未核验，不进入有效列表，不替换有效档。
- 部分失败、缺代码、坏状态、坏时间戳不替换已有有效档，也不把昨天显示成今天。
- `2003` 明确报无权，不购买、不升级、不改 Key。日志和响应不含 Key。
- 不修改 `71ebdb7` 的维表修复行为，不追加到 PR #1。
- 测试使用假客户端，不使用用户 Key，不访问生产接口。
- 不承诺 2 秒，也不回补启用前的历史 09:25。

## 文件

- 修改：`backend/app/plugins/fuyao/client.py`（新增 `auction_snapshot`）
- 修改：`backend/app/services/preferences.py`（三个偏好的读写，缺省即默认）
- 新建：`backend/app/services/auction_scan.py`
- 修改：`backend/app/api/abnormal.py`
- 修改：`backend/app/jobs/daily_pipeline.py`（09:26 闹钟）
- 修改：`backend/app/main.py`（启动补跑）
- 新建：`backend/tests/test_auction_scan.py`
- 修改：`frontend/src/lib/api.ts`
- 新建：`frontend/src/custom/abnormal/auctionScan.ts`
- 新建：`frontend/src/custom/abnormal/auctionScan.test.ts`
- 修改：`frontend/src/custom/abnormal/Page.tsx`

## 接口约定

```python
def auction_snapshot(self, thscodes: list[str], stage: str = "final") -> dict: ...

def get_auction_scan_settings() -> dict: ...
def set_auction_scan_settings(*, enabled: bool, min_pct: float, min_volume_ratio: float) -> dict: ...

def run_scan(data_dir: Path, *, mode: str, now: datetime, client=None, sleeper=None) -> dict: ...
def view_scan(data_dir: Path, *, today: date, on_date: date | None = None) -> dict: ...
def run_scheduled() -> dict: ...
def run_catchup() -> dict: ...
def run_manual() -> dict: ...
```

`mode` 是 `auto`、`catchup` 或 `manual`。`run_scan` 返回的 `status` 只使用：`already_running`、`skipped_disabled`、`skipped_outside_window`、`skipped_exists`、`not_configured`、`calendar_unavailable`、`not_trading_day`、`universe_unavailable`、`valid_final`、`unverified`、`partial`、`failed`、`rejected`。

---

### Task 1: 客户端与偏好

**Files:**
- Modify: `backend/app/plugins/fuyao/client.py`
- Modify: `backend/app/services/preferences.py`
- Test: `backend/tests/test_auction_scan.py`

- [x] 写失败测试：超过 100 个代码在发请求前拒绝；合法调用的路径是 `/api/a-share/auction/snapshot` 且 `stage=final`。缺偏好文件时三个默认值为开、5、10。
- [x] 运行 `uv run pytest tests/test_auction_scan.py -q`，确认因方法不存在而失败。
- [x] 实现客户端方法和偏好读写。超过 100 个抛 `FuyaoError`，`code=1003`。
- [x] 再跑该测试，确认通过。

### Task 2: 采集门闩、覆盖和存储

**Files:**
- Create: `backend/app/services/auction_scan.py`
- Test: `backend/tests/test_auction_scan.py`

- [x] 先写规格列出的全部服务测试，使用假客户端和注入的上海时间。至少两份夹具：完整字段行，以及缺少 `auction_pct` 或 `auction_volume_ratio` 的行。
- [x] 运行测试，确认失败点是 `auction_scan` 不存在，而不是断言写错。
- [x] 实现：冻结合格股票池、串行分批、0.15 秒间隔、20 秒由调用方客户端承担、传输错误再试 2 次（等待 1 秒和 3 秒）、429/4001 耗尽后停止后续批、2001/2003 立即停止。仅完整有效结果原子写入 `final/date=YYYY-MM-DD.json`。窗口外完整结果写入 `unverified`。摘要写入 `last_run.json`。临时文件使用 `.json.tmp`。
- [x] 跑 `tests/test_auction_scan.py`，全部通过。

覆盖的规格条款：合格代码与冲突代码、重复代码、缺行、多余代码、`total` 不一致、空 item、`not_ready`、未文档化状态、时间戳跨日、损坏的旧档、写入中断、默认筛选与改筛选不增加请求、2003 不重试且日志无 Key、自动路径不重复请求、窗口内手动可替换、并发 `already_running`、09:25 前和 10:31 后不自动采集、周末/假日/日历失败不发快照请求。

### Task 3: HTTP、闹钟和启动补跑

**Files:**
- Modify: `backend/app/api/abnormal.py`
- Modify: `backend/app/jobs/daily_pipeline.py`
- Modify: `backend/app/main.py`
- Test: `backend/tests/test_auction_scan.py`

- [x] 测试 `GET/POST/PUT /api/abnormal/auction-scan`：今天的列表不读取昨天的有效档；设置保存后再次 GET 不调用扶摇；调度器注册了 `auction_scan`。不改 `/intraday` 与 `/overview`。
- [x] 实现三个路由。`GET` 的 `date` 缺省为上海今天。`POST` 走手动。09:26 的 Cron 只调用 `run_scheduled`。`main` 在调度器启动后调用 `run_catchup`，异常只记警告。
- [x] 跑拍卖测试和维表基线 115 项。

### Task 4: 扫描卡片

**Files:**
- Create: `frontend/src/custom/abnormal/auctionScan.ts`
- Create: `frontend/src/custom/abnormal/auctionScan.test.ts`
- Modify: `frontend/src/lib/api.ts`
- Modify: `frontend/src/custom/abnormal/Page.tsx`

- [x] 先写 `presentAuctionScan` 测试：非今天的行不能进今日列表；失败运行不能借用昨日列表；未核验行单独标注且不进今日列表；覆盖数字原样显示。
- [x] 跑 `pnpm exec vitest run src/custom/abnormal/auctionScan.test.ts`，确认失败。
- [x] 实现纯函数、API 方法和卡片。删除“约 2 秒、5547 只”。卡片失败不卸载风向标，也不影响另外两个页签。无权时链接到 `/settings?tab=data-sources`。
- [x] 跑 vitest 和 `pnpm build`。

### Task 5: 验证

- [x] `uv run ruff check` 只检查本次改动文件。
- [x] `uv run pytest tests/test_auction_scan.py -q`
- [x] 规格中的 115 项回归命令。
- [x] `pnpm build`
- [x] `git diff --check`
- [x] 确认 `instrument_sync.py` 相对 `71ebdb7` 无差异。

## 规格覆盖自查

| 规格要求 | 任务 |
| --- | --- |
| 仅 snapshot、stage=final、批大小 ≤100 | Task 1–2 |
| 百分数与量比原值、成交量不换算 | Task 2、4 |
| 日历成功才采集，失败不推断 | Task 2 |
| 09:26 闹钟、09:26–10:30 补跑、默认开启可关 | Task 2–3 |
| 调休周末没有闹钟，仅窗口内启动或手动 | Task 3 的闹钟范围；Task 2 的 catchup/manual |
| 完整原始档、本地筛选不重拉 | Task 2 |
| 下午未核验，不进有效列表 | Task 2、4 |
| 缺代码、坏批次、坏状态、坏时间戳不覆盖有效档 | Task 2 |
| 昨天不冒充今天 | Task 2–4 |
| 损坏档与中断写 | Task 2 |
| 单飞与自动不重复 | Task 2 |
| 429 停止后续批，503 可继续后续批 | Task 2 |
| 2003 诚实失败，无 Key | Task 2、4 |
| 不改风向标和盘中/偏移接口 | Task 3 |
| 不改维表修复 | Task 5 |
| 假客户端、两份夹具、115 基线 | Task 2、5 |
| Mac 休眠错过窗口不会自动补出结果 | Task 2 的 catchup 时间门闩；部署说明，不在本轮运行 |

没有待定实现步骤。未文档化的 `auction_phase` / `data_status` 只接受 `final` 和 `ready`。
