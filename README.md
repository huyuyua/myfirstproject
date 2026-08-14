# 沪深成交额温度采集器

结论：脚本以“今日沪深两市累计成交额 − 前一交易日同一时刻累计成交额”为零轴温度，每 10 分钟保存一次；正值表示放量，负值表示缩量。

## 数据口径

- 市场范围：沪市 + 深市，采用上证指数 `sh000001` 和深证成指 `sz399001` 的市场累计成交额。
- 温度差额：今日累计成交额 − 前一交易日同一时刻累计成交额，单位为元（页面展示为亿元）。
- 温度百分比：温度差额 ÷ 前一交易日同一时刻累计成交额 × 100%。
- 采集时间：交易日 `09:30-11:30`、`13:00-15:00`，每 10 分钟一次。
- 数据源：腾讯财经公开分时行情。该接口同时返回近 5 个交易日的逐分钟累计成交额，首次运行即可回填近 5 日并生成昨日同期基线。
- 说明：这里使用市场常见的“沪深两市成交额”口径，不包含港股，也不单独叠加北交所。

Tushare 的全市场实时日线 `rt_k` 需要单独开通权限。当前实现选择腾讯数据源，避免普通 Tushare 账号的实时权限或调用频次不足影响每 10 分钟采集。

## GitHub Actions 云端运行（推荐）

云端版本不依赖本地电脑，工作日北京时间 `09:30-11:30`、`13:00-15:00` 每 10 分钟由 GitHub Actions 运行一次。脚本会核对腾讯行情中的交易日期，周末或休市日不会保存陈旧数据或发送阈值告警。

仓库需要配置以下 Actions Secrets：

- `SMTP_USERNAME`：完整的163邮箱地址。
- `SMTP_APP_PASSWORD`：163邮箱“客户端授权码”，不是网页登录密码。
- `ALERT_TO`：可选；不配置时默认发送给 `SMTP_USERNAME`。

云端输出：

- `docs/index.html`：最新温度看板。
- `docs/latest.json`：最新完整数据。
- `docs/history/YYYY/YYYY-MM-DD.json`：按交易日归档的完整10分钟序列。
- `docs/history/YYYY/YYYY-MM-DD.csv`：Excel 可打开的历史明细。
- `state/alerts.json`：每日、每个方向的告警去重状态。

告警阈值为严格大于 `+30%` 或严格小于 `-30%`。同一交易日的放量、缩量方向分别最多发送一次邮件。

可在 GitHub 的 `Actions → A-share market temperature → Run workflow` 中勾选 `test_email`，人工发送一次163邮箱配置测试邮件。

## 异动公告邮件

仓库还包含独立工作流 `A-share abnormal announcements`，每天北京时间 `21:00` 在 GitHub 云端执行主扫描，次日 `08:00` 再从昨天 `00:00` 开始补漏扫描，不依赖本地电脑开机。两次扫描复用公告 ID 去重状态，早盘只补发晚间漏掉的新公告。它通过腾讯财经公开接口取得全量 A 股代码、批量公告列表和公告正文，并覆盖以下提醒口径：

- 公告正文中的 `20%` 收盘价涨跌幅偏离异动，以及明确写有 `20个交易日` 的异动。
- `30个交易日` 偏离异动；同时兼容在第 20—30 个交易日提前达到 `+200%/-70%` 等阈值的严重异常波动公告。
- 下跌方向异动的第 2 次和第 3 次：优先识别公告中的明确表述，未明确标序号时按同一股票近 30 自然日腾讯异动公告计数。

只有发现尚未提醒过的目标公告时才发送邮件，同一腾讯公告 ID 只发送一次。扫描记录持久化到：

- `docs/abnormal-announcements/latest.json`：最近一次扫描结果。
- `docs/abnormal-announcements/history/YYYY/YYYY-MM-DD.json`：按扫描日归档。
- `docs/abnormal-announcements/history/YYYY/YYYY-MM-DD.csv`：Excel 可打开的明细。
- `state/abnormal_announcements.json`：上次成功扫描时间、历史事件和邮件去重状态。

它复用现有的 `SMTP_USERNAME`、`SMTP_APP_PASSWORD`；可选的 `ALERT_TO` 未配置时默认发送给 `SMTP_USERNAME`。在 GitHub 的 `Actions → A-share abnormal announcements → Run workflow` 中勾选 `test_email`，可发送一封配置测试邮件，且不会推进正式扫描状态。

本地只扫描、不发邮件、不写状态：

```powershell
python -m pip install -r .\requirements-abnormal-announcements.txt
python .\abnormal_announcement_monitor.py --dry-run --lookback-days 3
```

## 每日收盘复盘邮件

独立工作流 `A-share daily close report` 在 A 股交易日北京时间 `15:30` 主运行，并在 `15:47`、`16:07`、`16:27` 提供 GitHub 调度兜底。报告哈希和收件人投递状态会阻止重复邮件；脚本通过腾讯收盘时间校验自动跳过周末、节假日和未完成收盘的数据，并生成包含以下内容的中文 HTML/纯文本报告：

- 上证、深证成指、创业板、科创50、沪深300的强弱和尾盘异动。
- 沪深成交额相对前一交易日的放量/缩量，以及沪深京涨跌家数。
- 涨停、跌停、炸板、封板率、成交额 Top20、行业板块 Top5。
- 动态核心龙头、14:30 后的抢筹/回流/兑现判断，以及有证据的次日机会与风险。

在 `Settings → Secrets and variables → Actions → Variables` 中维护：

- `DAILY_REPORT_RECIPIENTS`：逗号、分号或换行分隔；`SELF` 代表 `SMTP_USERNAME`，例如 `SELF,other@example.com`。实际收件人只保存在 GitHub Variable 中。
- `DAILY_REPORT_WATCHLIST`：可选自选股代码，例如 `600519,300750`；初始留空。

每个收件人会收到独立邮件，不会看到其他邮箱地址。每日报告与投递去重状态保存到：

- `docs/close-report/latest.html`、`latest.json`
- `docs/close-report/history/YYYY/YYYY-MM-DD.html/json`
- `state/daily_close_report.json`

本地实时检查但不写文件、不发邮件：

```powershell
python .\daily_close_report.py --dry-run --force
```

可在 GitHub 的 `Actions → A-share daily close report → Run workflow` 中使用 `test_email` 验证当前收件人，或使用 `resend` 强制重发当日报告。

## 立即运行

无需安装第三方包，使用 Python 3.9+：

```powershell
cd D:\amylocaltestproject\codex_auto
python .\market_temperature.py once
```

运行后会生成：

- `data\market_temperature.sqlite3`：SQLite 历史数据库，开启 WAL，重复采集按“交易日 + 时间”更新，不会产生重复行。
- `output\market_temperature.html`：带 0 轴的当天温度图，浏览器直接打开即可。
- `output\latest.json`：最新快照和当天全序列，方便后续程序接入。
- `logs\market_temperature.log`：滚动日志，单文件最大 5 MB，保留 5 份。

## 安装 Windows 任务计划

在普通 PowerShell 中执行即可，不要求管理员权限：

```powershell
cd D:\amylocaltestproject\codex_auto
powershell -ExecutionPolicy Bypass -File .\install_scheduled_task.ps1
```

任务名为 `AStockMarketTemperature`，每周一至周五 09:25 启动；脚本内部按 A 股时段每 10 分钟执行，15:00 后自动退出。电脑晚启动时，任务的 `StartWhenAvailable` 会补启动，腾讯近 5 日分时数据也会补齐当日已过去的采集点。

任务按当前 Windows 用户的“仅在已登录时运行”模式注册，不保存账户密码。若需要注销后仍在后台运行，应改用具备网络访问权限的服务账号重新注册任务。

立即试运行任务：

```powershell
powershell -ExecutionPolicy Bypass -File .\install_scheduled_task.ps1 -RunNow
```

卸载任务：

```powershell
powershell -ExecutionPolicy Bypass -File .\install_scheduled_task.ps1 -Uninstall
```

## 查询和导出历史

查询指定交易日：

```powershell
python .\market_temperature.py history --date 2026-08-07
```

导出日期区间为 Excel 可直接打开的 UTF-8 BOM CSV：

```powershell
python .\market_temperature.py history `
  --from-date 2026-08-01 `
  --to-date 2026-08-31 `
  --limit 10000 `
  --csv .\output\market_temperature_202608.csv
```

根据数据库重新生成看板：

```powershell
python .\market_temperature.py render
```

## 配置

配置文件为 `config.json`，可调整采集间隔、交易时段、网络超时及输出路径。任务计划中的采集间隔由 Python 配置控制，因此修改 `interval_minutes` 后无需重装任务。

建议保持两个指数代码不变，否则不同指数的成交额口径可能无法与既有历史连续比较。
