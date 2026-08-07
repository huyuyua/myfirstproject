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
