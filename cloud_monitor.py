# -*- coding: utf-8 -*-
"""GitHub Actions 云端入口：采集、归档并按阈值发送163邮件告警。"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import date, datetime
from email.message import EmailMessage
import html
from io import StringIO
import json
import os
from pathlib import Path
import smtplib
import ssl
import sys
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

from market_temperature import AppConfig, MarketTemperatureError, collect_once, configure_logging


APP_DIR = Path(__file__).resolve().parent
DEFAULT_CLOUD_CONFIG = APP_DIR / "cloud_config.json"
DEFAULT_DOCS_DIR = APP_DIR / "docs"
DEFAULT_ALERT_STATE = APP_DIR / "state" / "alerts.json"
DEFAULT_THRESHOLD = 30.0
SMTP_HOST = "smtp.163.com"
SMTP_PORT = 465


def atomic_write_text(path: Path, content: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(content, encoding=encoding)
    os.replace(temporary, path)


def alert_direction(temperature_pct: float | None, threshold: float = DEFAULT_THRESHOLD) -> str | None:
    """严格大于正阈值为 hot，严格小于负阈值为 cold。"""
    if temperature_pct is None:
        return None
    if temperature_pct > threshold:
        return "hot"
    if temperature_pct < -threshold:
        return "cold"
    return None


def is_in_trading_session(now: datetime, sessions: Sequence[tuple[str, str]]) -> bool:
    if now.weekday() >= 5:
        return False
    current = now.strftime("%H:%M")
    return any(start <= current <= end for start, end in sessions)


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MarketTemperatureError(f"JSON 文件读取失败: {path}: {exc}") from exc


def _history_csv(payload: Mapping[str, Any]) -> str:
    buffer = StringIO(newline="")
    # 固定为 LF，避免 Windows 文本写入再次转换 CRLF 后产生空白行。
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(
        [
            "trade_date",
            "minute",
            "shanghai_amount_yuan",
            "shenzhen_amount_yuan",
            "total_amount_yuan",
            "previous_trade_date",
            "previous_total_amount_yuan",
            "delta_amount_yuan",
            "temperature_pct",
            "source",
            "quote_at",
        ]
    )
    for row in payload["series"]:
        writer.writerow(
            [
                row["trade_date"],
                row["minute"],
                row["shanghai_amount_yuan"],
                row["shenzhen_amount_yuan"],
                row["total_amount_yuan"],
                row.get("previous_trade_date") or "",
                row.get("previous_total_amount_yuan") or "",
                "" if row.get("delta_amount_yuan") is None else row["delta_amount_yuan"],
                "" if row.get("temperature_pct") is None else row["temperature_pct"],
                row["source"],
                row["quote_at"],
            ]
        )
    # BOM 方便 Windows Excel 直接识别 UTF-8。
    return "\ufeff" + buffer.getvalue()


def _history_catalog(docs_dir: Path, generated_at: str) -> dict[str, Any]:
    entries: list[dict[str, str]] = []
    history_root = docs_dir / "history"
    for json_path in history_root.glob("[0-9][0-9][0-9][0-9]/*.json"):
        trade_date = json_path.stem
        try:
            date.fromisoformat(trade_date)
        except ValueError:
            continue
        relative_json = json_path.relative_to(docs_dir).as_posix()
        relative_csv = json_path.with_suffix(".csv").relative_to(docs_dir).as_posix()
        entries.append(
            {
                "trade_date": trade_date,
                "json": relative_json,
                "csv": relative_csv,
            }
        )
    entries.sort(key=lambda item: item["trade_date"], reverse=True)
    return {"schema_version": 1, "generated_at": generated_at, "days": entries}


def _history_index_html(catalog: Mapping[str, Any]) -> str:
    rows = "".join(
        "<tr>"
        f"<td>{html.escape(item['trade_date'])}</td>"
        f"<td><a href=\"../{html.escape(item['json'])}\">JSON</a></td>"
        f"<td><a href=\"../{html.escape(item['csv'])}\">CSV</a></td>"
        "</tr>"
        for item in catalog["days"]
    )
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>A股成交额温度历史</title>
  <style>
    body {{ font-family: "Microsoft YaHei", sans-serif; margin: 32px auto; max-width: 820px; padding: 0 18px; color: #0f172a; }}
    a {{ color: #2563eb; }} table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ text-align: left; padding: 10px; border-bottom: 1px solid #e2e8f0; }}
  </style>
</head>
<body>
  <p><a href="../index.html">← 返回最新温度</a></p>
  <h1>A股成交额温度历史</h1>
  <p>按交易日保存，每个文件包含当天全部10分钟快照。</p>
  <table><thead><tr><th>交易日</th><th>JSON</th><th>CSV</th></tr></thead><tbody>{rows}</tbody></table>
</body>
</html>
"""


def persist_history_artifacts(payload: Mapping[str, Any], docs_dir: Path = DEFAULT_DOCS_DIR) -> dict[str, Path]:
    trade_date = str(payload["trade_date"])
    date.fromisoformat(trade_date)
    if not payload.get("series"):
        raise MarketTemperatureError("latest.json 中没有盘中序列")

    year_dir = docs_dir / "history" / trade_date[:4]
    json_path = year_dir / f"{trade_date}.json"
    csv_path = year_dir / f"{trade_date}.csv"
    atomic_write_text(json_path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    atomic_write_text(csv_path, _history_csv(payload), encoding="utf-8")

    catalog = _history_catalog(docs_dir, str(payload["generated_at"]))
    catalog_path = docs_dir / "history" / "index.json"
    catalog_html_path = docs_dir / "history" / "index.html"
    atomic_write_text(catalog_path, json.dumps(catalog, ensure_ascii=False, indent=2) + "\n")
    atomic_write_text(catalog_html_path, _history_index_html(catalog))

    dashboard_path = docs_dir / "index.html"
    if dashboard_path.exists():
        dashboard = dashboard_path.read_text(encoding="utf-8")
        history_link = (
            '<section class="panel formula"><strong>历史追溯：</strong>'
            '<a href="history/index.html">查看每日 JSON / CSV 归档</a></section>'
        )
        if "history/index.html" not in dashboard:
            dashboard = dashboard.replace("</main>", f"{history_link}</main>")
            atomic_write_text(dashboard_path, dashboard)

    return {
        "json": json_path,
        "csv": csv_path,
        "catalog": catalog_path,
        "catalog_html": catalog_html_path,
    }


def _validate_email(value: str, variable_name: str) -> str:
    cleaned = value.strip()
    if not cleaned or "@" not in cleaned or "\r" in cleaned or "\n" in cleaned:
        raise MarketTemperatureError(f"{variable_name} 不是有效邮箱地址")
    return cleaned


@dataclass(frozen=True)
class MailConfig:
    username: str
    password: str
    recipient: str
    host: str = SMTP_HOST
    port: int = SMTP_PORT

    @classmethod
    def from_env(cls) -> "MailConfig":
        username = _validate_email(os.environ.get("SMTP_USERNAME", ""), "SMTP_USERNAME")
        password = os.environ.get("SMTP_APP_PASSWORD", "").strip()
        if not password:
            raise MarketTemperatureError("缺少 SMTP_APP_PASSWORD（163客户端授权码）")
        recipient = _validate_email(os.environ.get("ALERT_TO", username), "ALERT_TO")
        return cls(username=username, password=password, recipient=recipient)


def send_email(mail: MailConfig, subject: str, body: str) -> None:
    message = EmailMessage()
    message["From"] = mail.username
    message["To"] = mail.recipient
    message["Subject"] = subject
    message.set_content(body)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(mail.host, mail.port, timeout=30, context=context) as client:
        client.login(mail.username, mail.password)
        client.send_message(message)


def _amount_yi(value: int | None) -> str:
    return "--" if value is None else f"{value / 100_000_000:,.2f}"


def build_alert_message(latest: Mapping[str, Any], threshold: float) -> tuple[str, str]:
    temperature = float(latest["temperature_pct"])
    direction = alert_direction(temperature, threshold)
    label = "显著放量" if direction == "hot" else "显著缩量"
    subject = f"[A股温度告警] {latest['trade_date']} {latest['minute']} {temperature:+.2f}%"
    body = f"""A股成交额温度已越过告警阈值。

状态：{label}
交易日：{latest['trade_date']}
时间：{latest['minute']}
当前温度：{temperature:+.2f}%
告警阈值：大于 +{threshold:.0f}% 或小于 -{threshold:.0f}%
今日沪深累计成交额：{_amount_yi(latest['total_amount_yuan'])} 亿元
前一交易日同期成交额：{_amount_yi(latest.get('previous_total_amount_yuan'))} 亿元
同期差额：{_amount_yi(latest.get('delta_amount_yuan'))} 亿元

数据源：腾讯财经公开行情
计算口径：今日累计成交额相对前一交易日同期成交额的百分比差异。
"""
    return subject, body


def load_alert_state(path: Path = DEFAULT_ALERT_STATE) -> dict[str, Any]:
    state = _read_json(path, {"schema_version": 1, "alerts": {}})
    if not isinstance(state, dict) or not isinstance(state.get("alerts"), dict):
        raise MarketTemperatureError(f"告警状态文件结构异常: {path}")
    return state


def alert_already_sent(state: Mapping[str, Any], trade_date: str, direction: str) -> bool:
    return direction in state.get("alerts", {}).get(trade_date, {})


def record_alert(
    state: dict[str, Any],
    trade_date: str,
    direction: str,
    temperature_pct: float,
    sent_at: str,
) -> None:
    alerts = state.setdefault("alerts", {})
    alerts.setdefault(trade_date, {})[direction] = {
        "temperature_pct": temperature_pct,
        "sent_at": sent_at,
    }
    # 只保留最近120个交易日的去重状态。
    for old_date in sorted(alerts)[:-120]:
        alerts.pop(old_date, None)


def maybe_send_threshold_alert(
    payload: Mapping[str, Any],
    threshold: float,
    state_path: Path = DEFAULT_ALERT_STATE,
    sender: Callable[[MailConfig, str, str], None] = send_email,
) -> str:
    latest = payload["latest"]
    temperature_raw = latest.get("temperature_pct")
    temperature = None if temperature_raw is None else float(temperature_raw)
    direction = alert_direction(temperature, threshold)
    if direction is None:
        return "within-threshold"

    trade_date = str(latest["trade_date"])
    state = load_alert_state(state_path)
    if alert_already_sent(state, trade_date, direction):
        return "already-sent"

    subject, body = build_alert_message(latest, threshold)
    sender(MailConfig.from_env(), subject, body)
    sent_at = datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(timespec="seconds")
    record_alert(state, trade_date, direction, float(temperature), sent_at)
    atomic_write_text(state_path, json.dumps(state, ensure_ascii=False, indent=2) + "\n")
    return "sent"


def send_configuration_test(payload: Mapping[str, Any]) -> None:
    latest = payload["latest"]
    temperature = latest.get("temperature_pct")
    temperature_text = "--" if temperature is None else f"{float(temperature):+.2f}%"
    subject = "[A股温度监控] 云端邮件配置测试成功"
    body = f"""163邮箱 SMTP 配置测试成功。

最新行情：{latest['trade_date']} {latest['minute']}
最新温度：{temperature_text}
今日沪深累计成交额：{_amount_yi(latest['total_amount_yuan'])} 亿元

后续当温度严格大于 +30% 或严格小于 -30% 时，系统会自动发送告警。
"""
    send_email(MailConfig.from_env(), subject, body)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="GitHub Actions A股成交额温度监控")
    parser.add_argument("--config", type=Path, default=DEFAULT_CLOUD_CONFIG)
    parser.add_argument("--force", action="store_true", help="允许在交易时段外人工测试")
    parser.add_argument("--test-email", action="store_true", help="发送一次配置测试邮件，不触发阈值告警")
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.threshold <= 0:
        print("--threshold 必须大于 0", file=sys.stderr)
        return 2

    try:
        config = AppConfig.load(args.config)
        configure_logging(config)
        timezone = ZoneInfo(config.timezone)
        now = datetime.now(timezone)
        scheduled = os.environ.get("GITHUB_EVENT_NAME") == "schedule"
        if not args.force and not scheduled and not is_in_trading_session(now, config.sessions):
            print(f"当前不在A股交易时段，跳过：{now.isoformat(timespec='seconds')}")
            return 0

        result = collect_once(config)
        payload = _read_json(config.latest_json_path, None)
        if not isinstance(payload, dict):
            raise MarketTemperatureError("采集完成后未生成有效 latest.json")

        today = now.strftime("%Y-%m-%d")
        if result["trade_date"] != today and not args.force:
            print(f"行情最新交易日为 {result['trade_date']}，今天 {today} 不是有效交易日，跳过持久化和告警")
            return 0

        paths = persist_history_artifacts(payload, config.dashboard_path.parent)
        print(f"历史已保存：{paths['json']} / {paths['csv']}")

        if args.test_email:
            send_configuration_test(payload)
            print("163邮箱配置测试邮件已发送")
        elif result["trade_date"] == today:
            alert_result = maybe_send_threshold_alert(payload, args.threshold)
            print(f"阈值告警结果：{alert_result}")
        else:
            print("人工回填的是历史交易日，不执行阈值告警")
        return 0
    except Exception as exc:
        print(f"云端监控执行失败：{exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
