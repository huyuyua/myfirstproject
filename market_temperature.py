# -*- coding: utf-8 -*-
"""沪深两市成交额温度采集器。

温度定义：今日累计成交额 - 前一交易日同一时刻累计成交额。
温度百分比：上述差值 / 前一交易日同一时刻累计成交额 * 100。

数据来源为腾讯财经公开分时接口。脚本仅依赖 Python 标准库，适合通过
Windows 任务计划程序在每个工作日启动，并在 A 股交易时段每 10 分钟采集。
"""

from __future__ import annotations

import argparse
import csv
from contextlib import contextmanager
import html
import json
import logging
from logging.handlers import RotatingFileHandler
import math
import os
from pathlib import Path
import re
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date, datetime, time as datetime_time, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Iterable, Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo


APP_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = APP_DIR / "config.json"
LOGGER_NAME = "market_temperature"


class MarketTemperatureError(RuntimeError):
    """业务可预期异常。"""


class DataSourceError(MarketTemperatureError):
    """行情源请求或响应解析失败。"""


def _parse_hhmm(value: str) -> datetime_time:
    try:
        return datetime.strptime(value, "%H:%M").time()
    except ValueError as exc:
        raise MarketTemperatureError(f"非法交易时间配置: {value!r}") from exc


def _resolve_path(value: str, base_dir: Path) -> Path:
    path = Path(os.path.expandvars(value)).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path.resolve()


@dataclass(frozen=True)
class AppConfig:
    timezone: str
    interval_minutes: int
    sessions: tuple[tuple[str, str], ...]
    symbols: Mapping[str, str]
    request_timeout_seconds: float
    request_retries: int
    retry_backoff_seconds: float
    tencent_day_url: str
    tencent_quote_url: str
    database_path: Path
    dashboard_path: Path
    latest_json_path: Path
    log_path: Path

    @classmethod
    def load(cls, path: Path | None = None) -> "AppConfig":
        config_path = (path or DEFAULT_CONFIG_PATH).resolve()
        if config_path.exists():
            try:
                raw = json.loads(config_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise MarketTemperatureError(f"配置文件读取失败: {config_path}: {exc}") from exc
        else:
            raw = {}

        base_dir = config_path.parent
        sessions_raw = raw.get("sessions", [["09:30", "11:30"], ["13:00", "15:00"]])
        sessions: list[tuple[str, str]] = []
        for item in sessions_raw:
            if not isinstance(item, list) or len(item) != 2:
                raise MarketTemperatureError("sessions 必须是 [[\"09:30\", \"11:30\"], ...] 格式")
            start, end = str(item[0]), str(item[1])
            if _parse_hhmm(start) >= _parse_hhmm(end):
                raise MarketTemperatureError(f"交易时段起止时间错误: {start}-{end}")
            sessions.append((start, end))

        interval = int(raw.get("interval_minutes", 10))
        if interval <= 0 or interval > 60:
            raise MarketTemperatureError("interval_minutes 必须在 1 到 60 之间")

        symbols = raw.get(
            "symbols",
            {"shanghai": "sh000001", "shenzhen": "sz399001"},
        )
        if not isinstance(symbols, dict) or set(symbols) != {"shanghai", "shenzhen"}:
            raise MarketTemperatureError("symbols 必须同时配置 shanghai 和 shenzhen")

        config = cls(
            timezone=str(raw.get("timezone", "Asia/Shanghai")),
            interval_minutes=interval,
            sessions=tuple(sessions),
            symbols={key: str(value) for key, value in symbols.items()},
            request_timeout_seconds=float(raw.get("request_timeout_seconds", 12)),
            request_retries=int(raw.get("request_retries", 3)),
            retry_backoff_seconds=float(raw.get("retry_backoff_seconds", 1.0)),
            tencent_day_url=str(
                raw.get(
                    "tencent_day_url",
                    "https://web.ifzq.gtimg.cn/appstock/app/day/query?code={symbol}",
                )
            ),
            tencent_quote_url=str(
                raw.get("tencent_quote_url", "https://qt.gtimg.cn/q={symbols}")
            ),
            database_path=_resolve_path(
                str(raw.get("database_path", "data/market_temperature.sqlite3")), base_dir
            ),
            dashboard_path=_resolve_path(
                str(raw.get("dashboard_path", "output/market_temperature.html")), base_dir
            ),
            latest_json_path=_resolve_path(
                str(raw.get("latest_json_path", "output/latest.json")), base_dir
            ),
            log_path=_resolve_path(
                str(raw.get("log_path", "logs/market_temperature.log")), base_dir
            ),
        )
        if config.request_retries < 1:
            raise MarketTemperatureError("request_retries 至少为 1")
        # 初始化时验证时区名称，避免任务启动后才静默失败。
        ZoneInfo(config.timezone)
        return config


@dataclass(frozen=True)
class IndexMinute:
    trade_date: str
    minute: str
    amount_yuan: int


@dataclass(frozen=True)
class RawMarketSnapshot:
    trade_date: str
    minute: str
    quote_at: str
    shanghai_amount_yuan: int
    shenzhen_amount_yuan: int
    total_amount_yuan: int
    source: str = "tencent"
    is_backfill: int = 1


def market_slots(
    trade_date: str,
    sessions: Sequence[tuple[str, str]],
    interval_minutes: int,
    timezone: ZoneInfo,
) -> list[datetime]:
    day = date.fromisoformat(trade_date)
    result: list[datetime] = []
    for start_text, end_text in sessions:
        cursor = datetime.combine(day, _parse_hhmm(start_text), tzinfo=timezone)
        end = datetime.combine(day, _parse_hhmm(end_text), tzinfo=timezone)
        while cursor <= end:
            result.append(cursor)
            cursor += timedelta(minutes=interval_minutes)
    return result


def valid_slot_minutes(config: AppConfig) -> set[str]:
    # 日期不影响 HH:MM 序列，使用任意普通日期生成即可。
    timezone = ZoneInfo(config.timezone)
    return {
        item.strftime("%H:%M")
        for item in market_slots("2000-01-03", config.sessions, config.interval_minutes, timezone)
    }


class TencentFinanceClient:
    """腾讯财经指数分时数据客户端。"""

    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )

    def __init__(self, config: AppConfig):
        self.config = config
        self.logger = logging.getLogger(LOGGER_NAME)

    def _get(self, url: str, encoding: str = "utf-8") -> str:
        last_error: Exception | None = None
        for attempt in range(1, self.config.request_retries + 1):
            request = Request(
                url,
                headers={
                    "User-Agent": self.USER_AGENT,
                    "Referer": "https://finance.qq.com/",
                    "Accept": "application/json,text/plain,*/*",
                },
            )
            try:
                with urlopen(request, timeout=self.config.request_timeout_seconds) as response:
                    status = getattr(response, "status", 200)
                    if status != 200:
                        raise DataSourceError(f"HTTP {status}")
                    return response.read().decode(encoding, errors="strict")
            except (HTTPError, URLError, TimeoutError, UnicodeDecodeError, OSError) as exc:
                last_error = exc
                self.logger.warning(
                    "腾讯行情请求失败（第 %s/%s 次）: %s",
                    attempt,
                    self.config.request_retries,
                    exc,
                )
                if attempt < self.config.request_retries:
                    time.sleep(self.config.retry_backoff_seconds * attempt)
        raise DataSourceError(f"腾讯行情请求连续失败: {last_error}")

    @staticmethod
    def parse_day_payload(payload: str, symbol: str) -> dict[tuple[str, str], IndexMinute]:
        try:
            body = json.loads(payload)
            if body.get("code") != 0:
                raise DataSourceError(f"腾讯分时接口返回错误: {body.get('msg') or body.get('code')}")
            days = body["data"][symbol]["data"]
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            raise DataSourceError(f"腾讯分时响应结构异常 ({symbol})") from exc

        result: dict[tuple[str, str], IndexMinute] = {}
        for day in days:
            raw_date = str(day.get("date", ""))
            if not re.fullmatch(r"\d{8}", raw_date):
                continue
            trade_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
            for raw_row in day.get("data", []):
                parts = str(raw_row).split()
                if len(parts) < 4 or not re.fullmatch(r"\d{4}", parts[0]):
                    continue
                minute = f"{parts[0][:2]}:{parts[0][2:]}"
                try:
                    amount = int(Decimal(parts[3]).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
                except (InvalidOperation, ValueError):
                    continue
                if amount < 0:
                    continue
                result[(trade_date, minute)] = IndexMinute(trade_date, minute, amount)

        if not result:
            raise DataSourceError(f"腾讯分时响应没有有效成交额 ({symbol})")
        return result

    def _fetch_symbol_history(self, symbol: str) -> dict[tuple[str, str], IndexMinute]:
        url = self.config.tencent_day_url.format(symbol=quote(symbol, safe=""))
        return self.parse_day_payload(self._get(url), symbol)

    def fetch_market_history(self) -> list[RawMarketSnapshot]:
        """并行拉取沪深指数近 5 个交易日的分钟累计成交额并合并。"""
        histories: dict[str, dict[tuple[str, str], IndexMinute]] = {}
        failures: dict[str, Exception] = {}
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="tencent-index") as pool:
            futures = {
                pool.submit(self._fetch_symbol_history, symbol): market
                for market, symbol in self.config.symbols.items()
            }
            for future in as_completed(futures):
                market = futures[future]
                try:
                    histories[market] = future.result()
                except Exception as exc:  # 汇总并保留两个并行请求的错误。
                    failures[market] = exc

        if failures:
            detail = "; ".join(f"{key}: {value}" for key, value in failures.items())
            raise DataSourceError(f"沪深分时数据未完整返回: {detail}")

        shanghai = histories["shanghai"]
        shenzhen = histories["shenzhen"]
        common_keys = sorted(set(shanghai).intersection(shenzhen))
        allowed_minutes = valid_slot_minutes(self.config)
        timezone = ZoneInfo(self.config.timezone)
        now = datetime.now(timezone)
        snapshots: list[RawMarketSnapshot] = []
        for trade_date, minute in common_keys:
            if minute not in allowed_minutes:
                continue
            sh_amount = shanghai[(trade_date, minute)].amount_yuan
            sz_amount = shenzhen[(trade_date, minute)].amount_yuan
            quote_at = datetime.combine(
                date.fromisoformat(trade_date),
                _parse_hhmm(minute),
                tzinfo=timezone,
            ).isoformat(timespec="seconds")
            quote_dt = datetime.fromisoformat(quote_at)
            is_backfill = int(now - quote_dt > timedelta(minutes=self.config.interval_minutes + 2))
            snapshots.append(
                RawMarketSnapshot(
                    trade_date=trade_date,
                    minute=minute,
                    quote_at=quote_at,
                    shanghai_amount_yuan=sh_amount,
                    shenzhen_amount_yuan=sz_amount,
                    total_amount_yuan=sh_amount + sz_amount,
                    is_backfill=is_backfill,
                )
            )
        if not snapshots:
            raise DataSourceError("沪深分时数据没有共同的 10 分钟快照")
        return snapshots

    @staticmethod
    def parse_quote_payload(
        payload: str,
        symbols: Sequence[str],
        timezone: ZoneInfo,
    ) -> RawMarketSnapshot:
        values: dict[str, tuple[datetime, int]] = {}
        for symbol in symbols:
            match = re.search(rf"v_{re.escape(symbol)}=\"([^\"]*)\"", payload)
            if not match:
                raise DataSourceError(f"腾讯实时报价缺少 {symbol}")
            fields = match.group(1).split("~")
            if len(fields) <= 37:
                raise DataSourceError(f"腾讯实时报价字段不足 ({symbol}: {len(fields)})")
            try:
                quote_dt = datetime.strptime(fields[30], "%Y%m%d%H%M%S").replace(tzinfo=timezone)
                # 字段 35 为 最新价/成交量/成交额，第三项是精确的累计成交额（元）。
                compound = fields[35].split("/")
                raw_amount = compound[2] if len(compound) >= 3 else ""
                if not raw_amount:
                    # 字段 37 的单位为万元。
                    raw_amount = str(Decimal(fields[37]) * Decimal("10000"))
                amount = int(Decimal(raw_amount).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
            except (ValueError, InvalidOperation, IndexError) as exc:
                raise DataSourceError(f"腾讯实时报价关键字段异常 ({symbol})") from exc
            values[symbol] = (quote_dt, amount)

        dates = {item[0].date() for item in values.values()}
        if len(dates) != 1:
            raise DataSourceError("沪深实时报价日期不一致")
        quote_dt = min(item[0] for item in values.values())
        sh_dt, sh_amount = values[symbols[0]]
        sz_dt, sz_amount = values[symbols[1]]
        if abs((sh_dt - sz_dt).total_seconds()) > 120:
            raise DataSourceError("沪深实时报价时间相差超过 2 分钟")
        return RawMarketSnapshot(
            trade_date=quote_dt.strftime("%Y-%m-%d"),
            minute=quote_dt.strftime("%H:%M"),
            quote_at=quote_dt.isoformat(timespec="seconds"),
            shanghai_amount_yuan=sh_amount,
            shenzhen_amount_yuan=sz_amount,
            total_amount_yuan=sh_amount + sz_amount,
            is_backfill=0,
        )

    def fetch_current_quote(self) -> RawMarketSnapshot:
        symbols = [self.config.symbols["shanghai"], self.config.symbols["shenzhen"]]
        url = self.config.tencent_quote_url.format(symbols=quote(",".join(symbols), safe=","))
        payload = self._get(url, encoding="gb18030")
        return self.parse_quote_payload(payload, symbols, ZoneInfo(self.config.timezone))


class SnapshotRepository:
    """SQLite 行情快照存储与计算层。"""

    def __init__(self, database_path: Path):
        self.database_path = database_path
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    @contextmanager
    def _connection(self):
        """sqlite3.Connection 的 with 只负责事务，不会自动关闭文件句柄。"""
        connection = self._connect()
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._connection() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = NORMAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS market_temperature_snapshots (
                    trade_date TEXT NOT NULL,
                    minute TEXT NOT NULL,
                    quote_at TEXT NOT NULL,
                    first_captured_at TEXT NOT NULL,
                    last_captured_at TEXT NOT NULL,
                    capture_count INTEGER NOT NULL DEFAULT 1,
                    shanghai_amount_yuan INTEGER NOT NULL CHECK (shanghai_amount_yuan >= 0),
                    shenzhen_amount_yuan INTEGER NOT NULL CHECK (shenzhen_amount_yuan >= 0),
                    total_amount_yuan INTEGER NOT NULL CHECK (total_amount_yuan >= 0),
                    previous_trade_date TEXT,
                    previous_minute TEXT,
                    previous_total_amount_yuan INTEGER,
                    delta_amount_yuan INTEGER,
                    temperature_pct REAL,
                    previous_close_amount_yuan INTEGER,
                    delta_vs_previous_close_yuan INTEGER,
                    source TEXT NOT NULL,
                    is_backfill INTEGER NOT NULL DEFAULT 0 CHECK (is_backfill IN (0, 1)),
                    PRIMARY KEY (trade_date, minute)
                );

                CREATE INDEX IF NOT EXISTS idx_market_temperature_quote_at
                    ON market_temperature_snapshots (quote_at);
                CREATE INDEX IF NOT EXISTS idx_market_temperature_source_date
                    ON market_temperature_snapshots (source, trade_date);

                CREATE TABLE IF NOT EXISTS market_temperature_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )

    def upsert(self, snapshots: Iterable[RawMarketSnapshot], captured_at: str) -> int:
        rows = list(snapshots)
        if not rows:
            return 0
        with self._connection() as connection:
            connection.executemany(
                """
                INSERT INTO market_temperature_snapshots (
                    trade_date, minute, quote_at,
                    first_captured_at, last_captured_at, capture_count,
                    shanghai_amount_yuan, shenzhen_amount_yuan, total_amount_yuan,
                    source, is_backfill
                ) VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_date, minute) DO UPDATE SET
                    quote_at = excluded.quote_at,
                    last_captured_at = excluded.last_captured_at,
                    capture_count = market_temperature_snapshots.capture_count + 1,
                    shanghai_amount_yuan = excluded.shanghai_amount_yuan,
                    shenzhen_amount_yuan = excluded.shenzhen_amount_yuan,
                    total_amount_yuan = excluded.total_amount_yuan,
                    source = excluded.source,
                    is_backfill = MIN(market_temperature_snapshots.is_backfill, excluded.is_backfill)
                """,
                [
                    (
                        row.trade_date,
                        row.minute,
                        row.quote_at,
                        captured_at,
                        captured_at,
                        row.shanghai_amount_yuan,
                        row.shenzhen_amount_yuan,
                        row.total_amount_yuan,
                        row.source,
                        row.is_backfill,
                    )
                    for row in rows
                ],
            )
            connection.execute(
                """
                INSERT INTO market_temperature_metadata (key, value, updated_at)
                VALUES ('last_successful_collection_at', ?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
                """,
                (captured_at, captured_at),
            )
        return len(rows)

    def recalculate(self) -> None:
        """重算全部快照，确保回填旧数据后后续日期的基线也随之更新。"""
        with self._connection() as connection:
            dates = [
                row["trade_date"]
                for row in connection.execute(
                    "SELECT DISTINCT trade_date FROM market_temperature_snapshots ORDER BY trade_date"
                )
            ]
            for index, trade_date in enumerate(dates):
                if index == 0:
                    connection.execute(
                        """
                        UPDATE market_temperature_snapshots
                        SET previous_trade_date = NULL,
                            previous_minute = NULL,
                            previous_total_amount_yuan = NULL,
                            delta_amount_yuan = NULL,
                            temperature_pct = NULL,
                            previous_close_amount_yuan = NULL,
                            delta_vs_previous_close_yuan = NULL
                        WHERE trade_date = ?
                        """,
                        (trade_date,),
                    )
                    continue

                previous_date = dates[index - 1]
                previous_rows = {
                    row["minute"]: int(row["total_amount_yuan"])
                    for row in connection.execute(
                        """
                        SELECT minute, total_amount_yuan
                        FROM market_temperature_snapshots
                        WHERE trade_date = ?
                        """,
                        (previous_date,),
                    )
                }
                previous_close = connection.execute(
                    """
                    SELECT minute, total_amount_yuan
                    FROM market_temperature_snapshots
                    WHERE trade_date = ?
                    ORDER BY minute DESC
                    LIMIT 1
                    """,
                    (previous_date,),
                ).fetchone()
                previous_close_amount = int(previous_close["total_amount_yuan"])

                current_rows = connection.execute(
                    """
                    SELECT minute, total_amount_yuan
                    FROM market_temperature_snapshots
                    WHERE trade_date = ?
                    """,
                    (trade_date,),
                ).fetchall()
                for current in current_rows:
                    minute = current["minute"]
                    current_amount = int(current["total_amount_yuan"])
                    previous_amount = previous_rows.get(minute)
                    delta = None if previous_amount is None else current_amount - previous_amount
                    temperature_pct = (
                        None
                        if previous_amount in (None, 0)
                        else round(delta / previous_amount * 100, 6)
                    )
                    connection.execute(
                        """
                        UPDATE market_temperature_snapshots
                        SET previous_trade_date = ?,
                            previous_minute = ?,
                            previous_total_amount_yuan = ?,
                            delta_amount_yuan = ?,
                            temperature_pct = ?,
                            previous_close_amount_yuan = ?,
                            delta_vs_previous_close_yuan = ?
                        WHERE trade_date = ? AND minute = ?
                        """,
                        (
                            previous_date,
                            minute if previous_amount is not None else None,
                            previous_amount,
                            delta,
                            temperature_pct,
                            previous_close_amount,
                            current_amount - previous_close_amount,
                            trade_date,
                            minute,
                        ),
                    )

    def latest_trade_date(self) -> str | None:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT MAX(trade_date) AS trade_date FROM market_temperature_snapshots"
            ).fetchone()
            return row["trade_date"] if row and row["trade_date"] else None

    def rows_for_date(self, trade_date: str) -> list[dict[str, Any]]:
        with self._connection() as connection:
            return [
                dict(row)
                for row in connection.execute(
                    """
                    SELECT *
                    FROM market_temperature_snapshots
                    WHERE trade_date = ?
                    ORDER BY minute
                    """,
                    (trade_date,),
                )
            ]

    def query_rows(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if start_date:
            clauses.append("trade_date >= ?")
            params.append(start_date)
        if end_date:
            clauses.append("trade_date <= ?")
            params.append(end_date)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        limit_sql = ""
        if limit is not None:
            limit_sql = " LIMIT ?"
            params.append(limit)
        with self._connection() as connection:
            return [
                dict(row)
                for row in connection.execute(
                    f"""
                    SELECT * FROM market_temperature_snapshots
                    {where}
                    ORDER BY trade_date DESC, minute DESC
                    {limit_sql}
                    """,
                    params,
                )
            ]


def _atomic_write_text(path: Path, content: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(content, encoding=encoding)
    os.replace(temporary, path)


def _yi(value: int | None) -> float | None:
    return None if value is None else value / 100_000_000


def _format_yi(value: int | None, signed: bool = False) -> str:
    converted = _yi(value)
    if converted is None:
        return "--"
    return f"{converted:+,.2f}" if signed else f"{converted:,.2f}"


def _format_pct(value: float | None, signed: bool = False) -> str:
    if value is None:
        return "--"
    return f"{value:+.2f}%" if signed else f"{value:.2f}%"


def _temperature_label(value: float | None) -> str:
    if value is None:
        return "基线不足"
    if value >= 10:
        return "显著放量"
    if value >= 3:
        return "偏热"
    if value > -3:
        return "中性"
    if value > -10:
        return "偏冷"
    return "显著缩量"


def _dashboard_svg(rows: Sequence[dict[str, Any]]) -> str:
    width, height = 1120, 360
    left, right, top, bottom = 64, 24, 30, 52
    chart_width = width - left - right
    chart_height = height - top - bottom
    zero_y = top + chart_height / 2
    values = [abs(float(row["temperature_pct"])) for row in rows if row["temperature_pct"] is not None]
    extent = max(5.0, max(values, default=0.0) * 1.15)
    extent = math.ceil(extent / 5) * 5
    count = max(1, len(rows))
    step = chart_width / count
    bar_width = max(4.0, min(26.0, step * 0.7))

    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="成交额温度零轴图">',
        '<rect width="100%" height="100%" fill="#ffffff" rx="12"/>',
    ]
    for fraction in (0.0, 0.25, 0.5, 0.75, 1.0):
        y = top + chart_height * fraction
        value = extent * (1 - 2 * fraction)
        stroke = "#64748b" if fraction == 0.5 else "#e2e8f0"
        stroke_width = 2 if fraction == 0.5 else 1
        parts.append(
            f'<line x1="{left}" y1="{y:.2f}" x2="{width-right}" y2="{y:.2f}" '
            f'stroke="{stroke}" stroke-width="{stroke_width}"/>'
        )
        parts.append(
            f'<text x="{left-9}" y="{y+4:.2f}" text-anchor="end" '
            f'font-size="12" fill="#64748b">{value:+.0f}%</text>'
        )

    for index, row in enumerate(rows):
        value = row["temperature_pct"]
        x = left + step * index + (step - bar_width) / 2
        if value is not None:
            value_float = float(value)
            bar_height = min(abs(value_float) / extent, 1.0) * chart_height / 2
            y = zero_y - bar_height if value_float >= 0 else zero_y
            color = "#ef4444" if value_float >= 0 else "#10b981"
            tooltip = html.escape(
                f"{row['minute']} 温度 {value_float:+.2f}% / 差额 {_format_yi(row['delta_amount_yuan'], signed=True)} 亿元"
            )
            parts.append(
                f'<rect x="{x:.2f}" y="{y:.2f}" width="{bar_width:.2f}" height="{max(1, bar_height):.2f}" '
                f'fill="{color}" rx="2"><title>{tooltip}</title></rect>'
            )
        if index % 3 == 0 or index == len(rows) - 1:
            parts.append(
                f'<text x="{left + step * (index + 0.5):.2f}" y="{height-20}" '
                f'text-anchor="middle" font-size="11" fill="#64748b">{html.escape(row["minute"])}</text>'
            )
    parts.append(
        f'<text x="{left + chart_width / 2:.2f}" y="{height-3}" text-anchor="middle" '
        'font-size="11" fill="#94a3b8">交易时间（红色=放量，绿色=缩量）</text>'
    )
    parts.append("</svg>")
    return "".join(parts)


def build_dashboard_document(trade_date: str, rows: Sequence[dict[str, Any]], generated_at: str) -> str:
    latest = rows[-1]
    temperature = latest["temperature_pct"]
    direction_class = "hot" if temperature is not None and temperature >= 0 else "cold"
    previous_date = latest["previous_trade_date"] or "--"
    cards = [
        ("当前累计成交额", f"{_format_yi(latest['total_amount_yuan'])} 亿元", ""),
        ("前一交易日同期", f"{_format_yi(latest['previous_total_amount_yuan'])} 亿元", ""),
        ("温度差额", f"{_format_yi(latest['delta_amount_yuan'], signed=True)} 亿元", direction_class),
        ("成交额温度", _format_pct(temperature, signed=True), direction_class),
    ]
    cards_html = "".join(
        f'<section class="card {css}"><span>{html.escape(title)}</span><strong>{html.escape(value)}</strong></section>'
        for title, value, css in cards
    )
    table_rows = []
    for row in reversed(rows):
        css = "hot-text" if row["temperature_pct"] is not None and row["temperature_pct"] >= 0 else "cold-text"
        table_rows.append(
            "<tr>"
            f"<td>{html.escape(row['minute'])}</td>"
            f"<td>{_format_yi(row['total_amount_yuan'])}</td>"
            f"<td>{_format_yi(row['previous_total_amount_yuan'])}</td>"
            f'<td class="{css}">{_format_yi(row["delta_amount_yuan"], signed=True)}</td>'
            f'<td class="{css}">{_format_pct(row["temperature_pct"], signed=True)}</td>'
            f"<td>{_format_yi(row['delta_vs_previous_close_yuan'], signed=True)}</td>"
            "</tr>"
        )

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta http-equiv="refresh" content="600">
  <title>{trade_date} 沪深成交额温度</title>
  <style>
    :root {{ color-scheme: light; font-family: "Microsoft YaHei", "PingFang SC", sans-serif; }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; background: #f1f5f9; color: #0f172a; }}
    main {{ width: min(1180px, calc(100% - 32px)); margin: 28px auto 48px; }}
    header {{ display: flex; justify-content: space-between; gap: 20px; align-items: end; margin-bottom: 20px; }}
    h1 {{ margin: 0 0 7px; font-size: clamp(24px, 4vw, 38px); }}
    .subtle {{ color: #64748b; font-size: 13px; }}
    .badge {{ padding: 8px 14px; border-radius: 999px; font-weight: 700; background: #e2e8f0; white-space: nowrap; }}
    .cards {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; margin-bottom: 18px; }}
    .card {{ background: #fff; border-radius: 14px; padding: 18px; box-shadow: 0 4px 18px rgba(15,23,42,.06); }}
    .card span {{ display: block; color: #64748b; font-size: 13px; margin-bottom: 10px; }}
    .card strong {{ font-size: clamp(19px, 2.4vw, 28px); }}
    .card.hot strong, .hot-text {{ color: #dc2626; }}
    .card.cold strong, .cold-text {{ color: #059669; }}
    .panel {{ background: #fff; border-radius: 14px; padding: 18px; margin-bottom: 18px; box-shadow: 0 4px 18px rgba(15,23,42,.06); overflow-x: auto; }}
    .panel h2 {{ margin: 0 0 14px; font-size: 18px; }}
    svg {{ display: block; min-width: 760px; width: 100%; height: auto; }}
    table {{ width: 100%; border-collapse: collapse; font-variant-numeric: tabular-nums; }}
    th, td {{ padding: 10px 12px; text-align: right; border-bottom: 1px solid #e2e8f0; white-space: nowrap; }}
    th:first-child, td:first-child {{ text-align: left; }}
    th {{ color: #475569; font-size: 12px; }}
    td {{ font-size: 13px; }}
    .formula {{ line-height: 1.75; color: #475569; font-size: 13px; }}
    @media (max-width: 800px) {{ .cards {{ grid-template-columns: repeat(2, 1fr); }} header {{ align-items: start; flex-direction: column; }} }}
  </style>
</head>
<body>
<main>
  <header>
    <div>
      <h1>沪深成交额温度</h1>
      <div class="subtle">交易日 {trade_date} · 最新快照 {html.escape(latest['minute'])} · 对比 {html.escape(previous_date)} 同期</div>
    </div>
    <div class="badge">{html.escape(_temperature_label(temperature))}</div>
  </header>
  <div class="cards">{cards_html}</div>
  <section class="panel">
    <h2>以 0 轴为基准的盘中温度</h2>
    {_dashboard_svg(rows)}
  </section>
  <section class="panel">
    <h2>10 分钟明细（单位：亿元）</h2>
    <table>
      <thead><tr><th>时间</th><th>今日累计</th><th>昨日同期</th><th>同期差额</th><th>温度</th><th>较昨日全天</th></tr></thead>
      <tbody>{''.join(table_rows)}</tbody>
    </table>
  </section>
  <section class="panel formula">
    <strong>计算口径：</strong>沪市累计成交额 + 深市累计成交额；温度差额 = 今日累计成交额 − 前一交易日同一时刻累计成交额；
    温度百分比 = 温度差额 ÷ 前一交易日同一时刻累计成交额 × 100%。正值表示同比放量，负值表示同比缩量。<br>
    数据源：腾讯财经公开分时行情（上证指数 sh000001、深证成指 sz399001）。生成时间：{html.escape(generated_at)}。
  </section>
</main>
</body>
</html>
"""


def write_outputs(repository: SnapshotRepository, config: AppConfig, generated_at: str) -> str:
    trade_date = repository.latest_trade_date()
    if not trade_date:
        raise MarketTemperatureError("数据库中尚无行情快照")
    rows = repository.rows_for_date(trade_date)
    document = build_dashboard_document(trade_date, rows, generated_at)
    _atomic_write_text(config.dashboard_path, document)

    payload = {
        "schema_version": 1,
        "formula": "today_cumulative_amount - previous_trading_day_same_time_amount",
        "unit": {"amount": "CNY", "temperature_pct": "percent"},
        "generated_at": generated_at,
        "trade_date": trade_date,
        "latest": rows[-1],
        "series": rows,
    }
    _atomic_write_text(
        config.latest_json_path,
        json.dumps(payload, ensure_ascii=False, indent=2),
    )
    return trade_date


def collect_once(config: AppConfig) -> dict[str, Any]:
    logger = logging.getLogger(LOGGER_NAME)
    timezone = ZoneInfo(config.timezone)
    now = datetime.now(timezone)
    captured_at = now.isoformat(timespec="seconds")
    client = TencentFinanceClient(config)
    repository = SnapshotRepository(config.database_path)

    fallback_used = False
    try:
        snapshots = client.fetch_market_history()
    except DataSourceError as history_error:
        logger.warning("腾讯分时接口不可用，尝试实时报价降级: %s", history_error)
        snapshots = [client.fetch_current_quote()]
        fallback_used = True

    inserted = repository.upsert(snapshots, captured_at)
    repository.recalculate()
    latest_trade_date = write_outputs(repository, config, captured_at)
    latest_rows = repository.rows_for_date(latest_trade_date)
    latest = latest_rows[-1]
    result = {
        "captured_at": captured_at,
        "trade_date": latest_trade_date,
        "minute": latest["minute"],
        "total_amount_yuan": latest["total_amount_yuan"],
        "delta_amount_yuan": latest["delta_amount_yuan"],
        "temperature_pct": latest["temperature_pct"],
        "rows_upserted": inserted,
        "fallback_used": fallback_used,
    }
    logger.info(
        "采集成功: %s %s, 成交额=%s亿元, 温度差=%s亿元, 温度=%s, 写入=%s",
        latest_trade_date,
        latest["minute"],
        _format_yi(latest["total_amount_yuan"]),
        _format_yi(latest["delta_amount_yuan"], signed=True),
        _format_pct(latest["temperature_pct"], signed=True),
        inserted,
    )
    return result


def _sleep_until(target: datetime, logger: logging.Logger) -> None:
    while True:
        remaining = (target - datetime.now(target.tzinfo)).total_seconds()
        if remaining <= 0:
            return
        if remaining > 60:
            logger.debug("距离下次采集 %s 还有 %.0f 秒", target.strftime("%H:%M:%S"), remaining)
        time.sleep(min(remaining, 30))


def run_daemon(config: AppConfig) -> int:
    """当前工作日内按配置时段运行；任务计划程序下一工作日会再次启动。"""
    logger = logging.getLogger(LOGGER_NAME)
    timezone = ZoneInfo(config.timezone)
    now = datetime.now(timezone)
    if now.weekday() >= 5:
        logger.info("今天是周末，跳过采集: %s", now.date())
        return 0

    # 启动后先回填近 5 个交易日；电脑晚启动也不会丢失当天此前的 10 分钟点。
    try:
        collect_once(config)
    except MarketTemperatureError as exc:
        logger.error("启动回填失败，仍将等待盘中下一采集点: %s", exc)

    now = datetime.now(timezone)
    slots = [
        item.replace(second=5, microsecond=0)
        for item in market_slots(now.strftime("%Y-%m-%d"), config.sessions, config.interval_minutes, timezone)
    ]
    pending = [slot for slot in slots if slot > now]
    if not pending:
        logger.info("今日交易时段已经结束")
        return 0

    for slot in pending:
        try:
            _sleep_until(slot, logger)
            collect_once(config)
        except KeyboardInterrupt:
            logger.info("收到中断信号，采集器退出")
            return 130
        except Exception as exc:  # 单个采集点失败不应终止当日后续任务。
            logger.exception("%s 采集失败，将在下一采集点重试: %s", slot.strftime("%H:%M"), exc)
    logger.info("今日采集完成")
    return 0


def export_history(
    repository: SnapshotRepository,
    start_date: str | None,
    end_date: str | None,
    limit: int | None,
    csv_path: Path | None,
) -> int:
    rows = repository.query_rows(start_date, end_date, limit)
    if not rows:
        print("没有符合条件的历史数据。")
        return 0

    headers = [
        "交易日",
        "时间",
        "沪市累计(亿元)",
        "深市累计(亿元)",
        "两市累计(亿元)",
        "前一交易日",
        "前日同期(亿元)",
        "同期差额(亿元)",
        "温度(%)",
        "较前日全天(亿元)",
        "数据源",
    ]
    output_rows = [
        [
            row["trade_date"],
            row["minute"],
            f"{_yi(row['shanghai_amount_yuan']):.4f}",
            f"{_yi(row['shenzhen_amount_yuan']):.4f}",
            f"{_yi(row['total_amount_yuan']):.4f}",
            row["previous_trade_date"] or "",
            "" if row["previous_total_amount_yuan"] is None else f"{_yi(row['previous_total_amount_yuan']):.4f}",
            "" if row["delta_amount_yuan"] is None else f"{_yi(row['delta_amount_yuan']):+.4f}",
            "" if row["temperature_pct"] is None else f"{row['temperature_pct']:+.4f}",
            ""
            if row["delta_vs_previous_close_yuan"] is None
            else f"{_yi(row['delta_vs_previous_close_yuan']):+.4f}",
            row["source"],
        ]
        for row in rows
    ]

    if csv_path:
        csv_path = csv_path.resolve()
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(headers)
            writer.writerows(output_rows)
        print(f"已导出 {len(output_rows)} 条记录: {csv_path}")
        return 0

    widths = [max(len(str(value)) for value in [header, *[row[index] for row in output_rows]]) for index, header in enumerate(headers)]
    print("  ".join(header.ljust(widths[index]) for index, header in enumerate(headers)))
    print("  ".join("-" * width for width in widths))
    for row in output_rows:
        print("  ".join(str(value).ljust(widths[index]) for index, value in enumerate(row)))
    return 0


def configure_logging(config: AppConfig, verbose: bool = False) -> None:
    config.log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")

    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    console.setFormatter(formatter)
    logger.addHandler(console)

    file_handler = RotatingFileHandler(
        config.log_path,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


def _iso_date(value: str) -> str:
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise argparse.ArgumentTypeError("日期必须为 YYYY-MM-DD") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="沪深两市成交额温度采集与历史查询")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH, help="JSON 配置文件路径")
    parser.add_argument("--verbose", action="store_true", help="输出调试日志")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("once", help="立即采集并回填近 5 个交易日")
    subparsers.add_parser("daemon", help="交易时段内每 10 分钟持续采集")
    subparsers.add_parser("render", help="根据数据库重新生成 HTML 和 latest.json")

    history = subparsers.add_parser("history", help="查询或导出历史数据")
    history.add_argument("--date", type=_iso_date, help="只查询指定交易日")
    history.add_argument("--from-date", type=_iso_date, help="起始交易日")
    history.add_argument("--to-date", type=_iso_date, help="结束交易日")
    history.add_argument("--limit", type=int, default=200, help="最多返回记录数，默认 200")
    history.add_argument("--csv", type=Path, help="导出为 UTF-8 BOM CSV，方便 Excel 打开")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = AppConfig.load(args.config)
        configure_logging(config, args.verbose)
        if args.command == "once":
            result = collect_once(config)
            print(
                f"采集完成：{result['trade_date']} {result['minute']}，"
                f"两市成交额 {_format_yi(result['total_amount_yuan'])} 亿元，"
                f"温度 {_format_pct(result['temperature_pct'], signed=True)}"
            )
            print(f"看板：{config.dashboard_path}")
            print(f"数据库：{config.database_path}")
            return 0
        if args.command == "daemon":
            return run_daemon(config)
        if args.command == "render":
            generated_at = datetime.now(ZoneInfo(config.timezone)).isoformat(timespec="seconds")
            trade_date = write_outputs(SnapshotRepository(config.database_path), config, generated_at)
            print(f"已重新生成 {trade_date} 看板：{config.dashboard_path}")
            return 0
        if args.command == "history":
            start_date = args.date or args.from_date
            end_date = args.date or args.to_date
            if start_date and end_date and start_date > end_date:
                parser.error("起始日期不能晚于结束日期")
            if args.limit is not None and args.limit <= 0:
                parser.error("--limit 必须大于 0")
            return export_history(
                SnapshotRepository(config.database_path),
                start_date,
                end_date,
                args.limit,
                args.csv,
            )
    except KeyboardInterrupt:
        logging.getLogger(LOGGER_NAME).info("用户中断")
        return 130
    except Exception as exc:
        logging.getLogger(LOGGER_NAME).exception("执行失败: %s", exc)
        print(f"执行失败：{exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
