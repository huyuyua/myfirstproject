#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A股每日收盘复盘报告。

数据源为腾讯财经公开行情。正常模式仅在北京时间交易日收盘后生成并发送；
--force 可用于人工检查未收盘快照，--dry-run 不写文件也不发送邮件。
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import html
import json
import math
import os
import re
import smtplib
import ssl
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo


TIMEZONE = ZoneInfo("Asia/Shanghai")
TENCENT_STOCK_LIST_URL = (
    "https://proxy.finance.qq.com/cgi/cgi-bin/rank/hs/getBoardRankList"
)
TENCENT_BOARD_RANK_URL = (
    "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/mktHs/rank"
)
TENCENT_DAY_URL = "https://web.ifzq.gtimg.cn/appstock/app/day/query"
TENCENT_QUOTE_URL = "https://qt.gtimg.cn/q={symbols}"
SMTP_HOST = "smtp.163.com"
SMTP_PORT = 465
DEFAULT_DOCS_DIR = Path("docs/close-report")
DEFAULT_STATE_PATH = Path("state/daily_close_report.json")
STOCK_PAGE_SIZE = 100
STOCK_PAGE_OVERLAP = 20
MIN_UNIVERSE_SIZE = 5000
MIN_ACTIVE_SIZE = 4000
MAX_RECIPIENTS = 20
INDEX_SPECS = (
    ("sh000001", "上证指数", 0.20),
    ("sz399001", "深证成指", 0.20),
    ("sz399006", "创业板指", 0.20),
    ("sh000688", "科创50", 0.15),
    ("sh000300", "沪深300", 0.25),
)


class CloseReportError(RuntimeError):
    """报告生成失败。"""


class NonTradingDay(CloseReportError):
    """当天不是已完成交易的A股交易日。"""


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(content, encoding="utf-8", newline="\n")
    temporary.replace(path)


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return copy.deepcopy(default)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CloseReportError(f"JSON文件读取失败: {path}: {exc}") from exc


def normalize_symbol(raw: str) -> str:
    value = raw.strip().lower()
    if re.fullmatch(r"(?:sh|sz|bj)\d{6}", value):
        return value
    if not re.fullmatch(r"\d{6}", value):
        raise CloseReportError(f"股票代码格式无效: {raw}")
    # 北交所新代码段 92xxxx 必须先于上海 B 股 9xxxxx 判断。
    if value.startswith(("4", "8", "92")):
        return f"bj{value}"
    if value.startswith(("5", "6", "9")):
        return f"sh{value}"
    if value.startswith(("0", "1", "2", "3")):
        return f"sz{value}"
    raise CloseReportError(f"无法识别股票交易所: {raw}")


def parse_watchlist(raw: str) -> tuple[str, ...]:
    if not raw.strip():
        return ()
    result: list[str] = []
    seen: set[str] = set()
    for item in re.split(r"[,;\s]+", raw):
        if not item:
            continue
        symbol = normalize_symbol(item)
        if symbol not in seen:
            seen.add(symbol)
            result.append(symbol)
    return tuple(result)


def _validate_email(value: str) -> str:
    cleaned = value.strip()
    if (
        not cleaned
        or len(cleaned) > 254
        or "\r" in cleaned
        or "\n" in cleaned
        or not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", cleaned)
    ):
        raise CloseReportError(f"邮箱地址无效: {value!r}")
    return cleaned


def recipient_delivery_key(address: str) -> str:
    """生成公开去重状态使用的不可读收件人标识。"""
    normalized = _validate_email(address).casefold()
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def parse_recipients(raw: str, username: str) -> tuple[str, ...]:
    sender = _validate_email(username)
    source = raw.strip() or "SELF"
    recipients: list[str] = []
    seen: set[str] = set()
    for token in re.split(r"[,;\n]+", source):
        value = token.strip()
        if not value:
            continue
        address = sender if value.upper() == "SELF" else _validate_email(value)
        key = address.casefold()
        if key not in seen:
            seen.add(key)
            recipients.append(address)
    if not recipients:
        raise CloseReportError("DAILY_REPORT_RECIPIENTS 没有有效收件人")
    if len(recipients) > MAX_RECIPIENTS:
        raise CloseReportError(f"收件人不能超过 {MAX_RECIPIENTS} 个")
    return tuple(recipients)


@dataclass(frozen=True)
class MailConfig:
    username: str
    password: str
    recipients: tuple[str, ...]

    @classmethod
    def from_env(cls) -> "MailConfig":
        username = _validate_email(os.environ.get("SMTP_USERNAME", ""))
        password = os.environ.get("SMTP_APP_PASSWORD", "").strip()
        if not password:
            raise CloseReportError("缺少 SMTP_APP_PASSWORD（163客户端授权码）")
        recipients = parse_recipients(
            os.environ.get("DAILY_REPORT_RECIPIENTS", "SELF"), username
        )
        return cls(username=username, password=password, recipients=recipients)


@dataclass(frozen=True)
class MinutePoint:
    minute: str
    price: float
    activity: float
    amount_yuan: float | None


@dataclass(frozen=True)
class DaySeries:
    trade_date: str
    previous_close: float
    points: Mapping[str, MinutePoint]

    def at_or_before(self, minute: str) -> MinutePoint:
        candidates = [key for key in self.points if key <= minute]
        if not candidates:
            raise CloseReportError(f"{self.trade_date} 缺少 {minute} 前的分钟数据")
        return self.points[max(candidates)]


class TencentClient:
    """腾讯财经公开接口客户端，带指数退避重试与并发拉取。"""

    def __init__(self, timeout: float = 15.0, retries: int = 3, workers: int = 12):
        self.timeout = timeout
        self.retries = retries
        self.workers = workers
        self._lock = threading.Lock()

    @staticmethod
    def _url(base: str, params: Mapping[str, Any]) -> str:
        return f"{base}?{urlencode(params)}"

    def _request_bytes(self, url: str, encoding_hint: str = "json") -> bytes:
        last_error: Exception | None = None
        accept = "text/plain,*/*" if encoding_hint == "quote" else "application/json,*/*"
        for attempt in range(1, self.retries + 1):
            request = Request(
                url,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "Chrome/136.0 Safari/537.36"
                    ),
                    "Accept": accept,
                    "Accept-Language": "zh-CN,zh;q=0.9",
                    "Referer": "https://stockapp.finance.qq.com/mstats/",
                },
            )
            try:
                with urlopen(request, timeout=self.timeout) as response:
                    return response.read()
            except (HTTPError, URLError, TimeoutError, OSError) as exc:
                last_error = exc
                if attempt < self.retries:
                    time.sleep(min(1.2 * (2 ** (attempt - 1)), 5.0))
        raise CloseReportError(
            f"腾讯接口请求失败（重试 {self.retries} 次）: {url}: {last_error}"
        )

    def _request_json(self, url: str) -> Any:
        raw = self._request_bytes(url)
        try:
            return json.loads(raw.decode("utf-8-sig", errors="strict"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CloseReportError(f"腾讯接口JSON解析失败: {url}: {exc}") from exc

    @staticmethod
    def _stock_from_row(row: Mapping[str, Any]) -> dict[str, Any] | None:
        symbol = str(row.get("code") or "").lower()
        if not re.fullmatch(r"(?:sh|sz|bj)\d{6}", symbol):
            return None
        turnover_wan = as_float(row.get("turnover"))
        main_in = as_float(row.get("zllr"))
        main_out = as_float(row.get("zllc"))
        main_net_raw = as_float(row.get("zljlr"))
        main_flow_available = not (
            turnover_wan > 0 and main_in == 0 and main_out == 0 and main_net_raw == 0
        )
        return {
            "symbol": symbol,
            "code": symbol[2:],
            "name": str(row.get("name") or symbol),
            "price": as_float(row.get("zxj")),
            "change_pct": as_float(row.get("zdf")),
            "turnover_wan": turnover_wan,
            "turnover_yuan": int(round(turnover_wan * 10_000)),
            "volume_lots": as_float(row.get("volume")),
            "turnover_rate_pct": as_float(row.get("hsl")),
            "main_inflow_wan": main_in if main_flow_available else None,
            "main_outflow_wan": main_out if main_flow_available else None,
            "main_net_wan": main_net_raw if main_flow_available else None,
            "stock_type": str(row.get("stock_type") or ""),
        }

    def _stock_page(
        self, offset: int, sort_type: str, direct: str
    ) -> tuple[int, list[dict[str, Any]]]:
        url = self._url(
            TENCENT_STOCK_LIST_URL,
            {
                "_appver": "11.17.0",
                "board_code": "aStock",
                "sort_type": sort_type,
                "direct": direct,
                "offset": offset,
                "count": STOCK_PAGE_SIZE,
            },
        )
        payload = self._request_json(url)
        if payload.get("code") != 0:
            raise CloseReportError(f"腾讯A股排行接口失败: {payload.get('msg') or payload}")
        data = payload.get("data") or {}
        rows: list[dict[str, Any]] = []
        for raw in data.get("rank_list") or []:
            parsed = self._stock_from_row(raw)
            if parsed:
                rows.append(parsed)
        return int(data.get("total") or 0), rows

    def _universe_sorted(self, sort_type: str, direct: str) -> tuple[int, list[dict[str, Any]]]:
        total, first = self._stock_page(0, sort_type, direct)
        if total < MIN_UNIVERSE_SIZE or not first:
            raise CloseReportError(f"腾讯A股总数异常: {total}")
        step = STOCK_PAGE_SIZE - STOCK_PAGE_OVERLAP
        offsets = list(range(step, total, step))
        pages: dict[int, list[dict[str, Any]]] = {0: first}
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {
                executor.submit(self._stock_page, offset, sort_type, direct): offset
                for offset in offsets
            }
            for future in as_completed(futures):
                pages[futures[future]] = future.result()[1]
        return total, [item for offset in sorted(pages) for item in pages[offset]]

    def fetch_a_share_market(self) -> list[dict[str, Any]]:
        deduplicated: dict[str, dict[str, Any]] = {}
        expected = 0
        for sort_type, direct in (
            ("price", "up"),
            ("exchange", "up"),
            ("priceRatio", "down"),
        ):
            total, rows = self._universe_sorted(sort_type, direct)
            expected = max(expected, total)
            for row in rows:
                deduplicated[row["symbol"]] = row
            if len(deduplicated) >= expected:
                break
        if expected < MIN_UNIVERSE_SIZE or len(deduplicated) < expected:
            raise CloseReportError(
                f"腾讯A股列表不完整: 预期 {expected}，实际 {len(deduplicated)}"
            )
        return sorted(deduplicated.values(), key=lambda item: item["symbol"])

    @staticmethod
    def _parse_day_payload(payload: Mapping[str, Any], symbol: str) -> list[DaySeries]:
        try:
            raw_days = payload["data"][symbol]["data"]
        except (KeyError, TypeError) as exc:
            raise CloseReportError(f"腾讯分钟数据结构异常: {symbol}") from exc
        result: list[DaySeries] = []
        for raw_day in raw_days:
            raw_date = str(raw_day.get("date") or "")
            if not re.fullmatch(r"\d{8}", raw_date):
                continue
            trade_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
            points: dict[str, MinutePoint] = {}
            for raw_line in raw_day.get("data") or []:
                parts = str(raw_line).split()
                if len(parts) < 3 or not re.fullmatch(r"\d{4}", parts[0]):
                    continue
                minute = f"{parts[0][:2]}:{parts[0][2:]}"
                price_value = as_float(parts[1], math.nan)
                activity = as_float(parts[2], math.nan)
                amount = as_float(parts[3], math.nan) if len(parts) >= 4 else math.nan
                if not math.isfinite(price_value) or not math.isfinite(activity):
                    continue
                points[minute] = MinutePoint(
                    minute=minute,
                    price=price_value,
                    activity=activity,
                    amount_yuan=amount if math.isfinite(amount) else None,
                )
            previous_close = as_float(raw_day.get("prec"), math.nan)
            if points and math.isfinite(previous_close) and previous_close > 0:
                result.append(DaySeries(trade_date, previous_close, points))
        if not result:
            raise CloseReportError(f"腾讯分钟数据为空: {symbol}")
        return sorted(result, key=lambda item: item.trade_date, reverse=True)

    def fetch_day_series(self, symbol: str) -> list[DaySeries]:
        url = self._url(TENCENT_DAY_URL, {"code": symbol})
        return self._parse_day_payload(self._request_json(url), symbol)

    def fetch_many_day_series(
        self, symbols: Iterable[str], strict: bool = True
    ) -> dict[str, list[DaySeries]]:
        unique = tuple(dict.fromkeys(symbols))
        result: dict[str, list[DaySeries]] = {}
        errors: dict[str, Exception] = {}
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {executor.submit(self.fetch_day_series, symbol): symbol for symbol in unique}
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    result[symbol] = future.result()
                except Exception as exc:
                    errors[symbol] = exc
        if errors and strict:
            details = "; ".join(f"{key}: {value}" for key, value in list(errors.items())[:5])
            raise CloseReportError(f"分钟行情未完整返回（{len(errors)}只）: {details}")
        return result

    @staticmethod
    def _parse_quote_text(text: str) -> dict[str, dict[str, Any]]:
        quotes: dict[str, dict[str, Any]] = {}
        for match in re.finditer(r'v_((?:sh|sz|bj)\d{6})="([^"]*)"', text):
            symbol, body = match.groups()
            fields = body.split("~")
            if len(fields) < 49:
                continue
            compound = fields[35].split("/") if len(fields) > 35 else []
            amount_yuan = as_float(compound[2]) if len(compound) >= 3 else as_float(fields[37]) * 10_000
            timestamp: str | None = None
            if re.fullmatch(r"\d{14}", fields[30]):
                timestamp = datetime.strptime(fields[30], "%Y%m%d%H%M%S").replace(
                    tzinfo=TIMEZONE
                ).isoformat(timespec="seconds")
            quotes[symbol] = {
                "symbol": symbol,
                "name": fields[1],
                "price": as_float(fields[3]),
                "previous_close": as_float(fields[4]),
                "open": as_float(fields[5]),
                "timestamp": timestamp,
                "change_pct": as_float(fields[32]),
                "high": as_float(fields[33]),
                "low": as_float(fields[34]),
                "amount_yuan": int(round(amount_yuan)),
                "limit_up": as_float(fields[47], -1),
                "limit_down": as_float(fields[48], -1),
            }
        return quotes

    def _quote_chunk(self, symbols: Sequence[str]) -> dict[str, dict[str, Any]]:
        url = TENCENT_QUOTE_URL.format(symbols=quote(",".join(symbols), safe=","))
        raw = self._request_bytes(url, encoding_hint="quote")
        return self._parse_quote_text(raw.decode("gb18030", errors="replace"))

    def fetch_quotes(self, symbols: Iterable[str], chunk_size: int = 60) -> dict[str, dict[str, Any]]:
        unique = tuple(dict.fromkeys(symbols))
        chunks = [unique[index : index + chunk_size] for index in range(0, len(unique), chunk_size)]
        result: dict[str, dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=min(self.workers, 8)) as executor:
            futures = {executor.submit(self._quote_chunk, chunk): chunk for chunk in chunks}
            for future in as_completed(futures):
                result.update(future.result())
        missing = set(unique) - set(result)
        if missing:
            raise CloseReportError(f"腾讯实时报价缺少 {len(missing)} 个代码")
        return result

    def fetch_top_sectors(self, count: int = 5) -> list[dict[str, Any]]:
        url = self._url(
            TENCENT_BOARD_RANK_URL,
            {"l": count, "p": 1, "t": "01/averatio", "ordertype": "", "o": 0},
        )
        payload = self._request_json(url)
        if payload.get("code") != 0:
            raise CloseReportError(f"腾讯行业板块排行失败: {payload.get('msg') or payload}")
        result: list[dict[str, Any]] = []
        for rank, row in enumerate(payload.get("data") or [], 1):
            board_symbol = str(row.get("bd_code") or "")
            leader_symbol = str(row.get("nzg_code") or "").lower()
            if not board_symbol or not re.fullmatch(r"(?:sh|sz|bj)\d{6}", leader_symbol):
                continue
            result.append(
                {
                    "rank": rank,
                    "name": str(row.get("bd_name") or board_symbol),
                    "symbol": board_symbol,
                    "change_pct": as_float(row.get("bd_zdf")),
                    "change_5d_pct": as_float(row.get("bd_zdf5")),
                    "change_20d_pct": as_float(row.get("bd_zdf20")),
                    "leader_symbol": leader_symbol,
                    "leader_code": leader_symbol[2:],
                    "leader_name": str(row.get("nzg_name") or leader_symbol),
                    "leader_change_pct": as_float(row.get("nzg_zdf")),
                }
            )
        if len(result) < count:
            raise CloseReportError(f"腾讯行业板块排行不足 {count} 条")
        return result[:count]


def classify_index(day_change_pct: float, tail_change_pct: float) -> tuple[str, str]:
    strength = "强" if day_change_pct >= 0.5 else "弱" if day_change_pct <= -0.5 else "中性"
    if tail_change_pct >= 0.3:
        tail = "尾盘拉升"
    elif tail_change_pct <= -0.3:
        tail = "尾盘下挫"
    else:
        tail = "尾盘平稳"
    return strength, tail


def classify_turnover(change_pct: float) -> str:
    if change_pct > 5:
        return "放量"
    if change_pct < -5:
        return "缩量"
    return "基本持平"


def _near(value: float, target: float) -> bool:
    return target > 0 and abs(value - target) <= max(0.011, abs(target) * 0.0002)


def limit_status(quote_row: Mapping[str, Any]) -> dict[str, bool]:
    price = as_float(quote_row.get("price"))
    high = as_float(quote_row.get("high"))
    low = as_float(quote_row.get("low"))
    limit_up = as_float(quote_row.get("limit_up"), -1)
    limit_down = as_float(quote_row.get("limit_down"), -1)
    has_limit = limit_up > 0 and limit_down > 0
    touched_up = has_limit and (high > limit_up or _near(high, limit_up))
    touched_down = has_limit and (low < limit_down or _near(low, limit_down))
    sealed_up = touched_up and _near(price, limit_up)
    sealed_down = touched_down and _near(price, limit_down)
    return {
        "has_limit": has_limit,
        "touched_up": touched_up,
        "sealed_up": sealed_up,
        "failed_up": touched_up and not sealed_up,
        "touched_down": touched_down,
        "sealed_down": sealed_down,
    }


def classify_tail_flow(
    market_at_1430_pct: float,
    weighted_tail_pct: float,
    tail_intensity: float,
    positive_sector_count: int,
    top20_positive_ratio: float,
) -> str:
    if market_at_1430_pct <= 0 and weighted_tail_pct >= 0.25 and positive_sector_count >= 3:
        return "回流"
    if (
        market_at_1430_pct > 0
        and weighted_tail_pct >= 0.25
        and tail_intensity >= 1.10
        and top20_positive_ratio >= 0.60
    ):
        return "抢筹"
    if weighted_tail_pct <= -0.25 and (
        tail_intensity >= 1.10 or top20_positive_ratio <= 0.40
    ):
        return "兑现"
    sector_signal = positive_sector_count - 2.5
    stock_signal = top20_positive_ratio - 0.5
    if abs(weighted_tail_pct) >= 0.10 and (
        weighted_tail_pct * sector_signal < 0 or weighted_tail_pct * stock_signal < 0
    ):
        return "分化"
    return "平稳"


def compute_market_state(
    weighted_index_pct: float,
    weighted_tail_pct: float,
    up_count: int,
    down_count: int,
    limit_up_count: int,
    limit_down_count: int,
    turnover_change_pct: float,
    sector_concentrated: bool = False,
) -> tuple[float, str, dict[str, float]]:
    breadth_denominator = max(1, up_count + down_count)
    components = {
        "index": clamp(weighted_index_pct / 1.5, -1, 1) * 30,
        "breadth": (up_count - down_count) / breadth_denominator * 25,
        "limit": (limit_up_count - limit_down_count)
        / max(10, limit_up_count + limit_down_count + 10)
        * 20,
        "turnover": clamp(turnover_change_pct / 15, -1, 1) * 15,
        "tail": clamp(weighted_tail_pct / 0.5, -1, 1) * 10,
    }
    score = round(sum(components.values()), 1)
    if score >= 50:
        state = "强势进攻"
    elif score >= 20:
        state = "震荡偏强"
    elif score <= -50:
        state = "弱势退潮"
    elif score <= -20:
        state = "震荡偏弱"
    else:
        breadth_value = components["breadth"]
        conflict = weighted_index_pct * breadth_value < 0 or sector_concentrated
        state = "结构分化" if conflict else "平衡震荡"
    return score, state, {key: round(value, 1) for key, value in components.items()}


def _series_for_date(series: Sequence[DaySeries], trade_date: str) -> DaySeries | None:
    return next((item for item in series if item.trade_date == trade_date), None)


def _last_point(series: DaySeries) -> MinutePoint:
    if not series.points:
        raise CloseReportError(f"{series.trade_date} 分钟行情为空")
    return series.points[max(series.points)]


def _pct(current: float, base: float) -> float:
    return 0.0 if base == 0 else (current / base - 1) * 100


def select_trade_date(
    index_histories: Mapping[str, Sequence[DaySeries]],
    now: datetime,
    force: bool,
) -> tuple[str, bool]:
    common_dates: set[str] | None = None
    for symbol, _, _ in INDEX_SPECS:
        dates = {item.trade_date for item in index_histories.get(symbol, ())}
        common_dates = dates if common_dates is None else common_dates.intersection(dates)
    if not common_dates:
        raise CloseReportError("五个指数没有共同交易日")
    trade_date = max(common_dates)
    complete = all(
        "15:00" in (_series_for_date(index_histories[symbol], trade_date) or DaySeries("", 1, {})).points
        for symbol, _, _ in INDEX_SPECS
    )
    today = now.date().isoformat()
    if not force:
        if now.weekday() >= 5 or trade_date != today or not complete:
            raise NonTradingDay(
                f"跳过：北京时间 {today} 没有完整收盘行情（最近行情 {trade_date}）"
            )
    return trade_date, complete


def build_index_and_turnover_metrics(
    histories: Mapping[str, Sequence[DaySeries]], trade_date: str
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    indices: list[dict[str, Any]] = []
    for symbol, name, weight in INDEX_SPECS:
        series = _series_for_date(histories[symbol], trade_date)
        if series is None:
            raise CloseReportError(f"{name} 缺少 {trade_date} 数据")
        end = series.points.get("15:00") or _last_point(series)
        point_1430 = series.at_or_before("14:30")
        day_change = _pct(end.price, series.previous_close)
        at_1430_change = _pct(point_1430.price, series.previous_close)
        tail_change = _pct(end.price, point_1430.price)
        strength, tail_label = classify_index(day_change, tail_change)
        indices.append(
            {
                "symbol": symbol,
                "name": name,
                "weight": weight,
                "previous_close": series.previous_close,
                "close": end.price,
                "end_minute": end.minute,
                "change_pct": round(day_change, 3),
                "at_1430_pct": round(at_1430_change, 3),
                "tail_change_pct": round(tail_change, 3),
                "strength": strength,
                "tail_label": tail_label,
            }
        )

    market_symbols = ("sh000001", "sz399001")
    current_amount = 0.0
    current_1430_amount = 0.0
    for symbol in market_symbols:
        series = _series_for_date(histories[symbol], trade_date)
        assert series is not None
        end = series.points.get("15:00") or _last_point(series)
        at_1430 = series.at_or_before("14:30")
        if end.amount_yuan is None or at_1430.amount_yuan is None:
            raise CloseReportError(f"{symbol} 缺少成交额分钟字段")
        current_amount += end.amount_yuan
        current_1430_amount += at_1430.amount_yuan

    older_common = set(item.trade_date for item in histories[market_symbols[0]])
    older_common.intersection_update(item.trade_date for item in histories[market_symbols[1]])
    older_dates = sorted((item for item in older_common if item < trade_date), reverse=True)
    if not older_dates:
        raise CloseReportError("缺少前一交易日成交额")
    previous_date = older_dates[0]
    previous_amount = 0.0
    tail_shares: list[float] = []
    for older_date in older_dates[:4]:
        day_total = 0.0
        day_1430 = 0.0
        valid = True
        for symbol in market_symbols:
            series = _series_for_date(histories[symbol], older_date)
            if series is None:
                valid = False
                break
            end = series.points.get("15:00") or _last_point(series)
            at_1430 = series.at_or_before("14:30")
            if end.amount_yuan is None or at_1430.amount_yuan is None:
                valid = False
                break
            day_total += end.amount_yuan
            day_1430 += at_1430.amount_yuan
        if not valid or day_total <= 0:
            continue
        if older_date == previous_date:
            previous_amount = day_total
        tail_shares.append((day_total - day_1430) / day_total)
    if previous_amount <= 0:
        raise CloseReportError("前一交易日成交额无效")
    change_pct = _pct(current_amount, previous_amount)
    current_tail_share = (
        max(0.0, current_amount - current_1430_amount) / current_amount
        if current_amount > 0
        else 0.0
    )
    median_tail_share = statistics.median(tail_shares) if tail_shares else current_tail_share
    tail_intensity = current_tail_share / median_tail_share if median_tail_share > 0 else 1.0
    turnover = {
        "today_amount_yuan": int(round(current_amount)),
        "previous_trade_date": previous_date,
        "previous_amount_yuan": int(round(previous_amount)),
        "change_yuan": int(round(current_amount - previous_amount)),
        "change_pct": round(change_pct, 3),
        "label": classify_turnover(change_pct),
        "tail_share_pct": round(current_tail_share * 100, 3),
        "previous_4d_tail_share_median_pct": round(median_tail_share * 100, 3),
        "tail_intensity": round(tail_intensity, 3),
    }
    return indices, turnover


def build_breadth(stocks: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    active = [item for item in stocks if as_float(item.get("price")) > 0]
    if len(active) < MIN_ACTIVE_SIZE:
        raise CloseReportError(f"有效交易股票不足: {len(active)}")
    changes = [as_float(item.get("change_pct")) for item in active]
    up_count = sum(1 for value in changes if value > 0.005)
    down_count = sum(1 for value in changes if value < -0.005)
    flat_count = len(changes) - up_count - down_count
    up_2 = sum(1 for value in changes if value >= 2)
    down_2 = sum(1 for value in changes if value <= -2)
    median_change = statistics.median(changes)
    ratio = up_count / max(1, down_count)
    if ratio >= 1.5 and median_change > 0:
        label = "强赚钱效应"
    elif ratio >= 1.1 and median_change >= 0:
        label = "赚钱效应偏强"
    elif ratio <= 0.67 and median_change < 0:
        label = "亏钱效应明显"
    elif ratio <= 0.9 and median_change <= 0:
        label = "赚钱效应偏弱"
    else:
        label = "赚钱效应均衡"
    exchanges = {
        prefix: sum(1 for item in active if str(item["symbol"]).startswith(prefix))
        for prefix in ("sh", "sz", "bj")
    }
    return {
        "active_count": len(active),
        "up_count": up_count,
        "down_count": down_count,
        "flat_count": flat_count,
        "up_2pct_count": up_2,
        "down_2pct_count": down_2,
        "advance_decline_ratio": round(ratio, 3),
        "median_change_pct": round(median_change, 3),
        "label": label,
        "exchange_counts": exchanges,
    }


def build_sentiment(
    stocks: Sequence[Mapping[str, Any]], quotes: Mapping[str, Mapping[str, Any]]
) -> tuple[dict[str, Any], dict[str, dict[str, bool]]]:
    statuses: dict[str, dict[str, bool]] = {}
    for item in stocks:
        symbol = str(item["symbol"])
        quote_row = quotes.get(symbol)
        if quote_row is None:
            continue
        statuses[symbol] = limit_status(quote_row)
    limit_up = sum(1 for item in statuses.values() if item["sealed_up"])
    limit_down = sum(1 for item in statuses.values() if item["sealed_down"])
    touched_up = sum(1 for item in statuses.values() if item["touched_up"])
    failed_up = sum(1 for item in statuses.values() if item["failed_up"])
    seal_rate = limit_up / max(1, touched_up) * 100
    ratio = limit_up / max(1, limit_down)
    if limit_down >= 20 or limit_down >= limit_up:
        label = "情绪冰点"
    elif limit_up >= 40 and ratio >= 5 and seal_rate >= 65:
        label = "情绪高涨"
    elif ratio >= 2 and seal_rate >= 55:
        label = "情绪偏强"
    else:
        label = "情绪分歧"
    return (
        {
            "limit_up_count": limit_up,
            "limit_down_count": limit_down,
            "touched_up_count": touched_up,
            "failed_up_count": failed_up,
            "seal_rate_pct": round(seal_rate, 2),
            "limit_ratio": round(ratio, 2),
            "label": label,
        },
        statuses,
    )


def _stock_tail_metrics(
    stock: Mapping[str, Any],
    quote_row: Mapping[str, Any] | None,
    history: Sequence[DaySeries] | None,
    trade_date: str,
) -> dict[str, Any]:
    result = dict(stock)
    tail_pct: float | None = None
    series = _series_for_date(history or (), trade_date)
    if series is not None:
        end = series.points.get("15:00") or _last_point(series)
        point_1430 = series.at_or_before("14:30")
        tail_pct = _pct(end.price, point_1430.price)
    result["tail_change_pct"] = None if tail_pct is None else round(tail_pct, 3)
    turnover_wan = as_float(stock.get("turnover_wan"))
    main_net = stock.get("main_net_wan")
    result["main_net_ratio_pct"] = (
        None
        if main_net is None or turnover_wan <= 0
        else round(as_float(main_net) / turnover_wan * 100, 3)
    )
    if quote_row:
        result.update(
            {
                "previous_close": as_float(quote_row.get("previous_close")),
                "open": as_float(quote_row.get("open")),
                "high": as_float(quote_row.get("high")),
                "low": as_float(quote_row.get("low")),
                "limit_up": as_float(quote_row.get("limit_up"), -1),
                "limit_down": as_float(quote_row.get("limit_down"), -1),
            }
        )
    return result


def classify_leader_state(
    stock: Mapping[str, Any], status: Mapping[str, bool] | None
) -> str:
    if status and status.get("sealed_up"):
        return "加强"
    if status and status.get("failed_up"):
        return "走弱"
    tail = stock.get("tail_change_pct")
    tail_value = as_float(tail) if tail is not None else 0.0
    high = as_float(stock.get("high"))
    low = as_float(stock.get("low"))
    price = as_float(stock.get("price"))
    close_position = (price - low) / (high - low) if high > low else 0.5
    main_net = stock.get("main_net_wan")
    main_value = None if main_net is None else as_float(main_net)
    if tail_value >= 1 and close_position >= 0.70 and (main_value is None or main_value > 0):
        return "加强"
    if tail_value <= -1 or (close_position <= 0.35 and main_value is not None and main_value < 0):
        return "走弱"
    return "分歧"


def score_leader(
    stock: Mapping[str, Any],
    liquidity_rank: int,
    universe_size: int,
    sector_score: float,
) -> tuple[float, dict[str, float]]:
    if universe_size <= 1:
        liquidity = 25.0
    else:
        liquidity = 25 * (1 - (liquidity_rank - 1) / (universe_size - 1))
    previous_close = as_float(stock.get("previous_close"))
    limit_up = as_float(stock.get("limit_up"), -1)
    limit_band = _pct(limit_up, previous_close) if limit_up > 0 and previous_close > 0 else 10
    day_strength = clamp(as_float(stock.get("change_pct")) / max(1, limit_band), 0, 1) * 20
    main_ratio = stock.get("main_net_ratio_pct")
    main_flow = 0.0 if main_ratio is None else clamp(as_float(main_ratio) / 15, 0, 1) * 20
    tail_raw = stock.get("tail_change_pct")
    tail_value = 0.0 if tail_raw is None else as_float(tail_raw)
    tail_score = clamp((tail_value + 1) / 2, 0, 1) * 15
    components = {
        "liquidity": round(liquidity, 1),
        "day_strength": round(day_strength, 1),
        "main_flow": round(main_flow, 1),
        "sector": round(clamp(sector_score, 0, 20), 1),
        "tail": round(tail_score, 1),
    }
    return round(sum(components.values()), 1), components


def enrich_sectors(
    sectors: Sequence[Mapping[str, Any]],
    board_histories: Mapping[str, Sequence[DaySeries]],
    trade_date: str,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in sectors:
        item = dict(row)
        series = _series_for_date(board_histories.get(str(row["symbol"]), ()), trade_date)
        if series is None:
            item["tail_change_pct"] = None
        else:
            end = series.points.get("15:00") or _last_point(series)
            point_1430 = series.at_or_before("14:30")
            item["tail_change_pct"] = round(_pct(end.price, point_1430.price), 3)
        result.append(item)
    return result


def _quote_is_closed(quote_row: Mapping[str, Any], trade_date: str) -> bool:
    raw = quote_row.get("timestamp")
    if not raw:
        return False
    try:
        timestamp = datetime.fromisoformat(str(raw)).astimezone(TIMEZONE)
    except ValueError:
        return False
    return timestamp.date().isoformat() == trade_date and timestamp.strftime("%H:%M") >= "15:00"


def _main_flow_top(stocks: Sequence[Mapping[str, Any]], limit: int = 20) -> list[dict[str, Any]]:
    candidates = [item for item in stocks if item.get("main_net_wan") is not None]
    return sorted(candidates, key=lambda item: as_float(item.get("main_net_wan")), reverse=True)[
        :limit
    ]


def generate_outlook(report: Mapping[str, Any]) -> tuple[list[str], list[str]]:
    opportunities: list[str] = []
    risks: list[str] = []
    for sector in report["sectors"]:
        tail = sector.get("tail_change_pct")
        tail_value = as_float(tail) if tail is not None else -99
        if (
            as_float(sector["change_pct"]) >= 1.5
            and tail_value >= 0
            and sector.get("leader_state") == "加强"
            and as_float(sector["change_20d_pct"]) <= 25
        ):
            opportunities.append(
                f"方向延续：{sector['name']}当日上涨{sector['change_pct']:+.2f}%、"
                f"尾盘{tail_value:+.2f}%，龙头{sector['leader_name']}状态加强。"
            )
        elif (
            0 < as_float(sector["change_5d_pct"]) <= 5
            and tail_value >= 0.2
            and as_float(sector["change_pct"]) > 0
        ):
            opportunities.append(
                f"低位回流：{sector['name']}近5日仅{sector['change_5d_pct']:+.2f}%、"
                f"今日{sector['change_pct']:+.2f}%且尾盘继续回升{tail_value:+.2f}%。"
            )
        if (
            as_float(sector["change_20d_pct"]) >= 25
            and (tail_value < 0 or sector.get("leader_state") == "走弱")
        ):
            risks.append(
                f"高位兑现风险：{sector['name']}近20日上涨{sector['change_20d_pct']:+.2f}%、"
                f"尾盘{tail_value:+.2f}%，龙头状态{sector.get('leader_state', '未知')}。"
            )

    breadth = report["breadth"]
    turnover = report["turnover"]
    sentiment = report["sentiment"]
    tail_flow = report["tail_flow"]["label"]
    if tail_flow == "回流" and as_float(breadth["advance_decline_ratio"]) >= 1:
        opportunities.append(
            f"尾盘回流机会：上涨/下跌家数比为{breadth['advance_decline_ratio']:.2f}，"
            "尾盘由弱转强，可关注次日量能能否延续。"
        )
    if report["market"]["state"] in ("强势进攻", "震荡偏强") and sentiment[
        "label"
    ] in ("情绪高涨", "情绪偏强"):
        opportunities.append(
            f"情绪延续：市场状态为{report['market']['state']}，涨停{sentiment['limit_up_count']}家、"
            f"封板率{sentiment['seal_rate_pct']:.1f}%。"
        )

    if (
        report["market"]["weighted_index_pct"] > 0
        and (
            as_float(turnover["change_pct"]) < -5
            or as_float(breadth["advance_decline_ratio"]) < 0.8
        )
    ):
        risks.append(
            f"反弹质量风险：指数整体上涨，但成交额{turnover['change_pct']:+.2f}%、"
            f"上涨/下跌家数比仅{breadth['advance_decline_ratio']:.2f}。"
        )
    if sentiment["limit_down_count"] >= 10 and sentiment["limit_ratio"] < 1.5:
        risks.append(
            f"情绪下行风险：涨停{sentiment['limit_up_count']}家、跌停"
            f"{sentiment['limit_down_count']}家，涨跌停比{sentiment['limit_ratio']:.2f}。"
        )
    if tail_flow == "兑现":
        risks.append(
            f"尾盘兑现风险：市场尾盘{report['tail_flow']['weighted_tail_pct']:+.2f}%、"
            f"尾盘成交强度为近4日中位数的{report['tail_flow']['tail_intensity']:.2f}倍。"
        )
    if report["market"]["sector_concentrated"]:
        risks.append(
            f"结构集中风险：Top5板块平均上涨{report['market']['top5_sector_average_pct']:+.2f}%，"
            f"但全市场涨跌幅中位数为{breadth['median_change_pct']:+.2f}%。"
        )

    # 即便没有命中高置信度规则，也要用当天最强方向和最主要的量价短板
    # 回答“明天看什么”，且只陈述可验证依据，不生成买卖建议。
    if not opportunities and report["sectors"]:
        strongest = report["sectors"][0]
        opportunities.append(
            f"相对强势观察：{strongest['name']}位列板块第一，今日"
            f"{as_float(strongest['change_pct']):+.2f}%、近5日"
            f"{as_float(strongest['change_5d_pct']):+.2f}%，龙头"
            f"{strongest['leader_name']}状态{strongest.get('leader_state', '未知')}；"
            "次日需观察板块强度与龙头是否同步延续。"
        )
    if not risks:
        index_pct = as_float(report["market"]["weighted_index_pct"])
        turnover_pct = as_float(turnover["change_pct"])
        breadth_ratio = as_float(breadth["advance_decline_ratio"])
        if index_pct <= -0.5:
            risks.append(
                f"指数承压风险：五指数加权涨跌为{index_pct:+.2f}%，"
                f"成交额较昨日{turnover_pct:+.2f}%，弱势若伴随量能不足可能延续。"
            )
        elif turnover_pct < -5:
            risks.append(
                f"量能不足风险：沪深成交额较昨日{turnover_pct:+.2f}%（{turnover['label']}），"
                f"上涨/下跌家数比为{breadth_ratio:.2f}，次日需要量能确认。"
            )
        elif breadth_ratio < 1:
            risks.append(
                f"赚钱效应风险：上涨/下跌家数比仅{breadth_ratio:.2f}、"
                f"涨跌幅中位数{as_float(breadth['median_change_pct']):+.2f}%，指数表现未必代表多数股票。"
            )
        else:
            risks.append(
                f"强势分化风险：涨停{sentiment['limit_up_count']}家、封板率"
                f"{as_float(sentiment['seal_rate_pct']):.1f}%，Top5板块平均"
                f"{as_float(report['market']['top5_sector_average_pct']):+.2f}%；"
                "次日需关注量能与龙头反馈是否匹配。"
            )

    def unique(items: Sequence[str]) -> list[str]:
        result: list[str] = []
        for item in items:
            if item not in result:
                result.append(item)
        return result[:3]

    return unique(opportunities), unique(risks)


def build_report(
    client: TencentClient,
    now: datetime,
    force: bool = False,
    watchlist: Sequence[str] = (),
) -> dict[str, Any]:
    index_symbols = [item[0] for item in INDEX_SPECS]
    index_histories = client.fetch_many_day_series(index_symbols)
    trade_date, complete = select_trade_date(index_histories, now, force)
    indices, turnover = build_index_and_turnover_metrics(index_histories, trade_date)

    stocks = client.fetch_a_share_market()
    # 排除停牌或当日无成交股票，涨跌家数口径为“有效交易股票”。
    active = [
        item
        for item in stocks
        if as_float(item.get("price")) > 0
        and (
            as_float(item.get("turnover_wan")) > 0
            or as_float(item.get("volume_lots")) > 0
        )
    ]
    breadth = build_breadth(active)
    top20_base = sorted(active, key=lambda item: as_float(item.get("turnover_wan")), reverse=True)[
        :20
    ]
    main_flow_top = _main_flow_top(active, 20)
    sectors_base = client.fetch_top_sectors(5)

    quote_symbols: set[str] = set(index_symbols)
    # 必须覆盖全部有效交易股票，才能识别盘中触板后大幅回落的炸板股。
    quote_symbols.update(str(item["symbol"]) for item in active)
    quote_symbols.update(str(item["symbol"]) for item in top20_base)
    quote_symbols.update(str(item["symbol"]) for item in main_flow_top)
    quote_symbols.update(str(item["leader_symbol"]) for item in sectors_base)
    quote_symbols.update(watchlist)
    quotes = client.fetch_quotes(sorted(quote_symbols))
    if not force and not all(_quote_is_closed(quotes[symbol], trade_date) for symbol in index_symbols):
        raise NonTradingDay(f"跳过：{trade_date} 指数实时报价尚未全部收盘")

    sentiment, statuses = build_sentiment(active, quotes)
    board_histories = client.fetch_many_day_series(item["symbol"] for item in sectors_base)
    sectors = enrich_sectors(sectors_base, board_histories, trade_date)

    sealed_by_turnover = sorted(
        (
            item
            for item in active
            if statuses.get(str(item["symbol"]), {}).get("sealed_up")
        ),
        key=lambda item: as_float(item.get("turnover_wan")),
        reverse=True,
    )[:20]
    dynamic_symbols: set[str] = {
        str(item["symbol"])
        for item in [*top20_base, *main_flow_top, *sealed_by_turnover]
    }
    dynamic_symbols.update(str(item["leader_symbol"]) for item in sectors)
    detail_symbols = dynamic_symbols.union(watchlist)
    stock_histories = client.fetch_many_day_series(sorted(detail_symbols), strict=False)
    stock_by_symbol = {str(item["symbol"]): item for item in active}
    details: dict[str, dict[str, Any]] = {}
    for symbol in detail_symbols:
        stock = stock_by_symbol.get(symbol)
        if stock is None:
            continue
        details[symbol] = _stock_tail_metrics(
            stock, quotes.get(symbol), stock_histories.get(symbol), trade_date
        )

    top20 = [details.get(str(item["symbol"]), dict(item)) for item in top20_base]
    valid_top20_tail = [
        as_float(item["tail_change_pct"])
        for item in top20
        if item.get("tail_change_pct") is not None
    ]
    if complete and len(valid_top20_tail) < 15:
        raise CloseReportError(f"成交额Top20仅 {len(valid_top20_tail)} 只有完整尾盘数据")
    top20_positive_ratio = (
        sum(1 for value in valid_top20_tail if value > 0) / len(valid_top20_tail)
        if valid_top20_tail
        else 0.5
    )

    liquidity_order = sorted(active, key=lambda item: as_float(item["turnover_wan"]), reverse=True)
    liquidity_rank = {str(item["symbol"]): index for index, item in enumerate(liquidity_order, 1)}
    sector_scores = {
        str(item["leader_symbol"]): max(8.0, 23.0 - 3.0 * int(item["rank"]))
        for item in sectors
    }
    leaders: list[dict[str, Any]] = []
    for symbol in dynamic_symbols:
        detail = details.get(symbol)
        if detail is None:
            continue
        score, score_components = score_leader(
            detail,
            liquidity_rank.get(symbol, len(active)),
            len(active),
            sector_scores.get(symbol, 0.0),
        )
        enriched = dict(detail)
        enriched["score"] = score
        enriched["score_components"] = score_components
        enriched["state"] = classify_leader_state(detail, statuses.get(symbol))
        leaders.append(enriched)
    leaders.sort(key=lambda item: (as_float(item["score"]), as_float(item["turnover_wan"])), reverse=True)
    core_leaders = leaders[:5]
    detail_with_state = {str(item["symbol"]): item for item in leaders}

    for sector in sectors:
        leader = detail_with_state.get(str(sector["leader_symbol"]))
        sector["leader_state"] = leader["state"] if leader else "数据不足"
        sector["leader_tail_change_pct"] = leader.get("tail_change_pct") if leader else None
    watchlist_rows: list[dict[str, Any]] = []
    for symbol in watchlist:
        detail = details.get(symbol)
        if detail is None:
            watchlist_rows.append({"symbol": symbol, "code": symbol[2:], "status": "暂无有效行情"})
            continue
        row = dict(detail)
        row["state"] = classify_leader_state(detail, statuses.get(symbol))
        watchlist_rows.append(row)

    weighted_index_pct = sum(as_float(item["change_pct"]) * as_float(item["weight"]) for item in indices)
    market_at_1430_pct = sum(as_float(item["at_1430_pct"]) * as_float(item["weight"]) for item in indices)
    weighted_tail_pct = sum(as_float(item["tail_change_pct"]) * as_float(item["weight"]) for item in indices)
    positive_sector_count = sum(1 for item in sectors if as_float(item.get("tail_change_pct")) > 0)
    tail_label = classify_tail_flow(
        market_at_1430_pct,
        weighted_tail_pct,
        as_float(turnover["tail_intensity"]),
        positive_sector_count,
        top20_positive_ratio,
    )
    top5_sector_average = statistics.mean(as_float(item["change_pct"]) for item in sectors)
    sector_concentrated = top5_sector_average >= 2 and as_float(breadth["median_change_pct"]) < 0
    score, market_state, score_components = compute_market_state(
        weighted_index_pct,
        weighted_tail_pct,
        int(breadth["up_count"]),
        int(breadth["down_count"]),
        int(sentiment["limit_up_count"]),
        int(sentiment["limit_down_count"]),
        as_float(turnover["change_pct"]),
        sector_concentrated,
    )

    recognized_sectors = "、".join(str(item["name"]) for item in sectors[:3])
    recognized_leaders = "、".join(
        f"{item['name']}({item['state']})" for item in core_leaders[:3]
    )
    report: dict[str, Any] = {
        "schema_version": 1,
        "trade_date": trade_date,
        "generated_at": now.astimezone(TIMEZONE).isoformat(timespec="seconds"),
        "data_status": "complete" if complete else "provisional",
        "market": {
            "state": market_state,
            "score": score,
            "score_components": score_components,
            "weighted_index_pct": round(weighted_index_pct, 3),
            "sector_concentrated": sector_concentrated,
            "top5_sector_average_pct": round(top5_sector_average, 3),
        },
        "funds_recognize": {
            "sectors": recognized_sectors,
            "leaders": recognized_leaders,
            "summary": (
                f"资金主要聚焦{recognized_sectors}；核心活跃股为{recognized_leaders or '暂无明确共识龙头'}。"
            ),
        },
        "indices": indices,
        "turnover": turnover,
        "breadth": breadth,
        "sentiment": sentiment,
        "top_turnover": top20,
        "sectors": sectors,
        "core_leaders": core_leaders,
        "watchlist": watchlist_rows,
        "tail_flow": {
            "label": tail_label,
            "market_at_1430_pct": round(market_at_1430_pct, 3),
            "weighted_tail_pct": round(weighted_tail_pct, 3),
            "tail_intensity": turnover["tail_intensity"],
            "positive_sector_count": positive_sector_count,
            "top20_positive_ratio": round(top20_positive_ratio, 3),
        },
        "opportunities": [],
        "risks": [],
        "source": "Tencent Finance public market endpoints",
        "universe_note": "涨跌家数与涨跌停包含沪深京；成交额仅为沪深两市。",
        "disclaimer": "本报告仅为公开行情的规则化复盘，不构成任何投资建议。",
    }
    opportunities, risks = generate_outlook(report)
    report["opportunities"] = opportunities
    report["risks"] = risks
    canonical = copy.deepcopy(report)
    canonical.pop("generated_at", None)
    report_hash = hashlib.sha256(
        json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    report["report_hash"] = report_hash
    return report


def load_delivery_state(path: Path) -> dict[str, Any]:
    state = read_json(
        path,
        {
            "schema_version": 2,
            "recipient_key_scheme": "sha256-lowercase-email-v1",
            "reports": {},
        },
    )
    if not isinstance(state, dict) or not isinstance(state.get("reports"), dict):
        raise CloseReportError(f"邮件去重状态结构异常: {path}")
    # 兼容早期状态：加载时把明文邮箱键迁移为哈希，避免继续传播到公开仓库。
    for date_entry in state["reports"].values():
        if not isinstance(date_entry, dict):
            raise CloseReportError(f"邮件去重状态结构异常: {path}")
        for hash_entry in date_entry.values():
            if not isinstance(hash_entry, dict):
                raise CloseReportError(f"邮件去重状态结构异常: {path}")
            sent_to = hash_entry.get("sent_to", {})
            if not isinstance(sent_to, dict):
                raise CloseReportError(f"邮件去重状态结构异常: {path}")
            sanitized: dict[str, Any] = {}
            for key, sent_at in sent_to.items():
                value = str(key)
                if re.fullmatch(r"sha256:[0-9a-f]{64}", value):
                    sanitized[value] = sent_at
                else:
                    sanitized[recipient_delivery_key(value)] = sent_at
            hash_entry["sent_to"] = sanitized
    state["schema_version"] = 2
    state["recipient_key_scheme"] = "sha256-lowercase-email-v1"
    return state


def delivered_recipients(
    state: Mapping[str, Any], trade_date: str, report_hash: str
) -> set[str]:
    entry = (
        state.get("reports", {})
        .get(trade_date, {})
        .get(report_hash, {})
        .get("sent_to", {})
    )
    return {str(item) for item in entry} if isinstance(entry, Mapping) else set()


def record_delivery(
    state: dict[str, Any],
    trade_date: str,
    report_hash: str,
    recipient: str,
    sent_at: str,
) -> None:
    reports = state.setdefault("reports", {})
    date_entry = reports.setdefault(trade_date, {})
    hash_entry = date_entry.setdefault(report_hash, {"sent_to": {}})
    hash_entry.setdefault("sent_to", {})[recipient_delivery_key(recipient)] = sent_at
    hash_entry["last_updated_at"] = sent_at
    for old_date in sorted(reports)[:-120]:
        reports.pop(old_date, None)


def persist_report(report: Mapping[str, Any], docs_dir: Path) -> dict[str, Path]:
    trade_date = str(report["trade_date"])
    year = trade_date[:4]
    history_dir = docs_dir / "history" / year
    latest_json = docs_dir / "latest.json"
    latest_html = docs_dir / "latest.html"
    history_json = history_dir / f"{trade_date}.json"
    history_html = history_dir / f"{trade_date}.html"
    json_text = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    html_text = render_report_html(report)
    for path, content in (
        (latest_json, json_text),
        (latest_html, html_text),
        (history_json, json_text),
        (history_html, html_text),
    ):
        atomic_write_text(path, content)
    catalog = build_history_catalog(docs_dir)
    atomic_write_text(
        docs_dir / "index.json", json.dumps(catalog, ensure_ascii=False, indent=2) + "\n"
    )
    atomic_write_text(docs_dir / "index.html", render_history_index(catalog))
    return {
        "latest_json": latest_json,
        "latest_html": latest_html,
        "history_json": history_json,
        "history_html": history_html,
    }


def build_history_catalog(docs_dir: Path) -> dict[str, Any]:
    reports: list[dict[str, Any]] = []
    history_dir = docs_dir / "history"
    if history_dir.exists():
        for path in sorted(history_dir.glob("*/*.json"), reverse=True):
            payload = read_json(path, {})
            if not isinstance(payload, Mapping) or not payload.get("trade_date"):
                continue
            reports.append(
                {
                    "trade_date": payload["trade_date"],
                    "market_state": (payload.get("market") or {}).get("state"),
                    "score": (payload.get("market") or {}).get("score"),
                    "tail_flow": (payload.get("tail_flow") or {}).get("label"),
                    "html": path.with_suffix(".html").relative_to(docs_dir).as_posix(),
                    "json": path.relative_to(docs_dir).as_posix(),
                }
            )
    return {
        "schema_version": 1,
        "generated_at": datetime.now(TIMEZONE).isoformat(timespec="seconds"),
        "reports": reports,
    }


def _escape(value: Any) -> str:
    return html.escape(str(value), quote=True)


def _format_pct(value: Any, digits: int = 2) -> str:
    if value is None:
        return "--"
    return f"{as_float(value):+,.{digits}f}%"


def _format_yi(value: Any) -> str:
    if value is None:
        return "--"
    return f"{as_float(value) / 100_000_000:,.2f}亿元"


def _format_wan(value: Any) -> str:
    if value is None:
        return "N/A"
    number = as_float(value)
    if abs(number) >= 10_000:
        return f"{number / 10_000:+,.2f}亿元"
    return f"{number:+,.0f}万元"


def _color_class(value: Any) -> str:
    number = as_float(value)
    return "up" if number > 0 else "down" if number < 0 else "flat"


def _list_html(items: Sequence[str], empty: str) -> str:
    values = list(items) or [empty]
    return "".join(f"<li>{_escape(item)}</li>" for item in values)


def render_report_html(report: Mapping[str, Any]) -> str:
    market = report["market"]
    turnover = report["turnover"]
    breadth = report["breadth"]
    sentiment = report["sentiment"]
    indices_rows = "".join(
        "<tr>"
        f"<td>{_escape(item['name'])}</td>"
        f"<td class='{_color_class(item['change_pct'])}'>{_format_pct(item['change_pct'])}</td>"
        f"<td>{_escape(item['strength'])}</td>"
        f"<td class='{_color_class(item['tail_change_pct'])}'>{_format_pct(item['tail_change_pct'])}</td>"
        f"<td>{_escape(item['tail_label'])}</td>"
        "</tr>"
        for item in report["indices"]
    )
    top_rows = "".join(
        "<tr>"
        f"<td>{index}</td><td>{_escape(item.get('name', ''))}（{_escape(item.get('code', ''))}）</td>"
        f"<td>{_format_yi(item.get('turnover_yuan'))}</td>"
        f"<td class='{_color_class(item.get('change_pct'))}'>{_format_pct(item.get('change_pct'))}</td>"
        f"<td>{_format_wan(item.get('main_net_wan'))}</td>"
        f"<td>{_format_pct(item.get('main_net_ratio_pct')) if item.get('main_net_ratio_pct') is not None else 'N/A'}</td>"
        f"<td class='{_color_class(item.get('tail_change_pct'))}'>{_format_pct(item.get('tail_change_pct'))}</td>"
        "</tr>"
        for index, item in enumerate(report["top_turnover"], 1)
    )
    sector_rows = "".join(
        "<tr>"
        f"<td>{item['rank']}</td><td>{_escape(item['name'])}</td>"
        f"<td class='{_color_class(item['change_pct'])}'>{_format_pct(item['change_pct'])}</td>"
        f"<td>{_format_pct(item['change_5d_pct'])}</td><td>{_format_pct(item['change_20d_pct'])}</td>"
        f"<td class='{_color_class(item.get('tail_change_pct'))}'>{_format_pct(item.get('tail_change_pct'))}</td>"
        f"<td>{_escape(item['leader_name'])}（{_escape(item['leader_code'])}）/ {_escape(item.get('leader_state', '--'))}</td>"
        "</tr>"
        for item in report["sectors"]
    )
    leader_rows = "".join(
        "<tr>"
        f"<td>{index}</td><td>{_escape(item['name'])}（{_escape(item['code'])}）</td>"
        f"<td>{as_float(item['score']):.1f}</td>"
        f"<td class='{_color_class(item['change_pct'])}'>{_format_pct(item['change_pct'])}</td>"
        f"<td>{_format_yi(item['turnover_yuan'])}</td>"
        f"<td>{_format_wan(item.get('main_net_wan'))}</td>"
        f"<td class='{_color_class(item.get('tail_change_pct'))}'>{_format_pct(item.get('tail_change_pct'))}</td>"
        f"<td><strong>{_escape(item['state'])}</strong></td>"
        "</tr>"
        for index, item in enumerate(report["core_leaders"], 1)
    )
    watchlist_section = ""
    if report.get("watchlist"):
        watch_rows = "".join(
            "<tr>"
            f"<td>{_escape(item.get('name', item.get('symbol', '')))}</td>"
            f"<td>{_escape(item.get('code', ''))}</td>"
            f"<td>{_format_pct(item.get('change_pct'))}</td>"
            f"<td>{_format_pct(item.get('tail_change_pct'))}</td>"
            f"<td>{_escape(item.get('state', item.get('status', '--')))}</td>"
            "</tr>"
            for item in report["watchlist"]
        )
        watchlist_section = (
            "<h2>自选股</h2><table><thead><tr><th>名称</th><th>代码</th>"
            "<th>涨跌</th><th>尾盘</th><th>状态</th></tr></thead>"
            f"<tbody>{watch_rows}</tbody></table>"
        )
    provisional = (
        "<div class='warning'>本报告由强制模式生成，行情尚未确认完整收盘，仅用于测试。</div>"
        if report.get("data_status") != "complete"
        else ""
    )
    return f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{_escape(report['trade_date'])} A股收盘复盘</title>
<style>
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Microsoft YaHei",sans-serif;color:#172033;background:#f4f7fb;margin:0;padding:24px;line-height:1.55}}
.wrap{{max-width:1100px;margin:auto;background:#fff;border-radius:14px;padding:28px;box-shadow:0 8px 30px #17305518}}
h1{{margin:0 0 8px;font-size:28px}}h2{{margin-top:30px;border-left:4px solid #315efb;padding-left:10px;font-size:20px}}
.muted{{color:#6c778d}}.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin:18px 0}}
.card{{border:1px solid #e5eaf2;border-radius:10px;padding:14px;background:#fbfcff}}.card b{{display:block;font-size:21px;margin-top:5px}}
table{{width:100%;border-collapse:collapse;font-size:14px}}th,td{{padding:9px 8px;border-bottom:1px solid #e8edf5;text-align:left;white-space:nowrap}}
th{{background:#f6f8fc}}.up{{color:#d4380d}}.down{{color:#238636}}.flat{{color:#57606a}}
.summary{{background:#edf3ff;border-radius:10px;padding:16px;margin:16px 0}}.warning{{background:#fff4e5;color:#9a5700;padding:12px;border-radius:8px}}
.two{{display:grid;grid-template-columns:1fr 1fr;gap:16px}}.box{{border:1px solid #e5eaf2;border-radius:10px;padding:14px}}
@media(max-width:760px){{body{{padding:8px}}.wrap{{padding:16px;overflow:auto}}.two{{grid-template-columns:1fr}}}}
</style></head><body><main class="wrap">
<h1>{_escape(report['trade_date'])} A股收盘复盘</h1>
<div class="muted">生成时间：{_escape(report['generated_at'])}（北京时间）</div>{provisional}
<div class="summary"><strong>今天市场是什么状态？</strong> {_escape(market['state'])}（{market['score']:+.1f}分）<br>
<strong>今天资金最认可什么？</strong> {_escape(report['funds_recognize']['summary'])}<br>
<strong>14:30后资金方向：</strong> {_escape(report['tail_flow']['label'])}，加权指数尾盘 {_format_pct(report['tail_flow']['weighted_tail_pct'])}。</div>
<div class="cards">
<div class="card">市场状态<b>{_escape(market['state'])}</b></div>
<div class="card">沪深成交额<b>{_format_yi(turnover['today_amount_yuan'])}</b><span>{_escape(turnover['label'])} {_format_pct(turnover['change_pct'])}</span></div>
<div class="card">赚钱效应<b>{_escape(breadth['label'])}</b><span>上涨 {breadth['up_count']} / 下跌 {breadth['down_count']}</span></div>
<div class="card">市场情绪<b>{_escape(sentiment['label'])}</b><span>涨停 {sentiment['limit_up_count']} / 跌停 {sentiment['limit_down_count']}</span></div>
</div>
<h2>指数强弱与尾盘异动</h2><table><thead><tr><th>指数</th><th>全天</th><th>强弱</th><th>14:30后</th><th>判断</th></tr></thead><tbody>{indices_rows}</tbody></table>
<h2>成交与赚钱效应</h2><p>沪深成交额 {_format_yi(turnover['today_amount_yuan'])}，较 {turnover['previous_trade_date']} {_escape(turnover['label'])} {_format_pct(turnover['change_pct'])}；尾盘成交占比 {turnover['tail_share_pct']:.2f}%，为近4日中位数的 {turnover['tail_intensity']:.2f} 倍。</p>
<p>沪深京有效交易 {breadth['active_count']} 只：上涨 {breadth['up_count']}、下跌 {breadth['down_count']}、平盘 {breadth['flat_count']}；涨超2% {breadth['up_2pct_count']}、跌超2% {breadth['down_2pct_count']}，涨跌幅中位数 {_format_pct(breadth['median_change_pct'])}。</p>
<p>涨停 {sentiment['limit_up_count']}、跌停 {sentiment['limit_down_count']}、炸板 {sentiment['failed_up_count']}，封板率 {sentiment['seal_rate_pct']:.1f}%。</p>
<h2>成交额 Top20</h2><table><thead><tr><th>#</th><th>股票</th><th>成交额</th><th>涨跌</th><th>主力净流入</th><th>占比</th><th>尾盘</th></tr></thead><tbody>{top_rows}</tbody></table>
<h2>最强板块 Top5</h2><table><thead><tr><th>#</th><th>板块</th><th>今日</th><th>5日</th><th>20日</th><th>尾盘</th><th>领涨股/状态</th></tr></thead><tbody>{sector_rows}</tbody></table>
<h2>核心龙头</h2><table><thead><tr><th>#</th><th>股票</th><th>评分</th><th>涨跌</th><th>成交额</th><th>主力净流入</th><th>尾盘</th><th>状态</th></tr></thead><tbody>{leader_rows}</tbody></table>
{watchlist_section}
<h2>明日机会与风险</h2><div class="two"><section class="box"><strong>机会观察</strong><ol>{_list_html(report['opportunities'], '暂无满足规则的高置信度机会信号。')}</ol></section>
<section class="box"><strong>主要风险</strong><ol>{_list_html(report['risks'], '当前未出现高优先级风险信号，仍需关注次日量能。')}</ol></section></div>
<p class="muted">数据源：腾讯财经公开行情。{_escape(report['universe_note'])}<br>{_escape(report['disclaimer'])}</p>
</main></body></html>"""


def render_report_text(report: Mapping[str, Any]) -> str:
    market = report["market"]
    turnover = report["turnover"]
    breadth = report["breadth"]
    sentiment = report["sentiment"]
    lines = [
        f"{report['trade_date']} A股收盘复盘",
        f"生成时间：{report['generated_at']}（北京时间）",
        "",
        f"今天市场是什么状态：{market['state']}（{market['score']:+.1f}分）",
        f"今天资金最认可什么：{report['funds_recognize']['summary']}",
        f"14:30后资金方向：{report['tail_flow']['label']}，加权指数尾盘 {_format_pct(report['tail_flow']['weighted_tail_pct'])}",
        "",
        "【指数】",
    ]
    for item in report["indices"]:
        lines.append(
            f"- {item['name']}：全天 {_format_pct(item['change_pct'])}（{item['strength']}），"
            f"尾盘 {_format_pct(item['tail_change_pct'])}（{item['tail_label']}）"
        )
    lines.extend(
        [
            "",
            "【成交额】",
            f"- 沪深合计 {_format_yi(turnover['today_amount_yuan'])}，较{turnover['previous_trade_date']}"
            f"{turnover['label']} {_format_pct(turnover['change_pct'])}",
            f"- 尾盘成交占比 {turnover['tail_share_pct']:.2f}%，为近4日中位数的{turnover['tail_intensity']:.2f}倍",
            "",
            "【赚钱效应与涨跌停】",
            f"- {breadth['label']}：上涨{breadth['up_count']}、下跌{breadth['down_count']}、平盘{breadth['flat_count']}，"
            f"涨跌幅中位数 {_format_pct(breadth['median_change_pct'])}",
            f"- {sentiment['label']}：涨停{sentiment['limit_up_count']}、跌停{sentiment['limit_down_count']}、"
            f"炸板{sentiment['failed_up_count']}，封板率{sentiment['seal_rate_pct']:.1f}%",
            "",
            "【成交额Top20】",
        ]
    )
    for index, item in enumerate(report["top_turnover"], 1):
        lines.append(
            f"{index}. {item.get('name')}({item.get('code')}) 成交额{_format_yi(item.get('turnover_yuan'))} "
            f"涨跌{_format_pct(item.get('change_pct'))} 主力净流入{_format_wan(item.get('main_net_wan'))} "
            f"尾盘{_format_pct(item.get('tail_change_pct'))}"
        )
    lines.extend(["", "【板块Top5】"])
    for item in report["sectors"]:
        lines.append(
            f"{item['rank']}. {item['name']} 今日{_format_pct(item['change_pct'])}、"
            f"5日{_format_pct(item['change_5d_pct'])}、20日{_format_pct(item['change_20d_pct'])}、"
            f"尾盘{_format_pct(item.get('tail_change_pct'))}；龙头{item['leader_name']}({item['leader_state']})"
        )
    lines.extend(["", "【核心龙头】"])
    for index, item in enumerate(report["core_leaders"], 1):
        lines.append(
            f"{index}. {item['name']}({item['code']}) 评分{item['score']:.1f} "
            f"涨跌{_format_pct(item['change_pct'])} 尾盘{_format_pct(item.get('tail_change_pct'))} "
            f"状态{item['state']}"
        )
    lines.extend(["", "【明日机会】"])
    lines.extend(f"- {item}" for item in report["opportunities"] or ["暂无满足规则的高置信度机会信号。"])
    lines.extend(["", "【主要风险】"])
    lines.extend(f"- {item}" for item in report["risks"] or ["当前未出现高优先级风险信号，仍需关注次日量能。"])
    lines.extend(["", report["universe_note"], report["disclaimer"]])
    return "\n".join(lines) + "\n"


def render_history_index(catalog: Mapping[str, Any]) -> str:
    rows = "".join(
        "<tr>"
        f"<td>{_escape(item['trade_date'])}</td><td>{_escape(item.get('market_state'))}</td>"
        f"<td>{_escape(item.get('score'))}</td><td>{_escape(item.get('tail_flow'))}</td>"
        f"<td><a href='{_escape(item['html'])}'>HTML</a> · <a href='{_escape(item['json'])}'>JSON</a></td>"
        "</tr>"
        for item in catalog.get("reports", [])
    )
    return f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>A股收盘复盘历史</title><style>body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Microsoft YaHei",sans-serif;max-width:900px;margin:40px auto;padding:0 16px;color:#172033}}table{{width:100%;border-collapse:collapse}}th,td{{padding:10px;border-bottom:1px solid #ddd;text-align:left}}a{{color:#315efb}}</style></head>
<body><h1>A股收盘复盘历史</h1><p><a href="latest.html">查看最新报告</a></p><table><thead><tr><th>交易日</th><th>市场状态</th><th>评分</th><th>尾盘</th><th>文件</th></tr></thead><tbody>{rows}</tbody></table></body></html>"""


def build_email(report: Mapping[str, Any]) -> tuple[str, str, str]:
    subject = (
        f"[A股收盘复盘] {report['trade_date']} {report['market']['state']} | "
        f"尾盘{report['tail_flow']['label']}"
    )
    if report.get("data_status") != "complete":
        subject = f"[强制测试]{subject}"
    return subject, render_report_text(report), render_report_html(report)


def _send_one(
    mail: MailConfig,
    recipient: str,
    subject: str,
    plain_text: str,
    html_text: str | None = None,
    attempts: int = 2,
) -> None:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        message = EmailMessage()
        message["From"] = mail.username
        message["To"] = recipient
        message["Subject"] = subject
        message.set_content(plain_text)
        if html_text:
            message.add_alternative(html_text, subtype="html")
        try:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(
                SMTP_HOST, SMTP_PORT, timeout=30, context=context
            ) as client:
                client.login(mail.username, mail.password)
                refused = client.send_message(message)
                if refused:
                    raise CloseReportError(f"SMTP拒收: {sorted(refused)}")
            return
        except (OSError, smtplib.SMTPException, CloseReportError) as exc:
            last_error = exc
            if attempt < attempts:
                time.sleep(2)
    raise CloseReportError(f"发送给 {recipient} 失败: {last_error}")


def send_report(
    mail: MailConfig,
    report: Mapping[str, Any],
    state_path: Path,
    resend: bool = False,
) -> tuple[list[str], list[str]]:
    state = load_delivery_state(state_path)
    trade_date = str(report["trade_date"])
    report_hash = str(report["report_hash"])
    delivered = delivered_recipients(state, trade_date, report_hash)
    subject, plain_text, html_text = build_email(report)
    sent: list[str] = []
    skipped: list[str] = []
    failures: list[str] = []
    for recipient in mail.recipients:
        if not resend and recipient_delivery_key(recipient) in delivered:
            skipped.append(recipient)
            continue
        try:
            _send_one(mail, recipient, subject, plain_text, html_text)
            sent_at = datetime.now(TIMEZONE).isoformat(timespec="seconds")
            record_delivery(state, trade_date, report_hash, recipient, sent_at)
            atomic_write_text(
                state_path, json.dumps(state, ensure_ascii=False, indent=2) + "\n"
            )
            sent.append(recipient)
        except Exception as exc:
            failures.append(str(exc))
    if failures:
        raise CloseReportError("；".join(failures))
    return sent, skipped


def send_test_email(mail: MailConfig) -> None:
    subject = "[A股收盘复盘] 多收件人邮件配置测试成功"
    body = (
        "A股每日收盘复盘邮件配置测试成功。\n\n"
        "正式任务将在A股交易日北京时间15:30后运行。\n"
        "收件人通过GitHub变量 DAILY_REPORT_RECIPIENTS 管理；SELF代表当前163发件邮箱。\n"
    )
    failures: list[str] = []
    for recipient in mail.recipients:
        try:
            _send_one(mail, recipient, subject, body)
        except Exception as exc:
            failures.append(str(exc))
    if failures:
        raise CloseReportError("；".join(failures))


def send_failure_notification(message: str) -> None:
    try:
        username = _validate_email(os.environ.get("SMTP_USERNAME", ""))
        password = os.environ.get("SMTP_APP_PASSWORD", "").strip()
        if not password:
            return
        mail = MailConfig(username=username, password=password, recipients=(username,))
        _send_one(
            mail,
            username,
            "[A股收盘复盘失败] 请检查GitHub Actions",
            message + "\n\n请打开GitHub Actions查看本次运行日志。\n",
            attempts=1,
        )
    except Exception as exc:
        print(f"失败通知邮件也未能发送: {exc}", file=sys.stderr)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="GitHub Actions A股每日收盘复盘")
    parser.add_argument("--force", action="store_true", help="允许使用尚未确认完整收盘的最新快照")
    parser.add_argument("--dry-run", action="store_true", help="只分析并打印摘要，不写文件、不发邮件")
    parser.add_argument("--test-email", action="store_true", help="只测试当前收件人邮件配置")
    parser.add_argument("--resend", action="store_true", help="忽略当日报告的收件人去重状态")
    parser.add_argument("--docs-dir", type=Path, default=DEFAULT_DOCS_DIR)
    parser.add_argument("--state-path", type=Path, default=DEFAULT_STATE_PATH)
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--workers", type=int, default=12)
    return parser


def run(args: argparse.Namespace) -> dict[str, Any] | None:
    if args.test_email:
        mail = MailConfig.from_env()
        send_test_email(mail)
        print(f"测试邮件已分别发送给 {len(mail.recipients)} 个收件人")
        return None
    watchlist = parse_watchlist(os.environ.get("DAILY_REPORT_WATCHLIST", ""))
    client = TencentClient(timeout=args.timeout, retries=args.retries, workers=args.workers)
    report = build_report(client, datetime.now(TIMEZONE), force=args.force, watchlist=watchlist)
    print(
        f"分析完成: {report['trade_date']} {report['market']['state']} "
        f"{report['market']['score']:+.1f}分，尾盘{report['tail_flow']['label']}，"
        f"A股{report['breadth']['active_count']}只，涨停{report['sentiment']['limit_up_count']}只"
    )
    if args.dry_run:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return report
    persist_report(report, args.docs_dir)
    sent, skipped = send_report(
        MailConfig.from_env(), report, args.state_path, resend=args.resend
    )
    print(f"邮件发送完成: 新发送 {len(sent)}，去重跳过 {len(skipped)}")
    return report


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        run(args)
        return 0
    except NonTradingDay as exc:
        print(str(exc))
        return 0
    except Exception as exc:
        message = f"A股收盘复盘任务失败: {exc}"
        print(message, file=sys.stderr)
        if not args.dry_run and not args.test_email:
            send_failure_notification(message)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
