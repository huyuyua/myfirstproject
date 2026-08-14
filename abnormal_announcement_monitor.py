# -*- coding: utf-8 -*-
"""盘前扫描腾讯财经 A 股异动公告，归档并通过 163 邮箱提醒。"""

from __future__ import annotations

import argparse
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from email.message import EmailMessage
import html
from io import BytesIO, StringIO
import json
import os
from pathlib import Path
import re
import smtplib
import ssl
import sys
import threading
import time
from typing import Any, Iterable, Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo


APP_DIR = Path(__file__).resolve().parent
DEFAULT_STATE_PATH = APP_DIR / "state" / "abnormal_announcements.json"
DEFAULT_DOCS_DIR = APP_DIR / "docs" / "abnormal-announcements"
TIMEZONE = ZoneInfo("Asia/Shanghai")

TENCENT_STOCK_LIST_URL = (
    "https://proxy.finance.qq.com/cgi/cgi-bin/rank/hs/getBoardRankList"
)
TENCENT_NOTICE_LIST_URL = (
    "https://proxy.finance.qq.com/ifzqgtimg/appstock/news/info/search"
)
TENCENT_NOTICE_DETAIL_URL = (
    "https://proxy.finance.qq.com/ifzqgtimg/appstock/news/content/content"
)

SMTP_HOST = "smtp.163.com"
SMTP_PORT = 465
NOTICE_PAGE_SIZE = 51  # 腾讯当前公开接口允许的最大单页数量。
STOCK_PAGE_SIZE = 200
STOCK_PAGE_OVERLAP = 20
DEFAULT_BATCH_SIZE = 100
DEFAULT_WORKERS = 8
DEFAULT_LOOKBACK_DAYS = 3
DOWN_OCCURRENCE_WINDOW_DAYS = 30

CATEGORY_LABELS = {
    "twenty_percent_deviation": "20%偏离异动",
    "twenty_trading_days": "20个交易日异动",
    "thirty_day_deviation": "30日偏离/严重异动",
    "down_second_occurrence": "近30自然日第2次下跌异动",
    "down_third_occurrence": "近30自然日第3次下跌异动",
    "manual_review": "正文解析失败，需人工核验",
}


class MonitorError(RuntimeError):
    """可读的监控执行错误。"""


def atomic_write_text(path: Path, content: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(content, encoding=encoding)
    os.replace(temporary, path)


def chunks(values: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def parse_tencent_time(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.strip())
    except (TypeError, ValueError) as exc:
        raise MonitorError(f"腾讯公告时间格式异常: {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=TIMEZONE)
    return parsed.astimezone(TIMEZONE)


def parse_json_variable(text: str, variable: str | None = None) -> Any:
    cleaned = text.strip().lstrip("\ufeff")
    if variable:
        prefix = f"{variable}="
        if not cleaned.startswith(prefix):
            raise MonitorError(f"腾讯接口未返回预期变量 {variable}")
        cleaned = cleaned[len(prefix) :].strip()
    if cleaned.endswith(";"):
        cleaned = cleaned[:-1]
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise MonitorError(f"腾讯接口 JSON 解析失败: {exc}") from exc


class TencentClient:
    """腾讯财经公开接口客户端，带重试、并发安全的正文缓存。"""

    def __init__(self, timeout: float = 20.0, retries: int = 3, workers: int = DEFAULT_WORKERS):
        self.timeout = timeout
        self.retries = retries
        self.workers = workers
        self._detail_cache: dict[str, dict[str, Any]] = {}
        self._cache_lock = threading.Lock()

    @staticmethod
    def _url(base: str, params: Mapping[str, Any]) -> str:
        return f"{base}?{urlencode(params)}"

    def _request_bytes(self, url: str, referer: str = "https://gu.qq.com/") -> bytes:
        last_error: Exception | None = None
        for attempt in range(1, self.retries + 1):
            request = Request(
                url,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "Chrome/136.0 Safari/537.36"
                    ),
                    "Accept": "application/json,text/plain,*/*",
                    "Accept-Language": "zh-CN,zh;q=0.9",
                    "Referer": referer,
                },
            )
            try:
                with urlopen(request, timeout=self.timeout) as response:
                    return response.read()
            except (HTTPError, URLError, TimeoutError, OSError) as exc:
                last_error = exc
                if attempt < self.retries:
                    time.sleep(min(1.5 * (2 ** (attempt - 1)), 6.0))
        raise MonitorError(f"腾讯接口请求失败（重试 {self.retries} 次）: {url}: {last_error}")

    def _request_json(
        self,
        url: str,
        variable: str | None = None,
        referer: str = "https://gu.qq.com/",
    ) -> Any:
        raw = self._request_bytes(url, referer=referer)
        return parse_json_variable(raw.decode("utf-8-sig", errors="replace"), variable)

    def _stock_page(
        self, offset: int, sort_type: str, direct: str
    ) -> tuple[int, list[dict[str, str]]]:
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
        payload = self._request_json(
            url, referer="https://stockapp.finance.qq.com/mstats/"
        )
        if payload.get("code") != 0:
            raise MonitorError(f"腾讯 A 股列表接口失败: {payload.get('msg') or payload}")
        data = payload.get("data") or {}
        rows = data.get("rank_list") or []
        stocks: list[dict[str, str]] = []
        for row in rows:
            symbol = str(row.get("code") or "").lower()
            if not re.fullmatch(r"(?:sh|sz|bj)\d{6}", symbol):
                continue
            stocks.append({"symbol": symbol, "name": str(row.get("name") or symbol)})
        return int(data.get("total") or 0), stocks

    def _fetch_universe_sorted(
        self, sort_type: str, direct: str
    ) -> tuple[int, list[dict[str, str]]]:
        total, first = self._stock_page(0, sort_type, direct)
        if total <= 0 or not first:
            raise MonitorError("腾讯 A 股列表为空")
        # 相邻页保留 20 条重叠，吸收实时行情排序在请求期间的小幅变化。
        page_step = STOCK_PAGE_SIZE - STOCK_PAGE_OVERLAP
        offsets = list(range(page_step, total, page_step))
        pages: dict[int, list[dict[str, str]]] = {0: first}
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {
                executor.submit(self._stock_page, offset, sort_type, direct): offset
                for offset in offsets
            }
            for future in as_completed(futures):
                offset = futures[future]
                _, rows = future.result()
                pages[offset] = rows
        return total, [stock for offset in sorted(pages) for stock in pages[offset]]

    def fetch_a_share_universe(self) -> list[dict[str, str]]:
        # 行情排序在翻页期间可能变化。先按价格、再按成交额取并集；若仍不足，
        # 再用涨跌幅补齐。这样既沿用腾讯页面接口，也不会因名次变化漏股票。
        sort_orders = (("price", "up"), ("exchange", "up"), ("priceRatio", "down"))
        deduplicated: dict[str, dict[str, str]] = {}
        expected_total = 0
        for sort_type, direct in sort_orders:
            total, rows = self._fetch_universe_sorted(sort_type, direct)
            expected_total = max(expected_total, total)
            for stock in rows:
                deduplicated[stock["symbol"]] = stock
            if expected_total > 0 and len(deduplicated) >= expected_total:
                break
        if len(deduplicated) < expected_total:
            raise MonitorError(
                f"腾讯 A 股列表不完整: 预期 {expected_total}，实际 {len(deduplicated)}"
            )
        return sorted(deduplicated.values(), key=lambda item: item["symbol"])

    def _notice_page(self, symbols: Sequence[str], page: int) -> list[dict[str, Any]]:
        url = self._url(
            TENCENT_NOTICE_LIST_URL,
            {
                "page": page,
                "symbol": ",".join(symbols),
                "n": NOTICE_PAGE_SIZE,
                "_var": "finance_notice",
                "type": 0,
                "_appver": "1.0",
            },
        )
        payload = self._request_json(url, variable="finance_notice")
        if payload.get("code") != 0:
            raise MonitorError(f"腾讯公告列表接口失败: {payload.get('msg') or payload}")
        return list((payload.get("data") or {}).get("data") or [])

    def fetch_notice_window(
        self,
        symbols: Sequence[str],
        window_start: datetime,
        window_end: datetime,
        max_pages: int = 200,
    ) -> list[dict[str, Any]]:
        found: dict[str, dict[str, Any]] = {}
        page_seen_ids: set[str] = set()
        reached_window_start = False

        for page in range(1, max_pages + 1):
            rows = self._notice_page(symbols, page)
            if not rows:
                reached_window_start = True
                break

            current_ids = {str(row.get("id") or "") for row in rows}
            if current_ids and current_ids.issubset(page_seen_ids):
                raise MonitorError("腾讯公告分页重复，已停止以避免遗漏或死循环")
            page_seen_ids.update(current_ids)

            times: list[datetime] = []
            for row in rows:
                announcement_id = str(row.get("id") or "")
                published_at = parse_tencent_time(str(row.get("time") or ""))
                times.append(published_at)
                if window_start <= published_at <= window_end and announcement_id:
                    found[announcement_id] = row

            # 腾讯按发布时间倒序返回；一页已跨过起点时，后续页只会更旧。
            if times and min(times) < window_start:
                reached_window_start = True
                break
        if not reached_window_start:
            raise MonitorError(
                f"腾讯公告分页达到上限 {max_pages}，仍未覆盖到 {window_start.isoformat()}"
            )
        return list(found.values())

    def fetch_symbol_notice_history(
        self, symbol: str, window_start: datetime, window_end: datetime
    ) -> list[dict[str, Any]]:
        return self.fetch_notice_window([symbol], window_start, window_end, max_pages=40)

    @staticmethod
    def _extract_pdf_text(pdf_bytes: bytes) -> str:
        try:
            from pypdf import PdfReader
        except ImportError as exc:
            raise MonitorError("缺少 pypdf，无法解析腾讯公告 PDF") from exc

        try:
            reader = PdfReader(BytesIO(pdf_bytes))
            parts = [(page.extract_text() or "") for page in reader.pages[:40]]
        except Exception as exc:
            raise MonitorError(f"腾讯公告 PDF 文本提取失败: {exc}") from exc
        return "\n".join(parts).strip()

    def fetch_notice_detail(self, announcement_id: str) -> dict[str, Any]:
        normalized_id = (
            announcement_id if announcement_id.startswith("nos") else f"nos{announcement_id}"
        )
        with self._cache_lock:
            cached = self._detail_cache.get(normalized_id)
        if cached is not None:
            return cached

        url = self._url(
            TENCENT_NOTICE_DETAIL_URL,
            {"_var": "notice_detail", "id": normalized_id},
        )
        payload = self._request_json(url, variable="notice_detail")
        rows = payload.get("data") or []
        if payload.get("code") != 0 or not rows:
            raise MonitorError(
                f"腾讯公告正文接口失败: {normalized_id}: {payload.get('msg') or payload}"
            )
        row = dict(rows[0])
        inline_text = str(
            row.get("detail") or row.get("content") or row.get("detail_oem") or ""
        ).strip()
        pdf_url = str(row.get("pdf") or "").strip()
        if inline_text.lower().startswith(("http://", "https://")):
            pdf_url = pdf_url or inline_text
            inline_text = ""
        if pdf_url.startswith("http://"):
            pdf_url = "https://" + pdf_url[len("http://") :]

        status = "inline"
        text = inline_text
        if len(text) < 80 and pdf_url:
            text = self._extract_pdf_text(self._request_bytes(pdf_url))
            status = "pdf"
        if len(text) < 40:
            raise MonitorError(f"腾讯公告正文内容为空或过短: {normalized_id}")

        result = {
            "text": text,
            "pdf_url": pdf_url,
            "status": status,
            "raw": row,
        }
        with self._cache_lock:
            self._detail_cache[normalized_id] = result
        return result


def normalize_text(value: str) -> str:
    return re.sub(
        r"\s+",
        "",
        value.replace("％", "%")
        .replace("﹪", "%")
        .replace("＋", "+")
        .replace("－", "-"),
    )


def compact_snippet(text: str, start: int, end: int, radius: int = 65) -> str:
    left = max(0, start - radius)
    right = min(len(text), end + radius)
    snippet = text[left:right].strip("，。；;：:、 ")
    return snippet[:180]


@dataclass(frozen=True)
class Deviation:
    value: float
    direction: str
    position: int
    evidence: str


def extract_deviations(text: str) -> list[Deviation]:
    normalized = normalize_text(text)
    deviations: list[Deviation] = []
    seen: set[tuple[int, float, str]] = set()
    for match in re.finditer("偏离值", normalized):
        tail = normalized[match.end() : match.end() + 45]
        number_match = re.search(
            r"(?:累计)?(?:达到|超过|大于|为|达)?(?:约)?([+-]?\d+(?:\.\d+)?)%",
            tail,
        )
        if not number_match:
            continue
        value = float(number_match.group(1))
        prefix = normalized[max(0, match.start() - 28) : match.start()]
        if "跌幅" in prefix or value < 0:
            direction = "down"
        elif "涨幅" in prefix or value > 0 and "涨跌幅" not in prefix:
            direction = "up"
        else:
            direction = "unknown"
        key = (match.start(), value, direction)
        if key in seen:
            continue
        seen.add(key)
        end = match.end() + number_match.end()
        deviations.append(
            Deviation(
                value=value,
                direction=direction,
                position=match.start(),
                evidence=compact_snippet(normalized, match.start(), end),
            )
        )
    return deviations


CHINESE_DAYS = {"二十": 20, "三十": 30}


@dataclass(frozen=True)
class PeriodMention:
    days: int
    position: int
    evidence: str
    related_to_deviation: bool


def extract_period_mentions(text: str) -> list[PeriodMention]:
    normalized = normalize_text(text)
    pattern = re.compile(r"(?:连续|最近|近)?(\d{1,2}|二十|三十)个交易日(?:内)?")
    periods: list[PeriodMention] = []
    for match in pattern.finditer(normalized):
        raw_days = match.group(1)
        days = CHINESE_DAYS.get(raw_days, int(raw_days) if raw_days.isdigit() else 0)
        after = normalized[match.start() : min(len(normalized), match.end() + 160)]
        same_clause = re.split(r"[。；;]", after, maxsplit=1)[0]
        periods.append(
            PeriodMention(
                days=days,
                position=match.start(),
                evidence=compact_snippet(normalized, match.start(), match.end()),
                related_to_deviation=("偏离值" in same_clause),
            )
        )
    return periods


def _explicit_occurrence(text: str) -> tuple[int | None, str | None]:
    normalized = normalize_text(text)[:5000]
    for number, chinese in ((2, "二"), (3, "三")):
        patterns = (
            rf"第{chinese}次.{{0,12}}(?:股票)?(?:交易)?(?:异常波动|异动)",
            rf"(?:股票)?(?:交易)?(?:异常波动|异动).{{0,12}}第{chinese}次",
        )
        for pattern in patterns:
            match = re.search(pattern, normalized)
            if match:
                return number, compact_snippet(normalized, match.start(), match.end())
    return None, None


@dataclass
class Classification:
    direction: str
    categories: list[str] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)
    occurrence_number: int | None = None

    def add(self, category: str, evidence: str | None = None) -> None:
        if category not in self.categories:
            self.categories.append(category)
        if evidence and evidence not in self.evidence:
            self.evidence.append(evidence[:180])


def classify_announcement(title: str, detail_text: str) -> Classification:
    combined = f"{title}\n{detail_text}"
    normalized = normalize_text(combined)
    current_section = normalized[:5000]
    deviations = extract_deviations(current_section)
    periods = extract_period_mentions(current_section)

    directional = sorted(
        (item for item in deviations if item.direction != "unknown"),
        key=lambda item: item.position,
    )
    if directional:
        direction = directional[0].direction
    elif re.search(r"(?:收盘价格|收盘价|股票价格).{0,25}(?:累计下跌|跌幅)", current_section):
        direction = "down"
    elif re.search(r"(?:收盘价格|收盘价|股票价格).{0,25}(?:累计上涨|涨幅)", current_section):
        direction = "up"
    else:
        direction = "unknown"

    result = Classification(direction=direction)

    twenty_deviation = next(
        (item for item in deviations if 19.95 <= abs(item.value) <= 20.05), None
    )
    if twenty_deviation:
        result.add("twenty_percent_deviation", twenty_deviation.evidence)

    twenty_period = next(
        (item for item in periods if item.days == 20 and item.related_to_deviation), None
    )
    if twenty_period:
        result.add("twenty_trading_days", twenty_period.evidence)

    thirty_period = next(
        (item for item in periods if item.days == 30 and item.related_to_deviation), None
    )
    severe_title = "严重异常波动" in normalized
    severe_deviation = next(
        (
            item
            for item in deviations
            if (item.direction == "up" and abs(item.value) >= 199.0)
            or (item.direction == "down" and abs(item.value) >= 69.0)
            or (item.direction == "unknown" and abs(item.value) >= 199.0)
        ),
        None,
    )
    severe_period = next(
        (
            item
            for item in periods
            if 20 <= item.days <= 30 and item.related_to_deviation
        ),
        None,
    )
    if thirty_period or (severe_title and severe_period and severe_deviation):
        evidence = (
            severe_deviation.evidence
            if severe_deviation
            else thirty_period.evidence if thirty_period else severe_period.evidence
        )
        result.add("thirty_day_deviation", evidence)

    explicit_number, explicit_evidence = _explicit_occurrence(combined)
    if explicit_number in (2, 3) and direction == "down":
        result.occurrence_number = explicit_number
        result.add(
            "down_second_occurrence" if explicit_number == 2 else "down_third_occurrence",
            explicit_evidence,
        )
    return result


def is_abnormal_announcement_title(title: str) -> bool:
    normalized = normalize_text(title)
    bond_terms = ("可转换公司债券", "可转债", "债券交易异常波动")
    if any(term in normalized for term in bond_terms) and "股票交易异常波动" not in normalized:
        return False
    return "异常波动" in normalized or bool(
        re.search(r"(?:股票|股价).{0,8}(?:异动).{0,8}(?:公告|风险提示)", normalized)
    )


def stock_name_from_title(title: str, fallback: str) -> str:
    if not re.search(r"[：:]", title):
        return fallback
    candidate = re.split(r"[：:]", title, maxsplit=1)[0].strip()
    candidate = re.sub(r"^(?:\[[^\]]+\]|【[^】]+】)", "", candidate).strip()
    return candidate if 1 <= len(candidate) <= 20 else fallback


def announcement_url(symbol: str, announcement_id: str) -> str:
    normalized_id = (
        announcement_id if announcement_id.startswith("nos") else f"nos{announcement_id}"
    )
    return f"https://gu.qq.com/{symbol}/gp/notice/{normalized_id}"


def _record_from_announcement(
    raw: Mapping[str, Any],
    stock_names: Mapping[str, str],
    client: TencentClient,
) -> dict[str, Any]:
    announcement_id = str(raw.get("id") or "")
    symbol = str(raw.get("symbol") or "").lower()
    title = str(raw.get("title") or "").strip()
    published_at = parse_tencent_time(str(raw.get("time") or ""))
    fallback_name = stock_names.get(symbol, symbol)

    try:
        detail = client.fetch_notice_detail(announcement_id)
        detail_text = str(detail["text"])
        classification = classify_announcement(title, detail_text)
        detail_status = str(detail["status"])
        pdf_url = str(detail.get("pdf_url") or "")
        detail_error = ""
    except Exception as exc:
        detail_text = ""
        classification = Classification(direction="unknown")
        classification.add("manual_review", f"正文解析失败：{exc}")
        detail_status = "error"
        pdf_url = ""
        detail_error = str(exc)

    return {
        "id": announcement_id,
        "symbol": symbol,
        "code": symbol[2:] if len(symbol) > 2 else symbol,
        "name": stock_name_from_title(title, fallback_name),
        "title": title,
        "published_at": published_at.isoformat(timespec="seconds"),
        "direction": classification.direction,
        "categories": classification.categories,
        "category_labels": [CATEGORY_LABELS[item] for item in classification.categories],
        "evidence": classification.evidence[:4],
        "down_occurrence_30d": classification.occurrence_number,
        "detail_status": detail_status,
        "detail_error": detail_error,
        "detail_excerpt": re.sub(r"\s+", " ", detail_text).strip()[:500],
        "tencent_url": announcement_url(symbol, announcement_id),
        "pdf_url": pdf_url,
        "source": "tencent_finance",
    }


def process_candidates(
    announcements: Sequence[Mapping[str, Any]],
    stock_names: Mapping[str, str],
    client: TencentClient,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=client.workers) as executor:
        futures = [
            executor.submit(_record_from_announcement, raw, stock_names, client)
            for raw in announcements
        ]
        for future in as_completed(futures):
            records.append(future.result())
    return sorted(records, key=lambda item: (item["published_at"], item["symbol"]), reverse=True)


def _history_down_events(
    client: TencentClient,
    symbol: str,
    window_start: datetime,
    window_end: datetime,
) -> tuple[list[tuple[datetime, str]], list[str]]:
    raw_history = client.fetch_symbol_notice_history(symbol, window_start, window_end)
    abnormal = [row for row in raw_history if is_abnormal_announcement_title(str(row.get("title") or ""))]
    events: list[tuple[datetime, str]] = []
    errors: list[str] = []
    for row in abnormal:
        announcement_id = str(row.get("id") or "")
        try:
            detail = client.fetch_notice_detail(announcement_id)
            classification = classify_announcement(
                str(row.get("title") or ""), str(detail["text"])
            )
            if classification.direction == "down":
                events.append((parse_tencent_time(str(row.get("time") or "")), announcement_id))
        except Exception as exc:
            errors.append(f"{announcement_id}: {exc}")
    return sorted(set(events)), errors


def enrich_down_occurrences(records: list[dict[str, Any]], client: TencentClient) -> None:
    by_symbol: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        if record["direction"] == "down":
            by_symbol.setdefault(record["symbol"], []).append(record)

    for symbol, current_records in by_symbol.items():
        current_times = [datetime.fromisoformat(item["published_at"]) for item in current_records]
        history_start = min(current_times) - timedelta(days=DOWN_OCCURRENCE_WINDOW_DAYS)
        history_end = max(current_times) + timedelta(minutes=1)
        events, errors = _history_down_events(client, symbol, history_start, history_end)

        for record in current_records:
            current_time = datetime.fromisoformat(record["published_at"])
            lower_bound = current_time - timedelta(days=DOWN_OCCURRENCE_WINDOW_DAYS)
            occurrence = len(
                {
                    event_id
                    for event_time, event_id in events
                    if lower_bound <= event_time <= current_time
                }
            )
            record["down_occurrence_30d"] = occurrence or record.get("down_occurrence_30d")
            if occurrence in (2, 3):
                category = (
                    "down_second_occurrence" if occurrence == 2 else "down_third_occurrence"
                )
                if category not in record["categories"]:
                    record["categories"].append(category)
                    record["category_labels"].append(CATEGORY_LABELS[category])
                    record["evidence"].append(
                        f"按腾讯公告发布时间统计，近{DOWN_OCCURRENCE_WINDOW_DAYS}自然日内第{occurrence}次下跌方向异动"
                    )
            if errors:
                record["occurrence_check_warning"] = "；".join(errors[:3])


def _scan_batches(
    client: TencentClient,
    stocks: Sequence[Mapping[str, str]],
    window_start: datetime,
    window_end: datetime,
    batch_size: int,
) -> list[dict[str, Any]]:
    stock_batches = [
        [str(item["symbol"]) for item in batch]
        for batch in chunks(list(stocks), batch_size)
    ]
    all_rows: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=client.workers) as executor:
        futures = {
            executor.submit(
                client.fetch_notice_window, batch, window_start, window_end
            ): batch
            for batch in stock_batches
        }
        for future in as_completed(futures):
            batch = futures[future]
            try:
                rows = future.result()
            except Exception as exc:
                errors.append(f"{batch[0]}..{batch[-1]}: {exc}")
                continue
            for row in rows:
                announcement_id = str(row.get("id") or "")
                if announcement_id and is_abnormal_announcement_title(
                    str(row.get("title") or "")
                ):
                    all_rows[announcement_id] = row
    if errors:
        raise MonitorError("部分腾讯公告批次扫描失败：" + " | ".join(errors[:5]))
    return list(all_rows.values())


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MonitorError(f"JSON 文件读取失败: {path}: {exc}") from exc


def load_state(path: Path = DEFAULT_STATE_PATH) -> dict[str, Any]:
    state = _read_json(
        path,
        {
            "schema_version": 1,
            "last_successful_scan": None,
            "emailed_ids": {},
            "events": [],
        },
    )
    if not isinstance(state, dict):
        raise MonitorError(f"状态文件结构异常: {path}")
    if not isinstance(state.get("emailed_ids", {}), dict) or not isinstance(
        state.get("events", []), list
    ):
        raise MonitorError(f"状态文件结构异常: {path}")
    state.setdefault("schema_version", 1)
    state.setdefault("last_successful_scan", None)
    state.setdefault("emailed_ids", {})
    state.setdefault("events", [])
    return state


def determine_window_start(
    now: datetime,
    state: Mapping[str, Any],
    lookback_days: int,
    catch_up_previous_day: bool = False,
) -> datetime:
    regular_start = now - timedelta(days=lookback_days)
    last_scan = state.get("last_successful_scan")
    if last_scan:
        try:
            parsed = datetime.fromisoformat(str(last_scan)).astimezone(TIMEZONE)
            # 两小时重叠窗口，抵御腾讯公告入库延迟，邮件仍按公告 ID 去重。
            regular_start = max(parsed - timedelta(hours=2), now - timedelta(days=14))
        except ValueError:
            pass
    if catch_up_previous_day:
        previous_day_start = (now - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        # 早盘重新覆盖昨天全天；已发送公告仍由 emailed_ids 按公告 ID 去重。
        return min(regular_start, previous_day_start)
    return regular_start


@dataclass(frozen=True)
class MailConfig:
    username: str
    password: str
    recipient: str

    @classmethod
    def from_env(cls) -> "MailConfig":
        username = os.environ.get("SMTP_USERNAME", "").strip()
        password = os.environ.get("SMTP_APP_PASSWORD", "").strip()
        recipient = (os.environ.get("ALERT_TO") or username).strip()
        for variable, value in (("SMTP_USERNAME", username), ("ALERT_TO", recipient)):
            if not value or "@" not in value or "\r" in value or "\n" in value:
                raise MonitorError(f"{variable} 不是有效邮箱地址")
        if not password:
            raise MonitorError("缺少 SMTP_APP_PASSWORD（163客户端授权码）")
        return cls(username=username, password=password, recipient=recipient)


def send_email(mail: MailConfig, subject: str, body: str) -> None:
    message = EmailMessage()
    message["From"] = mail.username
    message["To"] = mail.recipient
    message["Subject"] = subject
    message.set_content(body)
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(
        SMTP_HOST, SMTP_PORT, timeout=30, context=context
    ) as client:
        client.login(mail.username, mail.password)
        client.send_message(message)


def build_alert_email(
    records: Sequence[Mapping[str, Any]], scan_at: datetime
) -> tuple[str, str]:
    stock_count = len({str(item["symbol"]) for item in records})
    subject = f"[A股异动公告] {scan_at:%Y-%m-%d} {stock_count}只股票 / {len(records)}条"
    lines = [
        f"盘前异动公告扫描完成：{scan_at:%Y-%m-%d %H:%M:%S}（北京时间）",
        f"共命中 {stock_count} 只股票、{len(records)} 条新公告。",
        "",
    ]
    for index, record in enumerate(records, 1):
        labels = "、".join(record.get("category_labels") or ["待核验"])
        direction = {"up": "上涨", "down": "下跌", "unknown": "待确认"}.get(
            str(record.get("direction")), "待确认"
        )
        lines.extend(
            [
                f"{index}. {record['name']}（{record['code']}）",
                f"发布时间：{record['published_at']}",
                f"命中类型：{labels}",
                f"异动方向：{direction}",
                f"公告标题：{record['title']}",
            ]
        )
        occurrence = record.get("down_occurrence_30d")
        if occurrence:
            lines.append(f"近30自然日下跌异动次数：{occurrence}")
        for evidence in record.get("evidence") or []:
            lines.append(f"依据：{evidence}")
        lines.append(f"腾讯公告：{record['tencent_url']}")
        if record.get("pdf_url"):
            lines.append(f"公告PDF：{record['pdf_url']}")
        lines.append("")
    lines.extend(
        [
            "分类说明：同时覆盖“偏离值20%”“20个交易日”“30日偏离/严重异动”，",
            "下跌方向第2/3次异动按腾讯公告发布时间在近30自然日内计数。",
            "数据源：腾讯财经公开 A 股代码、公告列表及公告正文接口。",
            "本邮件仅用于信息提醒，不构成投资建议。",
        ]
    )
    return subject, "\n".join(lines)


def build_test_email(summary: Mapping[str, Any]) -> tuple[str, str]:
    subject = "[A股异动公告监控] 163邮件配置测试成功"
    body = f"""163邮箱 SMTP 配置测试成功。

扫描时间：{summary['scan_at']}
扫描股票数：{summary['stock_count']}
扫描窗口：{summary['window_start']} 至 {summary['window_end']}
发现异动公告：{summary['abnormal_announcement_count']} 条
命中提醒规则：{summary['matched_count']} 条

正式任务在工作日北京时间 08:00 运行；只有发现尚未发送过的目标公告时才发送提醒。
"""
    return subject, body


def _records_csv(records: Sequence[Mapping[str, Any]]) -> str:
    buffer = StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(
        [
            "published_at",
            "symbol",
            "code",
            "name",
            "title",
            "direction",
            "categories",
            "down_occurrence_30d",
            "tencent_url",
            "pdf_url",
        ]
    )
    for record in records:
        writer.writerow(
            [
                record.get("published_at", ""),
                record.get("symbol", ""),
                record.get("code", ""),
                record.get("name", ""),
                record.get("title", ""),
                record.get("direction", ""),
                "|".join(record.get("category_labels") or []),
                record.get("down_occurrence_30d") or "",
                record.get("tencent_url", ""),
                record.get("pdf_url", ""),
            ]
        )
    return "\ufeff" + buffer.getvalue()


def _history_index(docs_dir: Path, generated_at: str) -> dict[str, Any]:
    days: list[dict[str, str]] = []
    for path in docs_dir.glob("history/[0-9][0-9][0-9][0-9]/*.json"):
        days.append(
            {
                "date": path.stem,
                "json": path.relative_to(docs_dir).as_posix(),
                "csv": path.with_suffix(".csv").relative_to(docs_dir).as_posix(),
            }
        )
    days.sort(key=lambda item: item["date"], reverse=True)
    return {"schema_version": 1, "generated_at": generated_at, "days": days}


def _history_index_html(catalog: Mapping[str, Any]) -> str:
    rows = "".join(
        "<tr>"
        f"<td>{html.escape(item['date'])}</td>"
        f"<td><a href=\"{html.escape(item['json'])}\">JSON</a></td>"
        f"<td><a href=\"{html.escape(item['csv'])}\">CSV</a></td>"
        "</tr>"
        for item in catalog["days"]
    )
    return f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>A股盘前异动公告历史</title>
<style>body{{font-family:"Microsoft YaHei",sans-serif;max-width:900px;margin:32px auto;padding:0 18px;color:#0f172a}}table{{width:100%;border-collapse:collapse}}th,td{{padding:10px;border-bottom:1px solid #e2e8f0;text-align:left}}a{{color:#2563eb}}</style>
</head><body><h1>A股盘前异动公告历史</h1>
<p>腾讯财经公开公告；工作日北京时间08:00扫描。只用于信息提醒，不构成投资建议。</p>
<table><thead><tr><th>扫描日期</th><th>JSON</th><th>CSV</th></tr></thead><tbody>{rows}</tbody></table>
</body></html>
"""


def persist_artifacts(summary: Mapping[str, Any], docs_dir: Path) -> None:
    scan_date = str(summary["scan_at"])[:10]
    year_dir = docs_dir / "history" / scan_date[:4]
    daily_json = year_dir / f"{scan_date}.json"
    daily_csv = year_dir / f"{scan_date}.csv"

    existing = _read_json(
        daily_json,
        {"schema_version": 1, "date": scan_date, "runs": [], "announcements": []},
    )
    runs = list(existing.get("runs") or [])
    runs.append(
        {
            "scan_at": summary["scan_at"],
            "window_start": summary["window_start"],
            "window_end": summary["window_end"],
            "stock_count": summary["stock_count"],
            "abnormal_announcement_count": summary["abnormal_announcement_count"],
            "matched_count": summary["matched_count"],
            "new_email_count": summary["new_email_count"],
        }
    )
    merged = {
        str(item["id"]): item
        for item in [*(existing.get("announcements") or []), *summary["announcements"]]
    }
    daily_payload = {
        "schema_version": 1,
        "date": scan_date,
        "runs": runs[-20:],
        "announcements": sorted(
            merged.values(), key=lambda item: item["published_at"], reverse=True
        ),
    }
    atomic_write_text(
        daily_json, json.dumps(daily_payload, ensure_ascii=False, indent=2) + "\n"
    )
    atomic_write_text(daily_csv, _records_csv(daily_payload["announcements"]))
    atomic_write_text(
        docs_dir / "latest.json",
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
    )
    catalog = _history_index(docs_dir, str(summary["scan_at"]))
    atomic_write_text(
        docs_dir / "index.json", json.dumps(catalog, ensure_ascii=False, indent=2) + "\n"
    )
    atomic_write_text(docs_dir / "index.html", _history_index_html(catalog))


def update_state(
    state: dict[str, Any],
    records: Sequence[Mapping[str, Any]],
    emailed: Sequence[Mapping[str, Any]],
    scan_at: datetime,
) -> dict[str, Any]:
    merged_events = {
        str(item["id"]): dict(item)
        for item in [*(state.get("events") or []), *records]
    }
    cutoff = scan_at - timedelta(days=180)
    state["events"] = sorted(
        (
            item
            for item in merged_events.values()
            if datetime.fromisoformat(str(item["published_at"])) >= cutoff
        ),
        key=lambda item: item["published_at"],
        reverse=True,
    )
    emailed_ids = state.setdefault("emailed_ids", {})
    for item in emailed:
        emailed_ids[str(item["id"])] = {
            "sent_at": scan_at.isoformat(timespec="seconds"),
            "published_at": item["published_at"],
        }
    # 限制状态文件体积，同时保留一年的邮件去重范围。
    for announcement_id, metadata in list(emailed_ids.items()):
        try:
            sent_at = datetime.fromisoformat(str(metadata["sent_at"]))
        except (KeyError, TypeError, ValueError):
            sent_at = scan_at
        if sent_at < scan_at - timedelta(days=365):
            emailed_ids.pop(announcement_id, None)
    state["last_successful_scan"] = scan_at.isoformat(timespec="seconds")
    state["schema_version"] = 1
    return state


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="腾讯财经 A 股异动公告监控")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--state-path", type=Path, default=DEFAULT_STATE_PATH)
    parser.add_argument("--docs-dir", type=Path, default=DEFAULT_DOCS_DIR)
    parser.add_argument("--dry-run", action="store_true", help="扫描但不发邮件、不写状态")
    parser.add_argument("--test-email", action="store_true", help="发送163配置测试邮件，不推进状态")
    parser.add_argument(
        "--catch-up-previous-day",
        action="store_true",
        help="从昨天00:00重新扫描，按公告ID仅补发遗漏项",
    )
    return parser


def run(args: argparse.Namespace, now: datetime | None = None) -> dict[str, Any]:
    if not 1 <= args.lookback_days <= 30:
        raise MonitorError("--lookback-days 必须在 1 到 30 之间")
    if not 10 <= args.batch_size <= 200:
        raise MonitorError("--batch-size 必须在 10 到 200 之间")
    if not 1 <= args.workers <= 16:
        raise MonitorError("--workers 必须在 1 到 16 之间")

    scan_at = (now or datetime.now(TIMEZONE)).astimezone(TIMEZONE)
    state = load_state(args.state_path)
    window_start = determine_window_start(
        scan_at,
        state,
        args.lookback_days,
        catch_up_previous_day=args.catch_up_previous_day,
    )
    client = TencentClient(timeout=args.timeout, retries=args.retries, workers=args.workers)

    stocks = client.fetch_a_share_universe()
    stock_names = {item["symbol"]: item["name"] for item in stocks}
    raw_candidates = _scan_batches(
        client, stocks, window_start, scan_at, args.batch_size
    )
    records = process_candidates(raw_candidates, stock_names, client)
    enrich_down_occurrences(records, client)
    matched = [item for item in records if item.get("categories")]
    emailed_ids = state.get("emailed_ids") or {}
    new_alerts = [item for item in matched if str(item["id"]) not in emailed_ids]

    summary: dict[str, Any] = {
        "schema_version": 1,
        "scan_at": scan_at.isoformat(timespec="seconds"),
        "window_start": window_start.isoformat(timespec="seconds"),
        "window_end": scan_at.isoformat(timespec="seconds"),
        "scan_mode": (
            "previous-day-catch-up" if args.catch_up_previous_day else "incremental"
        ),
        "stock_count": len(stocks),
        "abnormal_announcement_count": len(records),
        "matched_count": len(matched),
        "new_email_count": len(new_alerts),
        "announcements": records,
        "matched_announcements": matched,
        "new_alerts": new_alerts,
        "source": "Tencent Finance public endpoints",
    }

    if args.dry_run:
        return summary
    if args.test_email:
        subject, body = build_test_email(summary)
        send_email(MailConfig.from_env(), subject, body)
        return summary

    if new_alerts:
        subject, body = build_alert_email(new_alerts, scan_at)
        send_email(MailConfig.from_env(), subject, body)

    update_state(state, records, new_alerts, scan_at)
    atomic_write_text(
        args.state_path, json.dumps(state, ensure_ascii=False, indent=2) + "\n"
    )
    persist_artifacts(summary, args.docs_dir)
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        summary = run(args)
        print(
            "扫描完成："
            f"A股 {summary['stock_count']} 只，"
            f"异动公告 {summary['abnormal_announcement_count']} 条，"
            f"命中 {summary['matched_count']} 条，"
            f"新邮件 {summary['new_email_count']} 条"
        )
        for item in summary["new_alerts"]:
            print(
                f"- {item['name']}({item['code']}) "
                f"{'/'.join(item['category_labels'])} {item['title']}"
            )
        return 0
    except Exception as exc:
        message = f"A股盘前异动公告监控失败：{exc}"
        print(message, file=sys.stderr)
        if not args.dry_run:
            try:
                send_email(
                    MailConfig.from_env(),
                    "[A股异动公告监控失败] 请检查 GitHub Actions",
                    message + "\n\n请打开 GitHub Actions 查看本次运行日志。",
                )
            except Exception as mail_exc:
                print(f"失败通知邮件也未能发送：{mail_exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
