# -*- coding: utf-8 -*-

from datetime import datetime
import json
from pathlib import Path
import tempfile
import unittest
from zoneinfo import ZoneInfo

from abnormal_announcement_monitor import (
    CATEGORY_LABELS,
    classify_announcement,
    determine_window_start,
    is_abnormal_announcement_title,
    parse_json_variable,
    persist_artifacts,
)


TIMEZONE = ZoneInfo("Asia/Shanghai")


class TencentPayloadTests(unittest.TestCase):
    def test_json_variable_is_parsed(self):
        payload = parse_json_variable('finance_notice={"code":0,"data":[]};', "finance_notice")
        self.assertEqual(payload["code"], 0)

    def test_abnormal_title_filter(self):
        self.assertTrue(is_abnormal_announcement_title("某公司：股票交易异常波动公告"))
        self.assertTrue(is_abnormal_announcement_title("某公司股价异动风险提示公告"))
        self.assertFalse(is_abnormal_announcement_title("某公司：第二次临时股东会决议公告"))
        self.assertFalse(is_abnormal_announcement_title("某公司可转换公司债券交易异常波动公告"))


class ClassificationTests(unittest.TestCase):
    def test_twenty_percent_deviation_is_detected(self):
        text = (
            "公司股票连续3个交易日收盘价格涨幅偏离值累计超过20%，"
            "根据交易规则，属于股票交易异常波动情形。"
        )
        result = classify_announcement("股票交易异常波动公告", text)
        self.assertEqual(result.direction, "up")
        self.assertIn("twenty_percent_deviation", result.categories)

    def test_twenty_trading_days_is_detected(self):
        text = "公司股票连续20个交易日内收盘价格涨幅偏离值累计达到100%，属于严重异常波动。"
        result = classify_announcement("股票交易严重异常波动公告", text)
        self.assertIn("twenty_trading_days", result.categories)

    def test_early_30_day_threshold_is_detected(self):
        text = (
            "公司连续28个交易日内日收盘价涨幅偏离值累计达到202.36%（超过200%），"
            "属于股票交易严重异常波动情形。"
        )
        result = classify_announcement("关于股票交易严重异常波动暨风险提示的公告", text)
        self.assertEqual(result.direction, "up")
        self.assertIn("thirty_day_deviation", result.categories)

    def test_30_day_volume_risk_is_not_mistaken_for_deviation(self):
        text = (
            "公司股票连续3个交易日收盘价格涨幅偏离值累计超过20%，属于异常波动。"
            "公司最近30个交易日成交量较大，实际换手率较高，存在市场情绪过热风险。"
        )
        result = classify_announcement("股票交易异常波动暨风险提示公告", text)
        self.assertNotIn("thirty_day_deviation", result.categories)

    def test_down_direction_ignores_future_drop_risk_wording(self):
        text = (
            "公司股票收盘价格涨幅偏离值累计超过20%，属于股票交易异常波动。"
            "股票短期上涨较快，未来可能存在快速下跌风险。"
        )
        result = classify_announcement("股票交易异常波动暨风险提示公告", text)
        self.assertEqual(result.direction, "up")

    def test_explicit_second_down_occurrence_is_detected(self):
        text = (
            "公司股票收盘价格跌幅偏离值累计超过20%，属于股票交易异常波动。"
            "这是公司近期开具的第二次股票交易异常波动公告。"
        )
        result = classify_announcement("股票交易异常波动公告", text)
        self.assertEqual(result.direction, "down")
        self.assertEqual(result.occurrence_number, 2)
        self.assertIn("down_second_occurrence", result.categories)


class WindowTests(unittest.TestCase):
    def test_first_run_uses_configured_lookback(self):
        now = datetime(2026, 8, 10, 8, 0, tzinfo=TIMEZONE)
        start = determine_window_start(now, {"last_successful_scan": None}, 7)
        self.assertEqual(start, datetime(2026, 8, 3, 8, 0, tzinfo=TIMEZONE))

    def test_later_run_has_two_hour_overlap(self):
        now = datetime(2026, 8, 11, 8, 0, tzinfo=TIMEZONE)
        state = {"last_successful_scan": "2026-08-10T08:00:00+08:00"}
        start = determine_window_start(now, state, 7)
        self.assertEqual(start, datetime(2026, 8, 10, 6, 0, tzinfo=TIMEZONE))

    def test_morning_catchup_rescans_previous_calendar_day(self):
        now = datetime(2026, 8, 11, 8, 0, tzinfo=TIMEZONE)
        state = {"last_successful_scan": "2026-08-10T21:00:00+08:00"}
        start = determine_window_start(
            now, state, 3, catch_up_previous_day=True
        )
        self.assertEqual(start, datetime(2026, 8, 10, 0, 0, tzinfo=TIMEZONE))

    def test_catchup_keeps_older_unfinished_window(self):
        now = datetime(2026, 8, 11, 8, 0, tzinfo=TIMEZONE)
        state = {"last_successful_scan": "2026-08-08T21:00:00+08:00"}
        start = determine_window_start(
            now, state, 3, catch_up_previous_day=True
        )
        self.assertEqual(start, datetime(2026, 8, 8, 19, 0, tzinfo=TIMEZONE))


class ArtifactTests(unittest.TestCase):
    def test_json_csv_and_index_are_persisted(self):
        record = {
            "id": "nos1",
            "symbol": "sz000001",
            "code": "000001",
            "name": "测试银行",
            "title": "股票交易异常波动公告",
            "published_at": "2026-08-09T18:00:00+08:00",
            "direction": "up",
            "categories": ["twenty_percent_deviation"],
            "category_labels": [CATEGORY_LABELS["twenty_percent_deviation"]],
            "evidence": ["偏离值累计超过20%"],
            "down_occurrence_30d": None,
            "detail_status": "inline",
            "detail_error": "",
            "detail_excerpt": "测试",
            "tencent_url": "https://gu.qq.com/sz000001/gp/notice/nos1",
            "pdf_url": "",
            "source": "tencent_finance",
        }
        summary = {
            "schema_version": 1,
            "scan_at": "2026-08-10T08:00:00+08:00",
            "window_start": "2026-08-03T08:00:00+08:00",
            "window_end": "2026-08-10T08:00:00+08:00",
            "stock_count": 5500,
            "abnormal_announcement_count": 1,
            "matched_count": 1,
            "new_email_count": 1,
            "announcements": [record],
            "matched_announcements": [record],
            "new_alerts": [record],
            "source": "Tencent Finance public endpoints",
        }
        with tempfile.TemporaryDirectory() as temporary:
            docs = Path(temporary)
            persist_artifacts(summary, docs)
            daily = docs / "history" / "2026" / "2026-08-10.json"
            self.assertTrue(daily.exists())
            payload = json.loads(daily.read_text(encoding="utf-8"))
            self.assertEqual(payload["announcements"][0]["id"], "nos1")
            self.assertTrue((docs / "history" / "2026" / "2026-08-10.csv").exists())
            self.assertTrue((docs / "index.html").exists())


if __name__ == "__main__":
    unittest.main()
