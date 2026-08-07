# -*- coding: utf-8 -*-

from datetime import datetime
import json
from pathlib import Path
import tempfile
import unittest
from zoneinfo import ZoneInfo

from cloud_monitor import (
    alert_already_sent,
    alert_direction,
    is_in_trading_session,
    persist_history_artifacts,
    record_alert,
)


class AlertLogicTests(unittest.TestCase):
    def test_threshold_is_strict(self):
        self.assertIsNone(alert_direction(30.0))
        self.assertIsNone(alert_direction(-30.0))
        self.assertEqual(alert_direction(30.0001), "hot")
        self.assertEqual(alert_direction(-30.0001), "cold")

    def test_alert_is_deduplicated_by_date_and_direction(self):
        state = {"schema_version": 1, "alerts": {}}
        self.assertFalse(alert_already_sent(state, "2026-08-07", "hot"))
        record_alert(state, "2026-08-07", "hot", 31.2, "2026-08-07T10:00:00+08:00")
        self.assertTrue(alert_already_sent(state, "2026-08-07", "hot"))
        self.assertFalse(alert_already_sent(state, "2026-08-07", "cold"))


class SessionTests(unittest.TestCase):
    def test_a_share_sessions(self):
        timezone = ZoneInfo("Asia/Shanghai")
        sessions = (("09:30", "11:30"), ("13:00", "15:00"))
        self.assertTrue(is_in_trading_session(datetime(2026, 8, 7, 9, 30, tzinfo=timezone), sessions))
        self.assertFalse(is_in_trading_session(datetime(2026, 8, 7, 12, 0, tzinfo=timezone), sessions))
        self.assertFalse(is_in_trading_session(datetime(2026, 8, 8, 10, 0, tzinfo=timezone), sessions))


class HistoryArtifactTests(unittest.TestCase):
    def test_daily_json_csv_and_catalog_are_written(self):
        payload = {
            "schema_version": 1,
            "generated_at": "2026-08-07T10:00:05+08:00",
            "trade_date": "2026-08-07",
            "latest": {"minute": "10:00"},
            "series": [
                {
                    "trade_date": "2026-08-07",
                    "minute": "10:00",
                    "shanghai_amount_yuan": 50,
                    "shenzhen_amount_yuan": 70,
                    "total_amount_yuan": 120,
                    "previous_trade_date": "2026-08-06",
                    "previous_total_amount_yuan": 100,
                    "delta_amount_yuan": 20,
                    "temperature_pct": 20.0,
                    "source": "tencent",
                    "quote_at": "2026-08-07T10:00:00+08:00",
                }
            ],
        }
        with tempfile.TemporaryDirectory() as temporary:
            docs = Path(temporary)
            (docs / "index.html").write_text("<main></main>", encoding="utf-8")
            paths = persist_history_artifacts(payload, docs)
            self.assertTrue(paths["json"].exists())
            self.assertTrue(paths["csv"].exists())
            catalog = json.loads(paths["catalog"].read_text(encoding="utf-8"))
            self.assertEqual(catalog["days"][0]["trade_date"], "2026-08-07")
            self.assertIn("history/index.html", (docs / "index.html").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
