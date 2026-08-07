# -*- coding: utf-8 -*-

import json
from pathlib import Path
import tempfile
import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from market_temperature import (
    AppConfig,
    RawMarketSnapshot,
    SnapshotRepository,
    TencentFinanceClient,
    build_dashboard_document,
    market_slots,
)


class TencentPayloadTests(unittest.TestCase):
    def test_parse_day_payload_uses_cumulative_amount(self):
        payload = json.dumps(
            {
                "code": 0,
                "msg": "",
                "data": {
                    "sh000001": {
                        "data": [
                            {
                                "date": "20260807",
                                "data": [
                                    "0930 3896.49 4763106 9005773968.70",
                                    "0940 3899.64 88390696 180556725442.20",
                                ],
                            }
                        ]
                    }
                },
            }
        )
        rows = TencentFinanceClient.parse_day_payload(payload, "sh000001")
        self.assertEqual(rows[("2026-08-07", "09:30")].amount_yuan, 9_005_773_969)
        self.assertEqual(rows[("2026-08-07", "09:40")].amount_yuan, 180_556_725_442)

    def test_parse_quote_payload_sums_exact_amount_field(self):
        sh_fields = [""] * 50
        sz_fields = [""] * 50
        sh_fields[30] = "20260807143000"
        sz_fields[30] = "20260807143030"
        sh_fields[35] = "3900/100/100000000000"
        sz_fields[35] = "14000/100/150000000000"
        payload = f'v_sh000001="{"~".join(sh_fields)}";v_sz399001="{"~".join(sz_fields)}";'

        row = TencentFinanceClient.parse_quote_payload(
            payload,
            ["sh000001", "sz399001"],
            ZoneInfo("Asia/Shanghai"),
        )
        self.assertEqual(row.trade_date, "2026-08-07")
        self.assertEqual(row.minute, "14:30")
        self.assertEqual(row.total_amount_yuan, 250_000_000_000)


class RepositoryTests(unittest.TestCase):
    def test_previous_trading_day_same_time_temperature_is_persisted(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            repository = SnapshotRepository(Path(temp_dir) / "temperature.sqlite3")
            captured_at = "2026-08-07T10:00:05+08:00"
            repository.upsert(
                [
                    RawMarketSnapshot(
                        "2026-08-06",
                        "10:00",
                        "2026-08-06T10:00:00+08:00",
                        40_000_000_000,
                        60_000_000_000,
                        100_000_000_000,
                    ),
                    RawMarketSnapshot(
                        "2026-08-06",
                        "15:00",
                        "2026-08-06T15:00:00+08:00",
                        400_000_000_000,
                        600_000_000_000,
                        1_000_000_000_000,
                    ),
                    RawMarketSnapshot(
                        "2026-08-07",
                        "10:00",
                        "2026-08-07T10:00:00+08:00",
                        50_000_000_000,
                        70_000_000_000,
                        120_000_000_000,
                    ),
                ],
                captured_at,
            )
            repository.recalculate()

            current = repository.rows_for_date("2026-08-07")[0]
            self.assertEqual(current["previous_trade_date"], "2026-08-06")
            self.assertEqual(current["previous_total_amount_yuan"], 100_000_000_000)
            self.assertEqual(current["delta_amount_yuan"], 20_000_000_000)
            self.assertAlmostEqual(current["temperature_pct"], 20.0)
            self.assertEqual(current["delta_vs_previous_close_yuan"], -880_000_000_000)

    def test_dashboard_contains_zero_axis_and_signed_temperature(self):
        row = {
            "minute": "10:00",
            "total_amount_yuan": 120_000_000_000,
            "previous_trade_date": "2026-08-06",
            "previous_total_amount_yuan": 100_000_000_000,
            "delta_amount_yuan": 20_000_000_000,
            "temperature_pct": 20.0,
            "delta_vs_previous_close_yuan": -880_000_000_000,
        }
        document = build_dashboard_document(
            "2026-08-07",
            [row],
            "2026-08-07T10:00:05+08:00",
        )
        self.assertIn("以 0 轴为基准", document)
        self.assertIn("+20.00%", document)
        self.assertIn("红色=放量", document)


class ScheduleTests(unittest.TestCase):
    def test_default_schedule_has_26_ten_minute_points(self):
        slots = market_slots(
            "2026-08-07",
            (("09:30", "11:30"), ("13:00", "15:00")),
            10,
            ZoneInfo("Asia/Shanghai"),
        )
        self.assertEqual(len(slots), 26)
        self.assertEqual(slots[0].strftime("%H:%M"), "09:30")
        self.assertEqual(slots[-1].strftime("%H:%M"), "15:00")


if __name__ == "__main__":
    unittest.main()
