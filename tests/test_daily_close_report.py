# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

import daily_close_report as report


class RecipientTests(unittest.TestCase):
    def test_recipients_support_self_multiple_separators_and_dedupe(self) -> None:
        actual = report.parse_recipients(
            "SELF, secondary@example.com;OWNER@163.com\nthird@example.com",
            "owner@163.com",
        )
        self.assertEqual(
            actual,
            ("owner@163.com", "secondary@example.com", "third@example.com"),
        )

    def test_recipients_default_to_self(self) -> None:
        self.assertEqual(report.parse_recipients("", "owner@163.com"), ("owner@163.com",))

    def test_recipients_reject_invalid_address(self) -> None:
        with self.assertRaises(report.CloseReportError):
            report.parse_recipients("bad-address", "owner@163.com")

    def test_watchlist_normalizes_exchange_prefixes(self) -> None:
        self.assertEqual(
            report.parse_watchlist("600519,300750;920001 600519"),
            ("sh600519", "sz300750", "bj920001"),
        )


class ClassificationTests(unittest.TestCase):
    def test_index_strength_and_tail(self) -> None:
        self.assertEqual(report.classify_index(0.8, 0.35), ("强", "尾盘拉升"))
        self.assertEqual(report.classify_index(-0.8, -0.4), ("弱", "尾盘下挫"))
        self.assertEqual(report.classify_index(0.1, 0.05), ("中性", "尾盘平稳"))

    def test_turnover_labels(self) -> None:
        self.assertEqual(report.classify_turnover(5.1), "放量")
        self.assertEqual(report.classify_turnover(-5.1), "缩量")
        self.assertEqual(report.classify_turnover(5.0), "基本持平")

    def test_limit_status_for_all_price_bands_and_no_limit_stock(self) -> None:
        for previous, limit_up in ((10.0, 10.5), (10.0, 11.0), (10.0, 12.0), (10.0, 13.0)):
            status = report.limit_status(
                {
                    "price": limit_up,
                    "high": limit_up,
                    "low": previous,
                    "limit_up": limit_up,
                    "limit_down": round(previous - (limit_up - previous), 2),
                }
            )
            self.assertTrue(status["sealed_up"])
        self.assertFalse(
            report.limit_status(
                {"price": 15, "high": 16, "low": 10, "limit_up": -1, "limit_down": -1}
            )["has_limit"]
        )

    def test_failed_limit_is_marked(self) -> None:
        status = report.limit_status(
            {"price": 10.7, "high": 11.0, "low": 10.1, "limit_up": 11.0, "limit_down": 9.0}
        )
        self.assertTrue(status["touched_up"])
        self.assertTrue(status["failed_up"])
        self.assertFalse(status["sealed_up"])

    def test_tail_flow_all_five_labels(self) -> None:
        self.assertEqual(report.classify_tail_flow(-0.1, 0.3, 1.0, 3, 0.5), "回流")
        self.assertEqual(report.classify_tail_flow(0.3, 0.3, 1.2, 4, 0.7), "抢筹")
        self.assertEqual(report.classify_tail_flow(0.2, -0.3, 1.2, 1, 0.3), "兑现")
        self.assertEqual(report.classify_tail_flow(0.2, 0.15, 1.0, 1, 0.4), "分化")
        self.assertEqual(report.classify_tail_flow(0.1, 0.05, 1.0, 3, 0.55), "平稳")

    def test_market_state_strong_weak_and_structural(self) -> None:
        strong = report.compute_market_state(2, 0.7, 4000, 1000, 80, 2, 15)
        weak = report.compute_market_state(-2, -0.7, 1000, 4000, 2, 80, -15)
        structural = report.compute_market_state(0.2, 0.0, 1500, 3000, 20, 15, 0)
        self.assertEqual(strong[1], "强势进攻")
        self.assertEqual(weak[1], "弱势退潮")
        self.assertEqual(structural[1], "结构分化")

    def test_leader_strength_and_weakness(self) -> None:
        strong = {
            "tail_change_pct": 1.2,
            "high": 11,
            "low": 9,
            "price": 10.8,
            "main_net_wan": 500,
        }
        weak = {
            "tail_change_pct": -1.2,
            "high": 11,
            "low": 9,
            "price": 9.4,
            "main_net_wan": -500,
        }
        self.assertEqual(report.classify_leader_state(strong, None), "加强")
        self.assertEqual(report.classify_leader_state(weak, None), "走弱")
        self.assertEqual(
            report.classify_leader_state(strong, {"sealed_up": False, "failed_up": True}),
            "走弱",
        )

    def test_leader_score_has_expected_components(self) -> None:
        stock = {
            "change_pct": 10,
            "previous_close": 10,
            "limit_up": 11,
            "main_net_ratio_pct": 15,
            "tail_change_pct": 1,
        }
        score, components = report.score_leader(stock, 1, 5000, 20)
        self.assertEqual(score, 100)
        self.assertEqual(set(components), {"liquidity", "day_strength", "main_flow", "sector", "tail"})


class BreadthAndStateTests(unittest.TestCase):
    def test_breadth_counts_and_label(self) -> None:
        stocks = [
            {"symbol": f"sh60{index:04d}", "price": 10, "change_pct": 1}
            for index in range(4100)
        ]
        stocks.extend(
            {"symbol": f"sz00{index:04d}", "price": 10, "change_pct": -1}
            for index in range(1000)
        )
        actual = report.build_breadth(stocks)
        self.assertEqual(actual["up_count"], 4100)
        self.assertEqual(actual["down_count"], 1000)
        self.assertEqual(actual["label"], "强赚钱效应")

    def test_sentiment_counts_exact_limits(self) -> None:
        stocks = [{"symbol": "sh600001"}, {"symbol": "sz300001"}, {"symbol": "bj920001"}]
        quotes = {
            "sh600001": {"price": 11, "high": 11, "low": 10, "limit_up": 11, "limit_down": 9},
            "sz300001": {"price": 11, "high": 12, "low": 10, "limit_up": 12, "limit_down": 8},
            "bj920001": {"price": 7, "high": 10, "low": 7, "limit_up": 13, "limit_down": 7},
        }
        summary, statuses = report.build_sentiment(stocks, quotes)
        self.assertEqual(summary["limit_up_count"], 1)
        self.assertEqual(summary["failed_up_count"], 1)
        self.assertEqual(summary["limit_down_count"], 1)
        self.assertEqual(len(statuses), 3)

    def test_select_trade_date_skips_incomplete_day(self) -> None:
        point = report.MinutePoint("14:30", 10, 1, 1)
        histories = {
            symbol: [report.DaySeries("2026-08-10", 9, {"14:30": point})]
            for symbol, _, _ in report.INDEX_SPECS
        }
        now = datetime(2026, 8, 10, 15, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
        with self.assertRaises(report.NonTradingDay):
            report.select_trade_date(histories, now, False)
        self.assertEqual(report.select_trade_date(histories, now, True), ("2026-08-10", False))


class DeliveryStateTests(unittest.TestCase):
    def test_partial_failure_is_persisted_and_retry_only_sends_missing_recipient(self) -> None:
        mail = report.MailConfig(
            username="owner@163.com",
            password="secret",
            recipients=("owner@163.com", "secondary@example.com"),
        )
        minimal = {"trade_date": "2026-08-10", "report_hash": "hash-1"}
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "state.json"
            attempts: list[str] = []

            def first_send(_mail, recipient, *_args, **_kwargs):
                attempts.append(recipient)
                if recipient == "secondary@example.com":
                    raise report.CloseReportError("temporary failure")

            with patch.object(report, "build_email", return_value=("s", "p", "h")), patch.object(
                report, "_send_one", side_effect=first_send
            ):
                with self.assertRaises(report.CloseReportError):
                    report.send_report(mail, minimal, state_path)

            saved = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(
                report.delivered_recipients(saved, "2026-08-10", "hash-1"),
                {report.recipient_delivery_key("owner@163.com")},
            )
            retry_attempts: list[str] = []
            with patch.object(report, "build_email", return_value=("s", "p", "h")), patch.object(
                report, "_send_one", side_effect=lambda _mail, recipient, *_args, **_kwargs: retry_attempts.append(recipient)
            ):
                sent, skipped = report.send_report(mail, minimal, state_path)
            self.assertEqual(sent, ["secondary@example.com"])
            self.assertEqual(skipped, ["owner@163.com"])
            self.assertEqual(retry_attempts, ["secondary@example.com"])

    def test_plaintext_recipient_state_is_migrated_to_hash(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "reports": {
                            "2026-08-10": {
                                "hash-1": {
                                    "sent_to": {"owner@163.com": "2026-08-10T15:31:00+08:00"}
                                }
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            migrated = report.load_delivery_state(state_path)
            keys = report.delivered_recipients(migrated, "2026-08-10", "hash-1")
            self.assertEqual(keys, {report.recipient_delivery_key("owner@163.com")})
            self.assertFalse(any("@" in key for key in keys))
            self.assertEqual(migrated["schema_version"], 2)


class OutlookTests(unittest.TestCase):
    def test_outlook_produces_evidence_based_opportunity_and_risk(self) -> None:
        fixture = {
            "sectors": [
                {
                    "name": "机器人",
                    "change_pct": 3,
                    "change_5d_pct": 5,
                    "change_20d_pct": 12,
                    "tail_change_pct": 0.5,
                    "leader_name": "龙头A",
                    "leader_state": "加强",
                },
                {
                    "name": "高位题材",
                    "change_pct": 1,
                    "change_5d_pct": 10,
                    "change_20d_pct": 30,
                    "tail_change_pct": -0.5,
                    "leader_name": "龙头B",
                    "leader_state": "走弱",
                },
            ],
            "breadth": {"advance_decline_ratio": 1.2, "median_change_pct": -0.1},
            "turnover": {"change_pct": -6},
            "sentiment": {
                "label": "情绪分歧",
                "limit_up_count": 20,
                "limit_down_count": 12,
                "limit_ratio": 1.2,
                "seal_rate_pct": 50,
            },
            "tail_flow": {"label": "兑现", "weighted_tail_pct": -0.3, "tail_intensity": 1.2},
            "market": {
                "state": "结构分化",
                "weighted_index_pct": 0.2,
                "sector_concentrated": True,
                "top5_sector_average_pct": 2.5,
            },
        }
        opportunities, risks = report.generate_outlook(fixture)
        self.assertTrue(any("机器人" in item for item in opportunities))
        self.assertTrue(any("高位题材" in item for item in risks))
        self.assertLessEqual(len(opportunities), 3)
        self.assertLessEqual(len(risks), 3)

    def test_outlook_always_answers_opportunity_and_risk_with_evidence(self) -> None:
        fixture = {
            "sectors": [
                {
                    "name": "银行",
                    "change_pct": 0.3,
                    "change_5d_pct": 0.8,
                    "change_20d_pct": 2.0,
                    "tail_change_pct": 0.0,
                    "leader_name": "银行A",
                    "leader_state": "分歧",
                }
            ],
            "breadth": {"advance_decline_ratio": 1.1, "median_change_pct": 0.1},
            "turnover": {"change_pct": 1.0, "label": "基本持平"},
            "sentiment": {
                "label": "情绪分歧",
                "limit_up_count": 20,
                "limit_down_count": 5,
                "limit_ratio": 4.0,
                "seal_rate_pct": 60,
            },
            "tail_flow": {"label": "平稳", "weighted_tail_pct": 0, "tail_intensity": 1},
            "market": {
                "state": "平衡震荡",
                "weighted_index_pct": 0.1,
                "sector_concentrated": False,
                "top5_sector_average_pct": 0.3,
            },
        }
        opportunities, risks = report.generate_outlook(fixture)
        self.assertIn("银行", opportunities[0])
        self.assertIn("涨停20家", risks[0])


if __name__ == "__main__":
    unittest.main()
