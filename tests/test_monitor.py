from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tip_v1.config import get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.cli.monitor import build_monitor_report, save_monitor_snapshot


class MonitorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "monitor.sqlite3"
        self.settings = get_settings(self.db_path)
        initialize_database(self.settings)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_monitor_report_includes_storage_section_and_counts(self) -> None:
        with managed_connection(self.settings) as connection:
            connection.execute(
                """
                INSERT INTO microstructure_raw_book(asset_id, observed_at, source_hash, raw_payload)
                VALUES ('asset-a', 1700000000, 'hash-a', '{}')
                """
            )
            raw_book_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            connection.execute(
                """
                INSERT INTO microstructure_book_snapshots(
                    snapshot_sequence_id, raw_book_id, asset_id, market_id, timestamp,
                    best_bid, best_ask, mid_price, spread
                )
                VALUES (1, ?, 'asset-a', 'market-a', 1700000000, 0.40, 0.44, 0.42, 0.04)
                """,
                (raw_book_id,),
            )
            connection.execute(
                """
                INSERT INTO microstructure_agg_1m(
                    asset_id, minute_ts, avg_bid, avg_ask, spread_avg, spread_std, mid_price_avg, snapshot_count
                )
                VALUES ('asset-a', 1699999980, 0.40, 0.44, 0.04, 0.0, 0.42, 1)
                """
            )
            connection.execute(
                """
                INSERT INTO market_price_history(asset_id, timestamp, price)
                VALUES ('asset-a', 1700000000, 0.42)
                """
            )
            connection.execute(
                """
                INSERT INTO price_history_agg_1m(
                    asset_id, minute_ts, open_price, high_price, low_price, close_price, sample_count
                )
                VALUES ('asset-a', 1699999980, 0.42, 0.42, 0.42, 0.42, 1)
                """
            )
            connection.commit()

        report = build_monitor_report(self.settings)

        self.assertIn("=== STORAGE ===", report)
        self.assertIn("microstructure_raw_book:", report)
        self.assertIn("microstructure_agg_1m:", report)
        self.assertIn("market_price_history:", report)
        self.assertIn("price_history_agg_1m:", report)
        self.assertIn("micro retention check:", report)

    def test_monitor_report_shows_deltas_against_previous_run(self) -> None:
        with managed_connection(self.settings) as connection:
            connection.execute(
                """
                INSERT INTO candidate_wallets(wallet, status)
                VALUES ('0xone', 'NEW')
                """
            )
            connection.commit()

        save_monitor_snapshot(self.settings)

        with managed_connection(self.settings) as connection:
            connection.execute(
                """
                INSERT INTO candidate_wallets(wallet, status, profiled_at)
                VALUES ('0xtwo', 'PROFILED', CURRENT_TIMESTAMP)
                """
            )
            connection.execute(
                """
                INSERT INTO wallet_rankings(wallet, regime, followability_score, lifecycle_score, rank_score, tier)
                VALUES ('0xtwo', 'global', 0.6, 0.5, 0.7, 'A')
                """
            )
            connection.commit()

        report = build_monitor_report(self.settings)

        self.assertIn("=== PROGRESS ===", report)
        self.assertIn("Candidates:       2   (+1)", report)
        self.assertIn("Profiled:         1   (+1)", report)
        self.assertIn("Ranked:           1   (+1)", report)
        self.assertIn("Good traders:     1   (+1)", report)

    def test_monitor_report_includes_emerging_traders_section(self) -> None:
        with managed_connection(self.settings) as connection:
            connection.execute(
                """
                INSERT INTO wallet_profiles(
                    wallet, version, trader_type, confidence,
                    features_json, metrics_json, behavior_json, edge_json, failures_json
                )
                VALUES (?, 1, 'NOISE', 0.2, '{}', ?, '{}', '[]', '[]')
                """,
                (
                    "0x70ec111111111111111111111111111111111111",
                    '{"pnl_per_minute":0.5779,"lifecycle_score":0.39,"followability_score":0.0,"is_followable":false}',
                ),
            )
            connection.execute(
                """
                INSERT INTO wallet_profiles(
                    wallet, version, trader_type, confidence,
                    features_json, metrics_json, behavior_json, edge_json, failures_json
                )
                VALUES (?, 1, 'NOISE', 0.2, '{}', ?, '{}', '[]', '[]')
                """,
                (
                    "0x3d7a222222222222222222222222222222222222",
                    '{"pnl_per_minute":0.0533,"lifecycle_score":0.48,"followability_score":0.0,"is_followable":false}',
                ),
            )
            connection.commit()

        report = build_monitor_report(self.settings)

        self.assertIn("=== EMERGING TRADERS ===", report)
        self.assertIn("0x70ec...1111", report)
        self.assertIn("0.5779", report)
        self.assertIn("0.390", report)
        self.assertIn("0x3d7a...2222", report)
        self.assertIn("0.0533", report)

    def test_monitor_report_includes_pnl_distribution(self) -> None:
        with managed_connection(self.settings) as connection:
            connection.execute(
                """
                INSERT INTO wallet_profiles(
                    wallet, version, trader_type, confidence,
                    features_json, metrics_json, behavior_json, edge_json, failures_json
                )
                VALUES (?, 1, 'NOISE', 0.2, '{}', ?, '{}', '[]', '[]')
                """,
                (
                    "0xpnlhigh",
                    '{"total_pnl":1500.0,"pnl_per_day":250.0,"pnl_per_minute":0.1,"lifecycle_score":0.5,"followability_score":0.0,"is_followable":false}',
                ),
            )
            connection.execute(
                """
                INSERT INTO wallet_profiles(
                    wallet, version, trader_type, confidence,
                    features_json, metrics_json, behavior_json, edge_json, failures_json
                )
                VALUES (?, 1, 'NOISE', 0.2, '{}', ?, '{}', '[]', '[]')
                """,
                (
                    "0xpnlmid",
                    '{"total_pnl":500.0,"pnl_per_day":100.0,"pnl_per_minute":0.05,"lifecycle_score":0.4,"followability_score":0.0,"is_followable":false}',
                ),
            )
            connection.execute(
                """
                INSERT INTO wallet_profiles(
                    wallet, version, trader_type, confidence,
                    features_json, metrics_json, behavior_json, edge_json, failures_json
                )
                VALUES (?, 1, 'NOISE', 0.2, '{}', ?, '{}', '[]', '[]')
                """,
                (
                    "0xpnllow",
                    '{"total_pnl":10.0,"pnl_per_day":5.0,"pnl_per_minute":0.01,"lifecycle_score":0.2,"followability_score":0.0,"is_followable":false}',
                ),
            )
            connection.commit()

        report = build_monitor_report(self.settings)

        self.assertIn("=== PNL DISTRIBUTION ===", report)
        self.assertIn("Wallets profiled: 3", report)
        self.assertIn("> $1000", report)
        self.assertIn("$100-$1000", report)
        self.assertIn("< $100", report)
        self.assertIn("> $200", report)
        self.assertIn("$50-$200", report)
        self.assertIn("< $50", report)


if __name__ == "__main__":
    unittest.main()
