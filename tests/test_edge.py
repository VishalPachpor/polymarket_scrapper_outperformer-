from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tip_v1.config import get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.edge.extract_regime_edges import (
    extract_regime_edges,
    mfe_capture_bucket,
    render_regime_edge_report,
    time_after_mfe_bucket,
    time_to_mfe_bucket,
)


class EdgeBucketTests(unittest.TestCase):
    def test_mfe_capture_bucket_handles_thresholds(self) -> None:
        self.assertEqual(mfe_capture_bucket(10, 2), "<0.50")
        self.assertEqual(mfe_capture_bucket(10, 7), "0.50-0.80")
        self.assertEqual(mfe_capture_bucket(10, 9), ">0.80")

    def test_time_after_mfe_bucket_uses_remaining_hold_time(self) -> None:
        self.assertEqual(time_after_mfe_bucket(239, 180), "<1m")
        self.assertEqual(time_after_mfe_bucket(479, 180), "1-5m")
        self.assertEqual(time_after_mfe_bucket(1200, 180), ">5m")
        self.assertEqual(time_after_mfe_bucket(4000, 180), ">5m")

    def test_time_to_mfe_bucket_uses_finer_short_horizon_ranges(self) -> None:
        self.assertEqual(time_to_mfe_bucket(30), "<60s")
        self.assertEqual(time_to_mfe_bucket(120), "1-3m")
        self.assertEqual(time_to_mfe_bucket(420), "3-10m")
        self.assertEqual(time_to_mfe_bucket(900), ">10m")


class RegimeEdgeExtractionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "edge.sqlite3"
        self.settings = get_settings(self.db_path)
        initialize_database(self.settings)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _insert_closed_position(
        self,
        connection,
        *,
        wallet: str,
        entry_price: float,
        pnl: float,
        duration: int,
        mfe: float,
        mae: float,
        time_to_mfe: int,
    ) -> None:
        connection.execute(
            """
            INSERT INTO trades_raw(wallet, raw_json, dedupe_key)
            VALUES (?, ?, ?)
            """,
            (
                wallet,
                "{}",
                f"raw-{wallet}-{entry_price}-{duration}-{pnl}",
            ),
        )
        raw_trade_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])

        connection.execute(
            """
            INSERT INTO trade_events(
                raw_trade_id, trade_id, wallet, market_id, outcome, asset_id, side, price, size, timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?, 'BUY', ?, ?, ?)
            """,
            (
                raw_trade_id,
                f"{wallet}-{entry_price}-{duration}-{pnl}",
                wallet,
                f"market-{wallet}",
                "YES",
                f"asset-{wallet}",
                str(entry_price),
                "10",
                1_700_000_000 + duration,
            ),
        )
        trade_event_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])

        connection.execute(
            """
            INSERT INTO positions_reconstructed(
                wallet, market_id, outcome, entry_trade_event_id, exit_trade_event_id,
                entry_price, exit_price, size, pnl, entry_time, exit_time, duration,
                status, remaining_size, version
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'CLOSED', 0, 1)
            """,
            (
                wallet,
                f"market-{wallet}",
                "YES",
                trade_event_id,
                None,
                entry_price,
                entry_price + 0.05,
                10.0,
                pnl,
                1_700_000_000,
                1_700_000_000 + duration,
                duration,
            ),
        )
        position_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])

        connection.execute(
            """
            INSERT INTO position_path_metrics(
                position_id, wallet, version, asset_id, sample_count, start_timestamp, end_timestamp,
                entry_context_price, exit_context_price, max_price, min_price,
                mfe, mae, time_to_mfe, time_to_mae
            )
            VALUES (?, ?, 1, ?, 5, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                position_id,
                wallet,
                f"asset-{wallet}",
                1_700_000_000,
                1_700_000_000 + duration,
                entry_price,
                entry_price + 0.05,
                entry_price + 0.08,
                max(0.0, entry_price - 0.04),
                mfe,
                mae,
                time_to_mfe,
                max(1, min(duration, time_to_mfe // 2 or 1)),
            ),
        )

    def test_extract_regime_edges_finds_repeatable_edges_and_failures(self) -> None:
        with managed_connection(self.settings) as connection:
            connection.execute(
                """
                INSERT INTO wallet_rankings(wallet, regime, followability_score, lifecycle_score, rank_score, tier)
                VALUES
                    ('0xalpha', 'crypto_5m', 0.9, 0.8, 0.85, 'A'),
                    ('0xbeta', 'crypto_5m', 0.85, 0.7, 0.8, 'A'),
                    ('0xignored', 'crypto_5m', 0.55, 0.7, 0.7, 'B')
                """
            )

            self._insert_closed_position(
                connection,
                wallet="0xalpha",
                entry_price=0.05,
                pnl=4.0,
                duration=120,
                mfe=4.8,
                mae=-0.8,
                time_to_mfe=60,
            )
            self._insert_closed_position(
                connection,
                wallet="0xalpha",
                entry_price=0.08,
                pnl=3.5,
                duration=180,
                mfe=4.0,
                mae=-0.6,
                time_to_mfe=90,
            )
            self._insert_closed_position(
                connection,
                wallet="0xbeta",
                entry_price=0.04,
                pnl=5.0,
                duration=240,
                mfe=5.5,
                mae=-0.9,
                time_to_mfe=120,
            )
            self._insert_closed_position(
                connection,
                wallet="0xalpha",
                entry_price=0.45,
                pnl=-6.0,
                duration=2400,
                mfe=1.2,
                mae=-2.2,
                time_to_mfe=600,
            )
            self._insert_closed_position(
                connection,
                wallet="0xbeta",
                entry_price=0.55,
                pnl=-4.0,
                duration=3600,
                mfe=0.7,
                mae=-2.5,
                time_to_mfe=900,
            )
            self._insert_closed_position(
                connection,
                wallet="0xbeta",
                entry_price=0.40,
                pnl=-3.0,
                duration=4200,
                mfe=0.6,
                mae=-1.8,
                time_to_mfe=1200,
            )
            connection.commit()

        report = extract_regime_edges(
            "crypto_5m",
            settings=self.settings,
            wallet_limit=10,
            min_bucket_count=3,
        )

        self.assertEqual(report.wallets_used, ("0xalpha", "0xbeta"))
        self.assertEqual(report.total_positions, 6)

        edge_pairs = {(stat.dimension, stat.bucket) for stat in report.edges}
        failure_pairs = {(stat.dimension, stat.bucket) for stat in report.failures}

        self.assertIn(("entry_price", "<0.10"), edge_pairs)
        self.assertIn(("mfe_capture", ">0.80"), edge_pairs)
        self.assertIn(("time_to_mfe", "1-3m"), edge_pairs)
        self.assertIn(("time_after_mfe", ">5m"), failure_pairs)
        best_entry = next(
            stat for stat in report.bucket_stats["entry_price"] if stat.bucket == "<0.10"
        )
        worst_exit = next(
            stat for stat in report.bucket_stats["time_after_mfe"] if stat.bucket == ">5m"
        )
        self.assertGreater(best_entry.weighted_edge_score, 0.0)
        self.assertGreater(best_entry.early_mfe_hit_rate, 0.0)
        self.assertGreater(worst_exit.mfe_giveback_ratio, 0.0)

        rendered = render_regime_edge_report(report)
        self.assertIn("REGIME: crypto_5m", rendered)
        self.assertIn("=== ENTRY EDGE ===", rendered)
        self.assertIn("- <0.10", rendered)
        self.assertIn("=== EXIT FAILURE ===", rendered)
        self.assertIn(">5m", rendered)
        self.assertIn("score=", rendered)
        self.assertIn("fast_mfe=", rendered)
        self.assertIn("giveback=", rendered)

    def test_extract_regime_edges_returns_empty_report_when_no_wallets_qualify(self) -> None:
        report = extract_regime_edges("crypto_15m", settings=self.settings)

        self.assertEqual(report.wallets_used, ())
        self.assertEqual(report.total_positions, 0)
        self.assertEqual(report.edges, ())
        self.assertEqual(report.failures, ())

    def test_extract_regime_edges_can_select_emerging_wallets(self) -> None:
        with managed_connection(self.settings) as connection:
            connection.execute(
                """
                INSERT INTO candidate_wallets(wallet, status)
                VALUES ('0xgamma', 'PROFILED')
                """
            )
            connection.execute(
                """
                INSERT INTO candidate_wallet_sources(wallet, source, regime)
                VALUES ('0xgamma', 'market_scanner', 'crypto_5m')
                """
            )
            connection.execute(
                """
                INSERT INTO wallet_profiles(
                    wallet, version, trader_type, confidence,
                    features_json, metrics_json, behavior_json, edge_json, failures_json
                )
                VALUES (
                    '0xgamma', 1, 'NOISE', 0.2,
                    '{}',
                    '{"pnl_per_minute":0.12,"lifecycle_score":0.45,"followability_score":0.0,"is_followable":false}',
                    '{}', '[]', '[]'
                )
                """
            )
            self._insert_closed_position(
                connection,
                wallet="0xgamma",
                entry_price=0.09,
                pnl=2.0,
                duration=90,
                mfe=2.5,
                mae=-0.5,
                time_to_mfe=45,
            )
            connection.commit()

        report = extract_regime_edges(
            "crypto_5m",
            settings=self.settings,
            selection_mode="emerging",
            wallet_limit=10,
            min_lifecycle_score=0.3,
            min_pnl_per_minute=0.02,
            min_bucket_count=1,
        )

        self.assertEqual(report.wallets_used, ("0xgamma",))


if __name__ == "__main__":
    unittest.main()
