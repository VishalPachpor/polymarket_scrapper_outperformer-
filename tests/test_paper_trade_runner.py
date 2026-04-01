from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tip_v1.config import get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.paper.paper_trade_runner import (
    BUY,
    NO_SIGNAL,
    PaperTradeConfig,
    PaperTradeRunner,
    classify_latency,
    compute_paper_signal_metrics,
    discover_active_assets,
    latest_trade_timestamp,
)
from tip_v1.signals.momentum_trigger import MomentumSignal


class PaperTradeRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "paper.sqlite3"
        self.settings = get_settings(self.db_path)
        initialize_database(self.settings)
        self.config = PaperTradeConfig(
            polling_interval_seconds=5,
            active_asset_lookback_seconds=600,
            active_asset_limit=10,
            tracking_window_seconds=300,
            minimum_win_move=0.03,
        )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _insert_trade_event(
        self,
        connection,
        *,
        asset_id: str,
        market_id: str,
        timestamp: int,
        price: str = "0.40",
    ) -> None:
        connection.execute(
            """
            INSERT INTO trades_raw(wallet, raw_json, dedupe_key)
            VALUES ('0xseed', '{}', ?)
            """,
            (f"seed-{asset_id}-{timestamp}",),
        )
        raw_trade_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            """
            INSERT INTO trade_events(
                raw_trade_id, trade_id, wallet, market_id, outcome, asset_id, side, price, size, timestamp
            )
            VALUES (?, ?, '0xseed', ?, 'YES', ?, 'BUY', ?, '10', ?)
            """,
            (raw_trade_id, f"trade-{asset_id}-{timestamp}", market_id, asset_id, price, timestamp),
        )

    def test_runner_records_signal_then_resolves_outcome_from_future_prices(self) -> None:
        runner = PaperTradeRunner(settings=self.settings, config=self.config)
        with managed_connection(self.settings) as connection:
            self._insert_trade_event(connection, asset_id="asset-1", market_id="market-1", timestamp=1_700_000_000)
            connection.commit()

        with patch(
            "tip_v1.paper.paper_trade_runner.generate_momentum_signal_from_db",
            return_value=MomentumSignal(
                market_id="market-1",
                asset_id="asset-1",
                timestamp=1_700_000_000,
                price=0.40,
                signal_type=BUY,
                confidence=0.82,
                dry_run=True,
            ),
        ):
            first_cycle = runner.run_once(now_ts=1_700_000_000)

        self.assertEqual(first_cycle.signals_recorded, 1)
        self.assertEqual(first_cycle.latency_status, "REALTIME")
        self.assertTrue(first_cycle.signal_eligible)

        with managed_connection(self.settings) as connection:
            for ts, price in [
                (1_700_000_060, 0.42),
                (1_700_000_120, 0.46),
                (1_700_000_170, 0.39),
                (1_700_000_180, 0.43),
                (1_700_000_240, 0.39),
            ]:
                connection.execute(
                    """
                    INSERT INTO market_price_history(asset_id, timestamp, price)
                    VALUES ('asset-1', ?, ?)
                    """,
                    (ts, price),
                )
            connection.commit()

        with patch(
            "tip_v1.paper.paper_trade_runner.generate_momentum_signal_from_db",
            return_value=MomentumSignal(
                market_id="market-1",
                asset_id="asset-1",
                timestamp=1_700_000_300,
                price=0.40,
                signal_type=NO_SIGNAL,
                confidence=0.0,
                dry_run=True,
                reason="cooldown_active",
            ),
        ):
            second_cycle = runner.run_once(now_ts=1_700_000_301)

        self.assertEqual(second_cycle.outcomes_resolved, 1)
        self.assertEqual(second_cycle.metrics.resolved_signals, 1)
        self.assertAlmostEqual(second_cycle.metrics.avg_mfe, 0.06)
        self.assertAlmostEqual(second_cycle.metrics.avg_mae, -0.01)
        self.assertAlmostEqual(second_cycle.metrics.avg_time_to_peak, 120.0)
        self.assertAlmostEqual(second_cycle.metrics.avg_return_180s, 0.03)
        self.assertAlmostEqual(second_cycle.metrics.avg_return_180s_5s, 0.013672795851, places=10)
        self.assertAlmostEqual(second_cycle.metrics.avg_return_180s_10s, 0.013672795851, places=10)
        self.assertAlmostEqual(second_cycle.metrics.avg_return_180s_15s, 0.013672795851, places=10)
        self.assertEqual(second_cycle.metrics.execution_verdict, "EDGE SURVIVES EXECUTION")
        self.assertEqual(second_cycle.latency_status, "STALE")
        self.assertFalse(second_cycle.signal_eligible)

        with managed_connection(self.settings) as connection:
            signal_row = connection.execute(
                "SELECT status, entry_price, confidence FROM paper_signals"
            ).fetchone()
            outcome_row = connection.execute(
                """
                SELECT
                    sample_count, mfe, mae, time_to_peak, return_60s, return_180s,
                    entry_price_5s, entry_price_10s, entry_price_15s,
                    return_180s_5s, return_180s_10s, return_180s_15s,
                    slippage_applied, feasible_10s, reversed_quickly, win_flag
                FROM paper_signal_outcomes
                """
            ).fetchone()

        self.assertEqual(signal_row["status"], "RESOLVED")
        self.assertEqual(int(outcome_row["sample_count"]), 5)
        self.assertAlmostEqual(float(outcome_row["mfe"]), 0.06)
        self.assertAlmostEqual(float(outcome_row["mae"]), -0.01)
        self.assertEqual(int(outcome_row["time_to_peak"]), 120)
        self.assertAlmostEqual(float(outcome_row["return_60s"]), 0.02)
        self.assertAlmostEqual(float(outcome_row["return_180s"]), 0.03)
        self.assertAlmostEqual(float(outcome_row["entry_price_5s"]), 0.4242)
        self.assertAlmostEqual(float(outcome_row["entry_price_10s"]), 0.4242)
        self.assertAlmostEqual(float(outcome_row["entry_price_15s"]), 0.4242)
        self.assertAlmostEqual(float(outcome_row["return_180s_5s"]), 0.013672795851, places=10)
        self.assertAlmostEqual(float(outcome_row["return_180s_10s"]), 0.013672795851, places=10)
        self.assertAlmostEqual(float(outcome_row["return_180s_15s"]), 0.013672795851, places=10)
        self.assertAlmostEqual(float(outcome_row["slippage_applied"]), 0.01)
        self.assertEqual(int(outcome_row["feasible_10s"]), 1)
        self.assertEqual(int(outcome_row["reversed_quickly"]), 1)
        self.assertEqual(int(outcome_row["win_flag"]), 1)

    def test_runner_skips_duplicate_open_signal_for_same_asset(self) -> None:
        runner = PaperTradeRunner(settings=self.settings, config=self.config)
        with managed_connection(self.settings) as connection:
            self._insert_trade_event(connection, asset_id="asset-1", market_id="market-1", timestamp=1_700_000_000)
            connection.commit()

        buy_signal = MomentumSignal(
            market_id="market-1",
            asset_id="asset-1",
            timestamp=1_700_000_000,
            price=0.40,
            signal_type=BUY,
            confidence=0.8,
            dry_run=True,
        )
        with patch(
            "tip_v1.paper.paper_trade_runner.generate_momentum_signal_from_db",
            return_value=buy_signal,
        ):
            first_cycle = runner.run_once(now_ts=1_700_000_000)
            second_cycle = runner.run_once(now_ts=1_700_000_030)

        self.assertEqual(first_cycle.signals_recorded, 1)
        self.assertEqual(second_cycle.signals_recorded, 0)
        self.assertGreaterEqual(second_cycle.signals_skipped, 1)

        with managed_connection(self.settings) as connection:
            row = connection.execute("SELECT COUNT(*) AS n FROM paper_signals").fetchone()
        self.assertEqual(int(row["n"]), 1)

    def test_discover_active_assets_and_metrics(self) -> None:
        runner = PaperTradeRunner(settings=self.settings, config=self.config)
        with managed_connection(self.settings) as connection:
            self._insert_trade_event(connection, asset_id="asset-1", market_id="market-1", timestamp=1_700_000_000)
            self._insert_trade_event(connection, asset_id="asset-1", market_id="market-1", timestamp=1_700_000_030)
            self._insert_trade_event(connection, asset_id="asset-2", market_id="market-2", timestamp=1_700_000_010)
            connection.execute(
                """
                INSERT INTO paper_signals(
                    market_id, asset_id, signal_timestamp, entry_price, confidence, signal_type, dry_run, status
                )
                VALUES
                    ('market-1', 'asset-1', 1700000000, 0.40, 0.80, 'BUY', 1, 'RESOLVED'),
                    ('market-2', 'asset-2', 1700000010, 0.35, 0.60, 'BUY', 1, 'RESOLVED')
                """
            )
            connection.execute(
                """
                INSERT INTO paper_signal_outcomes(
                    signal_id, market_id, asset_id, entry_timestamp, resolved_timestamp, tracking_window_seconds,
                    sample_count, max_price, min_price, mfe, mae, time_to_peak, return_60s, return_180s, return_300s,
                    entry_price_5s, entry_price_10s, entry_price_15s,
                    return_60s_5s, return_60s_10s, return_60s_15s,
                    return_180s_5s, return_180s_10s, return_180s_15s,
                    return_300s_5s, return_300s_10s, return_300s_15s,
                    slippage_applied, feasible_10s, reversed_quickly, win_flag
                )
                VALUES
                    (1, 'market-1', 'asset-1', 1700000000, 1700000300, 300, 3, 0.45, 0.39, 0.05, -0.01, 90, 0.02, 0.05, 0.04, 0.404, 0.408, 0.412, 0.015, 0.010, 0.005, 0.030, 0.020, 0.010, 0.025, 0.015, 0.005, 0.01, 1, 0, 1),
                    (2, 'market-2', 'asset-2', 1700000010, 1700000310, 300, 2, 0.36, 0.31, 0.01, -0.04, 250, -0.01, -0.02, -0.01, 0.3535, 0.3570, 0.3605, -0.020, -0.025, -0.030, -0.010, -0.020, -0.030, -0.005, -0.015, -0.025, 0.01, 0, 1, 0)
                """
            )
            connection.commit()

            active_assets = discover_active_assets(connection, now_ts=1_700_000_100, config=self.config)
            metrics = compute_paper_signal_metrics(connection)

        self.assertEqual([asset.asset_id for asset in active_assets], ["asset-1", "asset-2"])
        self.assertEqual(metrics.resolved_signals, 2)
        self.assertAlmostEqual(metrics.signal_win_rate, 0.5)
        self.assertAlmostEqual(metrics.avg_mfe, 0.03)
        self.assertAlmostEqual(metrics.avg_mae, -0.025)
        self.assertAlmostEqual(metrics.avg_time_to_peak, 170.0)
        self.assertAlmostEqual(metrics.avg_return_60s, 0.005)
        self.assertAlmostEqual(metrics.avg_return_180s, 0.015)
        self.assertAlmostEqual(metrics.avg_return_300s, 0.015)
        self.assertAlmostEqual(metrics.move_1_to_3m_rate, 0.5)
        self.assertAlmostEqual(metrics.quick_reversal_rate, 0.5)
        self.assertAlmostEqual(metrics.avg_return_180s_5s, 0.01)
        self.assertAlmostEqual(metrics.avg_return_180s_10s, 0.0)
        self.assertAlmostEqual(metrics.avg_return_180s_15s, -0.01)
        self.assertEqual(metrics.execution_verdict, "EDGE IS LATENCY-SENSITIVE")

    def test_latency_classification_and_runner_pauses_signals_when_stale(self) -> None:
        runner = PaperTradeRunner(settings=self.settings, config=self.config)
        with managed_connection(self.settings) as connection:
            self._insert_trade_event(connection, asset_id="asset-1", market_id="market-1", timestamp=1_700_000_000)
            connection.commit()
            latest_ts = latest_trade_timestamp(connection)

        self.assertEqual(latest_ts, 1_700_000_000)
        latency_seconds, latency_status, signal_eligible = classify_latency(
            now_ts=1_700_000_500,
            latest_trade_ts=latest_ts,
            config=self.config,
        )
        self.assertEqual(latency_seconds, 500)
        self.assertEqual(latency_status, "STALE")
        self.assertFalse(signal_eligible)

        with patch(
            "tip_v1.paper.paper_trade_runner.generate_momentum_signal_from_db",
            return_value=MomentumSignal(
                market_id="market-1",
                asset_id="asset-1",
                timestamp=1_700_000_500,
                price=0.40,
                signal_type=BUY,
                confidence=0.9,
                dry_run=True,
            ),
        ):
            result = runner.run_once(now_ts=1_700_000_500)

        self.assertEqual(result.checked_assets, 1)
        self.assertEqual(result.signals_generated, 0)
        self.assertEqual(result.signals_recorded, 0)
        self.assertEqual(result.latency_status, "STALE")
        self.assertFalse(result.signal_eligible)


if __name__ == "__main__":
    unittest.main()
