from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tip_v1.config import get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.ingestion.fetch_trades import ingest_trade_payloads
from tip_v1.main import run_pipeline
from tip_v1.profiler.wallet_profiler import (
    WalletFeatures,
    classify_trader,
    compute_confidence,
    compute_features,
    compute_followability,
    extract_edge,
    extract_failures,
    is_followable_wallet,
    load_stored_profile,
    profile_wallet,
)


class WalletProfilerFeatureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "profiler_test.sqlite3"
        self.settings = get_settings(self.db_path)
        initialize_database(self.settings)
        self.wallet = "0xPROFILER_TEST"

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _ingest_and_run(self, payloads: list[dict]) -> None:
        ingest_trade_payloads(self.wallet, payloads, self.settings)
        run_pipeline(self.wallet, settings=self.settings, skip_fetch=True, skip_enrichment=True)

    def test_basic_feature_computation(self) -> None:
        payloads = [
            {"id": "t1", "conditionId": "m1", "outcome": "YES", "side": "BUY",  "price": 0.40, "size": 10, "timestamp": 1000},
            {"id": "t2", "conditionId": "m1", "outcome": "YES", "side": "SELL", "price": 0.60, "size": 10, "timestamp": 2000},
            {"id": "t3", "conditionId": "m2", "outcome": "NO",  "side": "BUY",  "price": 0.30, "size": 5,  "timestamp": 1500},
        ]
        self._ingest_and_run(payloads)

        f = compute_features(self.wallet, settings=self.settings)

        self.assertEqual(f.total_trades, 3)
        self.assertEqual(f.buy_count, 2)
        self.assertEqual(f.sell_count, 1)
        self.assertEqual(f.closed_positions, 1)
        self.assertEqual(f.open_positions, 1)
        self.assertEqual(f.unique_markets, 2)
        self.assertAlmostEqual(f.sell_to_buy_ratio, 0.5)
        self.assertAlmostEqual(f.open_inventory_ratio, 0.5)
        self.assertAlmostEqual(f.two_sided_market_ratio, 0.5)
        self.assertEqual(f.avg_hold_time, 1000.0)
        self.assertEqual(f.median_hold_time, 1000.0)
        self.assertAlmostEqual(f.win_rate, 1.0)
        self.assertGreater(f.avg_pnl, 0)

    def test_empty_wallet_returns_zero_features(self) -> None:
        f = compute_features(self.wallet, settings=self.settings)

        self.assertEqual(f.total_trades, 0)
        self.assertEqual(f.closed_positions, 0)
        self.assertEqual(f.open_positions, 0)
        self.assertAlmostEqual(f.win_rate, 0.0)
        self.assertAlmostEqual(f.profit_factor, 0.0)
        self.assertAlmostEqual(f.mfe_capture_ratio, 0.0)

    def test_buy_only_wallet(self) -> None:
        payloads = [
            {"id": f"b{i}", "conditionId": f"m{i}", "outcome": "YES", "side": "BUY", "price": 0.05, "size": 10, "timestamp": 1000 + i}
            for i in range(5)
        ]
        self._ingest_and_run(payloads)

        f = compute_features(self.wallet, settings=self.settings)

        self.assertEqual(f.buy_count, 5)
        self.assertEqual(f.sell_count, 0)
        self.assertEqual(f.closed_positions, 0)
        self.assertEqual(f.open_positions, 5)
        self.assertAlmostEqual(f.sell_to_buy_ratio, 0.0)
        self.assertAlmostEqual(f.open_inventory_ratio, 1.0)

    def test_entry_price_distribution(self) -> None:
        payloads = [
            {"id": "extreme1", "conditionId": "m1", "outcome": "YES", "side": "BUY", "price": 0.05, "size": 1, "timestamp": 1000},
            {"id": "extreme2", "conditionId": "m2", "outcome": "YES", "side": "BUY", "price": 0.95, "size": 1, "timestamp": 1001},
            {"id": "mid1",     "conditionId": "m3", "outcome": "YES", "side": "BUY", "price": 0.50, "size": 1, "timestamp": 1002},
            {"id": "mid2",     "conditionId": "m4", "outcome": "YES", "side": "BUY", "price": 0.45, "size": 1, "timestamp": 1003},
        ]
        self._ingest_and_run(payloads)

        f = compute_features(self.wallet, settings=self.settings)
        self.assertAlmostEqual(f.extreme_entry_ratio, 0.5)
        self.assertAlmostEqual(f.mid_entry_ratio, 0.5)


class ClassificationTests(unittest.TestCase):
    def _features(self, **overrides) -> WalletFeatures:
        defaults = {
            "total_trades": 100,
            "buy_count": 60,
            "sell_count": 40,
            "closed_positions": 30,
            "open_positions": 10,
            "unique_markets": 10,
            "sell_to_buy_ratio": 0.67,
            "open_inventory_ratio": 0.25,
            "two_sided_market_ratio": 0.50,
            "avg_hold_time": 3600.0,
            "median_hold_time": 3000.0,
            "win_rate": 0.55,
            "avg_pnl": 0.01,
            "profit_factor": 1.5,
            "mfe_capture_ratio": 0.50,
            "avg_time_to_mfe": 600.0,
            "avg_time_after_mfe": 400.0,
            "extreme_entry_ratio": 0.20,
            "mid_entry_ratio": 0.40,
            "realized_pnl_per_minute": 0.0,
            "total_realized_pnl": 0.0,
            "active_days": 0.0,
            "pnl_per_day": 0.0,
            "pnl_per_trade": 0.0,
            "capital_proxy": 0.0,
            "return_proxy": 0.0,
        }
        defaults.update(overrides)
        return WalletFeatures(**defaults)

    def test_structured_inventory(self) -> None:
        f = self._features(
            open_inventory_ratio=0.90,
            sell_to_buy_ratio=0.10,
            two_sided_market_ratio=0.60,
            closed_positions=50,
        )
        self.assertEqual(classify_trader(f), "STRUCTURED_INVENTORY")

    def test_repricing(self) -> None:
        f = self._features(
            two_sided_market_ratio=0.70,
            sell_to_buy_ratio=1.0,
            median_hold_time=3600.0,
            mfe_capture_ratio=0.75,
        )
        self.assertEqual(classify_trader(f), "REPRICING")

    def test_outcome(self) -> None:
        f = self._features(
            median_hold_time=30000.0,
            sell_to_buy_ratio=0.20,
            closed_positions=60,
        )
        self.assertEqual(classify_trader(f), "OUTCOME")

    def test_momentum(self) -> None:
        f = self._features(
            two_sided_market_ratio=0.20,
            extreme_entry_ratio=0.20,
            median_hold_time=1800.0,
            mfe_capture_ratio=0.30,
        )
        self.assertEqual(classify_trader(f), "MOMENTUM")

    def test_noise_fallback(self) -> None:
        f = self._features()
        self.assertEqual(classify_trader(f), "NOISE")

    def test_priority_structured_over_outcome(self) -> None:
        f = self._features(
            open_inventory_ratio=0.90,
            sell_to_buy_ratio=0.10,
            two_sided_market_ratio=0.60,
            closed_positions=50,
            median_hold_time=30000.0,
        )
        self.assertEqual(classify_trader(f), "STRUCTURED_INVENTORY")


class ConfidenceTests(unittest.TestCase):
    def test_empty_wallet_zero_confidence(self) -> None:
        f = WalletFeatures(
            total_trades=0, buy_count=0, sell_count=0,
            closed_positions=0, open_positions=0, unique_markets=0,
            sell_to_buy_ratio=0, open_inventory_ratio=0,
            two_sided_market_ratio=0, avg_hold_time=0, median_hold_time=0,
            win_rate=0, avg_pnl=0, profit_factor=0,
            mfe_capture_ratio=0, avg_time_to_mfe=0, avg_time_after_mfe=0,
            extreme_entry_ratio=0, mid_entry_ratio=0,
            realized_pnl_per_minute=0.0,
            total_realized_pnl=0.0,
            active_days=0.0,
            pnl_per_day=0.0,
            pnl_per_trade=0.0,
            capital_proxy=0.0,
            return_proxy=0.0,
        )
        self.assertEqual(compute_confidence(f), 0.0)

    def test_more_trades_higher_confidence(self) -> None:
        base = {
            "buy_count": 50, "sell_count": 50, "unique_markets": 5,
            "sell_to_buy_ratio": 1.0, "open_inventory_ratio": 0.2,
            "two_sided_market_ratio": 0.5, "avg_hold_time": 100, "median_hold_time": 100,
            "win_rate": 0.55, "avg_pnl": 0.01, "profit_factor": 1.5,
            "mfe_capture_ratio": 0.5, "avg_time_to_mfe": 100, "avg_time_after_mfe": 50,
            "extreme_entry_ratio": 0.1, "mid_entry_ratio": 0.5,
            "realized_pnl_per_minute": 0.0,
            "total_realized_pnl": 0.0,
            "active_days": 0.0,
            "pnl_per_day": 0.0,
            "pnl_per_trade": 0.0,
            "capital_proxy": 0.0,
            "return_proxy": 0.0,
        }
        small = WalletFeatures(total_trades=10, closed_positions=8, open_positions=2, **base)
        large = WalletFeatures(total_trades=1000, closed_positions=800, open_positions=200, **base)
        self.assertGreater(compute_confidence(large), compute_confidence(small))

    def test_followability_prefers_closed_and_recycled_flow(self) -> None:
        weak = WalletFeatures(
            total_trades=500, buy_count=490, sell_count=10,
            closed_positions=5, open_positions=300, unique_markets=50,
            sell_to_buy_ratio=0.02, open_inventory_ratio=0.98,
            two_sided_market_ratio=0.05, avg_hold_time=1000, median_hold_time=1000,
            win_rate=0.50, avg_pnl=0.0, profit_factor=1.0,
            mfe_capture_ratio=0.1, avg_time_to_mfe=100, avg_time_after_mfe=50,
            extreme_entry_ratio=0.8, mid_entry_ratio=0.1,
            realized_pnl_per_minute=0.0,
            total_realized_pnl=0.0,
            active_days=0.0,
            pnl_per_day=0.0,
            pnl_per_trade=0.0,
            capital_proxy=0.0,
            return_proxy=0.0,
        )
        strong = WalletFeatures(
            total_trades=500, buy_count=250, sell_count=240,
            closed_positions=120, open_positions=20, unique_markets=30,
            sell_to_buy_ratio=0.96, open_inventory_ratio=0.14,
            two_sided_market_ratio=0.70, avg_hold_time=2000, median_hold_time=1800,
            win_rate=0.62, avg_pnl=0.05, profit_factor=1.8,
            mfe_capture_ratio=0.72, avg_time_to_mfe=300, avg_time_after_mfe=120,
            extreme_entry_ratio=0.35, mid_entry_ratio=0.30,
            realized_pnl_per_minute=0.05,
            total_realized_pnl=100.0,
            active_days=2.0,
            pnl_per_day=50.0,
            pnl_per_trade=0.83,
            capital_proxy=25.0,
            return_proxy=0.10,
        )

        self.assertGreater(compute_followability(strong), compute_followability(weak))
        self.assertFalse(is_followable_wallet(weak))
        self.assertTrue(is_followable_wallet(strong))


class EdgeAndFailureTests(unittest.TestCase):
    def test_extreme_entry_edge(self) -> None:
        f = WalletFeatures(
            total_trades=100, buy_count=60, sell_count=40,
            closed_positions=30, open_positions=10, unique_markets=10,
            sell_to_buy_ratio=0.67, open_inventory_ratio=0.25,
            two_sided_market_ratio=0.50, avg_hold_time=3600, median_hold_time=3000,
            win_rate=0.55, avg_pnl=0.01, profit_factor=1.5,
            mfe_capture_ratio=0.80, avg_time_to_mfe=600, avg_time_after_mfe=400,
            extreme_entry_ratio=0.60, mid_entry_ratio=0.10,
            realized_pnl_per_minute=0.0,
            total_realized_pnl=0.0,
            active_days=0.0,
            pnl_per_day=0.0,
            pnl_per_trade=0.0,
            capital_proxy=0.0,
            return_proxy=0.0,
        )
        edge = extract_edge(f)
        self.assertTrue(any("extreme" in e.lower() for e in edge))
        self.assertTrue(any("exit timing" in e.lower() for e in edge))

    def test_holds_after_peak_failure(self) -> None:
        f = WalletFeatures(
            total_trades=100, buy_count=60, sell_count=40,
            closed_positions=30, open_positions=10, unique_markets=10,
            sell_to_buy_ratio=0.67, open_inventory_ratio=0.25,
            two_sided_market_ratio=0.50, avg_hold_time=3600, median_hold_time=3000,
            win_rate=0.40, avg_pnl=-0.01, profit_factor=0.8,
            mfe_capture_ratio=0.20, avg_time_to_mfe=300, avg_time_after_mfe=3000,
            extreme_entry_ratio=0.10, mid_entry_ratio=0.60,
            realized_pnl_per_minute=0.0,
            total_realized_pnl=0.0,
            active_days=0.0,
            pnl_per_day=0.0,
            pnl_per_trade=0.0,
            capital_proxy=0.0,
            return_proxy=0.0,
        )
        failures = extract_failures(f)
        self.assertTrue(any("peak" in fl.lower() for fl in failures))
        self.assertTrue(any("mid-range" in fl.lower() for fl in failures))
        self.assertTrue(any("exit timing" in fl.lower() for fl in failures))


class ProfileIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "profile_integration.sqlite3"
        self.settings = get_settings(self.db_path)
        initialize_database(self.settings)
        self.wallet = "0xINTEGRATION"

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_full_profile_pipeline(self) -> None:
        payloads = [
            {"id": "t1", "conditionId": "m1", "outcome": "YES", "side": "BUY",  "price": 0.40, "size": 10, "timestamp": 1000},
            {"id": "t2", "conditionId": "m1", "outcome": "YES", "side": "SELL", "price": 0.60, "size": 10, "timestamp": 2000},
        ]
        ingest_trade_payloads(self.wallet, payloads, self.settings)
        run_pipeline(self.wallet, settings=self.settings, skip_fetch=True, skip_enrichment=True)

        profile = profile_wallet(self.wallet, settings=self.settings)

        self.assertEqual(profile.wallet, self.wallet)
        self.assertIn(profile.trader_type, (
            "STRUCTURED_INVENTORY", "REPRICING", "OUTCOME", "MOMENTUM", "NOISE",
        ))
        self.assertGreaterEqual(profile.confidence, 0.0)
        self.assertLessEqual(profile.confidence, 1.0)
        self.assertIsInstance(profile.features, WalletFeatures)
        self.assertIsInstance(profile.edge, tuple)
        self.assertIsInstance(profile.failures, tuple)

    def test_profile_persists_and_loads_latest_snapshot(self) -> None:
        payloads = [
            {"id": "t1", "conditionId": "m1", "outcome": "YES", "side": "BUY",  "price": 0.40, "size": 10, "timestamp": 1000},
            {"id": "t2", "conditionId": "m1", "outcome": "YES", "side": "SELL", "price": 0.60, "size": 10, "timestamp": 2000},
        ]
        ingest_trade_payloads(self.wallet, payloads, self.settings)
        run_pipeline(self.wallet, settings=self.settings, skip_fetch=True, skip_enrichment=True)

        profile = profile_wallet(self.wallet, settings=self.settings, version=3)
        stored = load_stored_profile(self.wallet, settings=self.settings)

        self.assertIsNotNone(stored)
        assert stored is not None
        self.assertEqual(stored["wallet"], self.wallet)
        self.assertEqual(stored["version"], 3)
        self.assertEqual(stored["trader_type"], profile.trader_type)
        self.assertAlmostEqual(stored["confidence"], profile.confidence)
        self.assertAlmostEqual(stored["followability_score"], profile.followability_score)
        self.assertEqual(stored["is_followable"], profile.is_followable)
        self.assertEqual(stored["features"]["total_trades"], profile.features.total_trades)
        self.assertEqual(stored["metrics"], profile.metrics)
        self.assertEqual(stored["behavior"], profile.behavior)
        self.assertEqual(stored["edge"], list(profile.edge))
        self.assertEqual(stored["failures"], list(profile.failures))

    def test_reprofiling_updates_latest_row_and_appends_history(self) -> None:
        initial_payloads = [
            {"id": "t1", "conditionId": "m1", "outcome": "YES", "side": "BUY",  "price": 0.40, "size": 10, "timestamp": 1000},
            {"id": "t2", "conditionId": "m1", "outcome": "YES", "side": "SELL", "price": 0.60, "size": 10, "timestamp": 2000},
        ]
        ingest_trade_payloads(self.wallet, initial_payloads, self.settings)
        run_pipeline(self.wallet, settings=self.settings, skip_fetch=True, skip_enrichment=True)

        first = profile_wallet(self.wallet, settings=self.settings, version=1)

        new_payloads = [
            {"id": "t3", "conditionId": "m2", "outcome": "NO", "side": "BUY", "price": 0.25, "size": 5, "timestamp": 3000},
        ]
        ingest_trade_payloads(self.wallet, new_payloads, self.settings)
        run_pipeline(self.wallet, settings=self.settings, skip_fetch=True, skip_enrichment=True)

        second = profile_wallet(self.wallet, settings=self.settings, version=2)

        with managed_connection(self.settings) as connection:
            latest = connection.execute(
                """
                SELECT wallet, version, trader_type, confidence, features_json
                FROM wallet_profiles
                WHERE wallet = ?
                """,
                (self.wallet,),
            ).fetchone()
            run_count = connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM wallet_profile_runs
                WHERE wallet = ?
                """,
                (self.wallet,),
            ).fetchone()

        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(latest["wallet"], self.wallet)
        self.assertEqual(latest["version"], 2)
        self.assertEqual(latest["trader_type"], second.trader_type)
        self.assertNotEqual(first.features.total_trades, second.features.total_trades)
        self.assertEqual(second.features.total_trades, 3)
        self.assertIn('"total_trades":3', latest["features_json"])
        self.assertEqual(int(run_count["count"]), 2)

    def test_profile_wallet_can_skip_persistence(self) -> None:
        profile = profile_wallet(self.wallet, settings=self.settings, persist=False)

        with managed_connection(self.settings) as connection:
            latest_count = connection.execute(
                "SELECT COUNT(*) AS count FROM wallet_profiles WHERE wallet = ?",
                (self.wallet,),
            ).fetchone()
            history_count = connection.execute(
                "SELECT COUNT(*) AS count FROM wallet_profile_runs WHERE wallet = ?",
                (self.wallet,),
            ).fetchone()

        self.assertEqual(profile.wallet, self.wallet)
        self.assertEqual(int(latest_count["count"]), 0)
        self.assertEqual(int(history_count["count"]), 0)
        self.assertIsNone(load_stored_profile(self.wallet, settings=self.settings))

    def test_pnl_scale_metrics_require_sample_but_still_compute_context(self) -> None:
        payloads = []
        for i in range(19):
            entry_ts = 1_000 + i * 100
            exit_ts = entry_ts + 60
            payloads.extend([
                {"id": f"buy-{i}", "conditionId": f"m{i}", "outcome": "YES", "side": "BUY", "price": 0.40, "size": 10, "timestamp": entry_ts},
                {"id": f"sell-{i}", "conditionId": f"m{i}", "outcome": "YES", "side": "SELL", "price": 0.50, "size": 10, "timestamp": exit_ts},
            ])
        ingest_trade_payloads(self.wallet, payloads, self.settings)
        run_pipeline(self.wallet, settings=self.settings, skip_fetch=True, skip_enrichment=True)

        features = compute_features(self.wallet, settings=self.settings)

        self.assertEqual(features.closed_positions, 19)
        self.assertGreater(features.active_days, 0.0)
        self.assertGreater(features.pnl_per_trade, 0.0)
        self.assertGreater(features.capital_proxy, 0.0)
        self.assertEqual(features.total_realized_pnl, 0.0)
        self.assertEqual(features.pnl_per_day, 0.0)
        self.assertEqual(features.return_proxy, 0.0)
        self.assertEqual(features.realized_pnl_per_minute, 0.0)

    def test_profile_persists_pnl_scale_metrics(self) -> None:
        payloads = []
        for i in range(20):
            entry_ts = 1_000 + i * 120
            exit_ts = entry_ts + 60
            payloads.extend([
                {"id": f"buy-{i}", "conditionId": f"m{i}", "outcome": "YES", "side": "BUY", "price": 0.40, "size": 10, "timestamp": entry_ts},
                {"id": f"sell-{i}", "conditionId": f"m{i}", "outcome": "YES", "side": "SELL", "price": 0.50, "size": 10, "timestamp": exit_ts},
            ])
        ingest_trade_payloads(self.wallet, payloads, self.settings)
        run_pipeline(self.wallet, settings=self.settings, skip_fetch=True, skip_enrichment=True)

        profile = profile_wallet(self.wallet, settings=self.settings)

        self.assertGreater(profile.features.total_realized_pnl, 0.0)
        self.assertGreater(profile.features.active_days, 0.0)
        self.assertGreater(profile.features.pnl_per_day, 0.0)
        self.assertGreater(profile.features.pnl_per_trade, 0.0)
        self.assertGreater(profile.features.capital_proxy, 0.0)
        self.assertGreater(profile.features.return_proxy, 0.0)
        self.assertEqual(profile.metrics["total_pnl"], profile.features.total_realized_pnl)
        self.assertEqual(profile.metrics["pnl_per_day"], profile.features.pnl_per_day)
        self.assertEqual(profile.metrics["pnl_per_trade"], profile.features.pnl_per_trade)
        self.assertEqual(profile.metrics["capital_proxy"], profile.features.capital_proxy)
        self.assertEqual(profile.metrics["return_proxy"], profile.features.return_proxy)

    def test_empty_wallet_does_not_crash(self) -> None:
        profile = profile_wallet(self.wallet, settings=self.settings)

        self.assertEqual(profile.trader_type, "NOISE")
        self.assertEqual(profile.confidence, 0.0)
        self.assertEqual(profile.features.total_trades, 0)


if __name__ == "__main__":
    unittest.main()
