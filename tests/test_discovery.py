from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tip_v1.config import get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.discovery.orchestrator import run_discovery_cycle
from tip_v1.discovery.prefilter import (
    compute_prefilter_score,
    compute_sample_stats,
    passes_prefilter,
)


class PrefilterTests(unittest.TestCase):
    def test_compute_sample_stats_tracks_sides_and_markets(self) -> None:
        stats = compute_sample_stats(
            [
                {"conditionId": "m1", "side": "BUY"},
                {"conditionId": "m1", "side": "SELL"},
                {"conditionId": "m2", "side": "BUY"},
            ]
        )

        self.assertEqual(stats["trade_count"], 3)
        self.assertEqual(stats["buy_count"], 2)
        self.assertEqual(stats["sell_count"], 1)
        self.assertEqual(stats["unique_markets"], 2)
        self.assertEqual(stats["two_sided_markets"], 1)

    def test_prefilter_rejects_no_sell_wallets(self) -> None:
        stats = {
            "trade_count": 100,
            "buy_count": 100,
            "sell_count": 0,
            "unique_markets": 12,
            "two_sided_markets": 0,
        }

        passed, reason = passes_prefilter(stats)

        self.assertFalse(passed)
        self.assertEqual(reason, "no_sells")
        self.assertEqual(compute_prefilter_score(stats), 2.5649)


class DiscoveryCycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "discovery.sqlite3"
        self.settings = get_settings(self.db_path)
        initialize_database(self.settings)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    @patch("tip_v1.discovery.orchestrator.load_stored_profile")
    @patch("tip_v1.discovery.orchestrator.profile_wallet")
    @patch("tip_v1.discovery.orchestrator.run_pipeline")
    @patch("tip_v1.discovery.orchestrator.fetch_wallet_trades")
    @patch("tip_v1.discovery.orchestrator.fetch_recent_trades")
    @patch("tip_v1.discovery.orchestrator.scan_market_regime")
    @patch("tip_v1.discovery.orchestrator.fetch_default_leaderboard_wallets")
    def test_discovery_cycle_promotes_and_ranks_prefiltered_wallets(
        self,
        mock_leaderboard,
        mock_scan_market_regime,
        mock_recent_trades,
        mock_fetch_wallet_trades,
        mock_run_pipeline,
        mock_profile_wallet,
        mock_load_stored_profile,
    ) -> None:
        mock_leaderboard.return_value = [
            type(
                "LeaderboardWalletStub",
                (),
                {
                    "wallet": "0xgood",
                    "source": "leaderboard_monthly_profit",
                    "regime": "global",
                    "source_rank": 1,
                    "source_metadata": {"profit": 123},
                },
            )(),
            type(
                "LeaderboardWalletStub",
                (),
                {
                    "wallet": "0xbad",
                    "source": "leaderboard_monthly_profit",
                    "regime": "global",
                    "source_rank": 2,
                    "source_metadata": {"profit": 50},
                },
            )(),
        ]
        mock_scan_market_regime.return_value = [
            type(
                "MarketScannerWalletStub",
                (),
                {
                    "wallet": "0xgood",
                    "source": "market_scanner",
                    "regime": "crypto_5m",
                    "source_rank": None,
                    "source_metadata": {"market_id": "m-crypto-5m"},
                },
            )(),
        ]
        mock_recent_trades.return_value = [
            {"user": "0xgood"},
            {"user": "0xgood"},
            {"user": "0xgood"},
            {"user": "0xgood"},
            {"user": "0xgood"},
        ]

        def wallet_trades(wallet: str, **_: object):
            if wallet == "0xgood":
                return [
                    {"conditionId": "m1", "side": "BUY"},
                    {"conditionId": "m1", "side": "SELL"},
                    {"conditionId": "m2", "side": "BUY"},
                    {"conditionId": "m2", "side": "SELL"},
                    {"conditionId": "m3", "side": "BUY"},
                    {"conditionId": "m3", "side": "SELL"},
                    {"conditionId": "m4", "side": "BUY"},
                    {"conditionId": "m4", "side": "SELL"},
                    {"conditionId": "m5", "side": "BUY"},
                    {"conditionId": "m5", "side": "SELL"},
                    {"conditionId": "m6", "side": "BUY"},
                    {"conditionId": "m6", "side": "SELL"},
                    {"conditionId": "m7", "side": "BUY"},
                    {"conditionId": "m7", "side": "SELL"},
                    {"conditionId": "m8", "side": "BUY"},
                    {"conditionId": "m8", "side": "SELL"},
                    {"conditionId": "m9", "side": "BUY"},
                    {"conditionId": "m9", "side": "SELL"},
                    {"conditionId": "m10", "side": "BUY"},
                    {"conditionId": "m10", "side": "SELL"},
                ]
            return [
                {"conditionId": "m1", "side": "BUY"},
                {"conditionId": "m2", "side": "BUY"},
                {"conditionId": "m3", "side": "BUY"},
            ]

        mock_fetch_wallet_trades.side_effect = wallet_trades
        mock_load_stored_profile.return_value = {
            "wallet": "0xgood",
            "confidence": 0.6,
            "followability_score": 0.8,
            "metrics": {"lifecycle_score": 0.75, "profit_factor": 2.4, "pnl_per_minute_score": 0.5},
            "features": {"total_trades": 120},
        }

        result = run_discovery_cycle(
            self.settings,
            leaderboard_limit=10,
            recent_trade_limit=20,
            recent_trade_min_count=5,
            market_scan_limit=10,
            market_trade_limit=20,
            candidate_process_limit=10,
            prefilter_trade_limit=20,
        )

        self.assertEqual(result.promoted_count, 1)
        self.assertEqual(result.profiled_count, 1)
        mock_run_pipeline.assert_called_once_with(
            "0xgood",
            settings=self.settings,
            skip_enrichment=True,
        )
        mock_profile_wallet.assert_called_once_with("0xgood", settings=self.settings)

        with managed_connection(self.settings) as connection:
            candidates = {
                row["wallet"]: dict(row)
                for row in connection.execute(
                    "SELECT wallet, status, prefilter_passed, prefilter_reason, prefilter_score FROM candidate_wallets"
                ).fetchall()
            }
            ranking = connection.execute(
                "SELECT wallet, regime, followability_score, lifecycle_score, rank_score, tier FROM wallet_rankings WHERE wallet = '0xgood' ORDER BY regime"
            ).fetchall()
            source_count = connection.execute(
                "SELECT COUNT(*) AS count FROM candidate_wallet_sources WHERE wallet = '0xgood'"
            ).fetchone()["count"]
            regimes = {
                row["regime"]
                for row in connection.execute(
                    "SELECT regime FROM candidate_wallet_sources WHERE wallet = '0xgood'"
                ).fetchall()
            }

        self.assertEqual(candidates["0xgood"]["status"], "PROFILED")
        self.assertEqual(candidates["0xgood"]["prefilter_passed"], 1)
        self.assertEqual(candidates["0xbad"]["status"], "PREFILTER_REJECTED")
        self.assertEqual(candidates["0xbad"]["prefilter_reason"], "no_sells")
        self.assertGreater(float(candidates["0xgood"]["prefilter_score"]), 0.0)
        self.assertEqual(int(source_count), 3)
        self.assertEqual(regimes, {"global", "crypto_5m"})
        self.assertEqual(len(ranking), 2)
        self.assertEqual({row["regime"] for row in ranking}, {"global", "crypto_5m"})
        self.assertTrue(all(row["wallet"] == "0xgood" for row in ranking))
        self.assertTrue(all(float(row["followability_score"]) == 0.8 for row in ranking))
        self.assertTrue(all(float(row["lifecycle_score"]) == 0.75 for row in ranking))
        self.assertTrue(all(row["tier"] == "A" for row in ranking))

    @patch("tip_v1.discovery.orchestrator.load_stored_profile")
    @patch("tip_v1.discovery.orchestrator.profile_wallet")
    @patch("tip_v1.discovery.orchestrator.run_pipeline")
    @patch("tip_v1.discovery.orchestrator.fetch_wallet_trades")
    @patch("tip_v1.discovery.orchestrator.fetch_recent_trades")
    @patch("tip_v1.discovery.orchestrator.scan_market_regime")
    @patch("tip_v1.discovery.orchestrator.fetch_default_leaderboard_wallets")
    def test_discovery_cycle_survives_single_source_failure(
        self,
        mock_leaderboard,
        mock_scan_market_regime,
        mock_recent_trades,
        mock_fetch_wallet_trades,
        mock_run_pipeline,
        mock_profile_wallet,
        mock_load_stored_profile,
    ) -> None:
        mock_leaderboard.side_effect = RuntimeError("leaderboard offline")
        mock_scan_market_regime.return_value = []
        mock_recent_trades.return_value = [
            {"user": "0xgood"},
            {"user": "0xgood"},
            {"user": "0xgood"},
            {"user": "0xgood"},
            {"user": "0xgood"},
        ]
        mock_fetch_wallet_trades.return_value = [
            {"conditionId": "m1", "side": "BUY"},
            {"conditionId": "m1", "side": "SELL"},
            {"conditionId": "m2", "side": "BUY"},
            {"conditionId": "m2", "side": "SELL"},
            {"conditionId": "m3", "side": "BUY"},
            {"conditionId": "m3", "side": "SELL"},
            {"conditionId": "m4", "side": "BUY"},
            {"conditionId": "m4", "side": "SELL"},
            {"conditionId": "m5", "side": "BUY"},
            {"conditionId": "m5", "side": "SELL"},
            {"conditionId": "m6", "side": "BUY"},
            {"conditionId": "m6", "side": "SELL"},
            {"conditionId": "m7", "side": "BUY"},
            {"conditionId": "m7", "side": "SELL"},
            {"conditionId": "m8", "side": "BUY"},
            {"conditionId": "m8", "side": "SELL"},
            {"conditionId": "m9", "side": "BUY"},
            {"conditionId": "m9", "side": "SELL"},
            {"conditionId": "m10", "side": "BUY"},
            {"conditionId": "m10", "side": "SELL"},
        ]
        mock_load_stored_profile.return_value = {
            "wallet": "0xgood",
            "confidence": 0.7,
            "followability_score": 0.8,
            "metrics": {"lifecycle_score": 0.7, "profit_factor": 2.0},
            "features": {"total_trades": 100},
        }

        result = run_discovery_cycle(
            self.settings,
            leaderboard_limit=10,
            recent_trade_limit=20,
            recent_trade_min_count=5,
            market_scan_limit=10,
            market_trade_limit=20,
            candidate_process_limit=10,
            prefilter_trade_limit=20,
        )

        self.assertEqual(result.promoted_count, 1)
        self.assertEqual(result.ranked_count, 1)
        mock_run_pipeline.assert_called_once_with(
            "0xgood",
            settings=self.settings,
            skip_enrichment=True,
        )

    @patch("tip_v1.discovery.orchestrator.load_stored_profile")
    def test_update_rankings_filters_low_lifecycle_wallets(self, mock_load_stored_profile) -> None:
        with managed_connection(self.settings) as connection:
            connection.execute(
                """
                INSERT INTO candidate_wallets(wallet, status, profiled_at)
                VALUES ('0xlowlife', 'PROFILED', CURRENT_TIMESTAMP)
                """
            )
            connection.execute(
                """
                INSERT INTO wallet_rankings(wallet, regime, followability_score, lifecycle_score, rank_score, tier)
                VALUES ('0xlowlife', 'global', 0.9, 0.1, 0.9, 'A')
                """
            )
            connection.commit()

        mock_load_stored_profile.return_value = {
            "wallet": "0xlowlife",
            "confidence": 0.9,
            "followability_score": 0.9,
            "metrics": {"lifecycle_score": 0.1, "profit_factor": 3.0},
            "features": {"total_trades": 200},
        }

        from tip_v1.discovery.orchestrator import update_rankings

        ranked = update_rankings(self.settings)

        with managed_connection(self.settings) as connection:
            ranking = connection.execute(
                "SELECT wallet FROM wallet_rankings WHERE wallet = '0xlowlife' AND regime = 'global'"
            ).fetchone()

        self.assertEqual(ranked, 0)
        self.assertIsNone(ranking)


if __name__ == "__main__":
    unittest.main()
