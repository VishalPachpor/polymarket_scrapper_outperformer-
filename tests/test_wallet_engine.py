from __future__ import annotations

import json
import tempfile
import unittest
from collections import deque
from pathlib import Path
from unittest.mock import patch

from tip_v1.config import get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.live.trade_ingestor import LiveIngestionCycleResult
from tip_v1.wallet_engine.signal_builder import (
    WalletSignalConfig,
    build_wallet_signal,
    evaluate_wallet_signal,
)
from tip_v1.wallet_engine.trade_stream import DbBackedTradeStream, Trade, TradeStreamConfig
from tip_v1.wallet_engine.wallet_runtime import WalletEngineRuntime, WalletRuntimeConfig
from tip_v1.wallet_engine.wallet_scoring import load_wallet_base_scores, score_wallet_activity
from tip_v1.wallet_engine.wallet_state import WalletStateTracker


class WalletEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "wallet_engine.sqlite3"
        self.settings = get_settings(self.db_path)
        initialize_database(self.settings)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _seed_ranked_wallet(self, wallet: str, *, market_id: str = "market-1") -> None:
        with managed_connection(self.settings) as connection:
            connection.execute(
                "INSERT INTO trades_raw(wallet, raw_json, dedupe_key) VALUES (?, '{}', ?)",
                (wallet, f"seed-entry-{wallet}-{market_id}"),
            )
            raw_trade_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            connection.execute(
                """
                INSERT INTO trade_events(
                    raw_trade_id, trade_id, wallet, market_id, outcome, asset_id, side, price, size, timestamp
                )
                VALUES (?, ?, ?, ?, 'YES', 'asset-seed', 'BUY', '0.40', '10', 1700000000)
                """,
                (raw_trade_id, f"seed-trade-{wallet}-{market_id}", wallet, market_id),
            )
            trade_event_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            connection.execute(
                """
                INSERT INTO wallet_rankings(wallet, regime, followability_score, lifecycle_score, rank_score, tier)
                VALUES (?, 'global', 0.4, 0.7, 0.65, 'B')
                """,
                (wallet,),
            )
            connection.execute(
                """
                INSERT INTO wallet_profiles(wallet, version, trader_type, confidence, features_json, metrics_json, behavior_json, edge_json, failures_json)
                VALUES (?, 1, 'MOMENTUM', 0.8, ?, '{}', '{}', '[]', '[]')
                """,
                (
                    wallet,
                    json.dumps(
                        {
                            "realized_pnl_per_minute": 0.12,
                            "pnl_per_day": 150.0,
                        }
                    ),
                ),
            )
            connection.execute(
                """
                INSERT INTO positions_reconstructed(
                    wallet, market_id, outcome, entry_trade_event_id, exit_trade_event_id, entry_price, exit_price,
                    size, pnl, entry_time, exit_time, duration, status, remaining_size, version
                )
                VALUES (?, ?, 'YES', ?, ?, 0.40, 0.48, 10, 0.8, 1700000000, 1700000120, 120, 'CLOSED', 0, 1)
                """,
                (wallet, market_id, trade_event_id, trade_event_id),
            )
            connection.commit()

    def _seed_trade_event(self, wallet: str, *, event_id_suffix: str, market_id: str, asset_id: str, timestamp: int, price: str = "0.40") -> None:
        with managed_connection(self.settings) as connection:
            connection.execute(
                "INSERT INTO trades_raw(wallet, raw_json, dedupe_key) VALUES (?, '{}', ?)",
                (wallet, f"dedupe-{event_id_suffix}"),
            )
            raw_trade_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            connection.execute(
                """
                INSERT INTO trade_events(
                    raw_trade_id, trade_id, wallet, market_id, outcome, asset_id, side, price, size, timestamp
                )
                VALUES (?, ?, ?, ?, 'YES', ?, 'BUY', ?, '10', ?)
                """,
                (raw_trade_id, f"trade-{event_id_suffix}", wallet, market_id, asset_id, price, timestamp),
            )
            connection.commit()

    def test_load_wallet_base_scores_includes_best_markets(self) -> None:
        wallet = "0xabc"
        self._seed_ranked_wallet(wallet, market_id="market-special")

        scores = load_wallet_base_scores(settings=self.settings, limit=10)

        self.assertIn(wallet, scores)
        self.assertEqual(scores[wallet].best_markets, ("market-special",))
        self.assertAlmostEqual(scores[wallet].rank_score, 0.65)

    def test_signal_builder_accepts_specialist_burst_and_rejects_chasing(self) -> None:
        wallet = "0xabc"
        self._seed_ranked_wallet(wallet, market_id="market-1")
        base = load_wallet_base_scores(settings=self.settings, limit=10)[wallet]
        tracker = WalletStateTracker()
        trade_a = Trade(1, "trade-1", wallet, "market-1", "asset-1", "YES", "BUY", 0.40, 10.0, 1_700_000_000)
        trade_b = Trade(2, "trade-2", wallet, "market-1", "asset-1", "YES", "BUY", 0.405, 10.0, 1_700_000_005)
        tracker.update(trade_a)
        activity = tracker.update(trade_b)
        score = score_wallet_activity(base, activity)

        signal = build_wallet_signal(
            trade=trade_b,
            activity=activity,
            wallet_score=score,
            recent_market_trades=deque([trade_a, trade_b]),
            aligned_wallet_count=1,
        )
        self.assertIsNotNone(signal)

        chasing = build_wallet_signal(
            trade=Trade(3, "trade-3", wallet, "market-1", "asset-1", "YES", "BUY", 0.50, 10.0, 1_700_000_004),
            activity=activity,
            wallet_score=score,
            recent_market_trades=deque([trade_a]),
            aligned_wallet_count=1,
            config=WalletSignalConfig(max_price_move_pct=0.05),
        )
        self.assertIsNone(chasing)

        rejected = evaluate_wallet_signal(
            trade=Trade(3, "trade-3", wallet, "market-1", "asset-1", "YES", "BUY", 0.50, 10.0, 1_700_000_004),
            activity=activity,
            wallet_score=score,
            recent_market_trades=deque([trade_a]),
            aligned_wallet_count=1,
            config=WalletSignalConfig(max_price_move_pct=0.05),
        )
        self.assertFalse(rejected.accepted)
        self.assertEqual(rejected.rejection_reason, "chasing_move")

    @patch("tip_v1.wallet_engine.trade_stream.ingest_recent_trade_stream")
    def test_db_backed_trade_stream_emits_new_normalized_events(self, mock_ingest_recent_trade_stream) -> None:
        wallet = "0xabc"
        self._seed_trade_event(wallet, event_id_suffix="1", market_id="market-1", asset_id="asset-1", timestamp=1_700_000_000)
        mock_ingest_recent_trade_stream.return_value = LiveIngestionCycleResult(
            fetched_count=1,
            inserted_raw_count=0,
            duplicate_raw_count=1,
            missing_wallet_count=0,
            normalized_inserted_count=0,
            normalized_skipped_count=0,
            quarantined_count=0,
            failed_count=0,
            latency_seconds=0.1,
        )
        stream = DbBackedTradeStream(
            settings=self.settings,
            config=TradeStreamConfig(start_from_latest=False),
        )

        cycle = stream.run_once()

        self.assertEqual(cycle.emitted_count, 1)
        self.assertEqual(cycle.last_event_id, 1)

    @patch("tip_v1.wallet_engine.trade_stream.ingest_recent_trade_stream")
    def test_db_backed_trade_stream_survives_ingestion_timeout(self, mock_ingest_recent_trade_stream) -> None:
        mock_ingest_recent_trade_stream.side_effect = TimeoutError("timed out")
        stream = DbBackedTradeStream(
            settings=self.settings,
            config=TradeStreamConfig(start_from_latest=False),
        )

        cycle = stream.run_once()

        self.assertEqual(cycle.emitted_count, 0)
        self.assertEqual(cycle.ingestion.failed_count, 1)
        self.assertEqual(cycle.last_event_id, 0)

    @patch("tip_v1.wallet_engine.trade_stream.ingest_recent_trade_stream")
    def test_runtime_emits_signal_for_ranked_wallet_burst(self, mock_ingest_recent_trade_stream) -> None:
        wallet = "0xabc"
        self._seed_ranked_wallet(wallet)
        self._seed_trade_event(wallet, event_id_suffix="1", market_id="market-1", asset_id="asset-1", timestamp=1_700_000_000, price="0.40")
        self._seed_trade_event(wallet, event_id_suffix="2", market_id="market-1", asset_id="asset-1", timestamp=1_700_000_003, price="0.405")
        mock_ingest_recent_trade_stream.return_value = LiveIngestionCycleResult(
            fetched_count=2,
            inserted_raw_count=0,
            duplicate_raw_count=2,
            missing_wallet_count=0,
            normalized_inserted_count=0,
            normalized_skipped_count=0,
            quarantined_count=0,
            failed_count=0,
            latency_seconds=0.1,
        )

        signals = []
        runtime = WalletEngineRuntime(
            settings=self.settings,
            config=WalletRuntimeConfig(
                stream=TradeStreamConfig(start_from_latest=False),
            ),
            on_signal_callback=signals.append,
        )
        runtime.trade_stream._last_event_id = 1

        cycle = runtime.run_once()

        self.assertEqual(cycle.trades_seen, 2)
        self.assertEqual(cycle.top_wallet_trades, 2)
        self.assertEqual(cycle.signals_emitted, 1)
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].wallet, wallet)
        self.assertEqual(signals[0].market_id, "market-1")
        self.assertEqual(cycle.shadow_candidates, 2)
        self.assertEqual(cycle.rejection_counts, {"no_burst": 1})

    @patch("tip_v1.wallet_engine.trade_stream.ingest_recent_trade_stream")
    def test_runtime_shadow_mode_emits_candidates_even_when_rejected(self, mock_ingest_recent_trade_stream) -> None:
        wallet = "0xabc"
        self._seed_ranked_wallet(wallet)
        self._seed_trade_event(wallet, event_id_suffix="1", market_id="market-1", asset_id="asset-1", timestamp=1_700_000_000, price="0.40")
        mock_ingest_recent_trade_stream.return_value = LiveIngestionCycleResult(
            fetched_count=1,
            inserted_raw_count=0,
            duplicate_raw_count=1,
            missing_wallet_count=0,
            normalized_inserted_count=0,
            normalized_skipped_count=0,
            quarantined_count=0,
            failed_count=0,
            latency_seconds=0.1,
        )

        shadow_candidates = []
        runtime = WalletEngineRuntime(
            settings=self.settings,
            config=WalletRuntimeConfig(
                stream=TradeStreamConfig(start_from_latest=False),
                emit_shadow_candidates=True,
            ),
            on_shadow_candidate_callback=shadow_candidates.append,
        )
        runtime.trade_stream._last_event_id = 1

        cycle = runtime.run_once()

        self.assertEqual(cycle.signals_emitted, 0)
        self.assertEqual(cycle.shadow_candidates, 1)
        self.assertEqual(cycle.rejection_counts, {"no_burst": 1})
        self.assertEqual(len(shadow_candidates), 1)
        self.assertEqual(shadow_candidates[0].reason, "no_burst")


if __name__ == "__main__":
    unittest.main()
