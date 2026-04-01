from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tip_v1.config import get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.wallet_engine.signal_builder import WalletSignal
from tip_v1.wallet_engine.wallet_paper_tracker import (
    WalletPaperTracker,
    WalletPaperTrackerConfig,
    compute_wallet_paper_metrics,
    record_wallet_signal,
    resolve_wallet_signal_outcomes,
)
from tip_v1.wallet_engine.wallet_runtime import WalletRuntimeCycle


class WalletPaperTrackerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "wallet_paper.sqlite3"
        self.settings = get_settings(self.db_path)
        initialize_database(self.settings)
        self.config = WalletPaperTrackerConfig(
            horizons_seconds=(3, 5, 10),
            minimum_follow_through_move=0.01,
        )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _insert_trade_event(
        self,
        *,
        asset_id: str,
        market_id: str,
        timestamp: int,
        price: str,
        wallet: str = "0xfeed",
        side: str = "BUY",
    ) -> None:
        with managed_connection(self.settings) as connection:
            connection.execute(
                "INSERT INTO trades_raw(wallet, raw_json, dedupe_key) VALUES (?, '{}', ?)",
                (wallet, f"seed-{asset_id}-{timestamp}-{price}"),
            )
            raw_trade_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            connection.execute(
                """
                INSERT INTO trade_events(
                    raw_trade_id, trade_id, wallet, market_id, outcome, asset_id, side, price, size, timestamp
                )
                VALUES (?, ?, ?, ?, 'YES', ?, ?, ?, '10', ?)
                """,
                (raw_trade_id, f"trade-{asset_id}-{timestamp}-{price}", wallet, market_id, asset_id, side, price, timestamp),
            )
            connection.commit()

    def test_record_and_resolve_wallet_buy_signal(self) -> None:
        signal = WalletSignal(
            market_id="market-1",
            asset_id="asset-1",
            wallet="0xabc",
            side="BUY",
            timestamp=1_700_000_000,
            price=0.40,
            score=0.82,
            confidence="HIGH",
            aligned_wallet_count=2,
        )
        with managed_connection(self.settings) as connection:
            self.assertTrue(record_wallet_signal(connection, signal))
            connection.commit()

        self._insert_trade_event(asset_id="asset-1", market_id="market-1", timestamp=1_700_000_003, price="0.404")
        self._insert_trade_event(asset_id="asset-1", market_id="market-1", timestamp=1_700_000_005, price="0.412")
        self._insert_trade_event(asset_id="asset-1", market_id="market-1", timestamp=1_700_000_010, price="0.420")

        with managed_connection(self.settings) as connection:
            resolved = resolve_wallet_signal_outcomes(
                connection,
                now_ts=1_700_000_011,
                config=self.config,
            )
            metrics = compute_wallet_paper_metrics(connection)
            row = connection.execute(
                """
                SELECT return_3s, return_5s, return_10s, max_favorable_return, max_adverse_return,
                       follow_through_3s, follow_through_5s, follow_through_10s, time_to_move
                FROM wallet_paper_outcomes
                """
            ).fetchone()

        self.assertEqual(resolved, 1)
        self.assertAlmostEqual(float(row["return_3s"]), 0.01)
        self.assertAlmostEqual(float(row["return_5s"]), 0.03)
        self.assertAlmostEqual(float(row["return_10s"]), 0.05)
        self.assertAlmostEqual(float(row["max_favorable_return"]), 0.05)
        self.assertAlmostEqual(float(row["max_adverse_return"]), 0.01)
        self.assertEqual(int(row["follow_through_3s"]), 1)
        self.assertEqual(int(row["follow_through_5s"]), 1)
        self.assertEqual(int(row["follow_through_10s"]), 1)
        self.assertEqual(int(row["time_to_move"]), 3)
        self.assertEqual(metrics.resolved_signals, 1)
        self.assertAlmostEqual(metrics.follow_through_rate_5s, 1.0)

    def test_resolve_wallet_sell_signal_uses_signed_returns(self) -> None:
        signal = WalletSignal(
            market_id="market-2",
            asset_id="asset-2",
            wallet="0xsell",
            side="SELL",
            timestamp=1_700_000_100,
            price=0.60,
            score=0.71,
            confidence="MEDIUM",
            aligned_wallet_count=1,
        )
        with managed_connection(self.settings) as connection:
            self.assertTrue(record_wallet_signal(connection, signal))
            connection.commit()

        self._insert_trade_event(asset_id="asset-2", market_id="market-2", timestamp=1_700_000_103, price="0.594", side="SELL")
        self._insert_trade_event(asset_id="asset-2", market_id="market-2", timestamp=1_700_000_105, price="0.582", side="SELL")
        self._insert_trade_event(asset_id="asset-2", market_id="market-2", timestamp=1_700_000_110, price="0.570", side="SELL")

        with managed_connection(self.settings) as connection:
            resolved = resolve_wallet_signal_outcomes(
                connection,
                now_ts=1_700_000_111,
                config=self.config,
            )
            row = connection.execute(
                """
                SELECT return_3s, return_5s, return_10s, max_favorable_return
                FROM wallet_paper_outcomes
                WHERE market_id = 'market-2'
                """
            ).fetchone()

        self.assertEqual(resolved, 1)
        self.assertAlmostEqual(float(row["return_3s"]), 0.01)
        self.assertAlmostEqual(float(row["return_5s"]), 0.03)
        self.assertAlmostEqual(float(row["return_10s"]), 0.05)
        self.assertAlmostEqual(float(row["max_favorable_return"]), 0.05)

    def test_tracker_records_shadow_candidates_in_discovery_mode(self) -> None:
        class StubRuntime:
            def __init__(self) -> None:
                self.tracker: WalletPaperTracker | None = None

            def run_once(self):
                assert self.tracker is not None
                self.tracker._capture_signal(
                    WalletSignal(
                        market_id="market-shadow",
                        asset_id="asset-shadow",
                        wallet="0xshadow",
                        side="BUY",
                        timestamp=1_700_001_000,
                        price=0.35,
                        score=0.22,
                        confidence="LOW",
                        aligned_wallet_count=1,
                        reason="low_live_score",
                    )
                )
                return WalletRuntimeCycle(
                    trades_seen=1,
                    top_wallet_trades=1,
                    shadow_candidates=1,
                    signals_emitted=0,
                    registry_size=10,
                    last_event_id=10,
                    latency_seconds=0.2,
                    rejection_counts={"low_live_score": 1},
                )

        stub_runtime = StubRuntime()
        tracker = WalletPaperTracker(
            settings=self.settings,
            config=WalletPaperTrackerConfig(capture_shadow_candidates=True),
            runtime=stub_runtime,
        )
        stub_runtime.tracker = tracker

        cycle = tracker.run_once(now_ts=1_700_001_020)

        with managed_connection(self.settings) as connection:
            row = connection.execute(
                """
                SELECT wallet, market_id, signal_score, confidence
                FROM wallet_paper_signals
                WHERE market_id = 'market-shadow'
                """
            ).fetchone()

        self.assertEqual(cycle.runtime_signals, 0)
        self.assertEqual(cycle.captured_signals, 1)
        self.assertEqual(cycle.recorded_signals, 1)
        self.assertEqual(row["wallet"], "0xshadow")
        self.assertEqual(row["market_id"], "market-shadow")
        self.assertAlmostEqual(float(row["signal_score"]), 0.22)
        self.assertEqual(row["confidence"], "LOW")


if __name__ == "__main__":
    unittest.main()
