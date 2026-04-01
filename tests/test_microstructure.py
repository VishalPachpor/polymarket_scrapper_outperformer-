from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tip_v1.config import get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.ingestion.fetch_trades import ingest_trade_payloads
from tip_v1.microstructure.compute_features import compute_microstructure_features
from tip_v1.microstructure.fetch_microstructure import ingest_orderbook_payloads
from tip_v1.microstructure.join_with_trades import join_microstructure_with_trades
from tip_v1.microstructure.normalize_microstructure import normalize_book_snapshots
from tip_v1.normalization.normalize_trades import normalize_trades


class MicrostructurePipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "tip_microstructure.sqlite3"
        self.settings = get_settings(self.db_path)
        initialize_database(self.settings)
        self.wallet = "0xMICRO"

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _insert_trade(self) -> None:
        inserted, duplicates = ingest_trade_payloads(
            self.wallet,
            [
                {
                    "id": "trade-1",
                    "conditionId": "market-1",
                    "asset": "asset-1",
                    "outcome": "YES",
                    "side": "BUY",
                    "price": 0.56,
                    "size": 10,
                    "timestamp": 1060,
                }
            ],
            self.settings,
        )
        self.assertEqual(inserted, 1)
        self.assertEqual(duplicates, 0)
        result = normalize_trades(wallet=self.wallet, settings=self.settings)
        self.assertEqual(result.inserted_count, 1)
        self.assertEqual(result.failed_count, 0)

    def test_normalize_book_snapshots_and_join_without_lookahead(self) -> None:
        self._insert_trade()
        inserted, duplicates = ingest_orderbook_payloads(
            [
                {
                    "asset_id": "asset-1",
                    "observed_at": "2026-03-31T09:00:00Z",
                    "payload": {
                        "market": "market-1",
                        "asset_id": "asset-1",
                        "timestamp": "1000",
                        "bids": [{"price": "0.49", "size": "100"}, {"price": "0.48", "size": "80"}],
                        "asks": [{"price": "0.51", "size": "120"}, {"price": "0.52", "size": "70"}],
                        "min_order_size": "5",
                        "tick_size": "0.01",
                        "neg_risk": False,
                        "hash": "hash-1000",
                    },
                },
                {
                    "asset_id": "asset-1",
                    "observed_at": 1025,
                    "payload": {
                        "market": "market-1",
                        "asset_id": "asset-1",
                        "timestamp": "1025",
                        "bids": [{"price": "0.51", "size": "110"}, {"price": "0.50", "size": "90"}],
                        "asks": [{"price": "0.53", "size": "125"}, {"price": "0.54", "size": "75"}],
                        "min_order_size": "5",
                        "tick_size": "0.01",
                        "neg_risk": False,
                        "hash": "hash-1025",
                    },
                },
                {
                    "asset_id": "asset-1",
                    "observed_at": 1050,
                    "payload": {
                        "market": "market-1",
                        "asset_id": "asset-1",
                        "timestamp": "1050",
                        "bids": [{"price": "0.53", "size": "115"}, {"price": "0.52", "size": "85"}],
                        "asks": [{"price": "0.55", "size": "130"}, {"price": "0.56", "size": "65"}],
                        "min_order_size": "5",
                        "tick_size": "0.01",
                        "neg_risk": False,
                        "hash": "hash-1050",
                    },
                },
                {
                    "asset_id": "asset-1",
                    "observed_at": 1058,
                    "payload": {
                        "market": "market-1",
                        "asset_id": "asset-1",
                        "timestamp": "1058",
                        "bids": [{"price": "0.55", "size": "120"}, {"price": "0.54", "size": "80"}],
                        "asks": [{"price": "0.57", "size": "140"}, {"price": "0.58", "size": "60"}],
                        "min_order_size": "5",
                        "tick_size": "0.01",
                        "neg_risk": False,
                        "hash": "hash-1058",
                    },
                },
                {
                    "asset_id": "asset-1",
                    "observed_at": 1065,
                    "payload": {
                        "market": "market-1",
                        "asset_id": "asset-1",
                        "timestamp": "1065",
                        "bids": [{"price": "0.60", "size": "140"}],
                        "asks": [{"price": "0.62", "size": "150"}],
                        "min_order_size": "5",
                        "tick_size": "0.01",
                        "neg_risk": False,
                        "hash": "hash-1065",
                    },
                },
            ],
            self.settings,
        )
        self.assertEqual(inserted, 5)
        self.assertEqual(duplicates, 0)

        normalization = normalize_book_snapshots(self.settings)
        self.assertEqual(normalization.inserted_count, 5)

        features = compute_microstructure_features(self.settings)
        self.assertEqual(features.features_computed, 5)

        join_result = join_microstructure_with_trades(self.wallet, self.settings)
        self.assertEqual(join_result.trades_processed, 1)
        self.assertEqual(join_result.trades_with_snapshot, 1)
        self.assertEqual(join_result.high_confidence_count, 1)

        with managed_connection(self.settings) as connection:
            snapshot = connection.execute(
                """
                SELECT
                    asset_id,
                    market_id,
                    timestamp,
                    best_bid,
                    best_ask,
                    mid_price,
                    spread,
                    bid_depth_1,
                    ask_depth_1,
                    total_bid_depth,
                    total_ask_depth,
                    imbalance
                FROM microstructure_book_snapshots
                WHERE asset_id = ? AND timestamp = 1058
                """,
                ("asset-1",),
            ).fetchone()
            context = connection.execute(
                """
                SELECT
                    snapshot_timestamp,
                    snapshot_lag_seconds,
                    lag_confidence,
                    best_bid,
                    best_ask,
                    mid_price,
                    spread,
                    price_change_5s,
                    price_change_30s,
                    price_change_5m,
                    snapshots_per_minute
                FROM trade_entry_context
                WHERE wallet = ?
                """,
                (self.wallet,),
            ).fetchone()

        self.assertEqual(snapshot["asset_id"], "asset-1")
        self.assertEqual(snapshot["market_id"], "market-1")
        self.assertEqual(snapshot["timestamp"], 1058)
        self.assertAlmostEqual(snapshot["best_bid"], 0.55)
        self.assertAlmostEqual(snapshot["best_ask"], 0.57)
        self.assertAlmostEqual(snapshot["mid_price"], 0.56)
        self.assertAlmostEqual(snapshot["spread"], 0.02, places=12)
        self.assertAlmostEqual(snapshot["bid_depth_1"], 120.0)
        self.assertAlmostEqual(snapshot["ask_depth_1"], 140.0)
        self.assertAlmostEqual(snapshot["total_bid_depth"], 200.0)
        self.assertAlmostEqual(snapshot["total_ask_depth"], 200.0)
        self.assertAlmostEqual(snapshot["imbalance"], 0.5)
        self.assertEqual(context["snapshot_timestamp"], 1058)
        self.assertEqual(context["snapshot_lag_seconds"], 2)
        self.assertEqual(context["lag_confidence"], "HIGH")
        self.assertAlmostEqual(context["best_bid"], 0.55)
        self.assertAlmostEqual(context["best_ask"], 0.57)
        self.assertAlmostEqual(context["mid_price"], 0.56)
        self.assertAlmostEqual(context["price_change_5s"], (0.56 - 0.54) / 0.54, places=12)
        self.assertAlmostEqual(context["price_change_30s"], (0.56 - 0.52) / 0.52, places=12)
        self.assertIsNone(context["price_change_5m"])
        self.assertEqual(context["snapshots_per_minute"], 4.0)

    def test_trade_context_marks_missing_snapshot(self) -> None:
        self._insert_trade()

        join_result = join_microstructure_with_trades(self.wallet, self.settings)

        self.assertEqual(join_result.trades_processed, 1)
        self.assertEqual(join_result.trades_with_snapshot, 0)
        self.assertEqual(join_result.missing_snapshot_count, 1)

        with managed_connection(self.settings) as connection:
            context = connection.execute(
                """
                SELECT lag_confidence, snapshot_id, snapshot_timestamp
                FROM trade_entry_context
                WHERE wallet = ?
                """,
                (self.wallet,),
            ).fetchone()

        self.assertEqual(dict(context), {
            "lag_confidence": "MISSING",
            "snapshot_id": None,
            "snapshot_timestamp": None,
        })


if __name__ == "__main__":
    unittest.main()
