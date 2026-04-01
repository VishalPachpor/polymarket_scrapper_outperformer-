from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tip_v1.config import get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.ingestion.fetch_trades import ingest_trade_payloads
from tip_v1.microstructure.collector import (
    LiveMicrostructureCollector,
    build_asset_poll_plan,
)
from tip_v1.normalization.normalize_trades import normalize_trades


class MicrostructureCollectorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "collector.sqlite3"
        self.settings = get_settings(self.db_path)
        initialize_database(self.settings)
        self.wallet = "0xCOLLECTOR"

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _insert_trade(self, asset_id: str, timestamp: int) -> None:
        inserted, duplicates = ingest_trade_payloads(
            self.wallet,
            [
                {
                    "id": f"trade-{asset_id}-{timestamp}",
                    "conditionId": f"market-{asset_id}",
                    "asset": asset_id,
                    "outcome": "YES",
                    "side": "BUY",
                    "price": 0.5,
                    "size": 10,
                    "timestamp": timestamp,
                }
            ],
            self.settings,
        )
        self.assertEqual(inserted, 1)
        self.assertEqual(duplicates, 0)
        normalize_trades(wallet=self.wallet, settings=self.settings)

    def test_build_asset_poll_plan_promotes_recently_active_assets(self) -> None:
        now_ts = 10_000
        self._insert_trade("asset-fast", now_ts - 5)
        self._insert_trade("asset-fast", now_ts - 4)
        self._insert_trade("asset-fast", now_ts - 3)
        self._insert_trade("asset-slow", now_ts - 10_000)

        with managed_connection(self.settings) as connection:
            connection.execute(
                """
                INSERT INTO microstructure_raw_book (
                    id,
                    asset_id,
                    observed_at,
                    source_hash,
                    raw_payload
                )
                VALUES (1, 'asset-fast', ?, 'hash-fast', '{}')
                """,
                (now_ts - 2,),
            )
            connection.execute(
                """
                INSERT INTO microstructure_book_snapshots (
                    snapshot_sequence_id,
                    raw_book_id,
                    asset_id,
                    market_id,
                    timestamp
                )
                VALUES (1, 1, 'asset-fast', 'market-asset-fast', ?)
                """,
                (now_ts - 2,),
            )
            connection.execute(
                """
                INSERT INTO microstructure_features (
                    snapshot_id,
                    asset_id,
                    timestamp,
                    price_change_30s,
                    snapshots_per_minute
                )
                VALUES (1, 'asset-fast', ?, 0.08, 12)
                """,
                (now_ts - 2,),
            )
            connection.commit()

        plans = build_asset_poll_plan([self.wallet], self.settings, now_ts=now_ts)
        by_asset = {plan.asset_id: plan for plan in plans}

        self.assertEqual(
            by_asset["asset-fast"].interval_seconds,
            self.settings.microstructure_fast_poll_seconds,
        )
        self.assertEqual(
            by_asset["asset-slow"].interval_seconds,
            self.settings.microstructure_slow_poll_seconds,
        )

    @patch("tip_v1.microstructure.collector.join_microstructure_with_trades")
    @patch("tip_v1.microstructure.collector.compute_microstructure_features")
    @patch("tip_v1.microstructure.collector.normalize_book_snapshots")
    @patch("tip_v1.microstructure.collector.ingest_orderbooks_for_assets")
    def test_collect_once_polls_due_assets_and_joins_wallet(
        self,
        mock_ingest,
        mock_normalize,
        mock_features,
        mock_join,
    ) -> None:
        now_ts = 20_000
        self._insert_trade("asset-1", now_ts - 1)

        mock_ingest.return_value = type("R", (), {"inserted_count": 1})()
        mock_normalize.return_value = type("R", (), {"inserted_count": 1})()
        mock_features.return_value = type("R", (), {"features_computed": 1})()

        collector = LiveMicrostructureCollector([self.wallet], self.settings)
        with patch("tip_v1.microstructure.collector.time.time", return_value=now_ts):
            result = collector.collect_once()

        self.assertEqual(result.polled_assets, 1)
        mock_ingest.assert_called_once()
        mock_join.assert_called_once_with(self.wallet, self.settings)


if __name__ == "__main__":
    unittest.main()
