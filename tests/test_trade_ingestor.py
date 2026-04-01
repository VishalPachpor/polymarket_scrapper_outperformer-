from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tip_v1.config import get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.live.trade_ingestor import (
    LiveIngestionConfig,
    _extract_trade_wallet,
    _ingest_raw_trade_payloads,
    ingest_recent_trade_stream,
)


class LiveTradeIngestorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "live.sqlite3"
        self.settings = get_settings(self.db_path)
        initialize_database(self.settings)
        self.config = LiveIngestionConfig(
            poll_interval_seconds=1,
            recent_trade_limit=50,
            normalize_batch_size=100,
        )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_extract_trade_wallet_prefers_user_then_wallet_fields(self) -> None:
        self.assertEqual(_extract_trade_wallet({"user": "0xabc"}), "0xabc")
        self.assertEqual(_extract_trade_wallet({"wallet": "0xdef"}), "0xdef")
        self.assertEqual(_extract_trade_wallet({"maker": "0x123"}), "0x123")
        self.assertEqual(_extract_trade_wallet({"proxyWallet": "0x456"}), "0x456")
        self.assertIsNone(_extract_trade_wallet({"maker": "not-a-wallet"}))

    @patch("tip_v1.live.trade_ingestor.fetch_recent_trades")
    def test_ingest_recent_trade_stream_inserts_and_normalizes_recent_trades(self, mock_fetch_recent_trades) -> None:
        mock_fetch_recent_trades.return_value = [
            {
                "id": "trade-1",
                "user": "0xaaa",
                "conditionId": "market-1",
                "outcome": "YES",
                "side": "BUY",
                "price": "0.41",
                "size": "10",
                "timestamp": 1_700_000_000,
                "asset": "asset-1",
            },
            {
                "id": "trade-2",
                "user": "0xbbb",
                "conditionId": "market-2",
                "outcome": "NO",
                "side": "SELL",
                "price": "0.58",
                "size": "5",
                "timestamp": 1_700_000_001,
                "asset": "asset-2",
            },
        ]

        result = ingest_recent_trade_stream(settings=self.settings, config=self.config)

        self.assertEqual(result.fetched_count, 2)
        self.assertEqual(result.inserted_raw_count, 2)
        self.assertEqual(result.duplicate_raw_count, 0)
        self.assertEqual(result.normalized_inserted_count, 2)

        with managed_connection(self.settings) as connection:
            raw_count = connection.execute("SELECT COUNT(*) AS n FROM trades_raw").fetchone()["n"]
            event_count = connection.execute("SELECT COUNT(*) AS n FROM trade_events").fetchone()["n"]

        self.assertEqual(int(raw_count), 2)
        self.assertEqual(int(event_count), 2)

    @patch("tip_v1.live.trade_ingestor.fetch_recent_trades")
    def test_ingest_recent_trade_stream_is_idempotent_across_duplicate_polls(self, mock_fetch_recent_trades) -> None:
        payload = [
            {
                "id": "trade-1",
                "user": "0xaaa",
                "conditionId": "market-1",
                "outcome": "YES",
                "side": "BUY",
                "price": "0.41",
                "size": "10",
                "timestamp": 1_700_000_000,
                "asset": "asset-1",
            }
        ]
        mock_fetch_recent_trades.return_value = payload

        first = ingest_recent_trade_stream(settings=self.settings, config=self.config)
        second = ingest_recent_trade_stream(settings=self.settings, config=self.config)

        self.assertEqual(first.inserted_raw_count, 1)
        self.assertEqual(second.inserted_raw_count, 0)
        self.assertEqual(second.duplicate_raw_count, 1)
        self.assertEqual(second.normalized_inserted_count, 0)

        with managed_connection(self.settings) as connection:
            raw_count = connection.execute("SELECT COUNT(*) AS n FROM trades_raw").fetchone()["n"]
            event_count = connection.execute("SELECT COUNT(*) AS n FROM trade_events").fetchone()["n"]

        self.assertEqual(int(raw_count), 1)
        self.assertEqual(int(event_count), 1)

    @patch("tip_v1.live.trade_ingestor.fetch_recent_trades")
    def test_ingest_recent_trade_stream_skips_payloads_without_wallets(self, mock_fetch_recent_trades) -> None:
        mock_fetch_recent_trades.return_value = [
            {
                "id": "trade-1",
                "conditionId": "market-1",
                "outcome": "YES",
                "side": "BUY",
                "price": "0.41",
                "size": "10",
                "timestamp": 1_700_000_000,
                "asset": "asset-1",
            }
        ]

        result = ingest_recent_trade_stream(settings=self.settings, config=self.config)

        self.assertEqual(result.fetched_count, 1)
        self.assertEqual(result.inserted_raw_count, 0)
        self.assertEqual(result.missing_wallet_count, 1)
        self.assertEqual(result.normalized_inserted_count, 0)

    def test_ingest_raw_trade_payloads_retries_when_database_is_locked(self) -> None:
        payload = [
            {
                "id": "trade-1",
                "user": "0xaaa",
                "conditionId": "market-1",
                "outcome": "YES",
                "side": "BUY",
                "price": "0.41",
                "size": "10",
                "timestamp": 1_700_000_000,
                "asset": "asset-1",
            }
        ]

        real_managed_connection = managed_connection
        call_count = {"n": 0}

        def flaky_connection(settings):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise sqlite3.OperationalError("database is locked")
            return real_managed_connection(settings)

        with patch("tip_v1.live.trade_ingestor.managed_connection", side_effect=flaky_connection), patch(
            "tip_v1.live.trade_ingestor.time.sleep"
        ):
            inserted, duplicates, missing = _ingest_raw_trade_payloads(
                payload,
                settings=self.settings,
                retry_attempts=2,
                retry_delay_seconds=0.01,
            )

        self.assertEqual(inserted, 1)
        self.assertEqual(duplicates, 0)
        self.assertEqual(missing, 0)

    def test_ingest_raw_trade_payloads_raises_after_retry_exhaustion_on_lock(self) -> None:
        payload = [
            {
                "id": "trade-1",
                "user": "0xaaa",
                "conditionId": "market-1",
                "outcome": "YES",
                "side": "BUY",
                "price": "0.41",
                "size": "10",
                "timestamp": 1_700_000_000,
                "asset": "asset-1",
            }
        ]

        with patch(
            "tip_v1.live.trade_ingestor.managed_connection",
            side_effect=sqlite3.OperationalError("database is locked"),
        ), patch("tip_v1.live.trade_ingestor.time.sleep"):
            with self.assertRaises(sqlite3.OperationalError):
                _ingest_raw_trade_payloads(
                    payload,
                    settings=self.settings,
                    retry_attempts=1,
                    retry_delay_seconds=0.01,
                )


if __name__ == "__main__":
    unittest.main()
