from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from urllib.error import URLError

from tip_v1.config import get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.enrichment.fetch_price_history import (
    _parse_history_payload,
    fetch_price_history_window,
    fetch_wallet_price_history,
)


class PriceHistoryFetchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "price_history.sqlite3"
        self.settings = get_settings(self.db_path)
        initialize_database(self.settings)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_parse_history_payload_accepts_common_shapes(self) -> None:
        self.assertEqual(_parse_history_payload({"history": [{"t": 1, "p": 0.5}]}), [{"t": 1, "p": 0.5}])
        self.assertEqual(_parse_history_payload({"data": {"history": [{"t": 2, "p": 0.6}]}}), [{"t": 2, "p": 0.6}])
        self.assertEqual(_parse_history_payload({"data": [{"t": 3, "p": 0.7}]}), [{"t": 3, "p": 0.7}])
        self.assertEqual(_parse_history_payload([{"t": 4, "p": 0.8}]), [{"t": 4, "p": 0.8}])

    def test_fetch_price_history_window_falls_back_to_curl_when_urlopen_fails(self) -> None:
        payload = {"history": [{"t": 100, "p": 0.42}]}
        completed = SimpleNamespace(stdout=json.dumps(payload))

        with (
            patch("tip_v1.enrichment.fetch_price_history.urlopen", side_effect=URLError("dns failed")),
            patch("tip_v1.enrichment.fetch_price_history.subprocess.run", return_value=completed) as mock_run,
        ):
            history = fetch_price_history_window(
                "asset-1",
                start_ts=90,
                end_ts=110,
                fidelity=1,
                settings=self.settings,
            )

        self.assertEqual(history, [{"t": 100, "p": 0.42}])
        mock_run.assert_called_once()

    def test_fetch_wallet_price_history_inserts_samples(self) -> None:
        with managed_connection(self.settings) as connection:
            connection.execute(
                """
                INSERT INTO trades_raw(wallet, raw_json, dedupe_key)
                VALUES ('0xwallet', '{}', 'dedupe-1')
                """
            )
            raw_trade_id_1 = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            connection.execute(
                """
                INSERT INTO trades_raw(wallet, raw_json, dedupe_key)
                VALUES ('0xwallet', '{}', 'dedupe-2')
                """
            )
            raw_trade_id_2 = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            connection.execute(
                """
                INSERT INTO trade_events(
                    raw_trade_id, trade_id, wallet, market_id, outcome, asset_id, side, price, size, timestamp
                )
                VALUES ('raw-1', 'trade-1', '0xwallet', 'market-1', 'YES', 'asset-1', 'BUY', 0.40, 10, 1000)
                """.replace("'raw-1'", str(raw_trade_id_1))
            )
            connection.execute(
                """
                INSERT INTO trade_events(
                    raw_trade_id, trade_id, wallet, market_id, outcome, asset_id, side, price, size, timestamp
                )
                VALUES ('raw-2', 'trade-2', '0xwallet', 'market-1', 'YES', 'asset-1', 'SELL', 0.50, 10, 1100)
                """.replace("'raw-2'", str(raw_trade_id_2))
            )
            connection.commit()

        with patch(
            "tip_v1.enrichment.fetch_price_history.fetch_price_history_window",
            return_value=[{"t": 1005, "p": 0.41}, {"t": 1095, "p": 0.49}],
        ):
            result = fetch_wallet_price_history("0xwallet", settings=self.settings)

        self.assertEqual(result.assets_requested, 1)
        self.assertEqual(result.assets_fetched, 1)
        self.assertEqual(result.assets_failed, 0)
        self.assertEqual(result.fetched_samples, 2)
        self.assertEqual(result.inserted_samples, 2)

        with managed_connection(self.settings) as connection:
            rows = connection.execute(
                """
                SELECT asset_id, timestamp, price
                FROM market_price_history
                ORDER BY timestamp
                """
            ).fetchall()

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["asset_id"], "asset-1")
        self.assertEqual(rows[0]["timestamp"], 1005)
        self.assertAlmostEqual(rows[0]["price"], 0.41)


if __name__ == "__main__":
    unittest.main()
