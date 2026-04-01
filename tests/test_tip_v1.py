from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tip_v1.config import get_settings
from tip_v1.db.db import get_connection, initialize_database, managed_connection
from tip_v1.enrichment.compute_path_metrics import compute_position_path_metrics
from tip_v1.ingestion.fetch_trades import ingest_trade_payloads
from tip_v1.normalization.normalize_trades import normalize_trade_payload, normalize_trades
from tip_v1.reconstruction.reconstruct_positions import (
    NegativeInventoryError,
    reconstruct_positions,
)


class TipV1PipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "tip_v1_test.sqlite3"
        self.settings = get_settings(self.db_path)
        initialize_database(self.settings)
        self.wallet = "0xTEST"

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _insert_and_reconstruct(self, payloads: list[dict]) -> None:
        inserted, duplicates = ingest_trade_payloads(self.wallet, payloads, self.settings)
        self.assertEqual(inserted, len(payloads))
        self.assertEqual(duplicates, 0)
        normalize_result = normalize_trades(wallet=self.wallet, settings=self.settings)
        self.assertEqual(normalize_result.failed_count, 0)
        self.assertEqual(normalize_result.quarantined_count, 0)
        reconstruct_positions(self.wallet, settings=self.settings)

    def test_normalize_outcome_aliases(self) -> None:
        normalized_yes = normalize_trade_payload(
            1,
            self.wallet,
            {
                "id": "trade-1",
                "conditionId": "market-1",
                "outcome": "yes",
                "side": "buy",
                "price": "0.40",
                "size": "10",
                "timestamp": "1000",
            },
        )
        normalized_no = normalize_trade_payload(
            2,
            self.wallet,
            {
                "id": "trade-2",
                "conditionId": "market-1",
                "outcome": "0",
                "side": "SELL",
                "price": "0.60",
                "size": "5",
                "timestamp": "1001",
            },
        )

        self.assertEqual(normalized_yes.outcome, "YES")
        self.assertEqual(normalized_no.outcome, "NO")

    def test_partial_fill_reconstruction(self) -> None:
        payloads = [
            {
                "id": "trade-1",
                "conditionId": "market-1",
                "outcome": "YES",
                "side": "BUY",
                "price": 0.40,
                "size": 100,
                "timestamp": 1000,
            },
            {
                "id": "trade-2",
                "conditionId": "market-1",
                "outcome": "YES",
                "side": "BUY",
                "price": 0.45,
                "size": 50,
                "timestamp": 1010,
            },
            {
                "id": "trade-3",
                "conditionId": "market-1",
                "outcome": "YES",
                "side": "SELL",
                "price": 0.60,
                "size": 120,
                "timestamp": 1020,
            },
            {
                "id": "trade-4",
                "conditionId": "market-1",
                "outcome": "YES",
                "side": "SELL",
                "price": 0.55,
                "size": 30,
                "timestamp": 1030,
            },
        ]

        self._insert_and_reconstruct(payloads)

        with managed_connection(self.settings) as connection:
            rows = connection.execute(
                """
                SELECT status, entry_price, exit_price, size, pnl, remaining_size
                FROM positions_reconstructed
                WHERE wallet = ?
                ORDER BY id
                """,
                (self.wallet,),
            ).fetchall()

        self.assertEqual(
            [tuple(row) for row in rows],
            [
                ("CLOSED", 0.4, 0.6, 100.0, 20.0, 0.0),
                ("CLOSED", 0.45, 0.6, 20.0, 3.0, 0.0),
                ("CLOSED", 0.45, 0.55, 30.0, 3.0, 0.0),
            ],
        )

    def test_interleaved_flow_keeps_remaining_inventory(self) -> None:
        payloads = [
            {
                "id": "trade-1",
                "conditionId": "market-1",
                "outcome": "YES",
                "side": "BUY",
                "price": 0.40,
                "size": 100,
                "timestamp": 1000,
            },
            {
                "id": "trade-2",
                "conditionId": "market-1",
                "outcome": "YES",
                "side": "SELL",
                "price": 0.50,
                "size": 40,
                "timestamp": 1010,
            },
            {
                "id": "trade-3",
                "conditionId": "market-1",
                "outcome": "YES",
                "side": "BUY",
                "price": 0.45,
                "size": 20,
                "timestamp": 1020,
            },
            {
                "id": "trade-4",
                "conditionId": "market-1",
                "outcome": "YES",
                "side": "SELL",
                "price": 0.55,
                "size": 30,
                "timestamp": 1030,
            },
        ]

        self._insert_and_reconstruct(payloads)

        with managed_connection(self.settings) as connection:
            rows = connection.execute(
                """
                SELECT status, entry_price, exit_price, size, pnl, remaining_size
                FROM positions_reconstructed
                WHERE wallet = ?
                ORDER BY id
                """,
                (self.wallet,),
            ).fetchall()

        self.assertEqual(
            [tuple(row) for row in rows],
            [
                ("CLOSED", 0.4, 0.5, 40.0, 4.0, 0.0),
                ("CLOSED", 0.4, 0.55, 30.0, 4.5, 0.0),
                ("OPEN", 0.4, None, 30.0, None, 30.0),
                ("OPEN", 0.45, None, 20.0, None, 20.0),
            ],
        )

    def test_same_timestamp_uses_trade_id_for_tie_breaking(self) -> None:
        payloads = [
            {
                "id": "trade-b",
                "conditionId": "market-1",
                "outcome": "YES",
                "side": "BUY",
                "price": 0.40,
                "size": 10,
                "timestamp": 1000,
            },
            {
                "id": "trade-a",
                "conditionId": "market-1",
                "outcome": "YES",
                "side": "BUY",
                "price": 0.50,
                "size": 10,
                "timestamp": 1000,
            },
            {
                "id": "trade-c",
                "conditionId": "market-1",
                "outcome": "YES",
                "side": "SELL",
                "price": 0.60,
                "size": 15,
                "timestamp": 1000,
            },
        ]

        self._insert_and_reconstruct(payloads)

        with managed_connection(self.settings) as connection:
            rows = connection.execute(
                """
                SELECT entry_price, exit_price, size, pnl
                FROM positions_reconstructed
                WHERE wallet = ? AND status = 'CLOSED'
                ORDER BY id
                """,
                (self.wallet,),
            ).fetchall()

        self.assertEqual(
            [tuple(row) for row in rows],
            [
                (0.5, 0.6, 10.0, 1.0),
                (0.4, 0.6, 5.0, 1.0),
            ],
        )

    def test_missing_outcome_is_quarantined(self) -> None:
        inserted, duplicates = ingest_trade_payloads(
            self.wallet,
            [
                {
                    "id": "trade-bad",
                    "conditionId": "market-1",
                    "side": "BUY",
                    "price": 0.40,
                    "size": 10,
                    "timestamp": 1000,
                }
            ],
            self.settings,
        )
        self.assertEqual(inserted, 1)
        self.assertEqual(duplicates, 0)

        result = normalize_trades(wallet=self.wallet, settings=self.settings)
        self.assertEqual(result.inserted_count, 0)
        self.assertEqual(result.failed_count, 1)
        self.assertEqual(result.quarantined_count, 1)

        with managed_connection(self.settings) as connection:
            quarantine = connection.execute(
                "SELECT wallet, reason FROM quarantined_trades WHERE wallet = ?",
                (self.wallet,),
            ).fetchone()

        self.assertIsNotNone(quarantine)
        self.assertEqual(quarantine["wallet"], self.wallet)
        self.assertIn("Missing outcome", quarantine["reason"])

    def test_conflicting_trade_id_is_quarantined(self) -> None:
        payloads = [
            {
                "id": "trade-1",
                "conditionId": "market-1",
                "outcome": "YES",
                "side": "BUY",
                "price": 0.40,
                "size": 10,
                "timestamp": 1000,
            },
            {
                "id": "trade-1",
                "conditionId": "market-1",
                "outcome": "YES",
                "side": "BUY",
                "price": 0.41,
                "size": 10,
                "timestamp": 1000,
            },
        ]

        inserted, duplicates = ingest_trade_payloads(self.wallet, payloads, self.settings)
        self.assertEqual(inserted, 2)
        self.assertEqual(duplicates, 0)

        result = normalize_trades(wallet=self.wallet, settings=self.settings)
        self.assertEqual(result.inserted_count, 1)
        self.assertEqual(result.failed_count, 1)
        self.assertEqual(result.quarantined_count, 1)

        with managed_connection(self.settings) as connection:
            quarantine = connection.execute(
                "SELECT reason FROM quarantined_trades WHERE wallet = ?",
                (self.wallet,),
            ).fetchone()

        self.assertIn("Canonical trade id collision", quarantine["reason"])

    def test_negative_inventory_raises_in_fail_fast_mode(self) -> None:
        inserted, duplicates = ingest_trade_payloads(
            self.wallet,
            [
                {
                    "id": "trade-1",
                    "conditionId": "market-1",
                    "outcome": "YES",
                    "side": "SELL",
                    "price": 0.50,
                    "size": 10,
                    "timestamp": 1000,
                }
            ],
            self.settings,
        )
        self.assertEqual(inserted, 1)
        self.assertEqual(duplicates, 0)

        result = normalize_trades(wallet=self.wallet, settings=self.settings)
        self.assertEqual(result.failed_count, 0)

        with self.assertRaises(NegativeInventoryError):
            reconstruct_positions(
                self.wallet,
                settings=self.settings,
                fail_on_unmatched_sells=True,
            )

    def test_decimal_cashflows_do_not_trigger_false_phantom_pnl(self) -> None:
        payloads = []
        for index in range(1, 101):
            payloads.append(
                {
                    "id": f"buy-{index}",
                    "conditionId": "market-1",
                    "outcome": "YES",
                    "side": "BUY",
                    "price": "0.123456789012",
                    "size": "0.987654321098",
                    "timestamp": 1000 + index,
                }
            )

        for index in range(1, 101):
            payloads.append(
                {
                    "id": f"sell-{index}",
                    "conditionId": "market-1",
                    "outcome": "YES",
                    "side": "SELL",
                    "price": "0.223456789012",
                    "size": "0.987654321098",
                    "timestamp": 2000 + index,
                }
            )

        self._insert_and_reconstruct(payloads)

        with managed_connection(self.settings) as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS positions, ROUND(SUM(pnl), 12) AS total_pnl
                FROM positions_reconstructed
                WHERE wallet = ? AND status = 'CLOSED'
                """,
                (self.wallet,),
            ).fetchone()

        self.assertEqual(row["positions"], 100)
        self.assertAlmostEqual(row["total_pnl"], 9.87654321098, places=9)

    def test_path_metrics_capture_mfe_and_mae_for_closed_position(self) -> None:
        payloads = [
            {
                "id": "trade-1",
                "conditionId": "market-1",
                "asset": "asset-1",
                "outcome": "YES",
                "side": "BUY",
                "price": 0.40,
                "size": 100,
                "timestamp": 1000,
            },
            {
                "id": "trade-2",
                "conditionId": "market-1",
                "asset": "asset-1",
                "outcome": "YES",
                "side": "SELL",
                "price": 0.50,
                "size": 100,
                "timestamp": 1100,
            },
        ]

        self._insert_and_reconstruct(payloads)

        with managed_connection(self.settings) as connection:
            connection.executemany(
                """
                INSERT INTO market_price_history (asset_id, timestamp, price)
                VALUES (?, ?, ?)
                """,
                [
                    ("asset-1", 1010, 0.35),
                    ("asset-1", 1030, 0.60),
                    ("asset-1", 1070, 0.45),
                ],
            )
            connection.commit()

        result = compute_position_path_metrics(self.wallet, settings=self.settings)
        self.assertEqual(result.positions_processed, 1)
        self.assertEqual(result.positions_with_path_data, 1)

        with managed_connection(self.settings) as connection:
            row = connection.execute(
                """
                SELECT sample_count, mfe, mae, time_to_mfe, time_to_mae, max_price, min_price
                FROM position_path_metrics
                WHERE wallet = ?
                """,
                (self.wallet,),
            ).fetchone()

        self.assertEqual(dict(row), {
            "sample_count": 5,
            "mfe": 20.0,
            "mae": -5.0,
            "time_to_mfe": 30,
            "time_to_mae": 10,
            "max_price": 0.6,
            "min_price": 0.35,
        })

    def test_path_metrics_for_open_position_use_latest_observed_price(self) -> None:
        payloads = [
            {
                "id": "trade-1",
                "conditionId": "market-1",
                "asset": "asset-1",
                "outcome": "YES",
                "side": "BUY",
                "price": 0.40,
                "size": 50,
                "timestamp": 1000,
            }
        ]

        self._insert_and_reconstruct(payloads)

        with managed_connection(self.settings) as connection:
            connection.executemany(
                """
                INSERT INTO market_price_history (asset_id, timestamp, price)
                VALUES (?, ?, ?)
                """,
                [
                    ("asset-1", 1010, 0.42),
                    ("asset-1", 1025, 0.38),
                    ("asset-1", 1040, 0.47),
                ],
            )
            connection.commit()

        compute_position_path_metrics(self.wallet, settings=self.settings)

        with managed_connection(self.settings) as connection:
            row = connection.execute(
                """
                SELECT sample_count, end_timestamp, exit_context_price, mfe, mae
                FROM position_path_metrics
                WHERE wallet = ?
                """,
                (self.wallet,),
            ).fetchone()

        self.assertEqual(dict(row), {
            "sample_count": 4,
            "end_timestamp": 1040,
            "exit_context_price": 0.47,
            "mfe": 3.5,
            "mae": -1.0,
        })

    def test_reconstruction_clears_stale_path_metrics_before_rebuild(self) -> None:
        payloads = [
            {
                "id": "trade-1",
                "conditionId": "market-1",
                "asset": "asset-1",
                "outcome": "YES",
                "side": "BUY",
                "price": 0.40,
                "size": 100,
                "timestamp": 1000,
            },
            {
                "id": "trade-2",
                "conditionId": "market-1",
                "asset": "asset-1",
                "outcome": "YES",
                "side": "SELL",
                "price": 0.50,
                "size": 100,
                "timestamp": 1100,
            },
        ]

        self._insert_and_reconstruct(payloads)
        with managed_connection(self.settings) as connection:
            connection.execute(
                """
                INSERT INTO market_price_history (asset_id, timestamp, price)
                VALUES (?, ?, ?)
                """,
                ("asset-1", 1050, 0.55),
            )
            connection.commit()

        compute_position_path_metrics(self.wallet, settings=self.settings)
        reconstruct_positions(self.wallet, settings=self.settings)

        with managed_connection(self.settings) as connection:
            path_metric_count = connection.execute(
                """
                SELECT COUNT(*)
                FROM position_path_metrics
                WHERE wallet = ?
                """,
                (self.wallet,),
            ).fetchone()[0]

        self.assertEqual(path_metric_count, 0)

    def test_initialize_database_migrates_legacy_metrics_table(self) -> None:
        connection = get_connection(self.settings)
        connection.execute("DROP TABLE IF EXISTS trader_metrics_summary")
        connection.execute(
            """
            CREATE TABLE trader_metrics_summary (
                wallet TEXT PRIMARY KEY,
                total_positions INTEGER NOT NULL,
                win_rate REAL NOT NULL,
                avg_pnl REAL NOT NULL,
                avg_hold_time REAL NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )
            """
        )
        connection.commit()
        connection.close()

        initialize_database(self.settings)

        with managed_connection(self.settings) as connection:
            columns = {
                row["name"]
                for row in connection.execute(
                    "PRAGMA table_info(trader_metrics_summary)"
                ).fetchall()
            }

        self.assertIn("win_count", columns)
        self.assertIn("loss_count", columns)
        self.assertIn("breakeven_count", columns)


if __name__ == "__main__":
    unittest.main()
