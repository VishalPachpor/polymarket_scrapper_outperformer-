from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tip_v1.config import get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.pre_signal_engine.core import build_pre_signal_dataset, load_pre_signal_dataset_csv, write_pre_signal_dataset_csv


class PreSignalDatasetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "pre_signal_dataset.sqlite3"
        self.settings = get_settings(self.db_path)
        initialize_database(self.settings)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _seed_trade(
        self,
        *,
        wallet: str,
        suffix: str,
        asset_id: str,
        market_id: str,
        side: str,
        price: float,
        size: float,
        timestamp: int,
    ) -> None:
        with managed_connection(self.settings) as connection:
            connection.execute(
                "INSERT INTO trades_raw(wallet, raw_json, dedupe_key) VALUES (?, '{}', ?)",
                (wallet, f"raw-{suffix}"),
            )
            raw_trade_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            connection.execute(
                """
                INSERT INTO trade_events(
                    raw_trade_id, trade_id, wallet, market_id, outcome, asset_id, side, price, size, timestamp
                )
                VALUES (?, ?, ?, ?, 'YES', ?, ?, ?, ?, ?)
                """,
                (
                    raw_trade_id,
                    f"trade-{suffix}",
                    wallet,
                    market_id,
                    asset_id,
                    side,
                    f"{price:.4f}",
                    f"{size:.4f}",
                    timestamp,
                ),
            )
            connection.commit()

    def test_build_pre_signal_dataset_creates_positive_and_negative_rows(self) -> None:
        wallet = "0xfast"
        self._seed_trade(
            wallet="0xother",
            suffix="neg-1",
            asset_id="asset-a",
            market_id="market-a",
            side="BUY",
            price=0.49,
            size=1.0,
            timestamp=60,
        )
        self._seed_trade(
            wallet="0xother",
            suffix="neg-2",
            asset_id="asset-a",
            market_id="market-a",
            side="BUY",
            price=0.50,
            size=2.0,
            timestamp=68,
        )
        self._seed_trade(
            wallet="0xother",
            suffix="pos-1",
            asset_id="asset-a",
            market_id="market-a",
            side="BUY",
            price=0.50,
            size=2.0,
            timestamp=90,
        )
        self._seed_trade(
            wallet="0xother",
            suffix="pos-2",
            asset_id="asset-a",
            market_id="market-a",
            side="BUY",
            price=0.51,
            size=3.0,
            timestamp=97,
        )
        self._seed_trade(
            wallet="0xother",
            suffix="pos-3",
            asset_id="asset-a",
            market_id="market-a",
            side="BUY",
            price=0.53,
            size=4.0,
            timestamp=99,
        )
        self._seed_trade(
            wallet=wallet,
            suffix="target",
            asset_id="asset-a",
            market_id="market-a",
            side="BUY",
            price=0.54,
            size=5.0,
            timestamp=100,
        )

        result = build_pre_signal_dataset(
            settings=self.settings,
            wallets=[wallet],
            window_seconds=10,
            prediction_horizon_seconds=10,
            limit=10,
            negative_ratio=1.0,
        )

        self.assertEqual(result.positive_examples, 1)
        self.assertEqual(result.negative_examples, 1)
        positive = next(row for row in result.rows if row.label == 1)
        self.assertEqual(positive.signal_time, 100)
        self.assertEqual(positive.target_side, "BUY")
        self.assertEqual(positive.trade_count, 3)
        self.assertAlmostEqual(positive.trade_velocity, 0.3)
        self.assertGreater(positive.directional_pressure, 0.0)
        self.assertGreater(positive.price_acceleration, 0.0)

    def test_dataset_csv_round_trip_preserves_rows(self) -> None:
        wallet = "0xfast"
        self._seed_trade(
            wallet=wallet,
            suffix="one",
            asset_id="asset-a",
            market_id="market-a",
            side="BUY",
            price=0.55,
            size=1.0,
            timestamp=100,
        )

        result = build_pre_signal_dataset(
            settings=self.settings,
            wallets=[wallet],
            window_seconds=10,
            prediction_horizon_seconds=10,
            limit=10,
            negative_ratio=0.0,
        )
        csv_path = write_pre_signal_dataset_csv(result.rows, output_path=Path(self.tempdir.name) / "dataset.csv")
        loaded = load_pre_signal_dataset_csv(csv_path)

        self.assertEqual(len(loaded), 1)
        self.assertEqual(loaded[0].asset_id, result.rows[0].asset_id)
        self.assertEqual(loaded[0].signal_time, result.rows[0].signal_time)


if __name__ == "__main__":
    unittest.main()
