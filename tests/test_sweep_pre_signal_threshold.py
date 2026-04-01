from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tip_v1.cli.simulate_wallet_entries import WalletEntryExitConfig
from tip_v1.config import get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.pre_signal_engine.core import (
    PreSignalDatasetRow,
    build_pre_signal_threshold_sweep_report,
    load_pre_signal_dataset_csv,
    sweep_pre_signal_thresholds,
    write_pre_signal_dataset_csv,
)


class SweepPreSignalThresholdTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "sweep_pre_signal.sqlite3"
        self.settings = get_settings(self.db_path)
        initialize_database(self.settings)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _seed_price_history(self, *, asset_id: str, samples: list[tuple[int, float]]) -> None:
        with managed_connection(self.settings) as connection:
            connection.executemany(
                """
                INSERT INTO market_price_history(asset_id, timestamp, price)
                VALUES (?, ?, ?)
                """,
                [(asset_id, timestamp, price) for timestamp, price in samples],
            )
            connection.commit()

    def test_threshold_sweep_reports_distribution_and_rows(self) -> None:
        dataset_path = Path(self.tempdir.name) / "pre_signal_dataset.csv"
        rows = [
            PreSignalDatasetRow(
                asset_id="asset-a",
                market_id="market-a",
                signal_time=100,
                label=1,
                target_side="BUY",
                wallet_count=1,
                source_trade_count=1,
                trade_count=5,
                trade_velocity=0.5,
                volume_total=10.0,
                volume_last_2s=4.0,
                volume_last_3s=6.0,
                volume_last_5s=7.0,
                volume_burst=0.6,
                burst_freshness=-0.3,
                buy_volume=8.0,
                sell_volume=2.0,
                directional_pressure=0.6,
                price_start=0.50,
                price_end=0.53,
                price_acceleration=0.03,
                price_move_3s=0.02,
                pre_move_vs_window=0.67,
                accel_prev=0.01,
                accel_change=0.01,
                micro_consistency=4.0,
                direction_switches=0,
                stability=1.0,
                last_side="BUY",
                window_seconds=10,
            ),
            PreSignalDatasetRow(
                asset_id="asset-b",
                market_id="market-b",
                signal_time=200,
                label=0,
                target_side="",
                wallet_count=0,
                source_trade_count=0,
                trade_count=4,
                trade_velocity=0.4,
                volume_total=8.0,
                volume_last_2s=2.0,
                volume_last_3s=4.0,
                volume_last_5s=5.0,
                volume_burst=0.5,
                burst_freshness=-0.375,
                buy_volume=2.0,
                sell_volume=6.0,
                directional_pressure=-0.5,
                price_start=0.50,
                price_end=0.48,
                price_acceleration=-0.02,
                price_move_3s=-0.01,
                pre_move_vs_window=0.50,
                accel_prev=-0.01,
                accel_change=0.0,
                micro_consistency=3.0,
                direction_switches=1,
                stability=2 / 3,
                last_side="SELL",
                window_seconds=10,
            ),
        ]
        write_pre_signal_dataset_csv(rows, output_path=dataset_path)
        self._seed_price_history(asset_id="asset-a", samples=[(100, 0.50), (102, 0.53)])
        self._seed_price_history(asset_id="asset-b", samples=[(200, 0.50), (201, 0.51)])

        loaded_rows = load_pre_signal_dataset_csv(dataset_path)
        sweep_rows = sweep_pre_signal_thresholds(
            dataset_path=dataset_path,
            thresholds=[0.0, 1.1],
            settings=self.settings,
            config=WalletEntryExitConfig(),
        )
        report = build_pre_signal_threshold_sweep_report(
            dataset_path=dataset_path,
            rows=loaded_rows,
            sweep_rows=sweep_rows,
        )

        self.assertEqual(len(sweep_rows), 2)
        self.assertEqual(sweep_rows[0].signals_emitted, 2)
        self.assertEqual(sweep_rows[1].signals_emitted, 0)
        self.assertIn("Score Distribution:", report)
        self.assertIn("Threshold Results:", report)


if __name__ == "__main__":
    unittest.main()
