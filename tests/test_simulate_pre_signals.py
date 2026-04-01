from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tip_v1.cli.simulate_wallet_entries import WalletEntryExitConfig
from tip_v1.config import get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.pre_signal_engine.core import PreSignalDatasetRow, simulate_pre_signals, write_pre_signal_dataset_csv


class SimulatePreSignalsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "simulate_pre_signals.sqlite3"
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

    def test_simulate_pre_signals_scores_and_simulates(self) -> None:
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

        result = simulate_pre_signals(
            dataset_path=dataset_path,
            settings=self.settings,
            threshold=0.0,
            config=WalletEntryExitConfig(
                take_profit=0.05,
                stop_loss=0.02,
                max_hold_seconds=180,
                entry_delay_seconds=0,
            ),
        )

        self.assertEqual(result.emitted_signals, 2)
        self.assertEqual(result.emitted_positive, 1)
        self.assertEqual(result.emitted_negative, 1)
        self.assertAlmostEqual(result.precision, 0.5)
        self.assertAlmostEqual(result.directional_accuracy, 1.0)
        self.assertAlmostEqual(result.total_return, 0.04)
        self.assertAlmostEqual(result.take_profit_rate, 0.5)
        self.assertAlmostEqual(result.stop_loss_rate, 0.5)

    def test_simulate_pre_signals_supports_score_band(self) -> None:
        dataset_path = Path(self.tempdir.name) / "pre_signal_dataset_band.csv"
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
                trade_count=40,
                trade_velocity=4.0,
                volume_total=80.0,
                volume_last_2s=30.0,
                volume_last_3s=60.0,
                volume_last_5s=70.0,
                volume_burst=0.75,
                burst_freshness=-0.5,
                buy_volume=70.0,
                sell_volume=10.0,
                directional_pressure=0.75,
                price_start=0.50,
                price_end=0.60,
                price_acceleration=0.10,
                price_move_3s=0.10,
                pre_move_vs_window=1.00,
                accel_prev=0.02,
                accel_change=0.08,
                micro_consistency=12.0,
                direction_switches=0,
                stability=1.0,
                last_side="BUY",
                window_seconds=10,
            ),
        ]
        write_pre_signal_dataset_csv(rows, output_path=dataset_path)
        self._seed_price_history(asset_id="asset-a", samples=[(100, 0.50), (102, 0.53)])
        self._seed_price_history(asset_id="asset-b", samples=[(200, 0.60), (202, 0.63)])

        result = simulate_pre_signals(
            dataset_path=dataset_path,
            settings=self.settings,
            threshold=0.0,
            max_threshold=1.0,
            config=WalletEntryExitConfig(),
        )
        band_result = simulate_pre_signals(
            dataset_path=dataset_path,
            settings=self.settings,
            threshold=0.0,
            max_threshold=0.60,
            config=WalletEntryExitConfig(),
        )

        self.assertEqual(result.emitted_signals, 2)
        self.assertLessEqual(band_result.emitted_signals, result.emitted_signals)
        self.assertLessEqual(band_result.emitted_positive, result.emitted_positive)


if __name__ == "__main__":
    unittest.main()
