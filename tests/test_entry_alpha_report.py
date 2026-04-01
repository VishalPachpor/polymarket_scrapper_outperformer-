from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tip_v1.cli.entry_alpha_report import (
    build_entry_alpha_report,
    load_wallet_entry_alpha_metrics,
)
from tip_v1.config import get_settings
from tip_v1.db.db import initialize_database, managed_connection


class EntryAlphaReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "entry_alpha.sqlite3"
        self.settings = get_settings(self.db_path)
        initialize_database(self.settings)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _seed_position(
        self,
        *,
        wallet: str,
        position_id_suffix: str,
        pnl: float,
        mfe: float,
        mae: float,
        time_to_mfe: int,
        duration: int = 300,
        entry_price: float = 0.50,
        size: float = 100.0,
    ) -> None:
        with managed_connection(self.settings) as connection:
            connection.execute(
                "INSERT INTO trades_raw(wallet, raw_json, dedupe_key) VALUES (?, '{}', ?)",
                (wallet, f"raw-{position_id_suffix}"),
            )
            raw_trade_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            connection.execute(
                """
                INSERT INTO trade_events(
                    raw_trade_id, trade_id, wallet, market_id, outcome, asset_id, side, price, size, timestamp
                )
                VALUES (?, ?, ?, ?, 'YES', ?, 'BUY', ?, ?, ?)
                """,
                (
                    raw_trade_id,
                    f"trade-{position_id_suffix}",
                    wallet,
                    f"market-{position_id_suffix}",
                    f"asset-{position_id_suffix}",
                    f"{entry_price:.4f}",
                    f"{size:.4f}",
                    1_700_000_000 + int(position_id_suffix),
                ),
            )
            trade_event_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            connection.execute(
                """
                INSERT INTO positions_reconstructed(
                    wallet, market_id, outcome, entry_trade_event_id, exit_trade_event_id, entry_price, exit_price,
                    size, pnl, entry_time, exit_time, duration, status, remaining_size, version
                )
                VALUES (?, ?, 'YES', ?, ?, ?, ?, ?, ?, ?, ?, ?, 'CLOSED', 0, 1)
                """,
                (
                    wallet,
                    f"market-{position_id_suffix}",
                    trade_event_id,
                    trade_event_id,
                    entry_price,
                    entry_price + (pnl / size),
                    size,
                    pnl,
                    1_700_000_000 + int(position_id_suffix),
                    1_700_000_000 + int(position_id_suffix) + duration,
                    duration,
                ),
            )
            position_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            connection.execute(
                """
                INSERT INTO position_path_metrics(
                    position_id, wallet, version, asset_id, sample_count, start_timestamp, end_timestamp,
                    entry_context_price, exit_context_price, max_price, min_price, mfe, mae, time_to_mfe, time_to_mae
                )
                VALUES (?, ?, 1, ?, 5, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    position_id,
                    wallet,
                    f"asset-{position_id_suffix}",
                    1_700_000_000 + int(position_id_suffix),
                    1_700_000_000 + int(position_id_suffix) + duration,
                    entry_price,
                    entry_price + (pnl / size),
                    entry_price + (mfe / size),
                    entry_price + (mae / size),
                    mfe,
                    mae,
                    time_to_mfe,
                    max(1, time_to_mfe // 2),
                ),
            )
            connection.commit()

    def test_load_wallet_entry_alpha_metrics_ranks_high_entry_low_capture_wallet(self) -> None:
        self._seed_position(wallet="0xgood_entry", position_id_suffix="1", pnl=2.0, mfe=10.0, mae=-1.0, time_to_mfe=30)
        self._seed_position(wallet="0xgood_entry", position_id_suffix="2", pnl=1.0, mfe=8.0, mae=-1.5, time_to_mfe=45)
        self._seed_position(wallet="0xgood_exit", position_id_suffix="3", pnl=4.0, mfe=5.0, mae=-1.0, time_to_mfe=90)
        self._seed_position(wallet="0xgood_exit", position_id_suffix="4", pnl=3.8, mfe=4.5, mae=-1.2, time_to_mfe=110)

        rows = load_wallet_entry_alpha_metrics(
            settings=self.settings,
            min_trades=2,
            top_n=10,
            mfe_return_threshold=0.10,
            weak_capture_threshold=0.50,
        )

        self.assertEqual([row.wallet for row in rows], ["0xgood_entry", "0xgood_exit"])
        self.assertAlmostEqual(rows[0].entry_alpha_rate, 1.0)
        self.assertLess(rows[0].avg_capture_ratio, rows[1].avg_capture_ratio)
        self.assertGreater(rows[0].execution_leak_rate, 0.0)

    def test_build_entry_alpha_report_for_single_wallet_includes_execution_metrics(self) -> None:
        self._seed_position(wallet="0xwallet", position_id_suffix="10", pnl=2.0, mfe=10.0, mae=-1.0, time_to_mfe=30)
        self._seed_position(wallet="0xwallet", position_id_suffix="11", pnl=-1.0, mfe=6.0, mae=-3.0, time_to_mfe=120)

        report = build_entry_alpha_report(
            settings=self.settings,
            wallet="0xwallet",
            min_trades=2,
            mfe_return_threshold=0.10,
        )

        self.assertIn("Entry Alpha Report: 0xwallet", report)
        self.assertIn("Entry Alpha Rate (>=10% MFE): 100.00%", report)
        self.assertIn("Capture Ratio:", report)
        self.assertIn("Giveback Ratio:", report)
        self.assertIn("Execution Leak Rate:", report)


if __name__ == "__main__":
    unittest.main()
