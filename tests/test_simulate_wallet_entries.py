from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tip_v1.cli.simulate_wallet_entries import (
    STOP_LOSS,
    TAKE_PROFIT,
    TIME_EXIT,
    WalletEntryExitConfig,
    render_wallet_entry_simulation_report,
    simulate_wallet_entries,
)
from tip_v1.config import get_settings
from tip_v1.db.db import initialize_database, managed_connection


class SimulateWalletEntriesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "wallet_entry_sim.sqlite3"
        self.settings = get_settings(self.db_path)
        initialize_database(self.settings)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _seed_position(
        self,
        *,
        wallet: str,
        suffix: str,
        entry_price: float,
        size: float,
        pnl: float,
        entry_time: int,
        duration: int,
        history: list[tuple[int, float]],
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
                VALUES (?, ?, ?, ?, 'YES', ?, 'BUY', ?, ?, ?)
                """,
                (
                    raw_trade_id,
                    f"trade-{suffix}",
                    wallet,
                    f"market-{suffix}",
                    f"asset-{suffix}",
                    f"{entry_price:.4f}",
                    f"{size:.4f}",
                    entry_time,
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
                    f"market-{suffix}",
                    trade_event_id,
                    trade_event_id,
                    entry_price,
                    entry_price + (pnl / size),
                    size,
                    pnl,
                    entry_time,
                    entry_time + duration,
                    duration,
                ),
            )
            connection.executemany(
                """
                INSERT INTO market_price_history(asset_id, timestamp, price)
                VALUES (?, ?, ?)
                """,
                [(f"asset-{suffix}", timestamp, price) for timestamp, price in history],
            )
            connection.commit()

    def test_simulate_wallet_entries_applies_tp_sl_and_time_exit(self) -> None:
        wallet = "0xwallet"
        self._seed_position(
            wallet=wallet,
            suffix="tp",
            entry_price=0.50,
            size=10.0,
            pnl=0.2,
            entry_time=1_700_000_000,
            duration=300,
            history=[(1_700_000_010, 0.53)],
        )
        self._seed_position(
            wallet=wallet,
            suffix="sl",
            entry_price=0.50,
            size=10.0,
            pnl=-0.1,
            entry_time=1_700_000_100,
            duration=300,
            history=[(1_700_000_110, 0.48)],
        )
        self._seed_position(
            wallet=wallet,
            suffix="time",
            entry_price=0.50,
            size=10.0,
            pnl=0.0,
            entry_time=1_700_000_200,
            duration=300,
            history=[(1_700_000_380, 0.51)],
        )

        result = simulate_wallet_entries(
            wallet=wallet,
            settings=self.settings,
            config=WalletEntryExitConfig(
                take_profit=0.05,
                stop_loss=0.02,
                max_hold_seconds=180,
            ),
        )

        self.assertEqual(result.total_trades, 3)
        self.assertAlmostEqual(result.total_pnl, 0.2)
        self.assertAlmostEqual(result.actual_wallet_pnl, 0.1)
        self.assertAlmostEqual(result.pnl_delta, 0.1)
        self.assertAlmostEqual(result.take_profit_rate, 1 / 3)
        self.assertAlmostEqual(result.stop_loss_rate, 1 / 3)
        self.assertAlmostEqual(result.time_exit_rate, 1 / 3)
        self.assertEqual([trade.exit_type for trade in result.trades], [TAKE_PROFIT, STOP_LOSS, TIME_EXIT])

    def test_render_report_can_compare_wallet_pnl(self) -> None:
        wallet = "0xwallet"
        self._seed_position(
            wallet=wallet,
            suffix="one",
            entry_price=0.50,
            size=10.0,
            pnl=0.2,
            entry_time=1_700_000_000,
            duration=300,
            history=[(1_700_000_010, 0.53)],
        )

        result = simulate_wallet_entries(
            wallet=wallet,
            settings=self.settings,
            config=WalletEntryExitConfig(),
        )
        report = render_wallet_entry_simulation_report(result, compare_wallet_pnl=True)

        self.assertIn("Wallet Entry Simulation: 0xwallet", report)
        self.assertIn("Wallet PnL:", report)
        self.assertIn("Simulated PnL:", report)
        self.assertIn("Delta:", report)

    def test_simulate_wallet_entries_reprices_after_entry_delay(self) -> None:
        wallet = "0xdelay"
        self._seed_position(
            wallet=wallet,
            suffix="delay",
            entry_price=0.50,
            size=10.0,
            pnl=-0.2,
            entry_time=1_700_000_000,
            duration=300,
            history=[
                (1_700_000_001, 0.50),
                (1_700_000_005, 0.54),
                (1_700_000_010, 0.53),
            ],
        )

        result = simulate_wallet_entries(
            wallet=wallet,
            settings=self.settings,
            config=WalletEntryExitConfig(
                take_profit=0.03,
                stop_loss=0.02,
                max_hold_seconds=20,
                entry_delay_seconds=5,
            ),
        )

        self.assertEqual(result.total_trades, 1)
        self.assertAlmostEqual(result.total_pnl, -0.1)
        self.assertEqual(result.entry_delay_seconds, 5)
        self.assertEqual(result.trades[0].simulated_entry_time, 1_700_000_005)
        self.assertAlmostEqual(result.trades[0].simulated_entry_price, 0.54)
        self.assertEqual(result.trades[0].exit_type, TIME_EXIT)


if __name__ == "__main__":
    unittest.main()
