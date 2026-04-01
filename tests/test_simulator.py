from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tip_v1.config import get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.simulator.simulate_strategy import (
    _passes_momentum_trigger,
    apply_strategy,
    get_strategy_config,
    load_positions,
    render_strategy_report,
    simulate,
)


class StrategySimulatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "simulator.sqlite3"
        self.settings = get_settings(self.db_path)
        initialize_database(self.settings)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _insert_position(
        self,
        connection,
        *,
        wallet: str,
        market_id: str,
        outcome: str,
        asset_id: str,
        entry_price: float,
        pnl: float,
        duration: int,
        entry_time: int,
        size: float = 10.0,
        mfe: float | None = None,
        mae: float = -0.5,
        time_to_mfe: int | None = None,
    ) -> None:
        connection.execute(
            """
            INSERT INTO trades_raw(wallet, raw_json, dedupe_key)
            VALUES (?, '{}', ?)
            """,
            (wallet, f"raw-{wallet}-{market_id}-{entry_time}"),
        )
        raw_trade_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])

        connection.execute(
            """
            INSERT INTO trade_events(
                raw_trade_id, trade_id, wallet, market_id, outcome, asset_id, side, price, size, timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?, 'BUY', ?, '10', ?)
            """,
            (
                raw_trade_id,
                f"trade-{wallet}-{market_id}-{entry_time}",
                wallet,
                market_id,
                outcome,
                asset_id,
                str(entry_price),
                entry_time,
            ),
        )
        entry_trade_event_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])

        connection.execute(
            """
            INSERT INTO positions_reconstructed(
                wallet, market_id, outcome, entry_trade_event_id, exit_trade_event_id,
                entry_price, exit_price, size, pnl, entry_time, exit_time, duration,
                status, remaining_size, version
            )
            VALUES (?, ?, ?, ?, NULL, ?, ?, 10, ?, ?, ?, ?, 'CLOSED', 0, 1)
            """,
            (
                wallet,
                market_id,
                outcome,
                entry_trade_event_id,
                entry_price,
                entry_price + 0.05,
                pnl,
                entry_time,
                entry_time + duration,
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
                asset_id,
                entry_time,
                entry_time + duration,
                entry_price,
                entry_price + 0.05,
                entry_price + 0.08,
                max(0.0, entry_price - 0.03),
                abs(pnl) + 1.0 if mfe is None else mfe,
                mae,
                min(duration, 120) if time_to_mfe is None else time_to_mfe,
                min(duration, 30),
            ),
        )

    def test_apply_strategy_respects_variant_rules(self) -> None:
        rows = [
            {"position_id": 1, "wallet": "0xw", "market_id": "m1", "outcome": "YES", "entry_price": 0.40, "size": 10.0, "entry_time": 1000, "duration": 45, "pnl": 2.0, "mfe": 2.5, "mae": -0.2, "time_to_mfe": 30, "time_after_mfe": 15, "prev_trade_price_60s": 0.34, "recent_trade_count_60s": 9},
            {"position_id": 2, "wallet": "0xw", "market_id": "m2", "outcome": "YES", "entry_price": 0.35, "size": 10.0, "entry_time": 1100, "duration": 120, "pnl": 3.0, "mfe": 3.5, "mae": -0.3, "time_to_mfe": 90, "time_after_mfe": 30, "prev_trade_price_60s": 0.30, "recent_trade_count_60s": 8},
            {"position_id": 3, "wallet": "0xw", "market_id": "m3", "outcome": "YES", "entry_price": 0.45, "size": 10.0, "entry_time": 1200, "duration": 240, "pnl": 4.0, "mfe": 4.5, "mae": -0.4, "time_to_mfe": 150, "time_after_mfe": 90, "prev_trade_price_60s": 0.39, "recent_trade_count_60s": 10},
            {"position_id": 4, "wallet": "0xw", "market_id": "m4", "outcome": "YES", "entry_price": 0.60, "size": 10.0, "entry_time": 1300, "duration": 120, "pnl": 5.0, "mfe": 5.5, "mae": -0.5, "time_to_mfe": 60, "time_after_mfe": 60, "prev_trade_price_60s": 0.54, "recent_trade_count_60s": 9},
            {"position_id": 5, "wallet": "0xw", "market_id": "m5", "outcome": "YES", "entry_price": 0.42, "size": 10.0, "entry_time": 1400, "duration": 360, "pnl": -2.0, "mfe": 4.0, "mae": -1.0, "time_to_mfe": 180, "time_after_mfe": 180, "prev_trade_price_60s": 0.37, "recent_trade_count_60s": 8},
            {"position_id": 6, "wallet": "0xw", "market_id": "m6", "outcome": "YES", "entry_price": 0.38, "size": 10.0, "entry_time": 1500, "duration": 420, "pnl": -1.0, "mfe": 2.0, "mae": -0.5, "time_to_mfe": 240, "time_after_mfe": 180, "prev_trade_price_60s": 0.35, "recent_trade_count_60s": 5},
            {"position_id": 7, "wallet": "0xw", "market_id": "m7", "outcome": "YES", "entry_price": 0.41, "size": 10.0, "entry_time": 1600, "duration": 300, "pnl": -3.0, "mfe": 0.0, "mae": -2.0, "time_to_mfe": None, "time_after_mfe": 300, "prev_trade_price_60s": 0.35, "recent_trade_count_60s": 8},
        ]

        baseline = apply_strategy(rows, get_strategy_config("baseline"))
        strict_timing = apply_strategy(rows, get_strategy_config("strict_timing"))
        soft_exit = apply_strategy(rows, get_strategy_config("soft_exit"))
        aggressive = apply_strategy(rows, get_strategy_config("aggressive"))

        self.assertEqual([trade.position_id for trade in baseline], [2, 3, 5, 7])
        self.assertEqual([round(trade.simulated_pnl, 2) for trade in baseline], [1.46, 1.88, 0.87, -1.24])
        self.assertEqual([trade.position_id for trade in strict_timing], [2, 3, 5, 7])
        self.assertEqual([round(trade.simulated_pnl, 2) for trade in strict_timing], [1.46, 1.88, 0.87, -1.24])
        self.assertEqual([round(trade.simulated_pnl, 2) for trade in soft_exit], [1.17, 1.5, 0.69, -1.24])
        self.assertEqual([trade.position_id for trade in aggressive], [1, 2, 3, 5, 7])
        self.assertEqual([round(trade.simulated_pnl, 2) for trade in aggressive], [0.51, 1.46, 1.88, 0.87, -1.24])

    def test_momentum_trigger_prefers_moderate_move_with_active_tape(self) -> None:
        config = get_strategy_config("baseline")
        self.assertTrue(
            _passes_momentum_trigger(
                {
                    "entry_price": 0.40,
                    "prev_trade_price_60s": 0.35,
                    "recent_trade_count_60s": 9,
                    "spread": None,
                    "imbalance": None,
                    "price_change_30s": None,
                    "volatility_30s": None,
                },
                config,
            )
        )
        self.assertFalse(
            _passes_momentum_trigger(
                {
                    "entry_price": 0.40,
                    "prev_trade_price_60s": 0.39,
                    "recent_trade_count_60s": 9,
                    "spread": None,
                    "imbalance": None,
                    "price_change_30s": None,
                    "volatility_30s": None,
                },
                config,
            )
        )
        self.assertFalse(
            _passes_momentum_trigger(
                {
                    "entry_price": 0.40,
                    "prev_trade_price_60s": 0.30,
                    "recent_trade_count_60s": 3,
                    "spread": None,
                    "imbalance": None,
                    "price_change_30s": None,
                    "volatility_30s": None,
                },
                config,
            )
        )

    def test_simulate_computes_metrics_for_baseline(self) -> None:
        with managed_connection(self.settings) as connection:
            self._insert_position(
                connection,
                wallet="0xalpha",
                market_id="market-a",
                outcome="YES",
                asset_id="asset-a",
                entry_price=0.40,
                pnl=3.0,
                duration=120,
                entry_time=1_700_000_000,
                mfe=4.0,
                mae=-0.3,
                time_to_mfe=90,
            )
            self._insert_position(
                connection,
                wallet="0xalpha",
                market_id="market-b",
                outcome="YES",
                asset_id="asset-b",
                entry_price=0.45,
                pnl=4.0,
                duration=240,
                entry_time=1_700_000_120,
                mfe=4.5,
                mae=-0.4,
                time_to_mfe=150,
            )
            self._insert_position(
                connection,
                wallet="0xalpha",
                market_id="market-c",
                outcome="YES",
                asset_id="asset-c",
                entry_price=0.42,
                pnl=-2.0,
                duration=300,
                entry_time=1_700_000_240,
                mfe=0.0,
                mae=-2.0,
                time_to_mfe=0,
            )
            connection.execute(
                """
                UPDATE trade_events
                SET timestamp = ?
                WHERE trade_id = ?
                """,
                (1_699_999_950, "trade-0xalpha-market-a-1700000000"),
            )
            connection.execute(
                """
                INSERT INTO trades_raw(wallet, raw_json, dedupe_key)
                VALUES ('0xseed', '{}', 'seed-a')
                """
            )
            raw_seed_a = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            connection.execute(
                """
                INSERT INTO trade_events(
                    raw_trade_id, trade_id, wallet, market_id, outcome, asset_id, side, price, size, timestamp
                )
                VALUES (?, 'seed-trade-a', '0xseed', 'market-a', 'YES', 'asset-a', 'BUY', '0.35', '10', ?)
                """,
                (raw_seed_a, 1_699_999_970),
            )
            connection.execute(
                """
                INSERT INTO trades_raw(wallet, raw_json, dedupe_key)
                VALUES ('0xseed', '{}', 'seed-b')
                """
            )
            raw_seed_b = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            connection.execute(
                """
                INSERT INTO trade_events(
                    raw_trade_id, trade_id, wallet, market_id, outcome, asset_id, side, price, size, timestamp
                )
                VALUES (?, 'seed-trade-b', '0xseed', 'market-b', 'YES', 'asset-b', 'BUY', '0.39', '10', ?)
                """,
                (raw_seed_b, 1_700_000_090),
            )
            connection.execute(
                """
                INSERT INTO trades_raw(wallet, raw_json, dedupe_key)
                VALUES ('0xseed', '{}', 'seed-c')
                """
            )
            raw_seed_c = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            connection.execute(
                """
                INSERT INTO trade_events(
                    raw_trade_id, trade_id, wallet, market_id, outcome, asset_id, side, price, size, timestamp
                )
                VALUES (?, 'seed-trade-c', '0xseed', 'market-c', 'YES', 'asset-c', 'BUY', '0.39', '10', ?)
                """,
                (raw_seed_c, 1_700_000_210),
            )
            for idx in range(7):
                connection.execute(
                    """
                    INSERT INTO trades_raw(wallet, raw_json, dedupe_key)
                    VALUES ('0xseed', '{}', ?)
                    """,
                    (f"seed-a-extra-{idx}",),
                )
                raw_seed = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
                connection.execute(
                    """
                    INSERT INTO trade_events(
                        raw_trade_id, trade_id, wallet, market_id, outcome, asset_id, side, price, size, timestamp
                    )
                    VALUES (?, ?, '0xseed', 'market-a', 'YES', 'asset-a', 'BUY', '0.36', '10', ?)
                    """,
                    (raw_seed, f"seed-trade-a-extra-{idx}", 1_699_999_971 + idx),
                )
            for idx in range(7):
                connection.execute(
                    """
                    INSERT INTO trades_raw(wallet, raw_json, dedupe_key)
                    VALUES ('0xseed', '{}', ?)
                    """,
                    (f"seed-b-extra-{idx}",),
                )
                raw_seed = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
                connection.execute(
                    """
                    INSERT INTO trade_events(
                        raw_trade_id, trade_id, wallet, market_id, outcome, asset_id, side, price, size, timestamp
                    )
                    VALUES (?, ?, '0xseed', 'market-b', 'YES', 'asset-b', 'BUY', '0.40', '10', ?)
                    """,
                    (raw_seed, f"seed-trade-b-extra-{idx}", 1_700_000_091 + idx),
                )
            for idx in range(7):
                connection.execute(
                    """
                    INSERT INTO trades_raw(wallet, raw_json, dedupe_key)
                    VALUES ('0xseed', '{}', ?)
                    """,
                    (f"seed-c-extra-{idx}",),
                )
                raw_seed = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
                connection.execute(
                    """
                    INSERT INTO trade_events(
                        raw_trade_id, trade_id, wallet, market_id, outcome, asset_id, side, price, size, timestamp
                    )
                    VALUES (?, ?, '0xseed', 'market-c', 'YES', 'asset-c', 'BUY', '0.40', '10', ?)
                    """,
                    (raw_seed, f"seed-trade-c-extra-{idx}", 1_700_000_211 + idx),
                )
            connection.commit()

        result = simulate(settings=self.settings, variant="baseline", wallet="0xalpha")

        self.assertEqual(result.total_trades, 3)
        self.assertEqual(result.trades_used, 2)
        self.assertAlmostEqual(result.total_pnl, 3.55)
        self.assertAlmostEqual(result.avg_pnl, 1.775)
        self.assertAlmostEqual(result.pnl_per_trade, 1.775)
        self.assertGreater(result.pnl_per_minute, 0.0)
        self.assertEqual(result.max_drawdown, 0.0)
        self.assertEqual(len(result.pnl_series), 2)

        report = render_strategy_report(result)
        self.assertIn("=== STRATEGY RESULTS ===", report)
        self.assertIn("Variant: baseline", report)
        self.assertIn("Trades used: 2", report)

    def test_load_positions_can_filter_by_regime_and_min_trades(self) -> None:
        with managed_connection(self.settings) as connection:
            connection.execute(
                """
                INSERT INTO candidate_wallets(wallet, status)
                VALUES ('0xalpha', 'PROFILED')
                """
            )
            connection.execute(
                """
                INSERT INTO candidate_wallet_sources(wallet, source, regime, observed_at)
                VALUES ('0xalpha', 'test', 'crypto_5m', CURRENT_TIMESTAMP)
                """
            )
            self._insert_position(
                connection,
                wallet="0xalpha",
                market_id="market-a",
                outcome="YES",
                asset_id="asset-a",
                entry_price=0.40,
                pnl=3.0,
                duration=120,
                entry_time=1_700_000_000,
            )
            self._insert_position(
                connection,
                wallet="0xalpha",
                market_id="market-b",
                outcome="YES",
                asset_id="asset-b",
                entry_price=0.45,
                pnl=1.0,
                duration=90,
                entry_time=1_700_000_100,
            )
            self._insert_position(
                connection,
                wallet="0xbeta",
                market_id="market-c",
                outcome="YES",
                asset_id="asset-c",
                entry_price=0.42,
                pnl=2.0,
                duration=120,
                entry_time=1_700_000_200,
            )
            connection.commit()

            rows = load_positions(connection, regime="crypto_5m", min_trades=2)

        self.assertEqual(len(rows), 2)
        self.assertTrue(all(row["wallet"] == "0xalpha" for row in rows))


if __name__ == "__main__":
    unittest.main()
