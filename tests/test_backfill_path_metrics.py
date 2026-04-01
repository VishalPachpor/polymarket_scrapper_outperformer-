from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tip_v1.cli.backfill_path_metrics import (
    backfill_path_metrics,
    find_wallets_missing_path_metrics,
)
from tip_v1.config import get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.enrichment.compute_path_metrics import compute_position_path_metrics


class BackfillPathMetricsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "backfill.sqlite3"
        self.settings = get_settings(self.db_path)
        initialize_database(self.settings)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _seed_closed_position(
        self,
        *,
        wallet: str,
        suffix: str,
        entry_price: float = 0.40,
        exit_price: float = 0.50,
        size: float = 10.0,
        entry_time: int = 1_700_000_000,
        exit_time: int = 1_700_000_100,
        rank_score: float = 0.5,
    ) -> None:
        with managed_connection(self.settings) as connection:
            connection.execute(
                "INSERT OR IGNORE INTO wallet_rankings(wallet, regime, followability_score, lifecycle_score, rank_score, tier) VALUES (?, 'global', 0.4, 0.6, ?, 'B')",
                (wallet, rank_score),
            )
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
                    f"market-{wallet}",
                    f"asset-{wallet}",
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
                    f"market-{wallet}",
                    trade_event_id,
                    trade_event_id,
                    entry_price,
                    exit_price,
                    size,
                    (exit_price - entry_price) * size,
                    entry_time,
                    exit_time,
                    exit_time - entry_time,
                ),
            )
            connection.executemany(
                """
                INSERT INTO market_price_history(asset_id, timestamp, price)
                VALUES (?, ?, ?)
                """,
                [
                    (f"asset-{wallet}", entry_time + 10, entry_price - 0.02),
                    (f"asset-{wallet}", entry_time + 30, entry_price + 0.15),
                    (f"asset-{wallet}", entry_time + 60, entry_price + 0.05),
                ],
            )
            connection.commit()

    def test_find_wallets_missing_path_metrics_prioritizes_missing_wallets(self) -> None:
        self._seed_closed_position(wallet="0xmissing", suffix="1", rank_score=0.9)
        self._seed_closed_position(wallet="0xcovered", suffix="2", rank_score=0.4)
        compute_position_path_metrics("0xcovered", settings=self.settings)

        targets = find_wallets_missing_path_metrics(
            settings=self.settings,
            min_trades=1,
            limit=10,
        )

        self.assertEqual([target.wallet for target in targets], ["0xmissing"])
        self.assertEqual(targets[0].missing_positions, 1)

    def test_backfill_path_metrics_repairs_missing_wallets_using_existing_history(self) -> None:
        self._seed_closed_position(wallet="0xmissing", suffix="1", rank_score=0.9)
        self._seed_closed_position(wallet="0xcovered", suffix="2", rank_score=0.4)
        compute_position_path_metrics("0xcovered", settings=self.settings)

        result = backfill_path_metrics(
            settings=self.settings,
            min_trades=1,
            limit=10,
            skip_price_fetch=True,
        )

        with managed_connection(self.settings) as connection:
            rows = connection.execute(
                """
                SELECT wallet, COUNT(*) AS c
                FROM position_path_metrics
                GROUP BY wallet
                ORDER BY wallet
                """
            ).fetchall()

        self.assertEqual(result.wallets_considered, 1)
        self.assertEqual(result.wallets_processed, 1)
        self.assertEqual(result.positions_with_path_data, 1)
        self.assertEqual([(row["wallet"], row["c"]) for row in rows], [("0xcovered", 1), ("0xmissing", 1)])


if __name__ == "__main__":
    unittest.main()
