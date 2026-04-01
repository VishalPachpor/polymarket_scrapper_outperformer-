from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from tip_v1.config import get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.maintenance.aggregate import run_aggregation
from tip_v1.maintenance.cleanup import run_cleanup
from tip_v1.maintenance.run import run_maintenance


class MaintenanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "maintenance.sqlite3"
        self.settings = get_settings(self.db_path)
        initialize_database(self.settings)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _insert_raw_book_snapshot(
        self,
        connection,
        *,
        asset_id: str,
        observed_at: int,
        snapshot_sequence_id: int,
        best_bid: float,
        best_ask: float,
    ) -> tuple[int, int]:
        connection.execute(
            """
            INSERT INTO microstructure_raw_book(asset_id, observed_at, source_hash, raw_payload)
            VALUES (?, ?, ?, ?)
            """,
            (asset_id, observed_at, f"hash-{asset_id}-{snapshot_sequence_id}", "{}"),
        )
        raw_book_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])

        spread = best_ask - best_bid
        mid_price = (best_ask + best_bid) / 2
        connection.execute(
            """
            INSERT INTO microstructure_book_snapshots(
                snapshot_sequence_id, raw_book_id, asset_id, market_id, timestamp,
                best_bid, best_ask, mid_price, spread,
                bid_depth_1, ask_depth_1, total_bid_depth, total_ask_depth, imbalance
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot_sequence_id,
                raw_book_id,
                asset_id,
                f"market-{asset_id}",
                observed_at,
                best_bid,
                best_ask,
                mid_price,
                spread,
                10.0,
                12.0,
                20.0,
                24.0,
                20.0 / 44.0,
            ),
        )
        snapshot_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        return raw_book_id, snapshot_id

    def _insert_trade_for_snapshot(
        self,
        connection,
        *,
        wallet: str,
        asset_id: str,
        snapshot_id: int,
        trade_timestamp: int,
    ) -> None:
        connection.execute(
            """
            INSERT INTO trades_raw(wallet, raw_json, dedupe_key)
            VALUES (?, ?, ?)
            """,
            (wallet, "{}", f"raw-{wallet}-{trade_timestamp}"),
        )
        raw_trade_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])

        connection.execute(
            """
            INSERT INTO trade_events(
                raw_trade_id, trade_id, wallet, market_id, outcome, asset_id,
                side, price, size, timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?, 'BUY', '0.42', '10', ?)
            """,
            (
                raw_trade_id,
                f"trade-{wallet}-{trade_timestamp}",
                wallet,
                f"market-{asset_id}",
                "YES",
                asset_id,
                trade_timestamp,
            ),
        )
        trade_event_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])

        connection.execute(
            """
            INSERT INTO trade_entry_context(
                trade_event_id, wallet, asset_id, market_id, outcome, trade_timestamp,
                snapshot_id, snapshot_timestamp, snapshot_lag_seconds, lag_confidence
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 'HIGH')
            """,
            (
                trade_event_id,
                wallet,
                asset_id,
                f"market-{asset_id}",
                "YES",
                trade_timestamp,
                snapshot_id,
                trade_timestamp,
            ),
        )

    def test_run_aggregation_builds_minute_level_tables(self) -> None:
        minute_ts = (int(time.time()) - 10_000) // 60 * 60
        with managed_connection(self.settings) as connection:
            self._insert_raw_book_snapshot(
                connection,
                asset_id="asset-a",
                observed_at=minute_ts + 5,
                snapshot_sequence_id=1,
                best_bid=0.40,
                best_ask=0.44,
            )
            self._insert_raw_book_snapshot(
                connection,
                asset_id="asset-a",
                observed_at=minute_ts + 25,
                snapshot_sequence_id=2,
                best_bid=0.42,
                best_ask=0.46,
            )

            connection.execute(
                """
                INSERT INTO market_price_history(asset_id, timestamp, price)
                VALUES
                    ('asset-a', ?, 0.40),
                    ('asset-a', ?, 0.45),
                    ('asset-a', ?, 0.43)
                """,
                (minute_ts + 1, minute_ts + 20, minute_ts + 50),
            )
            connection.commit()

        result = run_aggregation(self.settings, older_than_seconds=0)

        self.assertEqual(result.micro_minutes_inserted, 1)
        self.assertEqual(result.price_minutes_inserted, 1)

        with managed_connection(self.settings) as connection:
            micro_row = connection.execute(
                """
                SELECT avg_bid, avg_ask, spread_avg, mid_price_avg, snapshot_count
                FROM microstructure_agg_1m
                WHERE asset_id = 'asset-a' AND minute_ts = ?
                """,
                (minute_ts,),
            ).fetchone()
            price_row = connection.execute(
                """
                SELECT open_price, high_price, low_price, close_price, sample_count
                FROM price_history_agg_1m
                WHERE asset_id = 'asset-a' AND minute_ts = ?
                """,
                (minute_ts,),
            ).fetchone()

        self.assertAlmostEqual(float(micro_row["avg_bid"]), 0.41, places=6)
        self.assertAlmostEqual(float(micro_row["avg_ask"]), 0.45, places=6)
        self.assertAlmostEqual(float(micro_row["spread_avg"]), 0.04, places=6)
        self.assertAlmostEqual(float(micro_row["mid_price_avg"]), 0.43, places=6)
        self.assertEqual(int(micro_row["snapshot_count"]), 2)

        self.assertAlmostEqual(float(price_row["open_price"]), 0.40, places=6)
        self.assertAlmostEqual(float(price_row["high_price"]), 0.45, places=6)
        self.assertAlmostEqual(float(price_row["low_price"]), 0.40, places=6)
        self.assertAlmostEqual(float(price_row["close_price"]), 0.43, places=6)
        self.assertEqual(int(price_row["sample_count"]), 3)

    def test_cleanup_preserves_trade_referenced_microstructure(self) -> None:
        old_ts = int(time.time()) - 20_000
        with managed_connection(self.settings) as connection:
            _, kept_snapshot_id = self._insert_raw_book_snapshot(
                connection,
                asset_id="asset-keep",
                observed_at=old_ts,
                snapshot_sequence_id=10,
                best_bid=0.30,
                best_ask=0.34,
            )
            connection.execute(
                """
                INSERT INTO microstructure_features(snapshot_id, asset_id, timestamp)
                VALUES (?, 'asset-keep', ?)
                """,
                (kept_snapshot_id, old_ts),
            )
            self._insert_trade_for_snapshot(
                connection,
                wallet="0xkeep",
                asset_id="asset-keep",
                snapshot_id=kept_snapshot_id,
                trade_timestamp=old_ts,
            )

            _, drop_snapshot_id = self._insert_raw_book_snapshot(
                connection,
                asset_id="asset-drop",
                observed_at=old_ts - 60,
                snapshot_sequence_id=11,
                best_bid=0.50,
                best_ask=0.54,
            )
            connection.execute(
                """
                INSERT INTO microstructure_features(snapshot_id, asset_id, timestamp)
                VALUES (?, 'asset-drop', ?)
                """,
                (drop_snapshot_id, old_ts - 60),
            )
            connection.execute(
                """
                INSERT INTO market_price_history(asset_id, timestamp, price)
                VALUES ('asset-drop', ?, 0.52)
                """,
                (old_ts - 60,),
            )
            connection.execute(
                """
                INSERT INTO price_history_agg_1m(
                    asset_id, minute_ts, open_price, high_price, low_price, close_price, sample_count
                )
                VALUES ('asset-drop', ?, 0.52, 0.52, 0.52, 0.52, 1)
                """,
                ((old_ts - 60) // 60 * 60,),
            )
            connection.commit()

        cleanup = run_cleanup(
            self.settings,
            micro_raw_max_age_seconds=0,
            price_history_max_age_seconds=0,
            vacuum=False,
        )

        self.assertGreaterEqual(cleanup.raw_book_deleted, 2)
        self.assertEqual(cleanup.price_history_deleted, 1)

        with managed_connection(self.settings) as connection:
            kept_snapshot_count = connection.execute(
                "SELECT COUNT(*) AS n FROM microstructure_book_snapshots WHERE asset_id = 'asset-keep'"
            ).fetchone()["n"]
            kept_raw_count = connection.execute(
                "SELECT COUNT(*) AS n FROM microstructure_raw_book WHERE asset_id = 'asset-keep'"
            ).fetchone()["n"]
            kept_feature_count = connection.execute(
                "SELECT COUNT(*) AS n FROM microstructure_features WHERE asset_id = 'asset-keep'"
            ).fetchone()["n"]

            drop_snapshot_count = connection.execute(
                "SELECT COUNT(*) AS n FROM microstructure_book_snapshots WHERE asset_id = 'asset-drop'"
            ).fetchone()["n"]
            drop_raw_count = connection.execute(
                "SELECT COUNT(*) AS n FROM microstructure_raw_book WHERE asset_id = 'asset-drop'"
            ).fetchone()["n"]
            drop_feature_count = connection.execute(
                "SELECT COUNT(*) AS n FROM microstructure_features WHERE asset_id = 'asset-drop'"
            ).fetchone()["n"]
            drop_price_history_count = connection.execute(
                "SELECT COUNT(*) AS n FROM market_price_history WHERE asset_id = 'asset-drop'"
            ).fetchone()["n"]

        self.assertEqual(int(kept_snapshot_count), 1)
        self.assertEqual(int(kept_raw_count), 1)
        self.assertEqual(int(kept_feature_count), 1)

        self.assertEqual(int(drop_snapshot_count), 0)
        self.assertEqual(int(drop_raw_count), 0)
        self.assertEqual(int(drop_feature_count), 0)
        self.assertEqual(int(drop_price_history_count), 0)

    def test_run_maintenance_executes_aggregate_then_cleanup(self) -> None:
        old_ts = int(time.time()) - 20_000
        minute_ts = old_ts // 60 * 60
        with managed_connection(self.settings) as connection:
            self._insert_raw_book_snapshot(
                connection,
                asset_id="asset-maint",
                observed_at=old_ts,
                snapshot_sequence_id=20,
                best_bid=0.61,
                best_ask=0.65,
            )
            connection.execute(
                """
                INSERT INTO market_price_history(asset_id, timestamp, price)
                VALUES ('asset-maint', ?, 0.63)
                """,
                (old_ts,),
            )
            connection.commit()

        result = run_maintenance(
            self.settings,
            aggregate_cutoff_hours=0,
            micro_raw_max_age_hours=0,
            price_history_max_age_days=0,
            vacuum=False,
        )

        self.assertEqual(result.aggregation.micro_minutes_inserted, 1)
        self.assertEqual(result.aggregation.price_minutes_inserted, 1)
        self.assertGreaterEqual(result.cleanup.raw_book_deleted, 2)
        self.assertEqual(result.cleanup.price_history_deleted, 1)

        with managed_connection(self.settings) as connection:
            micro_agg_count = connection.execute(
                "SELECT COUNT(*) AS n FROM microstructure_agg_1m WHERE asset_id = 'asset-maint' AND minute_ts = ?",
                (minute_ts,),
            ).fetchone()["n"]
            price_agg_count = connection.execute(
                "SELECT COUNT(*) AS n FROM price_history_agg_1m WHERE asset_id = 'asset-maint' AND minute_ts = ?",
                (minute_ts,),
            ).fetchone()["n"]
            raw_count = connection.execute(
                "SELECT COUNT(*) AS n FROM microstructure_raw_book WHERE asset_id = 'asset-maint'"
            ).fetchone()["n"]
            price_history_count = connection.execute(
                "SELECT COUNT(*) AS n FROM market_price_history WHERE asset_id = 'asset-maint'"
            ).fetchone()["n"]

        self.assertEqual(int(micro_agg_count), 1)
        self.assertEqual(int(price_agg_count), 1)
        self.assertEqual(int(raw_count), 0)
        self.assertEqual(int(price_history_count), 0)


if __name__ == "__main__":
    unittest.main()
