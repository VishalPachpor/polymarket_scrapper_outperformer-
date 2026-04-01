from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tip_v1.config import get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.signals.momentum_trigger import (
    BUY,
    NO_SIGNAL,
    MomentumTriggerConfig,
    PriceSample,
    evaluate_momentum_signal,
    generate_momentum_signal_from_db,
    load_recent_trade_samples,
)


class MomentumTriggerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "signals.sqlite3"
        self.settings = get_settings(self.db_path)
        initialize_database(self.settings)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_signal_triggers_on_synthetic_momentum_series(self) -> None:
        samples = [
            PriceSample("market-1", "asset-1", 1_700_000_000, 0.34),
            PriceSample("market-1", "asset-1", 1_700_000_015, 0.36),
            PriceSample("market-1", "asset-1", 1_700_000_030, 0.38),
            PriceSample("market-1", "asset-1", 1_700_000_045, 0.40),
        ]

        signal = evaluate_momentum_signal(samples, now_ts=1_700_000_050, dry_run=True)

        self.assertEqual(signal.signal_type, BUY)
        self.assertEqual(signal.market_id, "market-1")
        self.assertEqual(signal.asset_id, "asset-1")
        self.assertEqual(signal.price, 0.40)
        self.assertTrue(signal.dry_run)
        self.assertGreater(signal.confidence, 0.0)

    def test_no_signal_in_flat_noise_conditions(self) -> None:
        samples = [
            PriceSample("market-1", "asset-1", 1_700_000_000, 0.39),
            PriceSample("market-1", "asset-1", 1_700_000_015, 0.392),
            PriceSample("market-1", "asset-1", 1_700_000_030, 0.391),
            PriceSample("market-1", "asset-1", 1_700_000_045, 0.393),
            PriceSample("market-1", "asset-1", 1_700_000_060, 0.394),
        ]

        signal = evaluate_momentum_signal(samples, now_ts=1_700_000_062)

        self.assertEqual(signal.signal_type, NO_SIGNAL)
        self.assertEqual(signal.reason, "insufficient_momentum")

    def test_stale_signal_is_rejected(self) -> None:
        samples = [
            PriceSample("market-1", "asset-1", 1_700_000_000, 0.34),
            PriceSample("market-1", "asset-1", 1_700_000_015, 0.36),
            PriceSample("market-1", "asset-1", 1_700_000_030, 0.38),
            PriceSample("market-1", "asset-1", 1_700_000_045, 0.40),
        ]

        signal = evaluate_momentum_signal(samples, now_ts=1_700_000_070)

        self.assertEqual(signal.signal_type, NO_SIGNAL)
        self.assertEqual(signal.reason, "stale_signal")

    def test_generate_signal_from_db_uses_recent_trade_series(self) -> None:
        with managed_connection(self.settings) as connection:
            for idx, (ts, price) in enumerate(
                [
                    (1_700_000_000, "0.34"),
                    (1_700_000_015, "0.36"),
                    (1_700_000_030, "0.38"),
                    (1_700_000_045, "0.40"),
                ]
            ):
                connection.execute(
                    """
                    INSERT INTO trades_raw(wallet, raw_json, dedupe_key)
                    VALUES ('0xseed', '{}', ?)
                    """,
                    (f"seed-{idx}",),
                )
                raw_trade_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
                connection.execute(
                    """
                    INSERT INTO trade_events(
                        raw_trade_id, trade_id, wallet, market_id, outcome, asset_id, side, price, size, timestamp
                    )
                    VALUES (?, ?, '0xseed', 'market-1', 'YES', 'asset-1', 'BUY', ?, '10', ?)
                    """,
                    (raw_trade_id, f"trade-{idx}", price, ts),
                )
            connection.commit()

            samples = load_recent_trade_samples(
                connection,
                asset_id="asset-1",
                now_ts=1_700_000_050,
                lookback_seconds=60,
            )

        self.assertEqual(len(samples), 4)
        self.assertEqual(samples[-1].price, 0.40)

        signal = generate_momentum_signal_from_db(
            settings=self.settings,
            asset_id="asset-1",
            now_ts=1_700_000_050,
            dry_run=True,
            config=MomentumTriggerConfig(),
        )

        self.assertEqual(signal.signal_type, BUY)
        self.assertEqual(signal.reason, None)


if __name__ == "__main__":
    unittest.main()
