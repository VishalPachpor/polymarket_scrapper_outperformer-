from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tip_v1.config import get_settings
from tip_v1.db.db import initialize_database
from tip_v1.ingestion.fetch_trades import ingest_trade_payloads
from tip_v1.main import run_pipeline
from tip_v1.validation.live_wallet import clear_wallet_state, snapshot_wallet_state
from tip_v1.validation.multi_wallet import (
    import_wallet_raw_data,
    run_combined_wallet_build,
    snapshot_multi_wallet_state,
    validate_market_identity_consistency,
)


class MultiWalletValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.wallet_a = "0xWALLET_A"
        self.wallet_b = "0xWALLET_B"

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _build_wallet_db(self, wallet: str, payloads: list[dict], name: str):
        settings = get_settings(self.root / f"{name}.sqlite3")
        initialize_database(settings)
        inserted, duplicates = ingest_trade_payloads(wallet, payloads, settings)
        self.assertEqual(inserted, len(payloads))
        self.assertEqual(duplicates, 0)
        run_pipeline(wallet, settings=settings, skip_fetch=True, skip_enrichment=True)
        return settings

    def test_combined_build_matches_union_of_individual_wallets(self) -> None:
        wallet_a_payloads = [
            {
                "id": "a-1",
                "conditionId": "market-1",
                "outcome": "YES",
                "asset": "asset-yes",
                "side": "BUY",
                "price": 0.40,
                "size": 10,
                "timestamp": 1000,
            },
            {
                "id": "a-2",
                "conditionId": "market-1",
                "outcome": "YES",
                "asset": "asset-yes",
                "side": "SELL",
                "price": 0.55,
                "size": 4,
                "timestamp": 1010,
            },
        ]
        wallet_b_payloads = [
            {
                "id": "b-1",
                "conditionId": "market-1",
                "outcome": "YES",
                "asset": "asset-yes",
                "side": "BUY",
                "price": 0.41,
                "size": 12,
                "timestamp": 1005,
            },
            {
                "id": "b-2",
                "conditionId": "market-2",
                "outcome": "UP",
                "asset": "asset-up",
                "side": "BUY",
                "price": 0.35,
                "size": 8,
                "timestamp": 1020,
            },
        ]

        wallet_a_settings = self._build_wallet_db(self.wallet_a, wallet_a_payloads, "wallet_a")
        wallet_b_settings = self._build_wallet_db(self.wallet_b, wallet_b_payloads, "wallet_b")

        individual_a = snapshot_wallet_state(self.wallet_a, wallet_a_settings)
        individual_b = snapshot_wallet_state(self.wallet_b, wallet_b_settings)

        combined_settings = get_settings(self.root / "combined.sqlite3")
        initialize_database(combined_settings)
        import_wallet_raw_data(self.wallet_a, wallet_a_settings, combined_settings)
        import_wallet_raw_data(self.wallet_b, wallet_b_settings, combined_settings)

        combined_baseline = run_combined_wallet_build(
            [self.wallet_a, self.wallet_b],
            combined_settings,
        )

        self.assertEqual(combined_baseline.wallet_snapshots[self.wallet_a], individual_a)
        self.assertEqual(combined_baseline.wallet_snapshots[self.wallet_b], individual_b)

        aggregate_before = snapshot_multi_wallet_state(
            [self.wallet_a, self.wallet_b],
            combined_settings,
        )
        for wallet in (self.wallet_a, self.wallet_b):
            clear_wallet_state(wallet, combined_settings, include_raw=False)
        combined_rebuild = run_combined_wallet_build(
            [self.wallet_a, self.wallet_b],
            combined_settings,
        )
        aggregate_after = snapshot_multi_wallet_state(
            [self.wallet_a, self.wallet_b],
            combined_settings,
        )

        self.assertEqual(aggregate_before, aggregate_after)
        self.assertEqual(
            combined_rebuild.wallet_snapshots[self.wallet_a],
            individual_a,
        )
        self.assertEqual(
            combined_rebuild.wallet_snapshots[self.wallet_b],
            individual_b,
        )

    def test_market_identity_conflict_is_detected(self) -> None:
        wallet_a_settings = self._build_wallet_db(
            self.wallet_a,
            [
                {
                    "id": "a-1",
                    "conditionId": "market-conflict",
                    "outcome": "YES",
                    "asset": "same-asset",
                    "side": "BUY",
                    "price": 0.40,
                    "size": 5,
                    "timestamp": 1000,
                }
            ],
            "conflict_a",
        )
        wallet_b_settings = self._build_wallet_db(
            self.wallet_b,
            [
                {
                    "id": "b-1",
                    "conditionId": "market-conflict",
                    "outcome": "NO",
                    "asset": "same-asset",
                    "side": "BUY",
                    "price": 0.60,
                    "size": 5,
                    "timestamp": 1001,
                }
            ],
            "conflict_b",
        )

        combined_settings = get_settings(self.root / "combined_conflict.sqlite3")
        initialize_database(combined_settings)
        import_wallet_raw_data(self.wallet_a, wallet_a_settings, combined_settings)
        import_wallet_raw_data(self.wallet_b, wallet_b_settings, combined_settings)
        run_combined_wallet_build(
            [self.wallet_a, self.wallet_b],
            combined_settings,
            fail_fast=False,
        )

        conflicts = validate_market_identity_consistency(
            [self.wallet_a, self.wallet_b],
            combined_settings,
        )

        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0].market_id, "market-conflict")
        self.assertEqual(conflicts[0].asset_id, "same-asset")
        self.assertEqual(conflicts[0].outcomes, ("NO", "YES"))


if __name__ == "__main__":
    unittest.main()
