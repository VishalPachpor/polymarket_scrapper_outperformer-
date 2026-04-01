from __future__ import annotations

import argparse
import time
from dataclasses import dataclass

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.microstructure.compute_features import compute_microstructure_features
from tip_v1.microstructure.fetch_microstructure import ingest_orderbooks_for_assets
from tip_v1.microstructure.join_with_trades import join_microstructure_with_trades
from tip_v1.microstructure.normalize_microstructure import normalize_book_snapshots


@dataclass(frozen=True)
class AssetPollPlan:
    asset_id: str
    activity_score: float
    interval_seconds: int


@dataclass(frozen=True)
class CollectorCycleResult:
    wallets: tuple[str, ...]
    planned_assets: int
    polled_assets: int
    inserted_raw: int
    normalized_snapshots: int
    computed_features: int


def discover_tracked_assets(
    wallets: list[str],
    settings: Settings | None = None,
) -> set[str]:
    settings = settings or get_settings()
    placeholders = ", ".join("?" for _ in wallets)
    if not placeholders:
        return set()

    with managed_connection(settings) as connection:
        rows = connection.execute(
            f"""
            SELECT DISTINCT asset_id
            FROM trade_events
            WHERE wallet IN ({placeholders}) AND asset_id IS NOT NULL
            ORDER BY asset_id
            """,
            wallets,
        ).fetchall()

    return {str(row["asset_id"]) for row in rows}


def _trade_recency_score(
    asset_id: str,
    *,
    now_ts: int,
    settings: Settings,
    connection,
) -> float:
    row = connection.execute(
        """
        SELECT COUNT(*) AS recent_trades
        FROM trade_events
        WHERE asset_id = ? AND timestamp >= ?
        """,
        (asset_id, now_ts - settings.microstructure_active_lookback_seconds),
    ).fetchone()
    return float(row["recent_trades"] or 0.0)


def _price_activity_score(
    asset_id: str,
    *,
    settings: Settings,
    connection,
) -> float:
    row = connection.execute(
        """
        SELECT price_change_30s, snapshots_per_minute
        FROM microstructure_features
        WHERE asset_id = ?
        ORDER BY timestamp DESC, snapshot_id DESC
        LIMIT 1
        """,
        (asset_id,),
    ).fetchone()
    if row is None:
        return 0.0
    return abs(float(row["price_change_30s"] or 0.0)) + (
        float(row["snapshots_per_minute"] or 0.0) / 60.0
    )


def build_asset_poll_plan(
    wallets: list[str],
    settings: Settings | None = None,
    *,
    now_ts: int | None = None,
) -> list[AssetPollPlan]:
    settings = settings or get_settings()
    now_ts = now_ts or int(time.time())
    asset_ids = sorted(discover_tracked_assets(wallets, settings))
    plans: list[AssetPollPlan] = []

    with managed_connection(settings) as connection:
        for asset_id in asset_ids:
            activity_score = _trade_recency_score(
                asset_id,
                now_ts=now_ts,
                settings=settings,
                connection=connection,
            ) + _price_activity_score(asset_id, settings=settings, connection=connection)

            if activity_score >= 3:
                interval_seconds = settings.microstructure_fast_poll_seconds
            elif activity_score >= 1:
                interval_seconds = settings.microstructure_base_poll_seconds
            else:
                interval_seconds = settings.microstructure_slow_poll_seconds

            plans.append(
                AssetPollPlan(
                    asset_id=asset_id,
                    activity_score=round(activity_score, 6),
                    interval_seconds=interval_seconds,
                )
            )

    return plans


class LiveMicrostructureCollector:
    def __init__(
        self,
        wallets: list[str],
        settings: Settings | None = None,
    ) -> None:
        self.wallets = tuple(wallets)
        self.settings = settings or get_settings()
        self._last_polled_at: dict[str, int] = {}
        initialize_database(self.settings)

    def collect_once(self) -> CollectorCycleResult:
        now_ts = int(time.time())
        plans = build_asset_poll_plan(list(self.wallets), self.settings, now_ts=now_ts)
        due_assets = [
            plan.asset_id
            for plan in plans
            if plan.asset_id not in self._last_polled_at
            or now_ts - self._last_polled_at[plan.asset_id] >= plan.interval_seconds
        ]

        inserted_raw = 0
        normalized_snapshots = 0
        computed_features = 0

        if due_assets:
            ingestion = ingest_orderbooks_for_assets(due_assets, self.settings)
            inserted_raw = ingestion.inserted_count
            normalization = normalize_book_snapshots(self.settings)
            normalized_snapshots = normalization.inserted_count
            features = compute_microstructure_features(self.settings)
            computed_features = features.features_computed

            for asset_id in due_assets:
                self._last_polled_at[asset_id] = now_ts

            for wallet in self.wallets:
                join_microstructure_with_trades(wallet, self.settings)

        return CollectorCycleResult(
            wallets=self.wallets,
            planned_assets=len(plans),
            polled_assets=len(due_assets),
            inserted_raw=inserted_raw,
            normalized_snapshots=normalized_snapshots,
            computed_features=computed_features,
        )

    def run_forever(self) -> None:
        while True:
            result = self.collect_once()
            print(
                "[MICRO-COLLECT]"
                f" wallets={','.join(result.wallets)}"
                f" planned_assets={result.planned_assets}"
                f" polled_assets={result.polled_assets}"
                f" inserted_raw={result.inserted_raw}"
                f" normalized={result.normalized_snapshots}"
                f" features={result.computed_features}"
            )
            time.sleep(self.settings.microstructure_fast_poll_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the live microstructure collector.")
    parser.add_argument(
        "--wallet",
        action="append",
        required=True,
        help="Tracked wallet to discover active assets from. Repeat for multiple wallets.",
    )
    parser.add_argument("--db-path", help="Override the SQLite database path.")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single collection cycle and exit.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings(args.db_path)
    collector = LiveMicrostructureCollector(args.wallet, settings)

    if args.once:
        result = collector.collect_once()
        print(
            "[MICRO-COLLECT]"
            f" wallets={','.join(result.wallets)}"
            f" planned_assets={result.planned_assets}"
            f" polled_assets={result.polled_assets}"
            f" inserted_raw={result.inserted_raw}"
            f" normalized={result.normalized_snapshots}"
            f" features={result.computed_features}"
        )
        return

    collector.run_forever()


if __name__ == "__main__":
    main()
