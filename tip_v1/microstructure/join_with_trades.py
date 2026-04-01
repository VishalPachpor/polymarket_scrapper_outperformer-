from __future__ import annotations

from dataclasses import dataclass

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import managed_connection


@dataclass(frozen=True)
class TradeContextJoinResult:
    wallet: str
    trades_processed: int
    trades_with_snapshot: int
    high_confidence_count: int
    low_confidence_count: int
    missing_snapshot_count: int

    @property
    def coverage_rate(self) -> float:
        if self.trades_processed == 0:
            return 0.0
        return round(self.trades_with_snapshot / self.trades_processed, 4)


def _lag_confidence(lag_seconds: int | None, *, max_lag_seconds: int) -> str:
    if lag_seconds is None:
        return "MISSING"
    if lag_seconds <= 2:
        return "HIGH"
    if lag_seconds <= max_lag_seconds:
        return "MEDIUM"
    return "LOW"


def join_microstructure_with_trades(
    wallet: str,
    settings: Settings | None = None,
) -> TradeContextJoinResult:
    settings = settings or get_settings()
    trades_processed = 0
    trades_with_snapshot = 0
    high_confidence_count = 0
    low_confidence_count = 0
    missing_snapshot_count = 0

    with managed_connection(settings) as connection:
        connection.execute(
            "DELETE FROM trade_entry_context WHERE wallet = ?",
            (wallet,),
        )

        trades = connection.execute(
            """
            SELECT id, wallet, asset_id, market_id, outcome, timestamp
            FROM trade_events
            WHERE wallet = ? AND asset_id IS NOT NULL
            ORDER BY timestamp, trade_id, id
            """,
            (wallet,),
        ).fetchall()

        for trade in trades:
            trades_processed += 1
            snapshot = connection.execute(
                """
                SELECT
                    snapshot.id,
                    snapshot.timestamp,
                    snapshot.best_bid,
                    snapshot.best_ask,
                    snapshot.mid_price,
                    snapshot.spread,
                    snapshot.total_bid_depth,
                    snapshot.total_ask_depth,
                    snapshot.imbalance,
                    features.price_change_5s,
                    features.price_change_30s,
                    features.price_change_5m,
                    features.volatility_30s,
                    features.volatility_5m,
                    features.snapshots_per_minute
                FROM microstructure_book_snapshots AS snapshot
                LEFT JOIN microstructure_features AS features
                    ON features.snapshot_id = snapshot.id
                WHERE snapshot.asset_id = ? AND snapshot.timestamp <= ?
                ORDER BY snapshot.timestamp DESC, snapshot.snapshot_sequence_id DESC
                LIMIT 1
                """,
                (trade["asset_id"], trade["timestamp"]),
            ).fetchone()

            snapshot_id = None
            snapshot_timestamp = None
            lag_seconds = None
            best_bid = None
            best_ask = None
            mid_price = None
            spread = None
            total_bid_depth = None
            total_ask_depth = None
            imbalance = None
            price_change_5s = None
            price_change_30s = None
            price_change_5m = None
            volatility_30s = None
            volatility_5m = None
            snapshots_per_minute = None

            if snapshot is not None:
                trades_with_snapshot += 1
                snapshot_id = int(snapshot["id"])
                snapshot_timestamp = int(snapshot["timestamp"])
                lag_seconds = int(trade["timestamp"]) - snapshot_timestamp
                best_bid = snapshot["best_bid"]
                best_ask = snapshot["best_ask"]
                mid_price = snapshot["mid_price"]
                spread = snapshot["spread"]
                total_bid_depth = snapshot["total_bid_depth"]
                total_ask_depth = snapshot["total_ask_depth"]
                imbalance = snapshot["imbalance"]
                price_change_5s = snapshot["price_change_5s"]
                price_change_30s = snapshot["price_change_30s"]
                price_change_5m = snapshot["price_change_5m"]
                volatility_30s = snapshot["volatility_30s"]
                volatility_5m = snapshot["volatility_5m"]
                snapshots_per_minute = snapshot["snapshots_per_minute"]

            lag_confidence = _lag_confidence(
                lag_seconds,
                max_lag_seconds=settings.snapshot_max_lag_seconds,
            )
            if lag_confidence == "HIGH":
                high_confidence_count += 1
            elif lag_confidence in {"LOW", "MISSING"}:
                low_confidence_count += 1
            if lag_confidence == "MISSING":
                missing_snapshot_count += 1

            connection.execute(
                """
                INSERT INTO trade_entry_context (
                    trade_event_id,
                    wallet,
                    asset_id,
                    market_id,
                    outcome,
                    trade_timestamp,
                    snapshot_id,
                    snapshot_timestamp,
                    snapshot_lag_seconds,
                    lag_confidence,
                    best_bid,
                    best_ask,
                    mid_price,
                    spread,
                    total_bid_depth,
                    total_ask_depth,
                    imbalance,
                    price_change_5s,
                    price_change_30s,
                    price_change_5m,
                    volatility_30s,
                    volatility_5m,
                    snapshots_per_minute
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(trade["id"]),
                    wallet,
                    str(trade["asset_id"]),
                    str(trade["market_id"]),
                    str(trade["outcome"]),
                    int(trade["timestamp"]),
                    snapshot_id,
                    snapshot_timestamp,
                    lag_seconds,
                    lag_confidence,
                    best_bid,
                    best_ask,
                    mid_price,
                    spread,
                    total_bid_depth,
                    total_ask_depth,
                    imbalance,
                    price_change_5s,
                    price_change_30s,
                    price_change_5m,
                    volatility_30s,
                    volatility_5m,
                    snapshots_per_minute,
                ),
            )

        connection.commit()

    return TradeContextJoinResult(
        wallet=wallet,
        trades_processed=trades_processed,
        trades_with_snapshot=trades_with_snapshot,
        high_confidence_count=high_confidence_count,
        low_confidence_count=low_confidence_count,
        missing_snapshot_count=missing_snapshot_count,
    )
