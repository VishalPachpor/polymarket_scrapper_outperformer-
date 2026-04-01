from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass
from dataclasses import replace

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.wallet_engine.signal_builder import WalletSignal
from tip_v1.wallet_engine.wallet_runtime import (
    WalletEngineRuntime,
    WalletRuntimeConfig,
)


logger = logging.getLogger(__name__)


BUY = "BUY"
SELL = "SELL"


@dataclass(frozen=True)
class WalletPaperTrackerConfig:
    polling_interval_seconds: int = 2
    horizons_seconds: tuple[int, ...] = (3, 5, 10)
    minimum_follow_through_move: float = 0.01
    max_open_signals_per_asset: int = 3
    capture_shadow_candidates: bool = True
    runtime: WalletRuntimeConfig = WalletRuntimeConfig()


@dataclass(frozen=True)
class WalletPaperMetrics:
    resolved_signals: int
    follow_through_rate_3s: float
    follow_through_rate_5s: float
    follow_through_rate_10s: float
    avg_return_3s: float
    avg_return_5s: float
    avg_return_10s: float
    avg_max_favorable_return: float
    avg_max_adverse_return: float
    avg_time_to_move: float


@dataclass(frozen=True)
class WalletPaperTrackerCycle:
    runtime_signals: int
    captured_signals: int
    recorded_signals: int
    skipped_signals: int
    resolved_signals: int
    metrics: WalletPaperMetrics


def _price_at_or_after(
    samples: list[tuple[int, float]],
    *,
    target_ts: int,
) -> float | None:
    for timestamp, price in samples:
        if timestamp >= target_ts:
            return price
    return None


def _signed_return(*, side: str, entry_price: float, future_price: float | None) -> float | None:
    if future_price is None or entry_price <= 0:
        return None
    if side == BUY:
        return (future_price - entry_price) / entry_price
    return (entry_price - future_price) / entry_price


def _open_signal_count(connection, *, asset_id: str | None) -> int:
    if asset_id is None:
        return 0
    row = connection.execute(
        """
        SELECT COUNT(*) AS n
        FROM wallet_paper_signals
        WHERE asset_id = ? AND status = 'OPEN'
        """,
        (asset_id,),
    ).fetchone()
    return int(row["n"] or 0)


def record_wallet_signal(connection, signal: WalletSignal) -> bool:
    cursor = connection.execute(
        """
        INSERT OR IGNORE INTO wallet_paper_signals(
            wallet,
            market_id,
            asset_id,
            side,
            signal_timestamp,
            entry_price,
            signal_score,
            confidence,
            aligned_wallet_count
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            signal.wallet,
            signal.market_id,
            signal.asset_id,
            signal.side,
            signal.timestamp,
            signal.price,
            signal.score,
            signal.confidence,
            signal.aligned_wallet_count,
        ),
    )
    return cursor.rowcount > 0


def resolve_wallet_signal_outcomes(
    connection,
    *,
    now_ts: int,
    config: WalletPaperTrackerConfig,
) -> int:
    max_horizon = max(config.horizons_seconds)
    due_rows = connection.execute(
        """
        SELECT
            s.id,
            s.wallet,
            s.market_id,
            s.asset_id,
            s.side,
            s.signal_timestamp,
            s.entry_price
        FROM wallet_paper_signals AS s
        LEFT JOIN wallet_paper_outcomes AS o
          ON o.signal_id = s.id
        WHERE s.status = 'OPEN'
          AND o.signal_id IS NULL
          AND s.signal_timestamp + ? <= ?
        ORDER BY s.signal_timestamp, s.id
        """,
        (max_horizon, now_ts),
    ).fetchall()

    resolved = 0
    for row in due_rows:
        asset_id = row["asset_id"]
        if asset_id is None:
            continue
        entry_timestamp = int(row["signal_timestamp"])
        horizon_end = entry_timestamp + max_horizon
        future_rows = connection.execute(
            """
            SELECT timestamp, price
            FROM trade_events
            WHERE asset_id = ?
              AND timestamp BETWEEN ? AND ?
            ORDER BY timestamp, id
            """,
            (asset_id, entry_timestamp, horizon_end),
        ).fetchall()
        samples = [(int(sample["timestamp"]), float(sample["price"])) for sample in future_rows]
        entry_price = float(row["entry_price"])
        side = str(row["side"])
        if samples:
            returns = {
                horizon: _signed_return(
                    side=side,
                    entry_price=entry_price,
                    future_price=_price_at_or_after(samples, target_ts=entry_timestamp + horizon),
                )
                for horizon in config.horizons_seconds
            }
            signed_path = [
                _signed_return(side=side, entry_price=entry_price, future_price=price)
                for _, price in samples
            ]
            signed_path = [value for value in signed_path if value is not None]
            max_favorable_return = max(signed_path) if signed_path else None
            max_adverse_return = min(signed_path) if signed_path else None
            time_to_move = None
            for timestamp, price in samples:
                signed = _signed_return(side=side, entry_price=entry_price, future_price=price)
                if signed is not None and signed >= config.minimum_follow_through_move:
                    time_to_move = timestamp - entry_timestamp
                    break
            follow_through = {
                horizon: 1 if (returns[horizon] or 0.0) >= config.minimum_follow_through_move else 0
                for horizon in config.horizons_seconds
            }
        else:
            returns = {horizon: None for horizon in config.horizons_seconds}
            max_favorable_return = None
            max_adverse_return = None
            time_to_move = None
            follow_through = {horizon: 0 for horizon in config.horizons_seconds}

        connection.execute(
            """
            INSERT INTO wallet_paper_outcomes(
                signal_id,
                wallet,
                market_id,
                asset_id,
                side,
                entry_timestamp,
                resolved_timestamp,
                sample_count,
                return_3s,
                return_5s,
                return_10s,
                max_favorable_return,
                max_adverse_return,
                time_to_move,
                follow_through_3s,
                follow_through_5s,
                follow_through_10s
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(row["id"]),
                str(row["wallet"]),
                str(row["market_id"]),
                asset_id,
                side,
                entry_timestamp,
                now_ts,
                len(samples),
                returns.get(3),
                returns.get(5),
                returns.get(10),
                max_favorable_return,
                max_adverse_return,
                time_to_move,
                follow_through.get(3, 0),
                follow_through.get(5, 0),
                follow_through.get(10, 0),
            ),
        )
        connection.execute(
            """
            UPDATE wallet_paper_signals
            SET status = 'RESOLVED'
            WHERE id = ?
            """,
            (int(row["id"]),),
        )
        resolved += 1
        logger.info(
            "wallet_paper_outcome_resolved signal_id=%s asset_id=%s r3=%s r5=%s r10=%s favorable=%s adverse=%s",
            int(row["id"]),
            asset_id,
            f"{(returns.get(3) or 0.0):.4f}" if returns.get(3) is not None else "NA",
            f"{(returns.get(5) or 0.0):.4f}" if returns.get(5) is not None else "NA",
            f"{(returns.get(10) or 0.0):.4f}" if returns.get(10) is not None else "NA",
            f"{(max_favorable_return or 0.0):.4f}" if max_favorable_return is not None else "NA",
            f"{(max_adverse_return or 0.0):.4f}" if max_adverse_return is not None else "NA",
        )
    return resolved


def compute_wallet_paper_metrics(connection) -> WalletPaperMetrics:
    row = connection.execute(
        """
        SELECT
            COUNT(*) AS resolved_signals,
            AVG(CAST(follow_through_3s AS REAL)) AS follow_through_rate_3s,
            AVG(CAST(follow_through_5s AS REAL)) AS follow_through_rate_5s,
            AVG(CAST(follow_through_10s AS REAL)) AS follow_through_rate_10s,
            AVG(return_3s) AS avg_return_3s,
            AVG(return_5s) AS avg_return_5s,
            AVG(return_10s) AS avg_return_10s,
            AVG(max_favorable_return) AS avg_max_favorable_return,
            AVG(max_adverse_return) AS avg_max_adverse_return,
            AVG(time_to_move) AS avg_time_to_move
        FROM wallet_paper_outcomes
        """
    ).fetchone()
    return WalletPaperMetrics(
        resolved_signals=int(row["resolved_signals"] or 0),
        follow_through_rate_3s=float(row["follow_through_rate_3s"] or 0.0),
        follow_through_rate_5s=float(row["follow_through_rate_5s"] or 0.0),
        follow_through_rate_10s=float(row["follow_through_rate_10s"] or 0.0),
        avg_return_3s=float(row["avg_return_3s"] or 0.0),
        avg_return_5s=float(row["avg_return_5s"] or 0.0),
        avg_return_10s=float(row["avg_return_10s"] or 0.0),
        avg_max_favorable_return=float(row["avg_max_favorable_return"] or 0.0),
        avg_max_adverse_return=float(row["avg_max_adverse_return"] or 0.0),
        avg_time_to_move=float(row["avg_time_to_move"] or 0.0),
    )


class WalletPaperTracker:
    def __init__(
        self,
        *,
        settings: Settings | None = None,
        config: WalletPaperTrackerConfig | None = None,
        runtime: WalletEngineRuntime | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.config = config or WalletPaperTrackerConfig()
        self._captured_signals: list[WalletSignal] = []
        if runtime is not None:
            self.runtime = runtime
        else:
            runtime_config = self.config.runtime
            if self.config.capture_shadow_candidates and not runtime_config.emit_shadow_candidates:
                runtime_config = replace(runtime_config, emit_shadow_candidates=True)
            self.runtime = WalletEngineRuntime(
                settings=self.settings,
                config=runtime_config,
                on_signal_callback=None if self.config.capture_shadow_candidates else self._capture_signal,
                on_shadow_candidate_callback=self._capture_signal if self.config.capture_shadow_candidates else None,
            )

    def _capture_signal(self, signal: WalletSignal) -> None:
        self._captured_signals.append(signal)

    def run_once(self, *, now_ts: int | None = None) -> WalletPaperTrackerCycle:
        cycle_now = int(now_ts if now_ts is not None else time.time())
        self._captured_signals = []
        runtime_cycle = self.runtime.run_once()

        with managed_connection(self.settings) as connection:
            recorded = 0
            skipped = 0
            for signal in self._captured_signals:
                if _open_signal_count(connection, asset_id=signal.asset_id) >= self.config.max_open_signals_per_asset:
                    skipped += 1
                    logger.info(
                        "wallet_paper_signal_skipped wallet=%s market_id=%s reason=open_signal_limit",
                        signal.wallet,
                        signal.market_id,
                    )
                    continue
                if record_wallet_signal(connection, signal):
                    recorded += 1
                    logger.info(
                        "wallet_paper_signal_recorded wallet=%s market_id=%s side=%s price=%.4f score=%.3f",
                        signal.wallet,
                        signal.market_id,
                        signal.side,
                        signal.price,
                        signal.score,
                    )
                else:
                    skipped += 1
            resolved = resolve_wallet_signal_outcomes(
                connection,
                now_ts=cycle_now,
                config=self.config,
            )
            metrics = compute_wallet_paper_metrics(connection)
            connection.commit()

        result = WalletPaperTrackerCycle(
            runtime_signals=runtime_cycle.signals_emitted,
            captured_signals=len(self._captured_signals),
            recorded_signals=recorded,
            skipped_signals=skipped,
            resolved_signals=resolved,
            metrics=metrics,
        )
        logger.info(
            "wallet_paper_cycle runtime_signals=%s captured=%s recorded=%s skipped=%s resolved=%s follow_through_5s=%.2f avg_return_5s=%.4f avg_return_10s=%.4f avg_time_to_move=%.2f",
            result.runtime_signals,
            result.captured_signals,
            result.recorded_signals,
            result.skipped_signals,
            result.resolved_signals,
            result.metrics.follow_through_rate_5s,
            result.metrics.avg_return_5s,
            result.metrics.avg_return_10s,
            result.metrics.avg_time_to_move,
        )
        return result

    def run_forever(self, *, max_cycles: int | None = None) -> None:
        cycle_count = 0
        while True:
            self.run_once()
            cycle_count += 1
            if max_cycles is not None and cycle_count >= max_cycles:
                return
            time.sleep(self.config.polling_interval_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run wallet-signal paper tracking.")
    parser.add_argument("--db-path", help="Override the SQLite database path.")
    parser.add_argument("--poll-interval", type=int, default=2, help="Polling interval in seconds.")
    parser.add_argument(
        "--accepted-only",
        action="store_true",
        help="Record only accepted wallet signals instead of all shadow candidates.",
    )
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit.")
    parser.add_argument("--max-cycles", type=int, help="Optional maximum cycles.")
    parser.add_argument("--log-level", default="INFO", help="Logging level.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    settings = get_settings(args.db_path)
    initialize_database(settings)
    tracker = WalletPaperTracker(
        settings=settings,
        config=WalletPaperTrackerConfig(
            polling_interval_seconds=args.poll_interval,
            capture_shadow_candidates=not args.accepted_only,
        ),
    )
    if args.once:
        tracker.run_once()
        return
    tracker.run_forever(max_cycles=args.max_cycles)


if __name__ == "__main__":
    main()
