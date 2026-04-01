from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.signals.momentum_trigger import BUY, NO_SIGNAL, generate_momentum_signal_from_db


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PaperTradeConfig:
    polling_interval_seconds: int = 10
    active_asset_lookback_seconds: int = 900
    active_asset_limit: int = 25
    tracking_window_seconds: int = 300
    minimum_win_move: float = 0.03
    quick_reversal_seconds: int = 180
    max_open_signals_per_asset: int = 1
    delay_buckets_seconds: tuple[int, ...] = (5, 10, 15)
    entry_slippage_fraction: float = 0.01
    realtime_latency_seconds: int = 30
    delayed_latency_seconds: int = 120
    max_signal_latency_seconds: int = 120


@dataclass(frozen=True)
class ActiveAsset:
    market_id: str
    asset_id: str
    last_trade_timestamp: int
    trade_count: int


@dataclass(frozen=True)
class PaperSignalMetrics:
    resolved_signals: int
    signal_win_rate: float
    avg_mfe: float
    avg_mae: float
    avg_time_to_peak: float
    avg_return_60s: float
    avg_return_180s: float
    avg_return_300s: float
    move_1_to_3m_rate: float
    quick_reversal_rate: float
    avg_return_180s_5s: float
    avg_return_180s_10s: float
    avg_return_180s_15s: float
    execution_verdict: str


@dataclass(frozen=True)
class PaperRunnerCycleResult:
    checked_assets: int
    signals_generated: int
    signals_recorded: int
    signals_skipped: int
    outcomes_resolved: int
    latest_trade_timestamp: int | None
    latency_seconds: int | None
    latency_status: str
    signal_eligible: bool
    metrics: PaperSignalMetrics


def latest_trade_timestamp(connection) -> int | None:
    row = connection.execute(
        """
        SELECT MAX(timestamp) AS latest_trade_timestamp
        FROM trade_events
        """
    ).fetchone()
    return int(row["latest_trade_timestamp"]) if row and row["latest_trade_timestamp"] is not None else None


def classify_latency(
    *,
    now_ts: int,
    latest_trade_ts: int | None,
    config: PaperTradeConfig,
) -> tuple[int | None, str, bool]:
    if latest_trade_ts is None:
        return None, "NO DATA", False

    latency_seconds = max(0, now_ts - latest_trade_ts)
    if latency_seconds <= config.realtime_latency_seconds:
        return latency_seconds, "REALTIME", True
    if latency_seconds <= config.delayed_latency_seconds:
        return latency_seconds, "DELAYED", True
    return latency_seconds, "STALE", False


def discover_active_assets(
    connection,
    *,
    now_ts: int,
    config: PaperTradeConfig,
) -> list[ActiveAsset]:
    rows = connection.execute(
        """
        SELECT
            market_id,
            asset_id,
            MAX(timestamp) AS last_trade_timestamp,
            COUNT(*) AS trade_count
        FROM trade_events
        WHERE asset_id IS NOT NULL
          AND timestamp >= ?
        GROUP BY market_id, asset_id
        ORDER BY last_trade_timestamp DESC, trade_count DESC, asset_id
        LIMIT ?
        """,
        (now_ts - config.active_asset_lookback_seconds, config.active_asset_limit),
    ).fetchall()
    return [
        ActiveAsset(
            market_id=str(row["market_id"]),
            asset_id=str(row["asset_id"]),
            last_trade_timestamp=int(row["last_trade_timestamp"]),
            trade_count=int(row["trade_count"]),
        )
        for row in rows
    ]


def _last_signal_timestamp(connection, *, asset_id: str) -> int | None:
    row = connection.execute(
        """
        SELECT MAX(signal_timestamp) AS last_signal_timestamp
        FROM paper_signals
        WHERE asset_id = ?
        """,
        (asset_id,),
    ).fetchone()
    return int(row["last_signal_timestamp"]) if row and row["last_signal_timestamp"] is not None else None


def _has_open_signal(connection, *, asset_id: str, config: PaperTradeConfig) -> bool:
    row = connection.execute(
        """
        SELECT COUNT(*) AS open_count
        FROM paper_signals
        WHERE asset_id = ? AND status = 'OPEN'
        """,
        (asset_id,),
    ).fetchone()
    return int(row["open_count"] or 0) >= config.max_open_signals_per_asset


def record_paper_signal(connection, signal) -> bool:
    cursor = connection.execute(
        """
        INSERT OR IGNORE INTO paper_signals(
            market_id,
            asset_id,
            signal_timestamp,
            entry_price,
            confidence,
            signal_type,
            dry_run
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(signal.market_id),
            str(signal.asset_id),
            int(signal.timestamp),
            float(signal.price),
            float(signal.confidence),
            str(signal.signal_type),
            1 if signal.dry_run else 0,
        ),
    )
    return cursor.rowcount > 0


def _price_return_at(
    samples: list[tuple[int, float]],
    *,
    entry_price: float,
    entry_timestamp: int,
    offset_seconds: int,
) -> float | None:
    target_ts = entry_timestamp + offset_seconds
    eligible = [price for timestamp, price in samples if timestamp <= target_ts]
    if not eligible:
        return None
    return round(float(eligible[-1]) - entry_price, 12)


def _price_at_or_after(
    samples: list[tuple[int, float]],
    *,
    target_ts: int,
) -> float | None:
    for timestamp, price in samples:
        if timestamp >= target_ts:
            return float(price)
    return None


def _compute_fractional_return(
    entry_price: float | None,
    future_price: float | None,
) -> float | None:
    if entry_price is None or future_price is None or entry_price <= 0:
        return None
    return round((future_price - entry_price) / entry_price, 12)


def _reversed_quickly(
    samples: list[tuple[int, float]],
    *,
    entry_price: float,
    entry_timestamp: int,
    config: PaperTradeConfig,
) -> bool:
    saw_early_strength = False
    threshold = entry_price + (config.minimum_win_move / 2.0)
    for timestamp, price in samples:
        if timestamp > entry_timestamp + config.quick_reversal_seconds:
            break
        if price >= threshold:
            saw_early_strength = True
            continue
        if saw_early_strength and price <= entry_price:
            return True
    return False


def resolve_paper_signal_outcomes(
    connection,
    *,
    now_ts: int,
    config: PaperTradeConfig,
) -> int:
    due_rows = connection.execute(
        """
        SELECT
            ps.id,
            ps.market_id,
            ps.asset_id,
            ps.signal_timestamp,
            ps.entry_price
        FROM paper_signals AS ps
        LEFT JOIN paper_signal_outcomes AS outcome
            ON outcome.signal_id = ps.id
        WHERE ps.status = 'OPEN'
          AND outcome.signal_id IS NULL
          AND ps.signal_timestamp + ? <= ?
        ORDER BY ps.signal_timestamp, ps.id
        """,
        (config.tracking_window_seconds, now_ts),
    ).fetchall()

    resolved = 0
    for row in due_rows:
        signal_id = int(row["id"])
        entry_timestamp = int(row["signal_timestamp"])
        entry_price = float(row["entry_price"])
        window_end = entry_timestamp + config.tracking_window_seconds

        history_rows = connection.execute(
            """
            SELECT timestamp, price
            FROM market_price_history
            WHERE asset_id = ? AND timestamp BETWEEN ? AND ?
            ORDER BY timestamp, id
            """,
            (row["asset_id"], entry_timestamp, window_end),
        ).fetchall()

        samples = [(int(sample["timestamp"]), float(sample["price"])) for sample in history_rows]
        if samples:
            max_timestamp, max_price = max(samples, key=lambda sample: (sample[1], -sample[0]))
            _, min_price = min(samples, key=lambda sample: (sample[1], sample[0]))
            mfe = round(max_price - entry_price, 12)
            mae = round(min_price - entry_price, 12)
            time_to_peak = max_timestamp - entry_timestamp
            reversed_quickly = _reversed_quickly(
                samples,
                entry_price=entry_price,
                entry_timestamp=entry_timestamp,
                config=config,
            )
            return_60s = _price_return_at(
                samples,
                entry_price=entry_price,
                entry_timestamp=entry_timestamp,
                offset_seconds=60,
            )
            return_180s = _price_return_at(
                samples,
                entry_price=entry_price,
                entry_timestamp=entry_timestamp,
                offset_seconds=180,
            )
            return_300s = _price_return_at(
                samples,
                entry_price=entry_price,
                entry_timestamp=entry_timestamp,
                offset_seconds=300,
            )
            future_price_60s = _price_at_or_after(samples, target_ts=entry_timestamp + 60)
            future_price_180s = _price_at_or_after(samples, target_ts=entry_timestamp + 180)
            future_price_300s = _price_at_or_after(samples, target_ts=entry_timestamp + 300)
            delayed_entry_prices: dict[int, float | None] = {}
            delayed_returns: dict[int, dict[int, float | None]] = {}
            for delay in config.delay_buckets_seconds:
                delayed_entry_price = _price_at_or_after(
                    samples,
                    target_ts=entry_timestamp + delay,
                )
                if delayed_entry_price is not None:
                    delayed_entry_price = round(
                        delayed_entry_price * (1.0 + config.entry_slippage_fraction),
                        12,
                    )
                delayed_entry_prices[delay] = delayed_entry_price
                delayed_returns[delay] = {
                    60: _compute_fractional_return(delayed_entry_price, future_price_60s),
                    180: _compute_fractional_return(delayed_entry_price, future_price_180s),
                    300: _compute_fractional_return(delayed_entry_price, future_price_300s),
                }
            feasible_10s = 1 if (delayed_returns.get(10, {}).get(180) or 0.0) > 0 else 0
            win_flag = 1 if mfe >= config.minimum_win_move else 0
        else:
            max_price = None
            min_price = None
            mfe = None
            mae = None
            time_to_peak = None
            reversed_quickly = False
            return_60s = None
            return_180s = None
            return_300s = None
            delayed_entry_prices = {delay: None for delay in config.delay_buckets_seconds}
            delayed_returns = {
                delay: {60: None, 180: None, 300: None}
                for delay in config.delay_buckets_seconds
            }
            feasible_10s = 0
            win_flag = 0

        connection.execute(
            """
            INSERT INTO paper_signal_outcomes(
                signal_id,
                market_id,
                asset_id,
                entry_timestamp,
                resolved_timestamp,
                tracking_window_seconds,
                sample_count,
                max_price,
                min_price,
                mfe,
                mae,
                time_to_peak,
                return_60s,
                return_180s,
                return_300s,
                entry_price_5s,
                entry_price_10s,
                entry_price_15s,
                return_60s_5s,
                return_60s_10s,
                return_60s_15s,
                return_180s_5s,
                return_180s_10s,
                return_180s_15s,
                return_300s_5s,
                return_300s_10s,
                return_300s_15s,
                slippage_applied,
                feasible_10s,
                reversed_quickly,
                win_flag
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                signal_id,
                str(row["market_id"]),
                str(row["asset_id"]),
                entry_timestamp,
                now_ts,
                config.tracking_window_seconds,
                len(samples),
                max_price,
                min_price,
                mfe,
                mae,
                time_to_peak,
                return_60s,
                return_180s,
                return_300s,
                delayed_entry_prices.get(5),
                delayed_entry_prices.get(10),
                delayed_entry_prices.get(15),
                delayed_returns.get(5, {}).get(60),
                delayed_returns.get(10, {}).get(60),
                delayed_returns.get(15, {}).get(60),
                delayed_returns.get(5, {}).get(180),
                delayed_returns.get(10, {}).get(180),
                delayed_returns.get(15, {}).get(180),
                delayed_returns.get(5, {}).get(300),
                delayed_returns.get(10, {}).get(300),
                delayed_returns.get(15, {}).get(300),
                config.entry_slippage_fraction,
                feasible_10s,
                1 if reversed_quickly else 0,
                win_flag,
            ),
        )
        connection.execute(
            """
            UPDATE paper_signals
            SET status = 'RESOLVED'
            WHERE id = ?
            """,
            (signal_id,),
        )
        resolved += 1
        logger.info(
            "paper_signal_outcome_resolved signal_id=%s asset_id=%s mfe=%s mae=%s time_to_peak=%s win=%s r180_5s=%s r180_10s=%s r180_15s=%s",
            signal_id,
            row["asset_id"],
            f"{mfe:.4f}" if mfe is not None else "NA",
            f"{mae:.4f}" if mae is not None else "NA",
            time_to_peak,
            win_flag,
            f"{(delayed_returns.get(5, {}).get(180) or 0.0):.4f}" if delayed_returns.get(5, {}).get(180) is not None else "NA",
            f"{(delayed_returns.get(10, {}).get(180) or 0.0):.4f}" if delayed_returns.get(10, {}).get(180) is not None else "NA",
            f"{(delayed_returns.get(15, {}).get(180) or 0.0):.4f}" if delayed_returns.get(15, {}).get(180) is not None else "NA",
        )

    return resolved


def compute_paper_signal_metrics(
    connection,
    *,
    since_ts: int | None = None,
) -> PaperSignalMetrics:
    clauses = []
    params: list[object] = []
    if since_ts is not None:
        clauses.append("entry_timestamp >= ?")
        params.append(since_ts)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    row = connection.execute(
        f"""
        SELECT
            COUNT(*) AS resolved_signals,
            AVG(CAST(win_flag AS REAL)) AS signal_win_rate,
            AVG(mfe) AS avg_mfe,
            AVG(mae) AS avg_mae,
            AVG(time_to_peak) AS avg_time_to_peak,
            AVG(return_60s) AS avg_return_60s,
            AVG(return_180s) AS avg_return_180s,
            AVG(return_300s) AS avg_return_300s,
            AVG(
                CASE
                    WHEN win_flag = 1 AND time_to_peak BETWEEN 60 AND 180
                    THEN 1.0
                    ELSE 0.0
                END
            ) AS move_1_to_3m_rate,
            AVG(CAST(reversed_quickly AS REAL)) AS quick_reversal_rate,
            AVG(return_180s_5s) AS avg_return_180s_5s,
            AVG(return_180s_10s) AS avg_return_180s_10s,
            AVG(return_180s_15s) AS avg_return_180s_15s
        FROM paper_signal_outcomes
        {where_sql}
        """,
        params,
    ).fetchone()

    resolved_signals = int(row["resolved_signals"] or 0)
    avg_return_180s_10s = float(row["avg_return_180s_10s"] or 0.0)
    avg_return_180s_5s = float(row["avg_return_180s_5s"] or 0.0)
    if resolved_signals == 0:
        execution_verdict = "NO DATA YET"
    elif avg_return_180s_10s > 0:
        execution_verdict = "EDGE SURVIVES EXECUTION"
    elif avg_return_180s_5s > 0:
        execution_verdict = "EDGE IS LATENCY-SENSITIVE"
    else:
        execution_verdict = "EDGE NOT TRADEABLE"
    return PaperSignalMetrics(
        resolved_signals=resolved_signals,
        signal_win_rate=float(row["signal_win_rate"] or 0.0),
        avg_mfe=float(row["avg_mfe"] or 0.0),
        avg_mae=float(row["avg_mae"] or 0.0),
        avg_time_to_peak=float(row["avg_time_to_peak"] or 0.0),
        avg_return_60s=float(row["avg_return_60s"] or 0.0),
        avg_return_180s=float(row["avg_return_180s"] or 0.0),
        avg_return_300s=float(row["avg_return_300s"] or 0.0),
        move_1_to_3m_rate=float(row["move_1_to_3m_rate"] or 0.0),
        quick_reversal_rate=float(row["quick_reversal_rate"] or 0.0),
        avg_return_180s_5s=avg_return_180s_5s,
        avg_return_180s_10s=avg_return_180s_10s,
        avg_return_180s_15s=float(row["avg_return_180s_15s"] or 0.0),
        execution_verdict=execution_verdict,
    )


class PaperTradeRunner:
    def __init__(
        self,
        *,
        settings: Settings | None = None,
        config: PaperTradeConfig | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.config = config or PaperTradeConfig()
        initialize_database(self.settings)

    def run_once(self, *, now_ts: int | None = None) -> PaperRunnerCycleResult:
        now_ts = int(time.time()) if now_ts is None else now_ts

        with managed_connection(self.settings) as connection:
            latest_trade_ts = latest_trade_timestamp(connection)
            latency_seconds, latency_status, signal_eligible = classify_latency(
                now_ts=now_ts,
                latest_trade_ts=latest_trade_ts,
                config=self.config,
            )
            active_assets = discover_active_assets(
                connection,
                now_ts=now_ts,
                config=self.config,
            )

            signals_generated = 0
            signals_recorded = 0
            signals_skipped = 0

            if not signal_eligible:
                signals_skipped = len(active_assets)
                logger.info(
                    "paper_signal_generation_paused latency_status=%s latency_seconds=%s active_assets=%s",
                    latency_status,
                    latency_seconds,
                    len(active_assets),
                )
            else:
                for asset in active_assets:
                    last_signal_timestamp = _last_signal_timestamp(connection, asset_id=asset.asset_id)
                    signal = generate_momentum_signal_from_db(
                        settings=self.settings,
                        asset_id=asset.asset_id,
                        market_id=asset.market_id,
                        now_ts=now_ts,
                        last_signal_timestamp=last_signal_timestamp,
                        dry_run=True,
                    )

                    if signal.signal_type != BUY:
                        signals_skipped += 1
                        logger.info(
                            "paper_signal_skipped asset_id=%s market_id=%s reason=%s",
                            asset.asset_id,
                            asset.market_id,
                            signal.reason or NO_SIGNAL,
                        )
                        continue

                    signals_generated += 1
                    if _has_open_signal(connection, asset_id=asset.asset_id, config=self.config):
                        signals_skipped += 1
                        logger.info(
                            "paper_signal_skipped asset_id=%s market_id=%s reason=open_signal_exists",
                            asset.asset_id,
                            asset.market_id,
                        )
                        continue

                    inserted = record_paper_signal(connection, signal)
                    if inserted:
                        signals_recorded += 1
                        logger.info(
                            "paper_signal_recorded asset_id=%s market_id=%s ts=%s price=%.4f confidence=%.3f",
                            signal.asset_id,
                            signal.market_id,
                            signal.timestamp,
                            signal.price,
                            signal.confidence,
                        )
                    else:
                        signals_skipped += 1
                        logger.info(
                            "paper_signal_skipped asset_id=%s market_id=%s reason=duplicate_signal",
                            signal.asset_id,
                            signal.market_id,
                        )

            outcomes_resolved = resolve_paper_signal_outcomes(
                connection,
                now_ts=now_ts,
                config=self.config,
            )
            metrics = compute_paper_signal_metrics(connection)
            connection.commit()

        logger.info(
            "paper_runner_cycle checked_assets=%s signals_generated=%s signals_recorded=%s signals_skipped=%s outcomes_resolved=%s latency=%s latency_status=%s signal_eligible=%s win_rate=%.2f avg_mfe=%.4f avg_mae=%.4f avg_ttp=%.2f edge_decay_180s=[%.4f,%.4f,%.4f,%.4f] verdict=%s",
            len(active_assets),
            signals_generated,
            signals_recorded,
            signals_skipped,
            outcomes_resolved,
            latency_seconds,
            latency_status,
            signal_eligible,
            metrics.signal_win_rate,
            metrics.avg_mfe,
            metrics.avg_mae,
            metrics.avg_time_to_peak,
            metrics.avg_return_180s,
            metrics.avg_return_180s_5s,
            metrics.avg_return_180s_10s,
            metrics.avg_return_180s_15s,
            metrics.execution_verdict,
        )

        return PaperRunnerCycleResult(
            checked_assets=len(active_assets),
            signals_generated=signals_generated,
            signals_recorded=signals_recorded,
            signals_skipped=signals_skipped,
            outcomes_resolved=outcomes_resolved,
            latest_trade_timestamp=latest_trade_ts,
            latency_seconds=latency_seconds,
            latency_status=latency_status,
            signal_eligible=signal_eligible,
            metrics=metrics,
        )

    def run_forever(self, *, max_cycles: int | None = None) -> None:
        cycle = 0
        while True:
            self.run_once()
            cycle += 1
            if max_cycles is not None and cycle >= max_cycles:
                break
            time.sleep(self.config.polling_interval_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the TIP V1 paper-trading observation loop.")
    parser.add_argument("--db-path", help="Override the SQLite database path.")
    parser.add_argument("--poll-interval", type=int, default=10, help="Polling interval in seconds.")
    parser.add_argument("--tracking-window", type=int, default=300, help="Outcome tracking window in seconds.")
    parser.add_argument("--min-win-move", type=float, default=0.03, help="Minimum favorable move required to count a signal as a win.")
    parser.add_argument("--active-lookback", type=int, default=900, help="How far back to scan trade activity for active assets.")
    parser.add_argument("--active-limit", type=int, default=25, help="Maximum active assets to check per cycle.")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING).")
    parser.add_argument("--once", action="store_true", help="Run a single observation cycle and exit.")
    parser.add_argument("--max-cycles", type=int, help="Optional maximum cycles for continuous mode.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    settings = get_settings(args.db_path)
    config = PaperTradeConfig(
        polling_interval_seconds=args.poll_interval,
        active_asset_lookback_seconds=args.active_lookback,
        active_asset_limit=args.active_limit,
        tracking_window_seconds=args.tracking_window,
        minimum_win_move=args.min_win_move,
    )
    runner = PaperTradeRunner(settings=settings, config=config)
    if args.once:
        runner.run_once()
        return
    runner.run_forever(max_cycles=args.max_cycles)


if __name__ == "__main__":
    main()
