from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import time
from dataclasses import dataclass
from typing import Any

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import initialize_database, managed_connection
from tip_v1.discovery.trade_scanner import fetch_recent_trades
from tip_v1.ingestion.fetch_trades import make_trade_fingerprint
from tip_v1.normalization.normalize_trades import normalize_trades


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LiveIngestionConfig:
    poll_interval_seconds: int = 3
    recent_trade_limit: int = 200
    normalize_batch_size: int = 500
    sqlite_retry_attempts: int = 3
    sqlite_retry_delay_seconds: float = 1.0


@dataclass(frozen=True)
class LiveIngestionCycleResult:
    fetched_count: int
    inserted_raw_count: int
    duplicate_raw_count: int
    missing_wallet_count: int
    normalized_inserted_count: int
    normalized_skipped_count: int
    quarantined_count: int
    failed_count: int
    latency_seconds: float


def _extract_trade_wallet(trade: dict[str, Any]) -> str | None:
    for field in ("user", "wallet", "maker", "taker", "proxyWallet"):
        wallet = trade.get(field)
        if isinstance(wallet, str) and wallet.startswith("0x"):
            return wallet.lower()
    return None


def _ingest_raw_trade_payloads(
    trades: list[dict[str, Any]],
    *,
    settings: Settings,
    retry_attempts: int = 3,
    retry_delay_seconds: float = 1.0,
) -> tuple[int, int, int]:
    last_error: sqlite3.OperationalError | None = None
    for attempt in range(retry_attempts + 1):
        inserted_raw_count = 0
        duplicate_raw_count = 0
        missing_wallet_count = 0
        try:
            with managed_connection(settings) as connection:
                for trade in trades:
                    wallet = _extract_trade_wallet(trade)
                    if wallet is None:
                        missing_wallet_count += 1
                        continue

                    dedupe_key = make_trade_fingerprint(wallet, trade)
                    raw_json = json.dumps(trade, sort_keys=True, separators=(",", ":"))
                    cursor = connection.execute(
                        """
                        INSERT OR IGNORE INTO trades_raw (wallet, raw_json, dedupe_key)
                        VALUES (?, ?, ?)
                        """,
                        (wallet, raw_json, dedupe_key),
                    )
                    inserted_raw_count += cursor.rowcount
                    duplicate_raw_count += 1 - cursor.rowcount

                connection.commit()
            return inserted_raw_count, duplicate_raw_count, missing_wallet_count
        except sqlite3.OperationalError as error:
            if "database is locked" not in str(error).lower():
                raise
            last_error = error
            if attempt >= retry_attempts:
                break
            delay = retry_delay_seconds * (attempt + 1)
            logger.warning(
                "[INGEST][LOCKED] retrying raw insert attempt=%s/%s delay=%.2fs",
                attempt + 1,
                retry_attempts,
                delay,
            )
            time.sleep(delay)

    assert last_error is not None
    raise last_error


def ingest_recent_trade_stream(
    *,
    settings: Settings | None = None,
    config: LiveIngestionConfig | None = None,
) -> LiveIngestionCycleResult:
    settings = settings or get_settings()
    config = config or LiveIngestionConfig()
    start = time.time()

    trades = fetch_recent_trades(
        settings=settings,
        limit=config.recent_trade_limit,
    )
    inserted_raw_count, duplicate_raw_count, missing_wallet_count = _ingest_raw_trade_payloads(
        trades,
        settings=settings,
        retry_attempts=config.sqlite_retry_attempts,
        retry_delay_seconds=config.sqlite_retry_delay_seconds,
    )
    normalization = normalize_trades(
        settings=settings,
        batch_size=config.normalize_batch_size,
    )
    latency_seconds = time.time() - start

    result = LiveIngestionCycleResult(
        fetched_count=len(trades),
        inserted_raw_count=inserted_raw_count,
        duplicate_raw_count=duplicate_raw_count,
        missing_wallet_count=missing_wallet_count,
        normalized_inserted_count=normalization.inserted_count,
        normalized_skipped_count=normalization.skipped_count,
        quarantined_count=normalization.quarantined_count,
        failed_count=normalization.failed_count,
        latency_seconds=latency_seconds,
    )
    logger.info(
        "[INGEST] fetched=%s inserted=%s duplicates=%s missing_wallet=%s normalized=%s skipped=%s quarantined=%s failed=%s latency=%.2fs",
        result.fetched_count,
        result.inserted_raw_count,
        result.duplicate_raw_count,
        result.missing_wallet_count,
        result.normalized_inserted_count,
        result.normalized_skipped_count,
        result.quarantined_count,
        result.failed_count,
        result.latency_seconds,
    )
    return result


def run_live_ingestion(
    *,
    settings: Settings | None = None,
    config: LiveIngestionConfig | None = None,
    max_cycles: int | None = None,
) -> None:
    settings = settings or get_settings()
    config = config or LiveIngestionConfig()
    initialize_database(settings)

    cycle = 0
    while True:
        try:
            ingest_recent_trade_stream(settings=settings, config=config)
        except Exception as exc:
            logger.exception("[INGEST][ERROR] %s", exc)

        cycle += 1
        if max_cycles is not None and cycle >= max_cycles:
            break
        time.sleep(config.poll_interval_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the TIP V1 live trade ingestor.")
    parser.add_argument("--db-path", help="Override the SQLite database path.")
    parser.add_argument("--poll-interval", type=int, default=3, help="Polling interval in seconds.")
    parser.add_argument("--limit", type=int, default=200, help="Recent trade batch size per poll.")
    parser.add_argument("--normalize-batch-size", type=int, default=500, help="Normalization batch size.")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING).")
    parser.add_argument("--once", action="store_true", help="Run a single ingestion cycle and exit.")
    parser.add_argument("--max-cycles", type=int, help="Optional maximum cycles for continuous mode.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    settings = get_settings(args.db_path)
    config = LiveIngestionConfig(
        poll_interval_seconds=args.poll_interval,
        recent_trade_limit=args.limit,
        normalize_batch_size=args.normalize_batch_size,
    )
    if args.once:
        initialize_database(settings)
        ingest_recent_trade_stream(settings=settings, config=config)
        return
    run_live_ingestion(settings=settings, config=config, max_cycles=args.max_cycles)


if __name__ == "__main__":
    main()
