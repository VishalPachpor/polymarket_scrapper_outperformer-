from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Callable

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import managed_connection
from tip_v1.live.trade_ingestor import (
    LiveIngestionConfig,
    LiveIngestionCycleResult,
    ingest_recent_trade_stream,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Trade:
    event_id: int
    trade_id: str
    wallet: str
    market_id: str
    asset_id: str | None
    outcome: str
    side: str
    price: float
    size: float
    timestamp: int


@dataclass(frozen=True)
class TradeStreamConfig:
    poll_interval_seconds: int = 2
    recent_trade_limit: int = 200
    normalize_batch_size: int = 500
    max_events_per_cycle: int = 500
    start_from_latest: bool = True


@dataclass(frozen=True)
class TradeStreamCycle:
    ingestion: LiveIngestionCycleResult
    emitted_count: int
    last_event_id: int
    latency_seconds: float


class DbBackedTradeStream:
    def __init__(
        self,
        *,
        settings: Settings | None = None,
        config: TradeStreamConfig | None = None,
        on_trade_callback: Callable[[Trade], None] | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.config = config or TradeStreamConfig()
        self.on_trade_callback = on_trade_callback
        self._last_event_id: int | None = None

    @property
    def last_event_id(self) -> int | None:
        return self._last_event_id

    def run_once(self) -> TradeStreamCycle:
        start = time.time()
        if self._last_event_id is None:
            self._last_event_id = self._initial_event_id()

        try:
            ingestion = ingest_recent_trade_stream(
                settings=self.settings,
                config=LiveIngestionConfig(
                    poll_interval_seconds=self.config.poll_interval_seconds,
                    recent_trade_limit=self.config.recent_trade_limit,
                    normalize_batch_size=self.config.normalize_batch_size,
                ),
            )
        except Exception as exc:
            logger.warning(
                "wallet_trade_stream_ingestion_failed error=%s",
                exc,
            )
            ingestion = LiveIngestionCycleResult(
                fetched_count=0,
                inserted_raw_count=0,
                duplicate_raw_count=0,
                missing_wallet_count=0,
                normalized_inserted_count=0,
                normalized_skipped_count=0,
                quarantined_count=0,
                failed_count=1,
                latency_seconds=0.0,
            )
        trades = self._load_new_trades(after_event_id=self._last_event_id)
        emitted = 0
        for trade in trades:
            emitted += 1
            if self.on_trade_callback is not None:
                self.on_trade_callback(trade)
        if trades:
            self._last_event_id = trades[-1].event_id

        cycle = TradeStreamCycle(
            ingestion=ingestion,
            emitted_count=emitted,
            last_event_id=self._last_event_id or 0,
            latency_seconds=time.time() - start,
        )
        logger.info(
            "wallet_trade_stream_cycle raw_inserted=%s normalized=%s emitted=%s last_event_id=%s latency=%.2fs",
            ingestion.inserted_raw_count,
            ingestion.normalized_inserted_count,
            cycle.emitted_count,
            cycle.last_event_id,
            cycle.latency_seconds,
        )
        return cycle

    def run_forever(self, *, max_cycles: int | None = None) -> None:
        cycle_count = 0
        while True:
            try:
                self.run_once()
            except Exception as exc:
                logger.exception("wallet_trade_stream_error error=%s", exc)

            cycle_count += 1
            if max_cycles is not None and cycle_count >= max_cycles:
                return
            time.sleep(self.config.poll_interval_seconds)

    def _initial_event_id(self) -> int:
        if not self.config.start_from_latest:
            return 0
        with managed_connection(self.settings) as connection:
            row = connection.execute(
                "SELECT COALESCE(MAX(id), 0) AS max_id FROM trade_events"
            ).fetchone()
        return int(row["max_id"] or 0)

    def _load_new_trades(self, *, after_event_id: int) -> list[Trade]:
        with managed_connection(self.settings) as connection:
            rows = connection.execute(
                """
                SELECT
                    id,
                    trade_id,
                    wallet,
                    market_id,
                    asset_id,
                    outcome,
                    side,
                    price,
                    size,
                    timestamp
                FROM trade_events
                WHERE id > ?
                ORDER BY id
                LIMIT ?
                """,
                (after_event_id, self.config.max_events_per_cycle),
            ).fetchall()
        return [
            Trade(
                event_id=int(row["id"]),
                trade_id=str(row["trade_id"]),
                wallet=str(row["wallet"]).lower(),
                market_id=str(row["market_id"]),
                asset_id=str(row["asset_id"]) if row["asset_id"] is not None else None,
                outcome=str(row["outcome"]),
                side=str(row["side"]),
                price=float(row["price"]),
                size=float(row["size"]),
                timestamp=int(row["timestamp"]),
            )
            for row in rows
        ]
