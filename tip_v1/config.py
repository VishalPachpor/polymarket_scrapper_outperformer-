from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent


@dataclass(frozen=True)
class Settings:
    database_path: Path
    trades_api_url: str
    markets_api_url: str
    price_history_api_url: str
    orderbook_api_url: str
    orderbooks_api_url: str
    request_timeout_seconds: float
    page_limit: int
    max_pages_per_run: int
    price_history_fidelity: int
    price_history_pre_buffer_seconds: int
    price_history_post_buffer_seconds: int
    snapshot_max_lag_seconds: int
    microstructure_depth_levels: int
    microstructure_batch_size: int
    microstructure_fast_poll_seconds: int
    microstructure_base_poll_seconds: int
    microstructure_slow_poll_seconds: int
    microstructure_active_lookback_seconds: int
    microstructure_price_move_threshold: float


def get_settings(database_path: str | Path | None = None) -> Settings:
    default_db = BASE_DIR / "tip_v1.sqlite3"
    resolved_path = Path(database_path) if database_path else Path(
        os.getenv("TIP_DB_PATH", default_db)
    )

    return Settings(
        database_path=resolved_path,
        trades_api_url=os.getenv(
            "TIP_TRADES_API_URL", "https://data-api.polymarket.com/trades"
        ),
        markets_api_url=os.getenv(
            "TIP_MARKETS_API_URL", "https://gamma-api.polymarket.com/markets"
        ),
        price_history_api_url=os.getenv(
            "TIP_PRICE_HISTORY_API_URL", "https://clob.polymarket.com/prices-history"
        ),
        orderbook_api_url=os.getenv(
            "TIP_ORDERBOOK_API_URL", "https://clob.polymarket.com/book"
        ),
        orderbooks_api_url=os.getenv(
            "TIP_ORDERBOOKS_API_URL", "https://clob.polymarket.com/books"
        ),
        request_timeout_seconds=float(os.getenv("TIP_REQUEST_TIMEOUT_SECONDS", "15")),
        page_limit=int(os.getenv("TIP_PAGE_LIMIT", "100")),
        max_pages_per_run=int(os.getenv("TIP_MAX_PAGES_PER_RUN", "5")),
        price_history_fidelity=int(os.getenv("TIP_PRICE_HISTORY_FIDELITY", "1")),
        price_history_pre_buffer_seconds=int(
            os.getenv("TIP_PRICE_HISTORY_PRE_BUFFER_SECONDS", "300")
        ),
        price_history_post_buffer_seconds=int(
            os.getenv("TIP_PRICE_HISTORY_POST_BUFFER_SECONDS", "300")
        ),
        snapshot_max_lag_seconds=int(
            os.getenv("TIP_SNAPSHOT_MAX_LAG_SECONDS", "5")
        ),
        microstructure_depth_levels=int(
            os.getenv("TIP_MICROSTRUCTURE_DEPTH_LEVELS", "3")
        ),
        microstructure_batch_size=int(
            os.getenv("TIP_MICROSTRUCTURE_BATCH_SIZE", "100")
        ),
        microstructure_fast_poll_seconds=int(
            os.getenv("TIP_MICROSTRUCTURE_FAST_POLL_SECONDS", "2")
        ),
        microstructure_base_poll_seconds=int(
            os.getenv("TIP_MICROSTRUCTURE_BASE_POLL_SECONDS", "5")
        ),
        microstructure_slow_poll_seconds=int(
            os.getenv("TIP_MICROSTRUCTURE_SLOW_POLL_SECONDS", "15")
        ),
        microstructure_active_lookback_seconds=int(
            os.getenv("TIP_MICROSTRUCTURE_ACTIVE_LOOKBACK_SECONDS", "600")
        ),
        microstructure_price_move_threshold=float(
            os.getenv("TIP_MICROSTRUCTURE_PRICE_MOVE_THRESHOLD", "0.05")
        ),
    )
