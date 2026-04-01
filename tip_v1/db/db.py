from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

from tip_v1.config import Settings, get_settings

_SQLITE_BUSY_TIMEOUT_MS = 30_000


def get_connection(settings: Settings | None = None) -> sqlite3.Connection:
    settings = settings or get_settings()
    settings.database_path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(
        settings.database_path,
        timeout=_SQLITE_BUSY_TIMEOUT_MS / 1000,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA temp_store=MEMORY")
    connection.execute(f"PRAGMA busy_timeout={_SQLITE_BUSY_TIMEOUT_MS}")
    return connection


@contextmanager
def managed_connection(settings: Settings | None = None):
    connection = get_connection(settings)
    try:
        yield connection
    finally:
        connection.close()


def _ensure_column(
    connection: sqlite3.Connection,
    *,
    table_name: str,
    column_name: str,
    column_sql: str,
) -> None:
    columns = {
        row["name"]
        for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    if column_name in columns:
        return

    connection.execute(
        f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}"
    )


def _has_single_column_unique_on_trade_id(connection: sqlite3.Connection) -> bool:
    """Return True if trade_events has a unique index on trade_id alone (old schema)."""
    for idx in connection.execute("PRAGMA index_list(trade_events)").fetchall():
        if idx["unique"] != 1:
            continue
        cols = [
            row["name"]
            for row in connection.execute(
                f"PRAGMA index_info({idx['name']})"
            ).fetchall()
        ]
        if cols == ["trade_id"]:
            return True
    return False


def _migrate_trade_events_to_wallet_trade_id_unique(
    connection: sqlite3.Connection,
) -> None:
    """Rebuild trade_events with UNIQUE(wallet, trade_id) instead of UNIQUE(trade_id)."""
    connection.execute("PRAGMA foreign_keys = OFF")
    connection.executescript(
        """
        CREATE TABLE trade_events_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_trade_id INTEGER NOT NULL,
            trade_id TEXT NOT NULL,
            wallet TEXT NOT NULL,
            market_id TEXT NOT NULL,
            outcome TEXT NOT NULL,
            asset_id TEXT,
            side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
            price TEXT NOT NULL,
            size TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(wallet, trade_id),
            FOREIGN KEY(raw_trade_id) REFERENCES trades_raw(id)
        );
        INSERT INTO trade_events_new
            SELECT id, raw_trade_id, trade_id, wallet, market_id, outcome,
                   asset_id, side, price, size, timestamp, ingested_at
            FROM trade_events;
        DROP TABLE trade_events;
        ALTER TABLE trade_events_new RENAME TO trade_events;
        """
    )
    connection.execute("PRAGMA foreign_keys = ON")


def _run_migrations(connection: sqlite3.Connection) -> None:
    tables = {
        row["name"]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }

    if "trade_events" in tables and _has_single_column_unique_on_trade_id(connection):
        _migrate_trade_events_to_wallet_trade_id_unique(connection)

    if "trader_metrics_summary" in tables:
        _ensure_column(
            connection,
            table_name="trader_metrics_summary",
            column_name="win_count",
            column_sql="INTEGER NOT NULL DEFAULT 0",
        )
        _ensure_column(
            connection,
            table_name="trader_metrics_summary",
            column_name="loss_count",
            column_sql="INTEGER NOT NULL DEFAULT 0",
        )
        _ensure_column(
            connection,
            table_name="trader_metrics_summary",
            column_name="breakeven_count",
            column_sql="INTEGER NOT NULL DEFAULT 0",
        )

    if "candidate_wallet_sources" in tables:
        _ensure_column(
            connection,
            table_name="candidate_wallet_sources",
            column_name="regime",
            column_sql="TEXT NOT NULL DEFAULT 'global'",
        )

    if "wallet_rankings" in tables:
        columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(wallet_rankings)").fetchall()
        }
        needs_rebuild = "regime" not in columns
        if needs_rebuild:
            connection.execute(
                """
                ALTER TABLE wallet_rankings RENAME TO wallet_rankings_legacy
                """
            )
            connection.executescript(
                """
                CREATE TABLE wallet_rankings (
                    wallet TEXT NOT NULL,
                    regime TEXT NOT NULL DEFAULT 'global',
                    followability_score REAL NOT NULL,
                    lifecycle_score REAL NOT NULL DEFAULT 0,
                    rank_score REAL NOT NULL,
                    tier TEXT NOT NULL,
                    ranked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (wallet, regime)
                );
                INSERT INTO wallet_rankings(wallet, regime, followability_score, lifecycle_score, rank_score, tier, ranked_at)
                SELECT wallet, 'global', followability_score, 0, rank_score, tier, ranked_at
                FROM wallet_rankings_legacy;
                DROP TABLE wallet_rankings_legacy;
                CREATE INDEX IF NOT EXISTS idx_wallet_rankings_regime_rank
                    ON wallet_rankings(regime, rank_score DESC);
                """
            )
        else:
            _ensure_column(
                connection,
                table_name="wallet_rankings",
                column_name="lifecycle_score",
                column_sql="REAL NOT NULL DEFAULT 0",
            )

    if "paper_signal_outcomes" in tables:
        for column_name, column_sql in (
            ("entry_price_5s", "REAL"),
            ("entry_price_10s", "REAL"),
            ("entry_price_15s", "REAL"),
            ("return_60s_5s", "REAL"),
            ("return_60s_10s", "REAL"),
            ("return_60s_15s", "REAL"),
            ("return_180s_5s", "REAL"),
            ("return_180s_10s", "REAL"),
            ("return_180s_15s", "REAL"),
            ("return_300s_5s", "REAL"),
            ("return_300s_10s", "REAL"),
            ("return_300s_15s", "REAL"),
            ("slippage_applied", "REAL NOT NULL DEFAULT 0.01"),
            ("feasible_10s", "INTEGER NOT NULL DEFAULT 0"),
        ):
            _ensure_column(
                connection,
                table_name="paper_signal_outcomes",
                column_name=column_name,
                column_sql=column_sql,
            )


def initialize_database(settings: Settings | None = None) -> None:
    settings = settings or get_settings()
    schema_path = Path(__file__).resolve().parent / "schema.sql"

    with managed_connection(settings) as connection:
        connection.executescript(schema_path.read_text(encoding="utf-8"))
        _run_migrations(connection)
        connection.commit()
