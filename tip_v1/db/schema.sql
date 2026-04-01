CREATE TABLE IF NOT EXISTS trades_raw (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    dedupe_key TEXT NOT NULL UNIQUE,
    normalized_at TIMESTAMP,
    normalization_error TEXT
);

CREATE TABLE IF NOT EXISTS ingestion_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    last_cursor TEXT,
    status TEXT NOT NULL,
    error_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS trade_events (
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

CREATE TABLE IF NOT EXISTS quarantined_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_trade_id INTEGER NOT NULL UNIQUE,
    wallet TEXT NOT NULL,
    reason TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(raw_trade_id) REFERENCES trades_raw(id)
);

CREATE TABLE IF NOT EXISTS positions_reconstructed (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet TEXT NOT NULL,
    market_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    entry_trade_event_id INTEGER NOT NULL,
    exit_trade_event_id INTEGER,
    entry_price REAL NOT NULL,
    exit_price REAL,
    size REAL NOT NULL,
    pnl REAL,
    entry_time INTEGER NOT NULL,
    exit_time INTEGER,
    duration INTEGER,
    status TEXT NOT NULL CHECK (status IN ('OPEN', 'CLOSED')),
    remaining_size REAL NOT NULL DEFAULT 0,
    version INTEGER NOT NULL,
    FOREIGN KEY(entry_trade_event_id) REFERENCES trade_events(id),
    FOREIGN KEY(exit_trade_event_id) REFERENCES trade_events(id)
);

CREATE TABLE IF NOT EXISTS trader_metrics_summary (
    wallet TEXT PRIMARY KEY,
    total_positions INTEGER NOT NULL,
    win_count INTEGER NOT NULL DEFAULT 0,
    loss_count INTEGER NOT NULL DEFAULT 0,
    breakeven_count INTEGER NOT NULL DEFAULT 0,
    win_rate REAL NOT NULL,
    avg_pnl REAL NOT NULL,
    avg_hold_time REAL NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS market_price_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    price REAL NOT NULL,
    fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(asset_id, timestamp, price)
);

CREATE TABLE IF NOT EXISTS position_path_metrics (
    position_id INTEGER PRIMARY KEY,
    wallet TEXT NOT NULL,
    version INTEGER NOT NULL,
    asset_id TEXT,
    sample_count INTEGER NOT NULL,
    start_timestamp INTEGER NOT NULL,
    end_timestamp INTEGER NOT NULL,
    entry_context_price REAL,
    exit_context_price REAL,
    max_price REAL,
    min_price REAL,
    mfe REAL,
    mae REAL,
    time_to_mfe INTEGER,
    time_to_mae INTEGER,
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(position_id) REFERENCES positions_reconstructed(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS microstructure_raw_book (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id TEXT NOT NULL,
    observed_at INTEGER NOT NULL,
    source_hash TEXT NOT NULL DEFAULT '',
    raw_payload TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(asset_id, observed_at, source_hash)
);

CREATE TABLE IF NOT EXISTS microstructure_book_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_sequence_id INTEGER NOT NULL UNIQUE,
    raw_book_id INTEGER NOT NULL UNIQUE,
    asset_id TEXT NOT NULL,
    market_id TEXT,
    timestamp INTEGER NOT NULL,
    best_bid REAL,
    best_ask REAL,
    mid_price REAL,
    spread REAL,
    bid_depth_1 REAL NOT NULL DEFAULT 0,
    bid_depth_2 REAL NOT NULL DEFAULT 0,
    bid_depth_3 REAL NOT NULL DEFAULT 0,
    ask_depth_1 REAL NOT NULL DEFAULT 0,
    ask_depth_2 REAL NOT NULL DEFAULT 0,
    ask_depth_3 REAL NOT NULL DEFAULT 0,
    total_bid_depth REAL NOT NULL DEFAULT 0,
    total_ask_depth REAL NOT NULL DEFAULT 0,
    imbalance REAL,
    min_order_size REAL,
    tick_size REAL,
    neg_risk INTEGER,
    book_hash TEXT,
    FOREIGN KEY(raw_book_id) REFERENCES microstructure_raw_book(id)
);

CREATE TABLE IF NOT EXISTS microstructure_features (
    snapshot_id INTEGER PRIMARY KEY,
    asset_id TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    price_change_5s REAL,
    price_change_30s REAL,
    price_change_5m REAL,
    volatility_30s REAL,
    volatility_5m REAL,
    snapshots_per_minute REAL,
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(snapshot_id) REFERENCES microstructure_book_snapshots(id)
);

CREATE TABLE IF NOT EXISTS trade_entry_context (
    trade_event_id INTEGER PRIMARY KEY,
    wallet TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    trade_timestamp INTEGER NOT NULL,
    snapshot_id INTEGER,
    snapshot_timestamp INTEGER,
    snapshot_lag_seconds INTEGER,
    lag_confidence TEXT NOT NULL,
    best_bid REAL,
    best_ask REAL,
    mid_price REAL,
    spread REAL,
    total_bid_depth REAL,
    total_ask_depth REAL,
    imbalance REAL,
    price_change_5s REAL,
    price_change_30s REAL,
    price_change_5m REAL,
    volatility_30s REAL,
    volatility_5m REAL,
    snapshots_per_minute REAL,
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(trade_event_id) REFERENCES trade_events(id),
    FOREIGN KEY(snapshot_id) REFERENCES microstructure_book_snapshots(id)
);

CREATE TABLE IF NOT EXISTS wallet_profiles (
    wallet TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    trader_type TEXT NOT NULL,
    confidence REAL NOT NULL,
    features_json TEXT NOT NULL,
    metrics_json TEXT NOT NULL,
    behavior_json TEXT NOT NULL,
    edge_json TEXT NOT NULL,
    failures_json TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS wallet_profile_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet TEXT NOT NULL,
    version INTEGER NOT NULL,
    trader_type TEXT NOT NULL,
    confidence REAL NOT NULL,
    features_json TEXT NOT NULL,
    metrics_json TEXT NOT NULL,
    behavior_json TEXT NOT NULL,
    edge_json TEXT NOT NULL,
    failures_json TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_wallet_profile_runs_wallet
    ON wallet_profile_runs(wallet, created_at);

CREATE TABLE IF NOT EXISTS candidate_wallets (
    wallet TEXT PRIMARY KEY,
    discovered_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_checked_at TIMESTAMP,
    next_check_at TIMESTAMP,
    status TEXT NOT NULL DEFAULT 'NEW',
    sample_trade_count INTEGER NOT NULL DEFAULT 0,
    sample_buy_count INTEGER NOT NULL DEFAULT 0,
    sample_sell_count INTEGER NOT NULL DEFAULT 0,
    sample_unique_markets INTEGER NOT NULL DEFAULT 0,
    prefilter_score REAL NOT NULL DEFAULT 0,
    prefilter_passed INTEGER NOT NULL DEFAULT 0,
    prefilter_reason TEXT,
    check_error_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    profiled_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS candidate_wallet_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet TEXT NOT NULL,
    source TEXT NOT NULL,
    regime TEXT NOT NULL DEFAULT 'global',
    source_rank INTEGER,
    source_metadata_json TEXT,
    observed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(wallet) REFERENCES candidate_wallets(wallet)
);

CREATE TABLE IF NOT EXISTS wallet_rankings (
    wallet TEXT NOT NULL,
    regime TEXT NOT NULL DEFAULT 'global',
    followability_score REAL NOT NULL,
    lifecycle_score REAL NOT NULL DEFAULT 0,
    rank_score REAL NOT NULL,
    tier TEXT NOT NULL,
    ranked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (wallet, regime)
);

CREATE TABLE IF NOT EXISTS monitor_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    candidate_count INTEGER NOT NULL,
    profiled_count INTEGER NOT NULL,
    ranked_count INTEGER NOT NULL,
    good_trader_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    signal_timestamp INTEGER NOT NULL,
    entry_price REAL NOT NULL,
    confidence REAL NOT NULL DEFAULT 0,
    signal_type TEXT NOT NULL,
    dry_run INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'RESOLVED')),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(asset_id, signal_timestamp, signal_type)
);

CREATE TABLE IF NOT EXISTS paper_signal_outcomes (
    signal_id INTEGER PRIMARY KEY,
    market_id TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    entry_timestamp INTEGER NOT NULL,
    resolved_timestamp INTEGER NOT NULL,
    tracking_window_seconds INTEGER NOT NULL,
    sample_count INTEGER NOT NULL,
    max_price REAL,
    min_price REAL,
    mfe REAL,
    mae REAL,
    time_to_peak INTEGER,
    return_60s REAL,
    return_180s REAL,
    return_300s REAL,
    entry_price_5s REAL,
    entry_price_10s REAL,
    entry_price_15s REAL,
    return_60s_5s REAL,
    return_60s_10s REAL,
    return_60s_15s REAL,
    return_180s_5s REAL,
    return_180s_10s REAL,
    return_180s_15s REAL,
    return_300s_5s REAL,
    return_300s_10s REAL,
    return_300s_15s REAL,
    slippage_applied REAL NOT NULL DEFAULT 0.01,
    feasible_10s INTEGER NOT NULL DEFAULT 0,
    reversed_quickly INTEGER NOT NULL DEFAULT 0,
    win_flag INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(signal_id) REFERENCES paper_signals(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS wallet_paper_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet TEXT NOT NULL,
    market_id TEXT NOT NULL,
    asset_id TEXT,
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    signal_timestamp INTEGER NOT NULL,
    entry_price REAL NOT NULL,
    signal_score REAL NOT NULL,
    confidence TEXT NOT NULL,
    aligned_wallet_count INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'RESOLVED')),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(wallet, market_id, asset_id, side, signal_timestamp)
);

CREATE TABLE IF NOT EXISTS wallet_paper_outcomes (
    signal_id INTEGER PRIMARY KEY,
    wallet TEXT NOT NULL,
    market_id TEXT NOT NULL,
    asset_id TEXT,
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    entry_timestamp INTEGER NOT NULL,
    resolved_timestamp INTEGER NOT NULL,
    sample_count INTEGER NOT NULL DEFAULT 0,
    return_3s REAL,
    return_5s REAL,
    return_10s REAL,
    max_favorable_return REAL,
    max_adverse_return REAL,
    time_to_move INTEGER,
    follow_through_3s INTEGER NOT NULL DEFAULT 0,
    follow_through_5s INTEGER NOT NULL DEFAULT 0,
    follow_through_10s INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(signal_id) REFERENCES wallet_paper_signals(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_trade_events_wallet ON trade_events(wallet);
CREATE INDEX IF NOT EXISTS idx_trade_events_wallet_time ON trade_events(wallet, timestamp);
CREATE INDEX IF NOT EXISTS idx_trade_events_wallet_outcome ON trade_events(wallet, outcome);
CREATE INDEX IF NOT EXISTS idx_trade_events_market ON trade_events(market_id);
CREATE INDEX IF NOT EXISTS idx_trade_events_time ON trade_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_trade_events_wallet_market_outcome_time
    ON trade_events(wallet, market_id, outcome, timestamp);
CREATE INDEX IF NOT EXISTS idx_trade_events_asset_time
    ON trade_events(asset_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_positions_wallet_version
    ON positions_reconstructed(wallet, version);
CREATE INDEX IF NOT EXISTS idx_positions_wallet_status
    ON positions_reconstructed(wallet, status);
CREATE INDEX IF NOT EXISTS idx_positions_wallet_time
    ON positions_reconstructed(wallet, entry_time, exit_time);
CREATE INDEX IF NOT EXISTS idx_quarantined_trades_wallet
    ON quarantined_trades(wallet);
CREATE INDEX IF NOT EXISTS idx_market_price_history_asset_time
    ON market_price_history(asset_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_position_path_metrics_wallet_version
    ON position_path_metrics(wallet, version);
CREATE INDEX IF NOT EXISTS idx_microstructure_raw_book_asset_time
    ON microstructure_raw_book(asset_id, observed_at);
CREATE INDEX IF NOT EXISTS idx_microstructure_book_snapshots_asset_time
    ON microstructure_book_snapshots(asset_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_microstructure_features_asset_time
    ON microstructure_features(asset_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_trade_entry_context_wallet_time
    ON trade_entry_context(wallet, trade_timestamp);
CREATE INDEX IF NOT EXISTS idx_candidate_wallets_status_next_check
    ON candidate_wallets(status, next_check_at);
CREATE INDEX IF NOT EXISTS idx_candidate_wallet_sources_wallet
    ON candidate_wallet_sources(wallet);
CREATE INDEX IF NOT EXISTS idx_candidate_wallet_sources_source_wallet
    ON candidate_wallet_sources(source, wallet);
CREATE INDEX IF NOT EXISTS idx_candidate_wallet_sources_regime_wallet
    ON candidate_wallet_sources(regime, wallet);
CREATE INDEX IF NOT EXISTS idx_wallet_rankings_regime_rank
    ON wallet_rankings(regime, rank_score DESC);
CREATE INDEX IF NOT EXISTS idx_monitor_runs_created_at
    ON monitor_runs(created_at);
CREATE INDEX IF NOT EXISTS idx_paper_signals_asset_status_time
    ON paper_signals(asset_id, status, signal_timestamp);
CREATE INDEX IF NOT EXISTS idx_paper_signal_outcomes_asset_time
    ON paper_signal_outcomes(asset_id, entry_timestamp);
CREATE INDEX IF NOT EXISTS idx_wallet_paper_signals_asset_status_time
    ON wallet_paper_signals(asset_id, status, signal_timestamp);
CREATE INDEX IF NOT EXISTS idx_wallet_paper_outcomes_asset_time
    ON wallet_paper_outcomes(asset_id, entry_timestamp);

-- ---------------------------------------------------------------------------
-- Aggregated microstructure: 1-minute OHLC snapshots (HOT data compressed)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS microstructure_agg_1m (
    asset_id TEXT NOT NULL,
    minute_ts INTEGER NOT NULL,
    avg_bid REAL,
    avg_ask REAL,
    spread_avg REAL,
    spread_std REAL,
    mid_price_avg REAL,
    snapshot_count INTEGER NOT NULL,
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (asset_id, minute_ts)
);

CREATE INDEX IF NOT EXISTS idx_microstructure_agg_1m_asset_time
    ON microstructure_agg_1m(asset_id, minute_ts);

-- ---------------------------------------------------------------------------
-- Aggregated price history: 1-minute OHLC bars (HOT data compressed)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS price_history_agg_1m (
    asset_id TEXT NOT NULL,
    minute_ts INTEGER NOT NULL,
    open_price REAL NOT NULL,
    high_price REAL NOT NULL,
    low_price REAL NOT NULL,
    close_price REAL NOT NULL,
    sample_count INTEGER NOT NULL,
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (asset_id, minute_ts)
);

CREATE INDEX IF NOT EXISTS idx_price_history_agg_1m_asset_time
    ON price_history_agg_1m(asset_id, minute_ts);
