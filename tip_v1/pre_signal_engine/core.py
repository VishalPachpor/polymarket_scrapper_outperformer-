from __future__ import annotations

import csv
from bisect import bisect_left
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from tip_v1.cli.simulate_wallet_entries import (
    NO_PATH,
    STOP_LOSS,
    TAKE_PROFIT,
    TIME_EXIT,
    WalletEntryExitConfig,
)
from tip_v1.config import Settings, get_settings
from tip_v1.db.db import managed_connection


BUY = "BUY"
SELL = "SELL"
FEATURE_COLUMNS = (
    "trade_velocity",
    "volume_burst",
    "directional_pressure",
    "micro_consistency",
    "price_move_3s",
    "pre_move_vs_window",
    "burst_freshness",
    "accel_change",
    "stability",
)
DATASET_COLUMNS = (
    "asset_id",
    "market_id",
    "signal_time",
    "wallet_entry_time",
    "label",
    "target_side",
    "wallet_count",
    "source_trade_count",
    "trade_count",
    "trade_velocity",
    "volume_total",
    "volume_last_2s",
    "volume_last_3s",
    "volume_last_5s",
    "volume_burst",
    "burst_freshness",
    "buy_volume",
    "sell_volume",
    "directional_pressure",
    "price_start",
    "price_end",
    "price_acceleration",
    "price_move_3s",
    "pre_move_vs_window",
    "accel_prev",
    "accel_change",
    "micro_consistency",
    "direction_switches",
    "stability",
    "last_side",
    "window_seconds",
)


@dataclass(frozen=True)
class PreSignalReference:
    asset_id: str
    market_id: str
    signal_time: int
    wallet_entry_time: int
    label: int
    target_side: str
    wallet_count: int
    source_trade_count: int


@dataclass(frozen=True)
class PreSignalDatasetRow:
    asset_id: str
    market_id: str
    signal_time: int
    label: int
    target_side: str
    wallet_count: int
    source_trade_count: int
    trade_count: int
    trade_velocity: float
    volume_total: float
    volume_last_2s: float
    volume_last_3s: float
    volume_last_5s: float
    volume_burst: float
    burst_freshness: float
    buy_volume: float
    sell_volume: float
    directional_pressure: float
    price_start: float
    price_end: float
    price_acceleration: float
    price_move_3s: float
    pre_move_vs_window: float
    accel_prev: float
    accel_change: float
    micro_consistency: float
    direction_switches: int
    stability: float
    last_side: str
    window_seconds: int
    wallet_entry_time: int = 0


@dataclass(frozen=True)
class PreSignalDatasetBuildResult:
    target_wallets: tuple[str, ...]
    positive_examples: int
    negative_examples: int
    rows: tuple[PreSignalDatasetRow, ...]


@dataclass(frozen=True)
class PreSignalSimulationTrade:
    asset_id: str
    market_id: str
    signal_time: int
    wallet_entry_time: int
    label: int
    target_side: str
    predicted_side: str
    score: float
    entry_time: int
    entry_price: float
    exit_time: int
    exit_price: float
    exit_type: str
    simulated_return: float
    hold_seconds: int


@dataclass(frozen=True)
class PreSignalSimulationResult:
    dataset_rows: int
    emitted_signals: int
    emitted_positive: int
    emitted_negative: int
    precision: float
    directional_accuracy: float
    avg_score_emitted: float
    win_rate: float
    avg_return: float
    total_return: float
    avg_hold_seconds: float
    take_profit_rate: float
    stop_loss_rate: float
    time_exit_rate: float
    no_path_rate: float
    threshold: float
    max_threshold: float | None
    entry_delay_seconds: int
    avg_lead_time_seconds: float
    median_lead_time_seconds: float
    p25_lead_time_seconds: float
    p75_lead_time_seconds: float
    trades: tuple[PreSignalSimulationTrade, ...]


@dataclass(frozen=True)
class ScoreDistributionSummary:
    count: int
    min_score: float
    p25_score: float
    median_score: float
    p75_score: float
    max_score: float
    mean_score: float


@dataclass(frozen=True)
class PreSignalThresholdSweepRow:
    threshold: float
    signals_emitted: int
    precision: float
    directional_accuracy: float
    avg_return: float
    total_return: float
    no_path_rate: float


@dataclass(frozen=True)
class _FeatureScales:
    mins: dict[str, float]
    maxs: dict[str, float]


def _first_price_at_or_after(samples: list[tuple[int, float]], *, target_ts: int) -> tuple[int, float] | None:
    for timestamp, price in samples:
        if timestamp >= target_ts:
            return (timestamp, price)
    return None


def _stable_unique_wallets(wallets: list[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for wallet in wallets:
        if wallet in seen:
            continue
        seen.add(wallet)
        ordered.append(wallet)
    return tuple(ordered)


def load_target_wallets(
    connection,
    *,
    wallets: list[str] | tuple[str, ...] | None = None,
) -> tuple[str, ...]:
    if wallets:
        return _stable_unique_wallets([str(wallet) for wallet in wallets])

    ranking_rows = connection.execute(
        """
        SELECT wallet
        FROM wallet_rankings
        ORDER BY rank_score DESC, wallet
        """
    ).fetchall()
    if ranking_rows:
        return tuple(str(row["wallet"]) for row in ranking_rows)

    wallet_rows = connection.execute(
        """
        SELECT DISTINCT wallet
        FROM trade_events
        WHERE asset_id IS NOT NULL
        ORDER BY wallet
        """
    ).fetchall()
    return tuple(str(row["wallet"]) for row in wallet_rows)


def _load_positive_references(
    connection,
    *,
    target_wallets: tuple[str, ...],
    limit: int,
) -> list[PreSignalReference]:
    if not target_wallets:
        return []

    placeholders = ",".join("?" for _ in target_wallets)
    rows = connection.execute(
        f"""
        SELECT
            asset_id,
            MIN(market_id) AS market_id,
            timestamp AS signal_time,
            side AS target_side,
            COUNT(*) AS source_trade_count,
            COUNT(DISTINCT wallet) AS wallet_count
        FROM trade_events
        WHERE asset_id IS NOT NULL
          AND wallet IN ({placeholders})
        GROUP BY asset_id, timestamp, side
        ORDER BY signal_time, asset_id, target_side
        LIMIT ?
        """,
        (*target_wallets, limit),
    ).fetchall()
    return [
        PreSignalReference(
            asset_id=str(row["asset_id"]),
            market_id=str(row["market_id"]),
            signal_time=int(row["signal_time"]),
            wallet_entry_time=int(row["signal_time"]),
            label=1,
            target_side=str(row["target_side"]),
            wallet_count=int(row["wallet_count"] or 0),
            source_trade_count=int(row["source_trade_count"] or 0),
        )
        for row in rows
    ]


def _load_negative_references(
    connection,
    *,
    positive_references: list[PreSignalReference],
    prediction_horizon_seconds: int,
    negative_limit: int,
) -> list[PreSignalReference]:
    if not positive_references or negative_limit <= 0:
        return []

    positive_assets = sorted({reference.asset_id for reference in positive_references})
    positive_times_by_asset: dict[str, list[int]] = defaultdict(list)
    for reference in positive_references:
        positive_times_by_asset[reference.asset_id].append(reference.signal_time)
    for signal_times in positive_times_by_asset.values():
        signal_times.sort()

    placeholders = ",".join("?" for _ in positive_assets)
    candidate_rows = connection.execute(
        f"""
        SELECT
            asset_id,
            MIN(market_id) AS market_id,
            timestamp AS signal_time
        FROM trade_events
        WHERE asset_id IN ({placeholders})
        GROUP BY asset_id, timestamp
        ORDER BY signal_time, asset_id
        """,
        tuple(positive_assets),
    ).fetchall()

    negatives: list[PreSignalReference] = []
    for row in candidate_rows:
        asset_id = str(row["asset_id"])
        signal_time = int(row["signal_time"])
        positive_times = positive_times_by_asset.get(asset_id, [])
        index = bisect_left(positive_times, signal_time)
        if index < len(positive_times) and positive_times[index] <= signal_time + prediction_horizon_seconds:
            continue

        negatives.append(
            PreSignalReference(
                asset_id=asset_id,
                market_id=str(row["market_id"]),
                signal_time=signal_time,
                wallet_entry_time=0,
                label=0,
                target_side="",
                wallet_count=0,
                source_trade_count=0,
            )
        )
        if len(negatives) >= negative_limit:
            break

    return negatives


def _load_trade_windows(
    connection,
    *,
    asset_ids: list[str],
    start_time: int,
    end_time: int,
) -> tuple[dict[str, list[tuple[int, str, float, float]]], dict[str, list[int]]]:
    if not asset_ids:
        return {}, {}

    placeholders = ",".join("?" for _ in asset_ids)
    rows = connection.execute(
        f"""
        SELECT asset_id, timestamp, side, CAST(price AS REAL) AS price, CAST(size AS REAL) AS size
        FROM trade_events
        WHERE asset_id IN ({placeholders})
          AND timestamp BETWEEN ? AND ?
        ORDER BY asset_id, timestamp, id
        """,
        (*asset_ids, start_time, end_time),
    ).fetchall()

    events_by_asset: dict[str, list[tuple[int, str, float, float]]] = defaultdict(list)
    timestamps_by_asset: dict[str, list[int]] = defaultdict(list)
    for row in rows:
        asset_id = str(row["asset_id"])
        timestamp = int(row["timestamp"])
        side = str(row["side"])
        price = float(row["price"])
        size = float(row["size"])
        events_by_asset[asset_id].append((timestamp, side, price, size))
        timestamps_by_asset[asset_id].append(timestamp)
    return dict(events_by_asset), dict(timestamps_by_asset)


def _longest_same_side_run(events: list[tuple[int, str, float, float]]) -> int:
    longest = 0
    current = 0
    previous_side = ""
    for _, side, _, _ in events:
        if side == previous_side:
            current += 1
        else:
            current = 1
            previous_side = side
        if current > longest:
            longest = current
    return longest


def _direction_switch_count(events: list[tuple[int, str, float, float]]) -> int:
    switches = 0
    previous_side = ""
    for _, side, _, _ in events:
        if previous_side and side != previous_side:
            switches += 1
        previous_side = side
    return switches


def _build_dataset_row(
    reference: PreSignalReference,
    *,
    window_seconds: int,
    asset_events: list[tuple[int, str, float, float]],
    asset_timestamps: list[int],
) -> PreSignalDatasetRow:
    window_start = reference.signal_time - window_seconds
    left = bisect_left(asset_timestamps, window_start)
    right = bisect_left(asset_timestamps, reference.signal_time)
    window_events = asset_events[left:right]

    trade_count = len(window_events)
    volume_total = sum(size for _, _, _, size in window_events)
    buy_volume = sum(size for _, side, _, size in window_events if side == BUY)
    sell_volume = sum(size for _, side, _, size in window_events if side == SELL)

    recent_start = reference.signal_time - min(3, window_seconds)
    recent_left = bisect_left(asset_timestamps, recent_start, left, right)
    recent_events = asset_events[recent_left:right]
    recent2_start = reference.signal_time - min(2, window_seconds)
    recent2_left = bisect_left(asset_timestamps, recent2_start, left, right)
    recent2_events = asset_events[recent2_left:right]
    recent5_start = reference.signal_time - min(5, window_seconds)
    recent5_left = bisect_left(asset_timestamps, recent5_start, left, right)
    recent5_events = asset_events[recent5_left:right]
    prev6_start = reference.signal_time - min(6, window_seconds)
    prev6_left = bisect_left(asset_timestamps, prev6_start, left, right)
    prev_events = asset_events[prev6_left:recent_left]

    volume_last_2s = sum(size for _, _, _, size in recent2_events)
    volume_last_3s = sum(size for _, _, _, size in recent_events)
    volume_last_5s = sum(size for _, _, _, size in recent5_events)

    price_start = float(window_events[0][2]) if window_events else 0.0
    price_end = float(window_events[-1][2]) if window_events else 0.0
    recent_price_start = float(recent_events[0][2]) if recent_events else price_end
    recent_price_end = float(recent_events[-1][2]) if recent_events else price_end
    prev_price_start = float(prev_events[0][2]) if prev_events else recent_price_start
    prev_price_end = float(prev_events[-1][2]) if prev_events else recent_price_start
    last_side = str(window_events[-1][1]) if window_events else ""

    directional_pressure = (buy_volume - sell_volume) / volume_total if volume_total > 0 else 0.0
    price_acceleration = price_end - price_start if window_events else 0.0
    price_move_3s = recent_price_end - recent_price_start if recent_events else 0.0
    accel_prev = prev_price_end - prev_price_start if prev_events else 0.0
    accel_change = price_move_3s - accel_prev
    total_move_abs = abs(price_acceleration)
    recent_move_abs = abs(price_move_3s)
    if total_move_abs <= 1e-12:
        pre_move_vs_window = 1.0 if recent_move_abs > 0 else 0.0
    else:
        pre_move_vs_window = max(0.0, min(1.0, recent_move_abs / total_move_abs))
    recent_burst = (volume_last_2s / volume_total) if volume_total > 0 else 0.0
    mid_burst = (volume_last_5s / volume_total) if volume_total > 0 else 0.0
    burst_freshness = recent_burst - mid_burst
    direction_switches = _direction_switch_count(window_events)
    max_switches = max(1, trade_count - 1)
    stability = max(0.0, min(1.0, 1.0 - (direction_switches / max_switches)))

    return PreSignalDatasetRow(
        asset_id=reference.asset_id,
        market_id=reference.market_id,
        signal_time=reference.signal_time,
        wallet_entry_time=reference.wallet_entry_time,
        label=reference.label,
        target_side=reference.target_side,
        wallet_count=reference.wallet_count,
        source_trade_count=reference.source_trade_count,
        trade_count=trade_count,
        trade_velocity=(trade_count / window_seconds) if window_seconds > 0 else 0.0,
        volume_total=volume_total,
        volume_last_2s=volume_last_2s,
        volume_last_3s=volume_last_3s,
        volume_last_5s=volume_last_5s,
        volume_burst=(volume_last_3s / volume_total) if volume_total > 0 else 0.0,
        burst_freshness=burst_freshness,
        buy_volume=buy_volume,
        sell_volume=sell_volume,
        directional_pressure=directional_pressure,
        price_start=price_start,
        price_end=price_end,
        price_acceleration=price_acceleration,
        price_move_3s=price_move_3s,
        pre_move_vs_window=pre_move_vs_window,
        accel_prev=accel_prev,
        accel_change=accel_change,
        micro_consistency=float(_longest_same_side_run(window_events)),
        direction_switches=direction_switches,
        stability=stability,
        last_side=last_side,
        window_seconds=window_seconds,
    )


def build_pre_signal_dataset(
    *,
    settings: Settings | None = None,
    wallets: list[str] | tuple[str, ...] | None = None,
    window_seconds: int = 10,
    prediction_horizon_seconds: int = 10,
    limit: int = 10_000,
    negative_ratio: float = 1.0,
) -> PreSignalDatasetBuildResult:
    settings = settings or get_settings()

    with managed_connection(settings) as connection:
        target_wallets = load_target_wallets(connection, wallets=wallets)
        positive_references = _load_positive_references(
            connection,
            target_wallets=target_wallets,
            limit=limit,
        )
        negative_limit = max(0, int(round(len(positive_references) * negative_ratio)))
        negative_references = _load_negative_references(
            connection,
            positive_references=positive_references,
            prediction_horizon_seconds=prediction_horizon_seconds,
            negative_limit=negative_limit,
        )
        references = positive_references + negative_references

        if not references:
            return PreSignalDatasetBuildResult(
                target_wallets=target_wallets,
                positive_examples=0,
                negative_examples=0,
                rows=(),
            )

        asset_ids = sorted({reference.asset_id for reference in references})
        min_time = min(reference.signal_time for reference in references) - window_seconds
        max_time = max(reference.signal_time for reference in references) - 1
        asset_events, asset_timestamps = _load_trade_windows(
            connection,
            asset_ids=asset_ids,
            start_time=min_time,
            end_time=max_time,
        )

    rows = [
        _build_dataset_row(
            reference,
            window_seconds=window_seconds,
            asset_events=asset_events.get(reference.asset_id, []),
            asset_timestamps=asset_timestamps.get(reference.asset_id, []),
        )
        for reference in references
    ]
    rows.sort(key=lambda row: (row.signal_time, row.asset_id, row.label))

    return PreSignalDatasetBuildResult(
        target_wallets=target_wallets,
        positive_examples=len(positive_references),
        negative_examples=len(negative_references),
        rows=tuple(rows),
    )


def write_pre_signal_dataset_csv(
    rows: list[PreSignalDatasetRow] | tuple[PreSignalDatasetRow, ...],
    *,
    output_path: str | Path,
) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=DATASET_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "asset_id": row.asset_id,
                    "market_id": row.market_id,
                    "signal_time": row.signal_time,
                    "wallet_entry_time": row.wallet_entry_time,
                    "label": row.label,
                    "target_side": row.target_side,
                    "wallet_count": row.wallet_count,
                    "source_trade_count": row.source_trade_count,
                    "trade_count": row.trade_count,
                    "trade_velocity": row.trade_velocity,
                    "volume_total": row.volume_total,
                    "volume_last_2s": row.volume_last_2s,
                    "volume_last_3s": row.volume_last_3s,
                    "volume_last_5s": row.volume_last_5s,
                    "volume_burst": row.volume_burst,
                    "burst_freshness": row.burst_freshness,
                    "buy_volume": row.buy_volume,
                    "sell_volume": row.sell_volume,
                    "directional_pressure": row.directional_pressure,
                    "price_start": row.price_start,
                    "price_end": row.price_end,
                    "price_acceleration": row.price_acceleration,
                    "price_move_3s": row.price_move_3s,
                    "pre_move_vs_window": row.pre_move_vs_window,
                    "accel_prev": row.accel_prev,
                    "accel_change": row.accel_change,
                    "micro_consistency": row.micro_consistency,
                    "direction_switches": row.direction_switches,
                    "stability": row.stability,
                    "last_side": row.last_side,
                    "window_seconds": row.window_seconds,
                }
            )
    return path


def load_pre_signal_dataset_csv(path: str | Path) -> list[PreSignalDatasetRow]:
    rows: list[PreSignalDatasetRow] = []
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append(
                PreSignalDatasetRow(
                    asset_id=str(row["asset_id"]),
                    market_id=str(row["market_id"]),
                    signal_time=int(row["signal_time"]),
                    wallet_entry_time=int(row.get("wallet_entry_time", row["signal_time"])),
                    label=int(row["label"]),
                    target_side=str(row["target_side"]),
                    wallet_count=int(row["wallet_count"]),
                    source_trade_count=int(row["source_trade_count"]),
                    trade_count=int(row["trade_count"]),
                    trade_velocity=float(row["trade_velocity"]),
                    volume_total=float(row["volume_total"]),
                    volume_last_2s=float(row.get("volume_last_2s", row["volume_last_3s"])),
                    volume_last_3s=float(row["volume_last_3s"]),
                    volume_last_5s=float(row.get("volume_last_5s", row["volume_last_3s"])),
                    volume_burst=float(row["volume_burst"]),
                    burst_freshness=float(row.get("burst_freshness", 0.0)),
                    buy_volume=float(row["buy_volume"]),
                    sell_volume=float(row["sell_volume"]),
                    directional_pressure=float(row["directional_pressure"]),
                    price_start=float(row["price_start"]),
                    price_end=float(row["price_end"]),
                    price_acceleration=float(row["price_acceleration"]),
                    price_move_3s=float(row.get("price_move_3s", row["price_acceleration"])),
                    pre_move_vs_window=float(row.get("pre_move_vs_window", 1.0)),
                    accel_prev=float(row.get("accel_prev", 0.0)),
                    accel_change=float(row.get("accel_change", 0.0)),
                    micro_consistency=float(row["micro_consistency"]),
                    direction_switches=int(row.get("direction_switches", 0)),
                    stability=float(row.get("stability", 1.0)),
                    last_side=str(row["last_side"]),
                    window_seconds=int(row["window_seconds"]),
                )
            )
    return rows


def build_pre_signal_dataset_report(
    result: PreSignalDatasetBuildResult,
    *,
    output_path: str | Path,
    window_seconds: int,
    prediction_horizon_seconds: int,
) -> str:
    return "\n".join(
        [
            "Pre-Signal Dataset Build",
            f"Target Wallets: {len(result.target_wallets)}",
            f"Positive Examples: {result.positive_examples}",
            f"Negative Examples: {result.negative_examples}",
            f"Total Rows: {len(result.rows)}",
            f"Window Seconds: {window_seconds}",
            f"Prediction Horizon: {prediction_horizon_seconds}s",
            f"Output: {Path(output_path)}",
        ]
    )


def _feature_strengths(row: PreSignalDatasetRow) -> dict[str, float]:
    return {
        "trade_velocity": row.trade_velocity,
        "volume_burst": row.volume_burst,
        "directional_pressure": abs(row.directional_pressure),
        "micro_consistency": row.micro_consistency,
        "price_move_3s": abs(row.price_move_3s),
        "pre_move_vs_window": row.pre_move_vs_window,
        "burst_freshness": max(0.0, row.burst_freshness),
        "accel_change": max(0.0, row.accel_change),
        "stability": row.stability,
    }


def _build_feature_scales(rows: list[PreSignalDatasetRow] | tuple[PreSignalDatasetRow, ...]) -> _FeatureScales:
    mins: dict[str, float] = {}
    maxs: dict[str, float] = {}
    for feature in FEATURE_COLUMNS:
        values = [_feature_strengths(row)[feature] for row in rows]
        mins[feature] = min(values) if values else 0.0
        maxs[feature] = max(values) if values else 0.0
    return _FeatureScales(mins=mins, maxs=maxs)


def _normalize_feature(value: float, *, min_value: float, max_value: float) -> float:
    if max_value <= min_value:
        return 0.0
    return (value - min_value) / (max_value - min_value)


def score_pre_signal_row(
    row: PreSignalDatasetRow,
    *,
    scales: _FeatureScales,
) -> float:
    strengths = _feature_strengths(row)
    normalized = {
        feature: _normalize_feature(
            strengths[feature],
            min_value=scales.mins[feature],
            max_value=scales.maxs[feature],
        )
        for feature in FEATURE_COLUMNS
    }

    quality_score = (
        2.0 * normalized["directional_pressure"]
        + 1.5 * normalized["volume_burst"]
        + 1.2 * normalized["trade_velocity"]
        + 0.8 * normalized["micro_consistency"]
    )
    quality_score = max(0.0, min(1.0, quality_score / (2.0 + 1.5 + 1.2 + 0.8)))
    freshness_score = (
        1.2 * normalized["burst_freshness"]
        + 1.0 * normalized["accel_change"]
        + 0.8 * normalized["stability"]
    )
    freshness_score = max(0.0, min(1.0, freshness_score / (1.2 + 1.0 + 0.8)))
    earliness_score = max(0.0, min(1.0, 1.0 - normalized["pre_move_vs_window"]))
    raw_score = (0.5 * quality_score) + (0.3 * freshness_score) + (0.2 * earliness_score)
    late_penalty = min(abs(row.price_move_3s) / 0.03, 1.0)
    final_score = raw_score * (1.0 - (0.4 * late_penalty))
    return max(0.0, min(1.0, final_score))


def summarize_pre_signal_scores(
    rows: list[PreSignalDatasetRow] | tuple[PreSignalDatasetRow, ...],
) -> dict[str, ScoreDistributionSummary]:
    scales = _build_feature_scales(rows)
    scored = [(row.label, score_pre_signal_row(row, scales=scales)) for row in rows]
    labels = {
        "all": [score for _, score in scored],
        "positive": [score for label, score in scored if label == 1],
        "negative": [score for label, score in scored if label == 0],
    }
    return {
        label: _summarize_scores(values)
        for label, values in labels.items()
    }


def _summarize_scores(values: list[float]) -> ScoreDistributionSummary:
    if not values:
        return ScoreDistributionSummary(
            count=0,
            min_score=0.0,
            p25_score=0.0,
            median_score=0.0,
            p75_score=0.0,
            max_score=0.0,
            mean_score=0.0,
        )
    ordered = sorted(values)
    return ScoreDistributionSummary(
        count=len(ordered),
        min_score=ordered[0],
        p25_score=_quantile(ordered, 0.25),
        median_score=_quantile(ordered, 0.50),
        p75_score=_quantile(ordered, 0.75),
        max_score=ordered[-1],
        mean_score=sum(ordered) / len(ordered),
    )


def _quantile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    position = (len(values) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    weight = position - lower
    return values[lower] * (1.0 - weight) + values[upper] * weight


def predict_pre_signal_side(row: PreSignalDatasetRow) -> str:
    if row.directional_pressure > 0:
        return BUY
    if row.directional_pressure < 0:
        return SELL
    if row.price_acceleration > 0:
        return BUY
    if row.price_acceleration < 0:
        return SELL
    if row.last_side in (BUY, SELL):
        return row.last_side
    return BUY


def _load_price_paths(
    connection,
    *,
    asset_ids: list[str],
    start_time: int,
    end_time: int,
) -> tuple[dict[str, list[tuple[int, float]]], dict[str, list[int]]]:
    if not asset_ids:
        return {}, {}

    placeholders = ",".join("?" for _ in asset_ids)
    rows = connection.execute(
        f"""
        SELECT asset_id, timestamp, price
        FROM market_price_history
        WHERE asset_id IN ({placeholders})
          AND timestamp BETWEEN ? AND ?
        ORDER BY asset_id, timestamp, id
        """,
        (*asset_ids, start_time, end_time),
    ).fetchall()

    paths_by_asset: dict[str, list[tuple[int, float]]] = defaultdict(list)
    timestamps_by_asset: dict[str, list[int]] = defaultdict(list)
    for row in rows:
        asset_id = str(row["asset_id"])
        timestamp = int(row["timestamp"])
        price = float(row["price"])
        paths_by_asset[asset_id].append((timestamp, price))
        timestamps_by_asset[asset_id].append(timestamp)
    return dict(paths_by_asset), dict(timestamps_by_asset)


def _slice_path(
    samples: list[tuple[int, float]],
    timestamps: list[int],
    *,
    start_time: int,
    end_time: int,
) -> list[tuple[int, float]]:
    left = bisect_left(timestamps, start_time)
    right = bisect_left(timestamps, end_time + 1)
    return samples[left:right]


def _simulate_signal_path(
    row: PreSignalDatasetRow,
    *,
    predicted_side: str,
    score: float,
    config: WalletEntryExitConfig,
    asset_path: list[tuple[int, float]],
) -> PreSignalTradeSimulation:
    delayed_signal_time = row.signal_time + config.entry_delay_seconds
    entry = _first_price_at_or_after(asset_path, target_ts=delayed_signal_time)
    if entry is None:
        return PreSignalSimulationTrade(
            asset_id=row.asset_id,
            market_id=row.market_id,
            signal_time=row.signal_time,
            wallet_entry_time=row.wallet_entry_time,
            label=row.label,
            target_side=row.target_side,
            predicted_side=predicted_side,
            score=score,
            entry_time=delayed_signal_time,
            entry_price=0.0,
            exit_time=delayed_signal_time,
            exit_price=0.0,
            exit_type=NO_PATH,
            simulated_return=0.0,
            hold_seconds=0,
        )

    entry_time, entry_price = entry
    if predicted_side == BUY:
        tp_price = entry_price * (1.0 + config.take_profit)
        sl_price = entry_price * (1.0 - config.stop_loss)
    else:
        tp_price = entry_price * (1.0 - config.take_profit)
        sl_price = entry_price * (1.0 + config.stop_loss)
    exit_deadline = entry_time + config.max_hold_seconds

    exit_type = TIME_EXIT
    exit_time = entry_time
    exit_price = entry_price

    for timestamp, price in asset_path:
        if timestamp < entry_time:
            continue
        if predicted_side == BUY:
            if price >= tp_price:
                exit_type = TAKE_PROFIT
                exit_time = timestamp
                exit_price = price
                break
            if price <= sl_price:
                exit_type = STOP_LOSS
                exit_time = timestamp
                exit_price = price
                break
        else:
            if price <= tp_price:
                exit_type = TAKE_PROFIT
                exit_time = timestamp
                exit_price = price
                break
            if price >= sl_price:
                exit_type = STOP_LOSS
                exit_time = timestamp
                exit_price = price
                break
        if timestamp >= exit_deadline:
            exit_time = timestamp
            exit_price = price
            exit_type = TIME_EXIT
            break
        exit_time = timestamp
        exit_price = price

    if predicted_side == BUY:
        simulated_return = ((exit_price - entry_price) / entry_price) if entry_price > 0 else 0.0
    else:
        simulated_return = ((entry_price - exit_price) / entry_price) if entry_price > 0 else 0.0

    return PreSignalSimulationTrade(
        asset_id=row.asset_id,
        market_id=row.market_id,
        signal_time=row.signal_time,
        wallet_entry_time=row.wallet_entry_time,
        label=row.label,
        target_side=row.target_side,
        predicted_side=predicted_side,
        score=score,
        entry_time=entry_time,
        entry_price=entry_price,
        exit_time=exit_time,
        exit_price=exit_price,
        exit_type=exit_type,
        simulated_return=simulated_return,
        hold_seconds=max(0, exit_time - entry_time),
    )


def simulate_pre_signals(
    *,
    dataset_path: str | Path,
    settings: Settings | None = None,
    threshold: float = 0.70,
    max_threshold: float | None = None,
    config: WalletEntryExitConfig | None = None,
) -> PreSignalSimulationResult:
    settings = settings or get_settings()
    config = config or WalletEntryExitConfig()
    rows = load_pre_signal_dataset_csv(dataset_path)
    scales = _build_feature_scales(rows)

    scored_rows: list[tuple[PreSignalDatasetRow, float, str]] = []
    for row in rows:
        score = score_pre_signal_row(row, scales=scales)
        if score < threshold:
            continue
        if max_threshold is not None and score > max_threshold:
            continue
        scored_rows.append((row, score, predict_pre_signal_side(row)))

    if not scored_rows:
        return PreSignalSimulationResult(
            dataset_rows=len(rows),
            emitted_signals=0,
            emitted_positive=0,
            emitted_negative=0,
            precision=0.0,
            directional_accuracy=0.0,
            avg_score_emitted=0.0,
            win_rate=0.0,
            avg_return=0.0,
            total_return=0.0,
            avg_hold_seconds=0.0,
            take_profit_rate=0.0,
            stop_loss_rate=0.0,
            time_exit_rate=0.0,
            no_path_rate=0.0,
            threshold=threshold,
            max_threshold=max_threshold,
            entry_delay_seconds=config.entry_delay_seconds,
            avg_lead_time_seconds=0.0,
            median_lead_time_seconds=0.0,
            p25_lead_time_seconds=0.0,
            p75_lead_time_seconds=0.0,
            trades=(),
        )

    min_time = min(row.signal_time for row, _, _ in scored_rows)
    max_time = max(row.signal_time + config.entry_delay_seconds + config.max_hold_seconds for row, _, _ in scored_rows)
    asset_ids = sorted({row.asset_id for row, _, _ in scored_rows})

    with managed_connection(settings) as connection:
        price_paths, path_timestamps = _load_price_paths(
            connection,
            asset_ids=asset_ids,
            start_time=min_time,
            end_time=max_time,
        )

    trades: list[PreSignalTradeSimulation] = []
    for row, score, predicted_side in scored_rows:
        asset_samples = price_paths.get(row.asset_id, [])
        asset_time_index = path_timestamps.get(row.asset_id, [])
        signal_start = row.signal_time
        signal_end = row.signal_time + config.entry_delay_seconds + config.max_hold_seconds
        signal_path = _slice_path(
            asset_samples,
            asset_time_index,
            start_time=signal_start,
            end_time=signal_end,
        )
        trades.append(
            _simulate_signal_path(
                row,
                predicted_side=predicted_side,
                score=score,
                config=config,
                asset_path=signal_path,
            )
        )

    total_return = sum(trade.simulated_return for trade in trades)
    emitted_signals = len(trades)
    emitted_positive = sum(1 for trade in trades if trade.label == 1)
    emitted_negative = emitted_signals - emitted_positive
    wins = sum(1 for trade in trades if trade.simulated_return > 0)
    positive_direction_hits = sum(
        1
        for trade in trades
        if trade.label == 1 and trade.target_side in (BUY, SELL) and trade.predicted_side == trade.target_side
    )
    exit_counts = {
        TAKE_PROFIT: sum(1 for trade in trades if trade.exit_type == TAKE_PROFIT),
        STOP_LOSS: sum(1 for trade in trades if trade.exit_type == STOP_LOSS),
        TIME_EXIT: sum(1 for trade in trades if trade.exit_type == TIME_EXIT),
        NO_PATH: sum(1 for trade in trades if trade.exit_type == NO_PATH),
    }
    lead_times = sorted(
        trade.wallet_entry_time - trade.signal_time
        for trade in trades
        if trade.label == 1 and trade.wallet_entry_time > 0
    )

    return PreSignalSimulationResult(
        dataset_rows=len(rows),
        emitted_signals=emitted_signals,
        emitted_positive=emitted_positive,
        emitted_negative=emitted_negative,
        precision=(emitted_positive / emitted_signals) if emitted_signals else 0.0,
        directional_accuracy=(positive_direction_hits / emitted_positive) if emitted_positive else 0.0,
        avg_score_emitted=(sum(trade.score for trade in trades) / emitted_signals) if emitted_signals else 0.0,
        win_rate=(wins / emitted_signals) if emitted_signals else 0.0,
        avg_return=(total_return / emitted_signals) if emitted_signals else 0.0,
        total_return=total_return,
        avg_hold_seconds=(sum(trade.hold_seconds for trade in trades) / emitted_signals) if emitted_signals else 0.0,
        take_profit_rate=(exit_counts[TAKE_PROFIT] / emitted_signals) if emitted_signals else 0.0,
        stop_loss_rate=(exit_counts[STOP_LOSS] / emitted_signals) if emitted_signals else 0.0,
        time_exit_rate=(exit_counts[TIME_EXIT] / emitted_signals) if emitted_signals else 0.0,
        no_path_rate=(exit_counts[NO_PATH] / emitted_signals) if emitted_signals else 0.0,
        threshold=threshold,
        max_threshold=max_threshold,
        entry_delay_seconds=config.entry_delay_seconds,
        avg_lead_time_seconds=(sum(lead_times) / len(lead_times)) if lead_times else 0.0,
        median_lead_time_seconds=_quantile(lead_times, 0.50) if lead_times else 0.0,
        p25_lead_time_seconds=_quantile(lead_times, 0.25) if lead_times else 0.0,
        p75_lead_time_seconds=_quantile(lead_times, 0.75) if lead_times else 0.0,
        trades=tuple(trades),
    )


def build_pre_signal_simulation_report(result: PreSignalSimulationResult) -> str:
    threshold_label = (
        f"{result.threshold:.2f} to {result.max_threshold:.2f}"
        if result.max_threshold is not None
        else f"{result.threshold:.2f}"
    )
    return "\n".join(
        [
            "Pre-Signal Simulation",
            f"Dataset Rows: {result.dataset_rows}",
            f"Signals Emitted: {result.emitted_signals}",
            f"Threshold: {threshold_label}",
            f"Entry Delay: {result.entry_delay_seconds}s",
            f"Precision: {result.precision:.2%}",
            f"Directional Accuracy: {result.directional_accuracy:.2%}",
            f"Avg Score Emitted: {result.avg_score_emitted:.4f}",
            f"Avg Lead Time To Wallet: {result.avg_lead_time_seconds:.1f}s",
            f"Median Lead Time To Wallet: {result.median_lead_time_seconds:.1f}s",
            f"Lead Time IQR: {result.p25_lead_time_seconds:.1f}s to {result.p75_lead_time_seconds:.1f}s",
            f"Win Rate: {result.win_rate:.2%}",
            f"Avg Return: {result.avg_return:+.4f}",
            f"Total Return: {result.total_return:+.4f}",
            f"Avg Time In Trade: {result.avg_hold_seconds:.1f}s",
            "",
            "Label Distribution:",
            f"- positive: {result.emitted_positive}",
            f"- negative: {result.emitted_negative}",
            "",
            "Exit Distribution:",
            f"- take_profit: {result.take_profit_rate:.2%}",
            f"- stop_loss: {result.stop_loss_rate:.2%}",
            f"- time_exit: {result.time_exit_rate:.2%}",
            f"- no_path: {result.no_path_rate:.2%}",
        ]
    )


def sweep_pre_signal_thresholds(
    *,
    dataset_path: str | Path,
    thresholds: list[float] | tuple[float, ...],
    settings: Settings | None = None,
    config: WalletEntryExitConfig | None = None,
) -> list[PreSignalThresholdSweepRow]:
    return [
        PreSignalThresholdSweepRow(
            threshold=threshold,
            signals_emitted=result.emitted_signals,
            precision=result.precision,
            directional_accuracy=result.directional_accuracy,
            avg_return=result.avg_return,
            total_return=result.total_return,
            no_path_rate=result.no_path_rate,
        )
        for threshold in thresholds
        for result in [
            simulate_pre_signals(
                dataset_path=dataset_path,
                settings=settings,
                threshold=threshold,
                max_threshold=None,
                config=config,
            )
        ]
    ]


def build_pre_signal_threshold_sweep_report(
    *,
    dataset_path: str | Path,
    rows: list[PreSignalDatasetRow] | tuple[PreSignalDatasetRow, ...],
    sweep_rows: list[PreSignalThresholdSweepRow] | tuple[PreSignalThresholdSweepRow, ...],
) -> str:
    summaries = summarize_pre_signal_scores(rows)
    lines = [
        "Pre-Signal Threshold Sweep",
        f"Dataset: {Path(dataset_path)}",
        "",
        "Score Distribution:",
    ]
    for label in ("all", "positive", "negative"):
        summary = summaries[label]
        lines.append(
            f"- {label}: n={summary.count} min={summary.min_score:.4f} p25={summary.p25_score:.4f} "
            f"median={summary.median_score:.4f} p75={summary.p75_score:.4f} max={summary.max_score:.4f} "
            f"mean={summary.mean_score:.4f}"
        )
    lines.extend(
        [
            "",
            "Threshold Results:",
        ]
    )
    for row in sweep_rows:
        lines.append(
            f"- threshold={row.threshold:.2f} | signals={row.signals_emitted} | precision={row.precision:.2%} | "
            f"directional_accuracy={row.directional_accuracy:.2%} | avg_return={row.avg_return:+.4f} | "
            f"total_return={row.total_return:+.4f} | no_path={row.no_path_rate:.2%}"
        )
    return "\n".join(lines)
