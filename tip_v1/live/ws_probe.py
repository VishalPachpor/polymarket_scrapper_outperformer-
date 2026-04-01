from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FeedProbeConfig:
    ws_url: str
    subscribe_message: dict[str, Any] | None = None
    max_samples: int = 200
    warmup_samples: int = 20


@dataclass
class FeedProbeState:
    max_samples: int = 200
    last_recv: float | None = None
    last_hash: str | None = None
    total_count: int = 0
    duplicate_count: int = 0
    latencies_ms: deque[float] = field(default_factory=lambda: deque(maxlen=200))
    deltas_sec: deque[float] = field(default_factory=lambda: deque(maxlen=200))
    batch_sizes: deque[int] = field(default_factory=lambda: deque(maxlen=200))

    def __post_init__(self) -> None:
        self.latencies_ms = deque(maxlen=self.max_samples)
        self.deltas_sec = deque(maxlen=self.max_samples)
        self.batch_sizes = deque(maxlen=self.max_samples)


def extract_trades(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, dict):
        if isinstance(data.get("trades"), list):
            return [row for row in data["trades"] if isinstance(row, dict)]
        if isinstance(data.get("data"), list):
            return [row for row in data["data"] if isinstance(row, dict)]
        if isinstance(data.get("payload"), list):
            return [row for row in data["payload"] if isinstance(row, dict)]
        if all(key in data for key in ("price", "timestamp")):
            return [data]
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    return []


def normalize_trade_timestamp(raw_ts: Any) -> float | None:
    if raw_ts in (None, ""):
        return None
    try:
        value = float(raw_ts)
    except (TypeError, ValueError):
        return None

    if value > 1_000_000_000_000:
        return value / 1000.0
    if value > 1_000_000_000:
        return value
    return None


def get_trade_ts(trade: dict[str, Any]) -> float | None:
    for key in ("timestamp", "ts", "time", "createdAt"):
        normalized = normalize_trade_timestamp(trade.get(key))
        if normalized is not None:
            return normalized
    return None


def classify_feed(state: FeedProbeState, *, warmup_samples: int = 20) -> str:
    if state.total_count < warmup_samples:
        return "WARMING_UP"

    avg_latency = (
        sum(state.latencies_ms) / len(state.latencies_ms)
        if state.latencies_ms
        else float("inf")
    )
    avg_delta = (
        sum(state.deltas_sec) / len(state.deltas_sec)
        if state.deltas_sec
        else float("inf")
    )
    avg_batch = (
        sum(state.batch_sizes) / len(state.batch_sizes)
        if state.batch_sizes
        else 0.0
    )
    dup_rate = state.duplicate_count / max(state.total_count, 1)

    if dup_rate > 0.5:
        return "SNAPSHOT"
    if avg_batch > 20 and avg_delta > 0.5:
        return "BATCHED"
    if avg_latency < 150 and avg_delta < 0.2:
        return "REALTIME"
    return "DELAYED"


def process_message(
    message: str,
    *,
    state: FeedProbeState,
    warmup_samples: int = 20,
    recv_time: float | None = None,
) -> dict[str, Any] | None:
    now = time.time() if recv_time is None else recv_time
    state.total_count += 1

    msg_hash = hashlib.md5(message.encode("utf-8")).hexdigest()
    is_duplicate = msg_hash == state.last_hash
    if is_duplicate:
        state.duplicate_count += 1
    state.last_hash = msg_hash

    try:
        data = json.loads(message)
    except json.JSONDecodeError:
        logger.warning("ws_probe_invalid_json size=%s", len(message))
        return None

    event_type = data.get("event_type") if isinstance(data, dict) else None
    trades = extract_trades(data)
    batch_size = len(trades)
    state.batch_sizes.append(batch_size)

    delta_sec = None
    if state.last_recv is not None:
        delta_sec = now - state.last_recv
        state.deltas_sec.append(delta_sec)
    state.last_recv = now

    latency_ms = None
    if trades:
        trade_ts = get_trade_ts(trades[0])
        if trade_ts is not None:
            latency_ms = max(0.0, (now - trade_ts) * 1000.0)
            state.latencies_ms.append(latency_ms)

    verdict = classify_feed(state, warmup_samples=warmup_samples)
    dup_rate = round(state.duplicate_count / max(state.total_count, 1), 3)
    summary = {
        "event_type": event_type,
        "delta_sec": round(delta_sec, 3) if delta_sec is not None else None,
        "batch_size": batch_size,
        "latency_ms": round(latency_ms, 2) if latency_ms is not None else None,
        "duplicate_frame": is_duplicate,
        "dup_rate": dup_rate,
        "verdict": verdict,
    }
    return summary


def parse_subscribe_message(raw: str | None) -> dict[str, Any] | None:
    if raw in (None, ""):
        return None
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("Subscribe message must be a JSON object.")
    return parsed


def run_probe(config: FeedProbeConfig) -> None:
    try:
        import websocket  # type: ignore
    except ModuleNotFoundError as exc:
        try:
            import websockets  # type: ignore  # noqa: F401
        except ModuleNotFoundError:
            raise RuntimeError(
                "ws_probe requires either the 'websocket-client' or 'websockets' package. Install one before running the probe."
            ) from exc
        run_probe_asyncio(config)
        return

    state = FeedProbeState(max_samples=config.max_samples)

    def on_message(ws, message: str) -> None:
        summary = process_message(
            message,
            state=state,
            warmup_samples=config.warmup_samples,
        )
        if summary is not None:
            print(summary, flush=True)

    def on_open(ws) -> None:
        print("Connected", flush=True)
        if config.subscribe_message is not None:
            ws.send(json.dumps(config.subscribe_message))

    def on_error(ws, error: Any) -> None:
        print(f"Error: {error}", flush=True)

    def on_close(ws, close_status_code: Any, close_msg: Any) -> None:
        print(f"Closed: code={close_status_code} msg={close_msg}", flush=True)

    app = websocket.WebSocketApp(
        config.ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    app.run_forever()


def run_probe_asyncio(config: FeedProbeConfig) -> None:
    import websockets  # type: ignore

    state = FeedProbeState(max_samples=config.max_samples)

    async def _runner() -> None:
        async with websockets.connect(config.ws_url) as ws:
            print("Connected", flush=True)
            if config.subscribe_message is not None:
                await ws.send(json.dumps(config.subscribe_message))

            async for message in ws:
                if isinstance(message, bytes):
                    message = message.decode("utf-8", errors="replace")
                summary = process_message(
                    str(message),
                    state=state,
                    warmup_samples=config.warmup_samples,
                )
                if summary is not None:
                    print(summary, flush=True)

    asyncio.run(_runner())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe WebSocket feed quality for TIP V1.")
    parser.add_argument("--ws-url", required=True, help="WebSocket URL to probe.")
    parser.add_argument(
        "--subscribe-message",
        help="Optional JSON subscription message to send on connect.",
    )
    parser.add_argument(
        "--max-samples",
        type=int,
        default=200,
        help="Rolling sample window used for feed classification.",
    )
    parser.add_argument(
        "--warmup-samples",
        type=int,
        default=20,
        help="Samples required before classifying the feed.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = FeedProbeConfig(
        ws_url=args.ws_url,
        subscribe_message=parse_subscribe_message(args.subscribe_message),
        max_samples=args.max_samples,
        warmup_samples=args.warmup_samples,
    )
    run_probe(config)


if __name__ == "__main__":
    main()
