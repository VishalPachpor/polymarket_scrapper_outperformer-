from __future__ import annotations

import json
import unittest

from tip_v1.live.ws_probe import (
    FeedProbeState,
    classify_feed,
    extract_trades,
    get_trade_ts,
    parse_subscribe_message,
    process_message,
)


class WebSocketProbeTests(unittest.TestCase):
    def test_extract_trades_accepts_common_shapes(self) -> None:
        self.assertEqual(
            extract_trades({"trades": [{"timestamp": 1_700_000_000, "price": 0.4}]}),
            [{"timestamp": 1_700_000_000, "price": 0.4}],
        )
        self.assertEqual(
            extract_trades({"data": [{"timestamp": 1_700_000_000, "price": 0.4}]}),
            [{"timestamp": 1_700_000_000, "price": 0.4}],
        )
        self.assertEqual(
            extract_trades([{"timestamp": 1_700_000_000, "price": 0.4}]),
            [{"timestamp": 1_700_000_000, "price": 0.4}],
        )

    def test_get_trade_ts_handles_seconds_and_milliseconds(self) -> None:
        self.assertEqual(get_trade_ts({"timestamp": 1_700_000_000}), 1_700_000_000.0)
        self.assertEqual(get_trade_ts({"ts": 1_700_000_000_000}), 1_700_000_000.0)
        self.assertIsNone(get_trade_ts({"timestamp": 12345}))

    def test_process_message_detects_duplicates_and_latency(self) -> None:
        state = FeedProbeState(max_samples=50)
        message = json.dumps(
            {
                "event_type": "last_trade_price",
                "trades": [{"timestamp": 1_700_000_000, "price": 0.4}],
            }
        )

        first = process_message(
            message,
            state=state,
            warmup_samples=2,
            recv_time=1_700_000_000.100,
        )
        second = process_message(
            message,
            state=state,
            warmup_samples=2,
            recv_time=1_700_000_000.300,
        )

        self.assertEqual(first["batch_size"], 1)
        self.assertEqual(first["event_type"], "last_trade_price")
        self.assertAlmostEqual(first["latency_ms"], 100.0)
        self.assertFalse(first["duplicate_frame"])
        self.assertTrue(second["duplicate_frame"])
        self.assertAlmostEqual(second["delta_sec"], 0.2)

    def test_classify_feed_reports_snapshot_and_realtime(self) -> None:
        snapshot_state = FeedProbeState(max_samples=50)
        snapshot_state.total_count = 30
        snapshot_state.duplicate_count = 20
        snapshot_state.deltas_sec.extend([1.0] * 30)
        snapshot_state.batch_sizes.extend([100] * 30)
        snapshot_state.latencies_ms.extend([400.0] * 30)
        self.assertEqual(classify_feed(snapshot_state, warmup_samples=20), "SNAPSHOT")

        realtime_state = FeedProbeState(max_samples=50)
        realtime_state.total_count = 30
        realtime_state.duplicate_count = 0
        realtime_state.deltas_sec.extend([0.05] * 30)
        realtime_state.batch_sizes.extend([1] * 30)
        realtime_state.latencies_ms.extend([80.0] * 30)
        self.assertEqual(classify_feed(realtime_state, warmup_samples=20), "REALTIME")

    def test_parse_subscribe_message_requires_object(self) -> None:
        self.assertEqual(
            parse_subscribe_message('{"type":"subscribe","channel":"trades"}'),
            {"type": "subscribe", "channel": "trades"},
        )
        with self.assertRaises(ValueError):
            parse_subscribe_message('["bad"]')


if __name__ == "__main__":
    unittest.main()
