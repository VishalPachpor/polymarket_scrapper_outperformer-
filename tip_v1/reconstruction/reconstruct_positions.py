from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from decimal import Decimal

from tip_v1.config import Settings, get_settings
from tip_v1.db.db import managed_connection


EPSILON = Decimal("0.000000000001")


def _to_decimal(value: float | int | str | Decimal) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _rounded(value: float | Decimal) -> float:
    return round(float(value), 12)


@dataclass(frozen=True)
class ReconstructionResult:
    event_count: int
    closed_positions: int
    open_positions: int
    unmatched_sells_volume: float


class NegativeInventoryError(RuntimeError):
    """Raised when a sell consumes more inventory than reconstructed buys."""


class PhantomPnLError(RuntimeError):
    """Raised when realized PnL does not match matched cashflows."""


def reconstruct_positions(
    wallet: str,
    settings: Settings | None = None,
    *,
    version: int = 1,
    fail_on_unmatched_sells: bool = False,
) -> ReconstructionResult:
    settings = settings or get_settings()
    event_count = 0
    closed_positions = 0
    open_positions = 0
    unmatched_sells_volume = 0.0

    with managed_connection(settings) as connection:
        connection.execute(
            """
            DELETE FROM position_path_metrics
            WHERE wallet = ? AND version = ?
            """,
            (wallet, version),
        )
        connection.execute(
            """
            DELETE FROM positions_reconstructed
            WHERE wallet = ? AND version = ?
            """,
            (wallet, version),
        )

        events = connection.execute(
            """
            SELECT id, trade_id, market_id, outcome, side, price, size, timestamp
            FROM trade_events
            WHERE wallet = ?
            ORDER BY market_id, outcome, timestamp, trade_id, id
            """,
            (wallet,),
        ).fetchall()
        event_count = len(events)

        grouped_events: dict[tuple[str, str], list] = defaultdict(list)
        for event in events:
            grouped_events[(event["market_id"], event["outcome"])].append(event)

        for (market_id, outcome), market_events in grouped_events.items():
            open_buys = deque()
            matched_buy_cost = Decimal("0")
            matched_sell_value = Decimal("0")
            realized_pnl = Decimal("0")

            for event in market_events:
                if event["side"] == "BUY":
                    open_buys.append(
                        {
                            "trade_event_id": event["id"],
                            "price": _to_decimal(event["price"]),
                            "remaining_size": _to_decimal(event["size"]),
                            "timestamp": int(event["timestamp"]),
                        }
                    )
                    continue

                sell_price = _to_decimal(event["price"])
                remaining_sell_size = _to_decimal(event["size"])

                while remaining_sell_size > EPSILON and open_buys:
                    buy_lot = open_buys[0]
                    matched_size = min(remaining_sell_size, buy_lot["remaining_size"])
                    pnl = (sell_price - buy_lot["price"]) * matched_size
                    duration = int(event["timestamp"]) - int(buy_lot["timestamp"])
                    matched_buy_cost += buy_lot["price"] * matched_size
                    matched_sell_value += sell_price * matched_size
                    realized_pnl += pnl

                    connection.execute(
                        """
                        INSERT INTO positions_reconstructed (
                            wallet,
                            market_id,
                            outcome,
                            entry_trade_event_id,
                            exit_trade_event_id,
                            entry_price,
                            exit_price,
                            size,
                            pnl,
                            entry_time,
                            exit_time,
                            duration,
                            status,
                            remaining_size,
                            version
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            wallet,
                            market_id,
                            outcome,
                            buy_lot["trade_event_id"],
                            int(event["id"]),
                            _rounded(buy_lot["price"]),
                            _rounded(sell_price),
                            _rounded(matched_size),
                            _rounded(pnl),
                            buy_lot["timestamp"],
                            int(event["timestamp"]),
                            duration,
                            "CLOSED",
                            0.0,
                            version,
                        ),
                    )
                    closed_positions += 1

                    buy_lot["remaining_size"] -= matched_size
                    remaining_sell_size -= matched_size

                    if buy_lot["remaining_size"] <= EPSILON:
                        open_buys.popleft()

                if remaining_sell_size > EPSILON:
                    unmatched_sells_volume += float(remaining_sell_size)
                    if fail_on_unmatched_sells:
                        raise NegativeInventoryError(
                            "Negative inventory detected for "
                            f"wallet={wallet} market_id={market_id} outcome={outcome} "
                            f"remaining_sell_size={_rounded(remaining_sell_size)}"
                        )

            expected_realized_pnl = matched_sell_value - matched_buy_cost
            if abs(realized_pnl - expected_realized_pnl) > EPSILON:
                raise PhantomPnLError(
                    "Phantom PnL detected for "
                    f"wallet={wallet} market_id={market_id} outcome={outcome} "
                    f"realized_pnl={_rounded(realized_pnl)} "
                    f"expected_realized_pnl={_rounded(expected_realized_pnl)}"
                )

            for open_lot in open_buys:
                connection.execute(
                    """
                    INSERT INTO positions_reconstructed (
                        wallet,
                        market_id,
                        outcome,
                        entry_trade_event_id,
                        exit_trade_event_id,
                        entry_price,
                        exit_price,
                        size,
                        pnl,
                        entry_time,
                        exit_time,
                        duration,
                        status,
                        remaining_size,
                        version
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        wallet,
                        market_id,
                        outcome,
                        open_lot["trade_event_id"],
                        None,
                        _rounded(open_lot["price"]),
                        None,
                        _rounded(open_lot["remaining_size"]),
                        None,
                        open_lot["timestamp"],
                        None,
                        None,
                        "OPEN",
                        _rounded(open_lot["remaining_size"]),
                        version,
                    ),
                )
                open_positions += 1

        connection.commit()

    return ReconstructionResult(
        event_count=event_count,
        closed_positions=closed_positions,
        open_positions=open_positions,
        unmatched_sells_volume=unmatched_sells_volume,
    )
