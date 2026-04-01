from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any


YES_ALIASES = {"YES", "Y", "TRUE", "1"}
NO_ALIASES = {"NO", "N", "FALSE", "0"}


def coalesce(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload and payload[key] not in (None, ""):
            return payload[key]
    return None


def normalize_side(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in {"BUY", "SELL"}:
        raise ValueError(f"Unsupported side '{normalized}'.")
    return normalized


def normalize_outcome(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    if not normalized:
        raise ValueError("Missing outcome.")
    if normalized in YES_ALIASES:
        return "YES"
    if normalized in NO_ALIASES:
        return "NO"
    return normalized


def normalize_decimal(value: Any) -> float:
    try:
        return float(Decimal(str(value)))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"Invalid decimal value '{value}'.") from exc


def normalize_decimal_text(value: Any) -> str:
    decimal_value = Decimal(str(value))
    normalized = format(decimal_value.normalize(), "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized or "0"


_TS_MILLIS_THRESHOLD = 1_000_000_000_000  # 13+ digits = milliseconds


def normalize_timestamp(value: Any) -> int:
    try:
        ts = int(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid timestamp '{value}'.") from exc

    if ts >= _TS_MILLIS_THRESHOLD:
        ts = ts // 1000

    if ts < 0:
        raise ValueError(f"Negative timestamp {ts}.")

    return ts
