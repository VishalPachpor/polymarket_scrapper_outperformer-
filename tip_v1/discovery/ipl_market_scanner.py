from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from tip_v1.config import Settings, get_settings


IPL_TAG_SLUG = "indian-premier-league"
EVENTS_API_URL_DEFAULT = "https://gamma-api.polymarket.com/events"


@dataclass(frozen=True)
class IPLMarket:
    condition_id: str
    event_slug: str
    event_title: str
    question: str
    outcomes: list[str]
    outcome_prices: list[float]
    clob_token_ids: list[str]
    closed: bool
    active: bool
    volume_num: float


def _request_json(url: str, timeout_seconds: float) -> Any:
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "tip-v1-discovery/0.1",
        },
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def _coerce_list(raw: Any) -> list[Any]:
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _events_url(settings: Settings) -> str:
    base = getattr(settings, "markets_api_url", "").rstrip("/")
    if base.endswith("/markets"):
        return base[: -len("/markets")] + "/events"
    return EVENTS_API_URL_DEFAULT


def fetch_ipl_events(
    *,
    settings: Settings | None = None,
    include_closed: bool = True,
    limit_per_page: int = 200,
) -> list[dict[str, Any]]:
    settings = settings or get_settings()
    base_url = _events_url(settings)
    states = [("false", "open")]
    if include_closed:
        states.append(("true", "closed"))

    seen_slugs: set[str] = set()
    events: list[dict[str, Any]] = []
    for closed_param, _label in states:
        offset = 0
        while True:
            params = {
                "limit": limit_per_page,
                "offset": offset,
                "closed": closed_param,
                "tag_slug": IPL_TAG_SLUG,
            }
            payload = _request_json(
                f"{base_url}?{urlencode(params)}",
                settings.request_timeout_seconds,
            )
            if not isinstance(payload, list) or not payload:
                break
            new_count = 0
            for event in payload:
                if not isinstance(event, dict):
                    continue
                slug = event.get("slug")
                if not isinstance(slug, str) or slug in seen_slugs:
                    continue
                seen_slugs.add(slug)
                events.append(event)
                new_count += 1
            if len(payload) < limit_per_page or new_count == 0:
                break
            offset += limit_per_page
    return events


def extract_ipl_markets(events: list[dict[str, Any]]) -> list[IPLMarket]:
    markets: list[IPLMarket] = []
    seen_condition_ids: set[str] = set()
    for event in events:
        event_slug = str(event.get("slug") or "")
        event_title = str(event.get("title") or "")
        for raw_market in event.get("markets", []) or []:
            if not isinstance(raw_market, dict):
                continue
            condition_id = raw_market.get("conditionId") or raw_market.get("condition_id")
            if not isinstance(condition_id, str) or condition_id in seen_condition_ids:
                continue
            seen_condition_ids.add(condition_id)

            outcome_prices_raw = _coerce_list(raw_market.get("outcomePrices"))
            try:
                outcome_prices = [float(p) for p in outcome_prices_raw]
            except (TypeError, ValueError):
                outcome_prices = []

            markets.append(
                IPLMarket(
                    condition_id=condition_id,
                    event_slug=event_slug,
                    event_title=event_title,
                    question=str(raw_market.get("question") or ""),
                    outcomes=[str(o) for o in _coerce_list(raw_market.get("outcomes"))],
                    outcome_prices=outcome_prices,
                    clob_token_ids=[str(t) for t in _coerce_list(raw_market.get("clobTokenIds"))],
                    closed=bool(raw_market.get("closed")),
                    active=bool(raw_market.get("active")),
                    volume_num=float(raw_market.get("volumeNum") or 0.0),
                )
            )
    return markets


def scan_ipl_markets(
    *,
    settings: Settings | None = None,
    include_closed: bool = True,
) -> list[IPLMarket]:
    events = fetch_ipl_events(settings=settings, include_closed=include_closed)
    return extract_ipl_markets(events)
