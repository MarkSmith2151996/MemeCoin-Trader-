"""PumpPortal token-trade price stream for live exit monitoring.

The stream is deliberately an additive mark source: the regular Jupiter poll
continues to run, so a disconnected websocket can never leave an open position
without a price check.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
from collections.abc import Awaitable, Callable

import websockets

log = logging.getLogger("pumpportal_price")

PUMPPORTAL_WS_URL = "wss://pumpportal.fun/api/data"

PriceHandler = Callable[[str, float], Awaitable[None]]
MintsProvider = Callable[[], Awaitable[set[str]]]


class PumpPortalPriceFeed:
    """Keep PumpPortal subscriptions aligned with held mints and publish marks."""

    def __init__(
        self,
        held_mints: MintsProvider,
        on_price: PriceHandler,
        *,
        url: str = PUMPPORTAL_WS_URL,
        refresh_interval_s: float = 1.0,
        reconnect_delay_s: float = 2.0,
    ) -> None:
        self._held_mints = held_mints
        self._on_price = on_price
        self._url = url
        self._refresh_interval_s = refresh_interval_s
        self._reconnect_delay_s = reconnect_delay_s

    async def run(self) -> None:
        """Reconnect indefinitely; callers keep Jupiter polling as the fallback."""
        log.info(
            "PRICE_FEED: PumpPortal WebSocket starting; "
            "fallback to Jupiter polling until connected",
        )
        while True:
            try:
                async with websockets.connect(
                    self._url,
                    ping_interval=20,
                    ping_timeout=20,
                ) as websocket:
                    log.info("PRICE_FEED: PumpPortal WebSocket connected")
                    await self._run_connection(websocket)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # A stream failure must not stop exit monitoring.
                log.warning(
                    "PRICE_FEED: fallback to Jupiter polling (PumpPortal disconnected: %s)",
                    exc,
                )
                await asyncio.sleep(self._reconnect_delay_s)

    async def _run_connection(self, websocket: object) -> None:
        subscribed: set[str] = set()
        while True:
            held = await self._held_mints()
            additions = held - subscribed
            removals = subscribed - held
            if additions:
                await websocket.send(
                    json.dumps({"method": "subscribeTokenTrade", "keys": sorted(additions)}),
                )
                subscribed.update(additions)
            if removals:
                await websocket.send(
                    json.dumps({"method": "unsubscribeTokenTrade", "keys": sorted(removals)}),
                )
                subscribed.difference_update(removals)

            try:
                raw = await asyncio.wait_for(websocket.recv(), timeout=self._refresh_interval_s)
            except TimeoutError:
                continue
            price = _parse_price_update(raw)
            if price is not None:
                await self._on_price(*price)


def _parse_price_update(raw: object) -> tuple[str, float] | None:
    """Normalize PumpPortal's token-trade payload without trusting malformed data."""
    try:
        payload = json.loads(raw) if isinstance(raw, str) else raw
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    mint = payload.get("mint")
    if not isinstance(mint, str) or not mint:
        return None

    for key in ("priceSol", "price_sol", "price"):
        value = _finite_positive(payload.get(key))
        if value is not None:
            return mint, value

    sol_amount = _finite_positive(payload.get("solAmount"))
    token_amount = _finite_positive(payload.get("tokenAmount"))
    if sol_amount is None or token_amount is None:
        return None
    return mint, sol_amount / token_amount


def _finite_positive(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) and parsed > 0 else None
