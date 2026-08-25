"""PumpPortal token-trade price stream for live exit monitoring.

The runtime retains stream marks in memory for high-frequency stop checks. A
stale stream invokes its supplied fail-closed handler once per held mint.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from collections.abc import Awaitable, Callable

import websockets

log = logging.getLogger("pumpportal_price")

PUMPPORTAL_WS_URL = "wss://pumpportal.fun/api/data"

PriceHandler = Callable[[str, float], Awaitable[None]]
MintsProvider = Callable[[], Awaitable[set[str]]]
StaleHandler = Callable[[str], Awaitable[None]]


class PumpPortalPriceFeed:
    """Keep PumpPortal subscriptions aligned with held mints and publish marks."""

    def __init__(
        self,
        held_mints: MintsProvider,
        on_price: PriceHandler,
        on_stale: StaleHandler | None = None,
        *,
        url: str = PUMPPORTAL_WS_URL,
        refresh_interval_s: float = 1.0,
        reconnect_delay_s: float = 2.0,
        stale_after_s: float = 15.0,
    ) -> None:
        self._held_mints = held_mints
        self._on_price = on_price
        self._on_stale = on_stale
        self._url = url
        self._refresh_interval_s = refresh_interval_s
        self._reconnect_delay_s = reconnect_delay_s
        self._stale_after_s = stale_after_s
        self._last_price_at: dict[str, float] = {}
        self._stale_notified: set[str] = set()

    async def run(self) -> None:
        """Reconnect indefinitely while retaining stale detection across reconnects."""
        log.info(
            "PRICE_FEED: PumpPortal WebSocket starting; stale stream triggers fail-closed handler",
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
            except Exception as exc:
                log.warning("PRICE_FEED: PumpPortal disconnected: %s", exc)
                await self._wait_to_reconnect()

    async def _wait_to_reconnect(self) -> None:
        """Keep evaluating stream freshness while reconnect backoff is in progress."""
        deadline = time.monotonic() + self._reconnect_delay_s
        while True:
            held = await self._held_mints()
            await self._notify_stale_mints(held)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            await asyncio.sleep(min(0.1, remaining))

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
                now = time.monotonic()
                for mint in additions:
                    self._last_price_at.setdefault(mint, now)
                    self._stale_notified.discard(mint)
            if removals:
                await websocket.send(
                    json.dumps({"method": "unsubscribeTokenTrade", "keys": sorted(removals)}),
                )
                subscribed.difference_update(removals)
                for mint in removals:
                    self._last_price_at.pop(mint, None)
                    self._stale_notified.discard(mint)

            try:
                raw = await asyncio.wait_for(websocket.recv(), timeout=self._refresh_interval_s)
            except TimeoutError:
                await self._notify_stale_mints(subscribed)
                continue
            price = _parse_price_update(raw)
            if price is not None and price[0] in subscribed:
                self._last_price_at[price[0]] = time.monotonic()
                self._stale_notified.discard(price[0])
                await self._on_price(*price)
            await self._notify_stale_mints(subscribed)

    async def _notify_stale_mints(
        self,
        subscribed: set[str],
    ) -> None:
        if self._on_stale is None:
            return
        now = time.monotonic()
        for mint in subscribed - self._stale_notified:
            elapsed = now - self._last_price_at.get(mint, now)
            if elapsed < self._stale_after_s:
                continue
            self._stale_notified.add(mint)
            log.warning(
                "PRICE_FEED: stale PumpPortal price mint=%s age=%.1fs; triggering fail-closed handler",
                mint[:16],
                elapsed,
            )
            try:
                await self._on_stale(mint)
            except Exception as exc:
                log.error("PRICE_FEED: stale handler failed mint=%s: %s", mint[:16], exc)


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
