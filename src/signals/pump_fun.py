"""pump.fun signal monitor with websocket buffering and HTTP fallback polling."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
from collections.abc import Awaitable, Callable, Iterable, Mapping
from time import monotonic
from typing import Any

import httpx
from websockets import connect as websocket_connect

from src.core.models import Signal, SignalSource as SignalSourceEnum, SignalType
from src.signals.base import SignalSource

HTTPFetcher = Callable[[httpx.AsyncClient, str], Awaitable[object]]
WebsocketConnector = Callable[[str], Any]

DEFAULT_HTTP_URLS = (
    "https://frontend-api-v3.pump.fun/coins?offset=0&limit=50&includeNsfw=true",
    "https://frontend-api-v3.pump.fun/coins/currently-live?offset=0&limit=50&includeNsfw=true",
)
DEFAULT_WS_URL = "wss://pumpportal.fun/api/data"
DEFAULT_SUBSCRIPTIONS = (
    {"method": "subscribeNewToken"},
    {"method": "subscribeMigration"},
)


async def _default_http_fetcher(client: httpx.AsyncClient, url: str) -> object:
    response = await client.get(url)
    response.raise_for_status()
    return response.json()


class PumpFunMonitor(SignalSource):
    """Normalize pump.fun launch and graduation events into local signals.

    The public pump.fun surface changes often and some public endpoints are undocumented.
    This monitor keeps the network layer injectable so tests can exercise normalization,
    dedupe, and error handling without live traffic.
    """

    def __init__(
        self,
        *,
        http_urls: Iterable[str] | None = None,
        websocket_url: str | None = None,
        websocket_subscriptions: Iterable[Mapping[str, object]] | None = None,
        http_timeout_s: float = 10.0,
        poll_interval_s: float = 10.0,
        http_fetcher: HTTPFetcher | None = None,
        websocket_connector: WebsocketConnector | None = None,
    ) -> None:
        self._http_urls = tuple(http_urls or DEFAULT_HTTP_URLS)
        self._websocket_url = websocket_url if websocket_url is not None else DEFAULT_WS_URL
        self._websocket_subscriptions = tuple(websocket_subscriptions or DEFAULT_SUBSCRIPTIONS)
        self._http_timeout_s = http_timeout_s
        self._poll_interval_s = poll_interval_s
        self._http_fetcher = http_fetcher or _default_http_fetcher
        self._websocket_connector = websocket_connector or websocket_connect

        self._client: httpx.AsyncClient | None = None
        self._queue: asyncio.Queue[dict[str, object]] = asyncio.Queue()
        self._seen_events: set[str] = set()
        self._ws_task: asyncio.Task[None] | None = None
        self._started = False
        self._last_http_poll_at = 0.0

    @property
    def name(self) -> str:
        return "pump_fun"

    async def start(self) -> None:
        if self._started:
            return

        self._client = httpx.AsyncClient(timeout=self._http_timeout_s)
        self._started = True

        if self._websocket_url:
            self._ws_task = asyncio.create_task(self._websocket_loop(), name="pump-fun-monitor")

    async def stop(self) -> None:
        self._started = False

        if self._ws_task is not None:
            self._ws_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._ws_task
            self._ws_task = None

        if self._client is not None:
            await self._client.aclose()
            self._client = None

        self._drain_queue()

    async def poll(self) -> list[Signal]:
        if not self._started:
            await self.start()

        await self._refresh_http_events_if_due()

        signals: list[Signal] = []
        while not self._queue.empty():
            payload = self._queue.get_nowait()
            signal = self._payload_to_signal(payload)
            if signal is not None:
                signals.append(signal)

        return signals

    async def _refresh_http_events_if_due(self) -> None:
        if self._client is None or not self._http_urls:
            return

        now = monotonic()
        if self._last_http_poll_at and (now - self._last_http_poll_at) < self._poll_interval_s:
            return

        self._last_http_poll_at = now
        for url in self._http_urls:
            try:
                payload = await self._http_fetcher(self._client, url)
            except Exception:
                continue

            for raw_event in self._extract_events(payload):
                self._enqueue_event(raw_event)

            if not self._queue.empty():
                return

    async def _websocket_loop(self) -> None:
        reconnect_delay_s = 1.0
        while self._started and self._websocket_url:
            try:
                async with self._websocket_connector(self._websocket_url) as websocket:
                    reconnect_delay_s = 1.0
                    for message in self._websocket_subscriptions:
                        await websocket.send(json.dumps(message))

                    async for raw_message in websocket:
                        payload = self._parse_websocket_message(raw_message)
                        if payload is not None:
                            self._enqueue_event(payload)
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(reconnect_delay_s)
                reconnect_delay_s = min(reconnect_delay_s * 2, 30.0)

    def _drain_queue(self) -> None:
        while not self._queue.empty():
            self._queue.get_nowait()

    def _enqueue_event(self, payload: Mapping[str, object]) -> None:
        event = dict(payload)
        event_key = self._event_key(event)
        if event_key in self._seen_events:
            return
        self._seen_events.add(event_key)
        self._queue.put_nowait(event)

    def _event_key(self, payload: Mapping[str, object]) -> str:
        for field_name in ("event_id", "eventId", "id", "signature", "txSignature"):
            value = payload.get(field_name)
            if isinstance(value, str) and value:
                return value

        mint_address = self._mint_address_from_payload(payload)
        event_type = self._signal_type_from_payload(payload).value
        if mint_address:
            return f"{event_type}:{mint_address}"

        return json.dumps(payload, sort_keys=True, default=str)

    def _payload_to_signal(self, payload: Mapping[str, object]) -> Signal | None:
        mint_address = self._mint_address_from_payload(payload)
        if not mint_address:
            return None

        signal_type = self._signal_type_from_payload(payload)
        confidence = self._confidence_for_payload(payload, signal_type)

        return Signal(
            source=SignalSourceEnum.PUMP_FUN,
            type=signal_type,
            mint_address=mint_address,
            confidence=confidence,
            message=self._message_for_payload(payload, signal_type),
            payload=dict(payload),
        )

    def _mint_address_from_payload(self, payload: Mapping[str, object]) -> str | None:
        for field_name in (
            "mint",
            "mintAddress",
            "mint_address",
            "baseMint",
            "tokenAddress",
            "coinMint",
            "address",
        ):
            value = payload.get(field_name)
            if isinstance(value, str) and value.strip():
                return value.strip()

        mint = payload.get("coin")
        if isinstance(mint, Mapping):
            return self._mint_address_from_payload(mint)

        return None

    def _signal_type_from_payload(self, payload: Mapping[str, object]) -> SignalType:
        event_name = " ".join(
            str(payload.get(field_name, ""))
            for field_name in (
                "event",
                "type",
                "eventType",
                "method",
                "channel",
                "txType",
                "tx_type",
                "pool",
            )
        ).lower()

        if any(keyword in event_name for keyword in ("graduat", "migration", "migrate", "raydium")):
            return SignalType.GRADUATION

        return SignalType.NEW_POOL

    def _confidence_for_payload(self, payload: Mapping[str, object], signal_type: SignalType) -> float:
        if signal_type == SignalType.GRADUATION:
            return 0.95

        if any(payload.get(field_name) for field_name in ("signature", "txSignature", "event_id", "eventId")):
            return 0.85

        return 0.7

    def _message_for_payload(self, payload: Mapping[str, object], signal_type: SignalType) -> str:
        symbol = payload.get("symbol") or payload.get("ticker") or payload.get("name")
        if isinstance(symbol, str) and symbol.strip():
            return f"pump.fun {signal_type.value.lower()} for {symbol.strip()}"
        return f"pump.fun {signal_type.value.lower()} detected"

    def _extract_events(self, payload: object) -> list[dict[str, object]]:
        if isinstance(payload, list):
            return [dict(item) for item in payload if isinstance(item, Mapping)]

        if isinstance(payload, Mapping):
            for field_name in ("data", "items", "results", "coins"):
                nested = payload.get(field_name)
                if isinstance(nested, list):
                    return [dict(item) for item in nested if isinstance(item, Mapping)]
            return [dict(payload)]

        return []

    def _parse_websocket_message(self, raw_message: object) -> dict[str, object] | None:
        if isinstance(raw_message, bytes):
            raw_message = raw_message.decode("utf-8", errors="ignore")

        if isinstance(raw_message, str):
            try:
                parsed = json.loads(raw_message)
            except json.JSONDecodeError:
                return None
        else:
            parsed = raw_message

        events = self._extract_events(parsed)
        return events[0] if events else None


class PumpFunSignalSource(PumpFunMonitor):
    """Backward-compatible alias for the bootstrap placeholder class name."""


def build_monitor_from_env() -> PumpFunMonitor:
    """Create a monitor from optional env overrides without changing global config.

    TODO: Replace these best-effort defaults once a stable pump.fun provider contract is
    selected for the project.
    """

    websocket_url = os.getenv("PUMP_FUN_WS_URL", DEFAULT_WS_URL)
    raw_http_urls = os.getenv("PUMP_FUN_HTTP_URLS", "")
    http_urls = tuple(url.strip() for url in raw_http_urls.split(",") if url.strip())
    return PumpFunMonitor(http_urls=http_urls or DEFAULT_HTTP_URLS, websocket_url=websocket_url)
