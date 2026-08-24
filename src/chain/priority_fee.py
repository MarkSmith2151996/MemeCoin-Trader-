"""Dynamic priority fee provider (MT-588).

Queries the connected RPC's ``getRecentPrioritizationFees`` method and returns
the 75th percentile of recent fees as the priority fee for the next
transaction, replacing the static ``PRIORITY_FEE_LAMPORTS`` constant.

The lookup is cached and refreshed at most every ``refresh_interval_s`` (30s)
instead of every cycle. The result is stored in module-level state by the
caller so it survives across cycles without re-querying.

RPC URL resolution order: QuickNode, ``PRIMARY_RPC_URL``, then Helius. Every failure degrades to ``None`` so callers
fall back to their existing static fee behavior — a fee lookup failure never
blocks a trade.
"""

from __future__ import annotations

import logging
import os
import re
import time
from collections.abc import Awaitable, Callable

import httpx

log = logging.getLogger("priority_fee")

# MT-589: strip query strings (API keys) from URLs inside exception text
# before logging — httpx error messages embed the request URL verbatim.
_URL_IN_TEXT_RE = re.compile(r"https?://[^\s'\"]+")

_DEFAULT_RPC_URL = "https://api.mainnet-beta.solana.com"
# MT-589: fallback RPC used when the configured endpoint fails (e.g. Helius
# 429 quota exhaustion, MT-510). getRecentPrioritizationFees is a cheap public
# method, so the public mainnet-beta endpoint is a reliable secondary source.
_FALLBACK_RPC_URL = "https://api.mainnet-beta.solana.com"
_DEFAULT_REFRESH_S = 30.0
_DEFAULT_PERCENTILE = 0.75
_DEFAULT_MIN_LAMPORTS = 1_000
_DEFAULT_MAX_LAMPORTS = 1_000_000
_RPC_FEE_CALL = "getRecentPrioritizationFees"
_SAMPLE_LIMIT = 20


def resolve_rpc_url() -> str:
    """Return the primary RPC for fee lookups (QuickNode first)."""
    return (
        os.environ.get("QUICKNODE_RPC_URL")
        or os.environ.get("PRIMARY_RPC_URL")
        or os.environ.get("HELIUS_RPC_URL")
        or _DEFAULT_RPC_URL
    )


def _redact_url(url: str) -> str:
    """Strip query strings (API keys) from an RPC URL before logging it."""
    return url.split("?", 1)[0] if url else url


def _sanitize_exc(exc: Exception) -> str:
    """Sanitized exception text — no URLs with query strings (API keys)."""
    return _URL_IN_TEXT_RE.sub(lambda m: m.group(0).split("?", 1)[0], str(exc))


class PriorityFeeProvider:
    """Cached 75th-percentile priority fee from the connected RPC.

    ``get_fee_lamports`` returns the cached fee immediately and refreshes at
    most once per ``refresh_interval_s``. A failed refresh keeps the previous
    value for up to ``stale_ttl_s``, after which lookups return ``None`` so
    the caller can fall back to static behavior.
    """

    def __init__(
        self,
        rpc_url: str | None = None,
        *,
        refresh_interval_s: float = _DEFAULT_REFRESH_S,
        percentile: float = _DEFAULT_PERCENTILE,
        min_lamports: int = _DEFAULT_MIN_LAMPORTS,
        max_lamports: int = _DEFAULT_MAX_LAMPORTS,
        sample_limit: int = _SAMPLE_LIMIT,
        stale_ttl_s: float = 300.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._rpc_url = rpc_url or resolve_rpc_url()
        # An injected endpoint is normally a test or one-off diagnostic; retain
        # the public fallback in that case instead of coupling it to local env.
        self._backup_rpc_url = (
            _FALLBACK_RPC_URL
            if rpc_url is not None
            else os.environ.get("BACKUP_RPC_URL") or os.environ.get("HELIUS_RPC_URL") or _FALLBACK_RPC_URL
        )
        self._refresh_interval_s = refresh_interval_s
        self._percentile = percentile
        self._min_lamports = min_lamports
        self._max_lamports = max_lamports
        self._sample_limit = sample_limit
        self._stale_ttl_s = stale_ttl_s
        self._client = client or httpx.AsyncClient(timeout=10.0)
        self._fee_lamports: int | None = None
        self._fee_fetched_at: float = 0.0
        self._refreshing: bool = False

    async def get_fee_lamports(self) -> int | None:
        """Return the cached 75th-percentile fee, refreshing if stale.

        ``None`` when no fee is known yet, the cache is too stale, or the RPC
        lookup failed — callers then fall back to static fee behavior.
        """
        now = time.monotonic()
        if (
            self._fee_lamports is None
            or now - self._fee_fetched_at >= self._refresh_interval_s
        ):
            await self.refresh()
        if self._fee_lamports is None:
            return None
        if time.monotonic() - self._fee_fetched_at < self._stale_ttl_s:
            return self._fee_lamports
        return None

    @property
    def cached_fee_lamports(self) -> int | None:
        """MT-589: last cached fee value without triggering a refresh.

        Used by the strategy loop's periodic ``dynamic_priority_fee`` log line
        and by the Jito tip calculation.
        """
        return self._fee_lamports

    async def refresh(self) -> None:
        """Force a fee refresh (single-flight — concurrent callers share one).

        MT-589: when the configured RPC fails (timeout, HTTP 429 quota
        exhaustion, transport error), the lookup retries once against the
        public mainnet-beta endpoint so the dynamic fee keeps flowing. A
        failure on both endpoints degrades to ``None`` (static fallback).
        """
        if self._refreshing:
            return
        self._refreshing = True
        try:
            fees = await self._fetch_recent_fees(self._rpc_url)
        except Exception as exc:  # noqa: BLE001 — a fee lookup must never crash a trade
            backup_rpc_url = self._backup_rpc_url
            if self._rpc_url != backup_rpc_url:
                log.warning(
                    "PRIORITY_FEE refresh failed on %s: %s — retrying backup RPC",
                    _redact_url(self._rpc_url), _sanitize_exc(exc),
                )
                try:
                    fees = await self._fetch_recent_fees(backup_rpc_url)
                except Exception as exc2:  # noqa: BLE001
                    log.warning(
                        "PRIORITY_FEE refresh failed on backup RPC too: %s — static fallback",
                        _sanitize_exc(exc2),
                    )
                    fees = []
            else:
                log.warning("PRIORITY_FEE refresh failed: %s — static fallback", _sanitize_exc(exc))
                fees = []
        try:
            if fees:
                fee = self._percentile_fee(fees)
                fee = max(self._min_lamports, min(fee, self._max_lamports))
                self._fee_lamports = fee
                self._fee_fetched_at = time.monotonic()
                log.info(
                    "PRIORITY_FEE lamports=%d p%.0f of %d recent fees (min=%d max=%d)",
                    fee, self._percentile * 100, len(fees), self._min_lamports,
                    self._max_lamports,
                )
            else:
                log.warning(
                    "PRIORITY_FEE refresh returned no fee samples — keeping cached "
                    "value if any; falling back to static fee when none",
                )
        finally:
            self._refreshing = False

    async def _fetch_recent_fees(self, rpc_url: str) -> list[int]:
        """POST ``getRecentPrioritizationFees`` and return fee lamport values."""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": _RPC_FEE_CALL,
            "params": [],
        }
        response = await self._client.post(rpc_url, json=payload)
        response.raise_for_status()
        body = response.json()
        if "error" in body:
            raise RuntimeError(body["error"])
        result = body.get("result")
        if not isinstance(result, list):
            raise RuntimeError(f"unexpected {_RPC_FEE_CALL} result type")
        fees: list[int] = []
        for entry in result[: self._sample_limit]:
            if isinstance(entry, dict):
                try:
                    fees.append(int(entry["prioritizationFee"]))
                except (KeyError, TypeError, ValueError):
                    continue
        return fees

    def _percentile_fee(self, fees: list[int]) -> int:
        """75th percentile of fee samples, per the MT-588 spec."""
        ordered = sorted(fees)
        if not ordered:
            return 0
        index = min(len(ordered) - 1, int((len(ordered) - 1) * self._percentile))
        return ordered[index]

    async def close(self) -> None:
        await self._client.aclose()


FeeCallback = Callable[[], Awaitable[int | None]]
