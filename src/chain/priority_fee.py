"""Dynamic priority fee provider (MT-588).

Queries the connected RPC's ``getRecentPrioritizationFees`` method and returns
the 75th percentile of recent fees as the priority fee for the next
transaction, replacing the static ``PRIORITY_FEE_LAMPORTS`` constant.

The lookup is cached and refreshed at most every ``refresh_interval_s`` (30s)
instead of every cycle. The result is stored in module-level state by the
caller so it survives across cycles without re-querying.

RPC URL resolution order: ``HELIUS_RPC_URL``, ``PRIMARY_RPC_URL``, then the
public mainnet-beta endpoint. Every failure degrades to ``None`` so callers
fall back to their existing static fee behavior — a fee lookup failure never
blocks a trade.
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Awaitable, Callable

import httpx

log = logging.getLogger("priority_fee")

_DEFAULT_RPC_URL = "https://api.mainnet-beta.solana.com"
_DEFAULT_REFRESH_S = 30.0
_DEFAULT_PERCENTILE = 0.75
_DEFAULT_MIN_LAMPORTS = 1_000
_DEFAULT_MAX_LAMPORTS = 1_000_000
_RPC_FEE_CALL = "getRecentPrioritizationFees"
_SAMPLE_LIMIT = 20


def resolve_rpc_url() -> str:
    """Return the best RPC URL for fee lookups (Helius first, per MT-588)."""
    return (
        os.environ.get("HELIUS_RPC_URL")
        or os.environ.get("PRIMARY_RPC_URL")
        or _DEFAULT_RPC_URL
    )


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

    async def refresh(self) -> None:
        """Force a fee refresh (single-flight — concurrent callers share one)."""
        if self._refreshing:
            return
        self._refreshing = True
        try:
            fees = await self._fetch_recent_fees()
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
        except Exception as exc:  # noqa: BLE001 — a fee lookup must never crash a trade
            log.warning("PRIORITY_FEE refresh failed: %s — static fallback", exc)
        finally:
            self._refreshing = False

    async def _fetch_recent_fees(self) -> list[int]:
        """POST ``getRecentPrioritizationFees`` and return fee lamport values."""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": _RPC_FEE_CALL,
            "params": [],
        }
        response = await self._client.post(self._rpc_url, json=payload)
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
