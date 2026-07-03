"""Minimal async Solana RPC client wrapper."""

from __future__ import annotations

import httpx


class SolanaRpcClient:
    def __init__(self, rpc_url: str, timeout_s: float = 15.0) -> None:
        self.rpc_url = rpc_url
        self._client = httpx.AsyncClient(timeout=timeout_s)

    async def call(self, method: str, params: list[object] | None = None) -> object:
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []}
        response = await self._client.post(self.rpc_url, json=payload)
        response.raise_for_status()
        body = response.json()
        if "error" in body:
            raise RuntimeError(body["error"])
        return body.get("result")

    async def close(self) -> None:
        await self._client.aclose()
