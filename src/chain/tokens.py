"""Token metadata lookup placeholders."""

from __future__ import annotations

from src.core.models import TokenInfo


async def fetch_token_info(mint_address: str) -> TokenInfo:
    """Return a minimal token record until RPC/enrichment providers are wired."""

    return TokenInfo(mint_address=mint_address)
