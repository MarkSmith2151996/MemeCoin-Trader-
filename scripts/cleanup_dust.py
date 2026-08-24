#!/usr/bin/env python3
"""Sell or burn SPL token dust below one US cent to reclaim account rent.

The default is a read-only report. Pass ``--confirm`` to submit Jupiter sells
and burn only balances that Jupiter cannot quote or sell. This command never
touches positions tracked by the strategy database.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

import httpx
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.chain.jupiter_swap import JupiterSwapClient, TokenAccountBalance  # noqa: E402
from src.execution.live import LiveExecutionAdapter  # noqa: E402
from src.execution.price_provider import JUPITER_PRICE_URL, WRAPPED_SOL_MINT  # noqa: E402

DUST_USD_LIMIT = 0.01


async def _usd_value(http: httpx.AsyncClient, account: TokenAccountBalance) -> float | None:
    response = await http.get(
        JUPITER_PRICE_URL,
        params={"ids": f"{account.mint},{WRAPPED_SOL_MINT}"},
    )
    response.raise_for_status()
    data = response.json()
    token = data.get(account.mint)
    if not isinstance(token, dict):
        return None
    try:
        price = float(token["usdPrice"])
    except (KeyError, TypeError, ValueError):
        return None
    if price <= 0:
        return None
    return price * account.raw_amount / 10**account.decimals


async def run(confirm: bool) -> int:
    load_dotenv(ROOT / ".env")
    client = JupiterSwapClient()
    adapter = LiveExecutionAdapter(client=client, max_price_impact_pct=100.0)
    accounts = await client.get_token_accounts()
    if accounts is None:
        print("Unable to read wallet token accounts.")
        return 1

    dust: list[TokenAccountBalance] = []
    async with httpx.AsyncClient(timeout=10.0) as http:
        for account in accounts:
            try:
                value = await _usd_value(http, account)
            except (httpx.HTTPError, ValueError):
                value = None
            if value is not None and value < DUST_USD_LIMIT:
                dust.append(account)
                print(f"DUST mint={account.mint} value_usd=${value:.6f}")

    if not dust:
        print("No priceable SPL dust below $0.01 found.")
        await client.close()
        return 0
    if not confirm:
        print(f"Found {len(dust)} dust account(s). Re-run with --confirm to sell or burn them.")
        await client.close()
        return 0

    for account in dust:
        amount = account.raw_amount / 10**account.decimals
        try:
            trade = await adapter.sell(account.mint, amount, slippage_bps=1000)
            print(f"SOLD mint={account.mint} signature={trade.tx_signature or '-'}")
            continue
        except Exception as exc:
            print(f"UNSELLABLE mint={account.mint} error={exc}; attempting burn")
        signature = await client.burn_token_account(account)
        print(
            f"{'BURNED' if signature else 'BURN_FAILED'} mint={account.mint} "
            f"signature={signature or '-'}",
        )
    await adapter.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--confirm", action="store_true", help="Submit sells and burns")
    args = parser.parse_args()
    return asyncio.run(run(args.confirm))


if __name__ == "__main__":
    raise SystemExit(main())
