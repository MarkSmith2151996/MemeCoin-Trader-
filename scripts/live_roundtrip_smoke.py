"""Execute one explicit micro-live Jupiter buy and full wallet-balance exit."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.chain.jupiter_swap import JupiterSwapClient
from src.execution.live import LiveExecutionAdapter

SMOKE_AMOUNT_SOL = 0.001


async def run(mint: str, *, sell_only: bool) -> None:
    client = JupiterSwapClient()
    adapter = LiveExecutionAdapter(client=client)
    try:
        if sell_only:
            balance = await client.get_token_balance(mint)
            if balance is None or balance <= 0:
                raise RuntimeError(f"wallet holds no sellable balance for {mint}")
            sell = await adapter.sell(mint, balance, slippage_bps=300)
            balance_after = sell.metadata.get("token_balance_after") if sell.metadata else None
            print(
                "LIVE SELL-ONLY OK "
                f"mint={mint} sell_signature={sell.tx_signature} "
                f"token_balance_after={balance_after}",
            )
            return
        buy = await adapter.buy(mint, SMOKE_AMOUNT_SOL)
        if buy.token_amount is None or buy.token_amount <= 0:
            raise RuntimeError("live buy returned no token fill")
        print(f"LIVE BUY CONFIRMED mint={mint} buy_signature={buy.tx_signature}", flush=True)
        sell = await adapter.sell(mint, buy.token_amount, slippage_bps=300)
        balance_after = sell.metadata.get("token_balance_after") if sell.metadata else None
        print(
            "LIVE ROUNDTRIP OK "
            f"mint={mint} buy_signature={buy.tx_signature} "
            f"sell_signature={sell.tx_signature} token_balance_after={balance_after}",
        )
    finally:
        await adapter.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mint", required=True, help="Liquid Solana token mint to buy and sell")
    parser.add_argument("--confirm", action="store_true", help="Authorize the 0.001 SOL live round trip")
    parser.add_argument("--sell-only", action="store_true", help="Sell the mint's existing wallet balance")
    args = parser.parse_args()

    load_dotenv()
    if not args.confirm:
        raise SystemExit("refusing live round trip without --confirm")
    if os.environ.get("EXECUTION_MODE", "paper").strip().lower() != "live":
        raise SystemExit("refusing live round trip unless EXECUTION_MODE=live")

    asyncio.run(run(args.mint, sell_only=args.sell_only))


if __name__ == "__main__":
    main()
