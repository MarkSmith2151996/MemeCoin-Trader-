"""Inspect and locally build a current Pump.fun direct trade.

The default path only reads RPC state and serializes a test transaction with an
ephemeral generated keypair. Passing ``--live`` is the sole route
which loads the configured wallet and submits a 0.001 SOL buy.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import os
import sys
from pathlib import Path

import httpx
from solders.hash import Hash
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.transaction import Transaction

# Allow ``python scripts/test_direct_executor.py`` from any working directory.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.chain.pumpfun import (  # noqa: E402
    calculate_buy_amount,
    derive_associated_bonding_curve,
    fetch_bonding_curve_state,
    maximum_input,
    minimum_output,
)
from src.chain.pumpfun_tx import build_buy_instructions  # noqa: E402
from src.execution.direct import DEFAULT_RPC_URL, DirectExecutor  # noqa: E402


async def inspect(mint_address: str, *, live: bool) -> None:
    rpc_url = os.environ.get("PRIMARY_RPC_URL") or DEFAULT_RPC_URL
    mint = Pubkey.from_string(mint_address)
    async with httpx.AsyncClient(timeout=15.0) as client:
        account = await fetch_bonding_curve_state(rpc_url, mint, http_client=client)

    associated_curve, _ = derive_associated_bonding_curve(
        mint,
        account.address,
        account.token_program,
    )
    print(f"bonding_curve={account.address}")
    print(f"bonding_curve_ata={associated_curve}")
    print(f"token_program={account.token_program} decimals={account.token_decimals}")
    print(f"complete={account.state.complete} sol_paired={account.state.is_sol_paired}")
    print(
        "reserves "
        f"virtual_token={account.state.virtual_token_reserves} "
        f"virtual_sol={account.state.virtual_sol_reserves} "
        f"real_token={account.state.real_token_reserves} "
        f"real_sol={account.state.real_sol_reserves}",
    )
    if account.state.complete:
        print("curve is complete; no direct bonding-curve transaction was built")
        return
    if not account.state.is_sol_paired:
        print("curve has a non-SOL quote asset; the SOL-only executor will not build a transaction")
        return

    spend_lamports = 50_000_000
    expected = calculate_buy_amount(
        spend_lamports,
        account.state.virtual_sol_reserves,
        account.state.virtual_token_reserves,
    )
    minimum = minimum_output(expected)
    maximum = maximum_input(spend_lamports)
    print(
        f"quote 0.05 SOL: expected_raw={expected} min_raw={minimum} max_cost={maximum}",
    )
    ephemeral = Keypair()
    instructions = build_buy_instructions(
        mint=mint,
        token_program=account.token_program,
        curve=account.state,
        user=ephemeral.pubkey(),
        amount=minimum,
        max_sol_cost=maximum,
    )
    transaction = Transaction.new_signed_with_payer(
        instructions,
        ephemeral.pubkey(),
        [ephemeral],
        Hash.from_string("11111111111111111111111111111111"),
    )
    serialized = base64.b64encode(bytes(transaction)).decode("ascii")
    print(f"buy_v2_accounts={len(instructions[-1].accounts)} serialized={serialized}")

    if live:
        executor = DirectExecutor(rpc_url=rpc_url)
        try:
            trade = await executor.buy(mint_address, 0.001)
        finally:
            await executor.close()
        print(f"LIVE buy confirmed signature={trade.tx_signature} tokens={trade.token_amount}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mint", help="active Pump.fun token mint")
    parser.add_argument("--live", action="store_true", help="submit a real 0.001 SOL buy")
    args = parser.parse_args()
    asyncio.run(inspect(args.mint, live=args.live))


if __name__ == "__main__":
    main()
