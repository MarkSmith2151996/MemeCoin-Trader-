"""Benchmark the direct Pump.fun buy path without submitting a transaction.

The script only calls read-only RPC methods and ``simulateTransaction``. It
never calls ``sendTransaction`` or submits a Jito bundle, so no SOL is spent.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import math
import os
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import httpx
import websockets
from dotenv import load_dotenv
from solders.hash import Hash
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.transaction import Transaction

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
load_dotenv(REPO_ROOT / ".env")

from src.chain.pumpfun import (  # noqa: E402
    calculate_buy_amount,
    derive_bonding_curve,
    fetch_bonding_curve_state,
    maximum_input,
    minimum_output,
)
from src.chain.pumpfun_tx import build_buy_instructions  # noqa: E402

DEFAULT_RPC_URL = "https://api.mainnet-beta.solana.com"
JITO_BUNDLES_URL = "https://mainnet.block-engine.jito.wtf/api/v1/bundles"
PUMPPORTAL_WS_URL = "wss://pumpportal.fun/api/data"
STEP_NAMES = (
    "PDA derivation",
    "Curve state read",
    "Price calculation",
    "Blockhash fetch",
    "Tx build + sign",
    "Simulate",
    "TOTAL",
)


@dataclass(frozen=True)
class TimingSummary:
    minimum_ms: float
    median_ms: float
    p95_ms: float
    p99_ms: float
    maximum_ms: float


def _elapsed_ms(start_ns: int) -> float:
    return (time.perf_counter_ns() - start_ns) / 1_000_000


def _summarize(samples: list[float]) -> TimingSummary:
    if not samples:
        raise ValueError("cannot summarize an empty timing list")
    ordered = sorted(samples)

    def percentile(percent: float) -> float:
        return ordered[math.ceil(percent * len(ordered)) - 1]

    midpoint = len(ordered) // 2
    median = (
        ordered[midpoint]
        if len(ordered) % 2
        else (ordered[midpoint - 1] + ordered[midpoint]) / 2
    )
    return TimingSummary(
        minimum_ms=ordered[0],
        median_ms=median,
        p95_ms=percentile(0.95),
        p99_ms=percentile(0.99),
        maximum_ms=ordered[-1],
    )


def _load_keypair() -> Keypair:
    secret = os.environ.get("WALLET_PRIVATE_KEY", "")
    if not secret:
        raise RuntimeError(
            "WALLET_PRIVATE_KEY is required to simulate a realistic signed transaction",
        )
    try:
        decoded = base64.b64decode(secret)
    except Exception as exc:
        raise RuntimeError("WALLET_PRIVATE_KEY is not valid base64") from exc
    if len(decoded) != 64:
        raise RuntimeError("WALLET_PRIVATE_KEY must decode to 64 bytes")
    return Keypair.from_bytes(decoded)


async def _rpc(client: httpx.AsyncClient, rpc_url: str, method: str, params: list[Any]) -> Any:
    response = await client.post(
        rpc_url,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
    )
    response.raise_for_status()
    payload = response.json()
    if "error" in payload:
        raise RuntimeError(f"RPC {method} failed: {payload['error']}")
    return payload["result"]


async def _wallet_balance(client: httpx.AsyncClient, rpc_url: str, wallet: Pubkey) -> int:
    result = await _rpc(client, rpc_url, "getBalance", [str(wallet), {"commitment": "confirmed"}])
    return int(result["value"])


async def _latest_blockhash(client: httpx.AsyncClient, rpc_url: str) -> Hash:
    result = await _rpc(client, rpc_url, "getLatestBlockhash", [{"commitment": "confirmed"}])
    return Hash.from_string(result["value"]["blockhash"])


async def _discover_active_mint(
    client: httpx.AsyncClient,
    rpc_url: str,
    timeout_s: float,
) -> Pubkey:
    """Subscribe to fresh Pump launches until a usable SOL bonding curve appears."""

    deadline = time.monotonic() + timeout_s
    async with websockets.connect(PUMPPORTAL_WS_URL, open_timeout=15) as socket:
        await socket.send(json.dumps({"method": "subscribeNewToken"}))
        while remaining := deadline - time.monotonic():
            try:
                raw_message = await asyncio.wait_for(socket.recv(), timeout=remaining)
                candidate = Pubkey.from_string(json.loads(raw_message)["mint"])
                account = await fetch_bonding_curve_state(rpc_url, candidate, http_client=client)
            except (TimeoutError, KeyError, TypeError, ValueError, httpx.HTTPError):
                continue
            if not account.state.complete and account.state.is_sol_paired:
                return candidate
    raise RuntimeError(f"no active SOL Pump bonding curve found within {timeout_s:.0f}s")


async def _simulate(client: httpx.AsyncClient, rpc_url: str, serialized_tx: bytes) -> None:
    result = await _rpc(
        client,
        rpc_url,
        "simulateTransaction",
        [
            base64.b64encode(serialized_tx).decode("ascii"),
            {
                "encoding": "base64",
                "commitment": "confirmed",
                "replaceRecentBlockhash": True,
            },
        ],
    )
    value = result.get("value", {})
    if value.get("err") is not None:
        raise RuntimeError(f"simulateTransaction returned an error: {value['err']}")


async def _benchmark_rpc(
    client: httpx.AsyncClient,
    rpc_url: str,
    mint: Pubkey,
    keypair: Keypair,
    runs: int,
) -> dict[str, list[float]]:
    timings = {step: [] for step in STEP_NAMES}
    spend_lamports = 1_000_000

    for _ in range(runs):
        total_started = time.perf_counter_ns()

        started = time.perf_counter_ns()
        curve_address, _ = derive_bonding_curve(mint)
        timings["PDA derivation"].append(_elapsed_ms(started))

        started = time.perf_counter_ns()
        account = await fetch_bonding_curve_state(rpc_url, mint, http_client=client)
        timings["Curve state read"].append(_elapsed_ms(started))
        if account.address != curve_address:
            raise RuntimeError("curve address changed during benchmark")
        if account.state.complete or not account.state.is_sol_paired:
            raise RuntimeError("selected Pump curve is no longer an active SOL bonding curve")

        started = time.perf_counter_ns()
        expected_tokens = calculate_buy_amount(
            spend_lamports,
            account.state.virtual_sol_reserves,
            account.state.virtual_token_reserves,
        )
        minimum_tokens = minimum_output(expected_tokens, 300)
        max_sol_cost = maximum_input(spend_lamports, 300)
        timings["Price calculation"].append(_elapsed_ms(started))

        started = time.perf_counter_ns()
        blockhash = await _latest_blockhash(client, rpc_url)
        timings["Blockhash fetch"].append(_elapsed_ms(started))

        started = time.perf_counter_ns()
        instructions = build_buy_instructions(
            mint=mint,
            token_program=account.token_program,
            curve=account.state,
            user=keypair.pubkey(),
            amount=minimum_tokens,
            max_sol_cost=max_sol_cost,
        )
        transaction = Transaction.new_signed_with_payer(
            instructions,
            keypair.pubkey(),
            [keypair],
            blockhash,
        )
        timings["Tx build + sign"].append(_elapsed_ms(started))

        started = time.perf_counter_ns()
        await _simulate(client, rpc_url, bytes(transaction))
        timings["Simulate"].append(_elapsed_ms(started))
        timings["TOTAL"].append(_elapsed_ms(total_started))

    return timings


async def _jito_ping(client: httpx.AsyncClient) -> float:
    started = time.perf_counter_ns()
    response = await client.head(JITO_BUNDLES_URL)
    # A response such as 405 still measures reachability and does not submit a bundle.
    if response.status_code >= 500:
        response.raise_for_status()
    return _elapsed_ms(started)


def _print_table(label: str, timings: dict[str, list[float]]) -> None:
    print(f"\nRPC: {label}")
    print("Step                 | Min     | Median  | P95     | P99     | Max")
    print("-------------------- | ------- | ------- | ------- | ------- | -------")
    for step in STEP_NAMES:
        stats = _summarize(timings[step])
        print(
            f"{step:<20} | {stats.minimum_ms:>6.2f}ms | {stats.median_ms:>6.2f}ms | "
            f"{stats.p95_ms:>6.2f}ms | {stats.p99_ms:>6.2f}ms | {stats.maximum_ms:>6.2f}ms",
        )


async def run(args: argparse.Namespace) -> None:
    keypair = _load_keypair()
    primary_rpc = os.environ.get("SOLANA_RPC_URL") or DEFAULT_RPC_URL
    rpc_urls = [primary_rpc]
    alternate_rpc = os.environ.get("SOLANA_RPC_URL_ALT")
    if alternate_rpc and alternate_rpc != primary_rpc:
        rpc_urls.append(alternate_rpc)

    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        mint = Pubkey.from_string(args.mint) if args.mint else await _discover_active_mint(
            client,
            primary_rpc,
            args.discovery_timeout,
        )
        print(f"mint={mint} runs={args.runs} amount=0.001 SOL slippage=300bps")
        print("Safety: simulateTransaction only; no sendTransaction or Jito bundle submission.")

        results: dict[str, dict[str, list[float]]] = {}
        balances: dict[str, dict[str, int]] = {}
        for rpc_url in rpc_urls:
            before = await _wallet_balance(client, rpc_url, keypair.pubkey())
            timings = await _benchmark_rpc(client, rpc_url, mint, keypair, args.runs)
            after = await _wallet_balance(client, rpc_url, keypair.pubkey())
            if after != before:
                raise RuntimeError(
                    f"wallet balance changed during simulation: before={before}, after={after}",
                )
            results[rpc_url] = timings
            balances[rpc_url] = {"before_lamports": before, "after_lamports": after}
            _print_table(rpc_url, timings)

        jito_ping_ms = await _jito_ping(client)

    print(f"\nJito ping             | {jito_ping_ms:.2f}ms (one-shot HEAD; no bundle submitted)")
    output = {
        "mint": str(mint),
        "runs": args.runs,
        "amount_lamports": 1_000_000,
        "slippage_bps": 300,
        "wallet_pubkey": str(keypair.pubkey()),
        "wallet_balances": balances,
        "jito_ping_ms": jito_ping_ms,
        "results_ms": results,
        "summaries_ms": {
            rpc_url: {step: asdict(_summarize(samples)) for step, samples in timings.items()}
            for rpc_url, timings in results.items()
        },
    }
    output_path = REPO_ROOT / "data" / "benchmark_results.json"
    output_path.write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")
    print(f"Raw timings saved to {output_path.relative_to(REPO_ROOT)}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mint",
        help="active SOL-paired Pump.fun mint; otherwise discover a fresh token",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=100,
        help="iterations per configured RPC (default: 100)",
    )
    parser.add_argument(
        "--discovery-timeout",
        type=float,
        default=120,
        help="seconds to wait for PumpPortal discovery (default: 120)",
    )
    args = parser.parse_args()
    if args.runs <= 0:
        parser.error("--runs must be positive")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
