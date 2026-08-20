"""Pump.fun bonding-curve address, state, and quote helpers.

The current Pump ``BondingCurve`` account is an Anchor account with a stable
115-byte payload followed by reserved padding.  The public interface renamed
the SOL reserve fields to quote reserve fields when non-SOL quote assets were
introduced; SOL aliases remain here for callers operating on SOL-paired coins.
"""

from __future__ import annotations

import base64
import struct
from dataclasses import dataclass

import httpx
from solders.pubkey import Pubkey

PUMP_FUN_PROGRAM = Pubkey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
ASSOCIATED_TOKEN_PROGRAM = Pubkey.from_string(
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
)
TOKEN_PROGRAM = Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
TOKEN_2022_PROGRAM = Pubkey.from_string("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
WSOL_MINT = Pubkey.from_string("So11111111111111111111111111111111111111112")
DEFAULT_PUBKEY = Pubkey.from_string("11111111111111111111111111111111")
BONDING_CURVE_DISCRIMINATOR = struct.pack("<Q", 6966180631402821399)


class CurveCompleteError(RuntimeError):
    """Raised when a token has already migrated off its Pump bonding curve."""


@dataclass(frozen=True, slots=True)
class BondingCurveState:
    """Decoded current Pump bonding-curve state; every amount is in raw units."""

    virtual_token_reserves: int
    virtual_quote_reserves: int
    real_token_reserves: int
    real_quote_reserves: int
    token_total_supply: int
    complete: bool
    creator: Pubkey
    is_mayhem_mode: bool
    is_cashback_coin: bool
    quote_mint: Pubkey

    @property
    def virtual_sol_reserves(self) -> int:
        """Compatibility alias for SOL-paired curves."""

        return self.virtual_quote_reserves

    @property
    def real_sol_reserves(self) -> int:
        """Compatibility alias for SOL-paired curves."""

        return self.real_quote_reserves

    @property
    def is_sol_paired(self) -> bool:
        return self.quote_mint == WSOL_MINT


@dataclass(frozen=True, slots=True)
class PumpCurveAccount:
    """A decoded curve together with the token program which owns its mint."""

    address: Pubkey
    state: BondingCurveState
    token_program: Pubkey
    token_decimals: int


def derive_bonding_curve(mint: Pubkey) -> tuple[Pubkey, int]:
    """Derive the Pump bonding-curve PDA from a token mint."""

    return Pubkey.find_program_address([b"bonding-curve", bytes(mint)], PUMP_FUN_PROGRAM)


def derive_associated_bonding_curve(
    mint: Pubkey,
    bonding_curve: Pubkey,
    token_program: Pubkey = TOKEN_PROGRAM,
) -> tuple[Pubkey, int]:
    """Derive the bonding curve's token holding account for ``mint``."""

    return Pubkey.find_program_address(
        [bytes(bonding_curve), bytes(token_program), bytes(mint)],
        ASSOCIATED_TOKEN_PROGRAM,
    )


def derive_associated_token_account(
    owner: Pubkey,
    mint: Pubkey,
    token_program: Pubkey,
) -> tuple[Pubkey, int]:
    """Derive an ATA for either a wallet or PDA owner."""

    return Pubkey.find_program_address(
        [bytes(owner), bytes(token_program), bytes(mint)],
        ASSOCIATED_TOKEN_PROGRAM,
    )


def decode_bonding_curve(data: bytes) -> BondingCurveState:
    """Decode a current Pump ``BondingCurve`` account from Borsh bytes.

    The known data section occupies bytes 0..114; current mainnet accounts are
    151 bytes because the remaining 36 bytes are reserved padding.
    """

    if len(data) < 115:
        raise ValueError(f"bonding curve data is truncated: expected >=115 bytes, got {len(data)}")
    if data[:8] != BONDING_CURVE_DISCRIMINATOR:
        raise ValueError("invalid pump bonding curve discriminator")

    reserves = struct.unpack_from("<QQQQQ", data, 8)
    raw_quote_mint = Pubkey.from_bytes(data[83:115])
    return BondingCurveState(
        virtual_token_reserves=reserves[0],
        virtual_quote_reserves=reserves[1],
        real_token_reserves=reserves[2],
        real_quote_reserves=reserves[3],
        token_total_supply=reserves[4],
        complete=bool(data[48]),
        creator=Pubkey.from_bytes(data[49:81]),
        is_mayhem_mode=bool(data[81]),
        is_cashback_coin=bool(data[82]),
        quote_mint=WSOL_MINT if raw_quote_mint == DEFAULT_PUBKEY else raw_quote_mint,
    )


async def fetch_bonding_curve_state(
    rpc_url: str,
    mint: Pubkey,
    *,
    http_client: httpx.AsyncClient,
) -> PumpCurveAccount:
    """Fetch the curve and mint in one RPC slot, then decode both locally."""

    bonding_curve, _ = derive_bonding_curve(mint)
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getMultipleAccounts",
        "params": [
            [str(bonding_curve), str(mint)],
            {"encoding": "base64", "commitment": "processed"},
        ],
    }
    response = await http_client.post(rpc_url, json=payload)
    response.raise_for_status()
    try:
        values = response.json()["result"]["value"]
        curve_account, mint_account = values
        encoded_curve = curve_account["data"][0]
        token_program = Pubkey.from_string(mint_account["owner"])
        token_decimals = int(base64.b64decode(mint_account["data"][0])[44])
    except (IndexError, KeyError, TypeError, ValueError) as exc:
        raise ValueError("RPC returned malformed Pump curve or mint account data") from exc
    if token_program not in (TOKEN_PROGRAM, TOKEN_2022_PROGRAM):
        raise ValueError(f"unsupported token program {token_program} for mint {mint}")
    return PumpCurveAccount(
        address=bonding_curve,
        state=decode_bonding_curve(base64.b64decode(encoded_curve)),
        token_program=token_program,
        token_decimals=token_decimals,
    )


def calculate_buy_amount(
    sol_amount: int,
    virtual_sol_reserves: int,
    virtual_token_reserves: int,
) -> int:
    """Return raw tokens bought for a raw SOL/quote input using x*y=k."""

    _validate_quote_inputs(sol_amount, virtual_sol_reserves, virtual_token_reserves)
    invariant = virtual_sol_reserves * virtual_token_reserves
    return virtual_token_reserves - invariant // (virtual_sol_reserves + sol_amount)


def calculate_sell_amount(
    token_amount: int,
    virtual_sol_reserves: int,
    virtual_token_reserves: int,
) -> int:
    """Return raw SOL/quote received for raw tokens sold using x*y=k."""

    _validate_quote_inputs(token_amount, virtual_sol_reserves, virtual_token_reserves)
    invariant = virtual_sol_reserves * virtual_token_reserves
    return virtual_sol_reserves - invariant // (virtual_token_reserves + token_amount)


def minimum_output(expected_output: int, slippage_bps: int = 100) -> int:
    """Apply a bounded slippage tolerance to a raw expected output amount."""

    if expected_output <= 0:
        raise ValueError("expected output must be positive")
    if not 0 <= slippage_bps < 10_000:
        raise ValueError("slippage_bps must be between 0 and 9999")
    return max(1, expected_output * (10_000 - slippage_bps) // 10_000)


def maximum_input(expected_input: int, slippage_bps: int = 100) -> int:
    """Apply a bounded slippage tolerance to a raw maximum input amount."""

    if expected_input <= 0:
        raise ValueError("expected input must be positive")
    if not 0 <= slippage_bps < 10_000:
        raise ValueError("slippage_bps must be between 0 and 9999")
    return (expected_input * (10_000 + slippage_bps) + 9_999) // 10_000


def _validate_quote_inputs(amount: int, quote_reserves: int, token_reserves: int) -> None:
    if amount <= 0:
        raise ValueError("amount must be positive")
    if quote_reserves <= 0 or token_reserves <= 0:
        raise ValueError("virtual reserves must be positive")
