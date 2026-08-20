"""Current Pump ``buy_v2`` and ``sell_v2`` instruction builders.

The legacy buy/sell account list is no longer sufficient on mainnet.  These
builders follow Pump's public v2 IDL: buy has 27 accounts, sell has 26, and
both use a discriminator followed by two little-endian u64 values.
"""

from __future__ import annotations

import random
import struct

from solders.instruction import AccountMeta, Instruction
from solders.pubkey import Pubkey
from spl.token.instructions import create_idempotent_associated_token_account

from src.chain.pumpfun import (
    ASSOCIATED_TOKEN_PROGRAM,
    TOKEN_PROGRAM,
    WSOL_MINT,
    BondingCurveState,
    derive_associated_bonding_curve,
    derive_associated_token_account,
)

PUMP_GLOBAL = Pubkey.from_string("4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf")
PUMP_EVENT_AUTHORITY = Pubkey.from_string("Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1")
PUMP_FEE_PROGRAM = Pubkey.from_string("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ")
SYSTEM_PROGRAM = Pubkey.from_string("11111111111111111111111111111111")
PUMP_FUN_PROGRAM = Pubkey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
BUY_V2_DISCRIMINATOR = bytes([184, 23, 238, 97, 103, 197, 211, 61])
SELL_V2_DISCRIMINATOR = bytes([93, 246, 130, 60, 231, 233, 64, 178])

NORMAL_FEE_RECIPIENTS = tuple(
    Pubkey.from_string(value)
    for value in (
        "62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV",
        "7VtfL8fvgNfhz17qKRMjzQEXgbdpnHHHQRh54R9jP2RJ",
        "7hTckgnGnLQR6sdH7YkqFTAA7VwTfYFaZ6EhEsU3saCX",
        "9rPYyANsfQZw3DnDmKE3YCQF5E8oD89UXoHn9JFEhJUz",
        "AVmoTthdrX6tKt4nDjco2D775W2YK3sDhxPcMmzUAmTY",
        "CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM",
        "FWsW1xNtWscwNmKv6wVsU1iTzRN6wmmk3MjxRP5tT7hz",
        "G5UZAVbAf46s7cKWoyKu8kYTip9DGTpbLZ2qa9Aq69dP",
    )
)
RESERVED_FEE_RECIPIENTS = tuple(
    Pubkey.from_string(value)
    for value in (
        "GesfTA3X2arioaHp8bbKdjG9vJtskViWACZoYvxp4twS",
        "4budycTjhs9fD6xw62VBducVTNgMgJJ5BgtKq7mAZwn6",
        "8SBKzEQU4nLSzcwF4a74F2iaUDQyTfjGndn6qUWBnrpR",
        "4UQeTP1T39KZ9Sfxzo3WR5skgsaP6NZa87BAkuazLEKH",
        "8sNeir4QsLsJdYpc9RZacohhK1Y5FLU3nC5LXgYB4aa6",
        "Fh9HmeLNUMVCvejxCtCL2DbYaRyBFVJ5xrWkLnMH6fdk",
        "463MEnMeGyJekNZFQSTUABBEbLnvMTALbT6ZmsxAbAdq",
        "6AUH3WEHucYZyC61hqpqYUWVto5qA5hjHuNQ32GNnNxA",
    )
)
BUYBACK_FEE_RECIPIENTS = tuple(
    Pubkey.from_string(value)
    for value in (
        "5YxQFdt3Tr9zJLvkFccqXVUwhdTWJQc1fFg2YPbxvxeD",
        "9M4giFFMxmFGXtc3feFzRai56WbBqehoSeRE5GK7gf7",
        "GXPFM2caqTtQYC2cJ5yJRi9VDkpsYZXzYdwYpGnLmtDL",
        "3BpXnfJaUTiwXnJNe7Ej1rcbzqTTQUvLShZaWazebsVR",
        "5cjcW9wExnJJiqgLjq7DEG75Pm6JBgE1hNv4B2vHXUW6",
        "EHAAiTxcdDwQ3U4bU6YcMsQGaekdzLS3B5SmYo46kJtL",
        "5eHhjP8JaYkz83CWwvGU2uMUXefd3AazWGx4gpcuEEYD",
        "A7hAgCzFw14fejgCp387JUJRMNyz4j89JKnhtKU8piqW",
    )
)


def build_buy_instructions(
    *,
    mint: Pubkey,
    token_program: Pubkey,
    curve: BondingCurveState,
    user: Pubkey,
    amount: int,
    max_sol_cost: int,
) -> list[Instruction]:
    """Create the buyer ATA idempotently then build a current ``buy_v2`` ix."""

    user_ata, _ = derive_associated_token_account(user, mint, token_program)
    return [
        create_idempotent_associated_token_account(user, user, mint, token_program),
        _build_v2_instruction(
            discriminator=BUY_V2_DISCRIMINATOR,
            mint=mint,
            token_program=token_program,
            curve=curve,
            user=user,
            amount=amount,
            limit=max_sol_cost,
            is_buy=True,
            user_ata=user_ata,
        ),
    ]


def build_sell_instruction(
    *,
    mint: Pubkey,
    token_program: Pubkey,
    curve: BondingCurveState,
    user: Pubkey,
    amount: int,
    min_sol_output: int,
) -> Instruction:
    """Build a current ``sell_v2`` ix for a SOL-paired Pump curve."""

    user_ata, _ = derive_associated_token_account(user, mint, token_program)
    return _build_v2_instruction(
        discriminator=SELL_V2_DISCRIMINATOR,
        mint=mint,
        token_program=token_program,
        curve=curve,
        user=user,
        amount=amount,
        limit=min_sol_output,
        is_buy=False,
        user_ata=user_ata,
    )


def _build_v2_instruction(
    *,
    discriminator: bytes,
    mint: Pubkey,
    token_program: Pubkey,
    curve: BondingCurveState,
    user: Pubkey,
    amount: int,
    limit: int,
    is_buy: bool,
    user_ata: Pubkey,
) -> Instruction:
    if amount <= 0 or limit <= 0:
        raise ValueError("Pump instruction amount and limit must be positive")
    if not curve.is_sol_paired:
        raise ValueError("direct executor currently supports SOL-paired Pump curves only")

    quote_mint = WSOL_MINT
    # The curve PDA owns the base-token ATA, so derive it before its ATA.
    bonding_curve, _ = Pubkey.find_program_address(
        [b"bonding-curve", bytes(mint)],
        PUMP_FUN_PROGRAM,
    )
    associated_base_curve, _ = derive_associated_bonding_curve(mint, bonding_curve, token_program)
    associated_quote_curve, _ = derive_associated_token_account(
        bonding_curve,
        quote_mint,
        TOKEN_PROGRAM,
    )
    creator_vault, _ = Pubkey.find_program_address(
        [b"creator-vault", bytes(curve.creator)],
        PUMP_FUN_PROGRAM,
    )
    user_volume, _ = Pubkey.find_program_address(
        [b"user_volume_accumulator", bytes(user)],
        PUMP_FUN_PROGRAM,
    )
    global_volume, _ = Pubkey.find_program_address(
        [b"global_volume_accumulator"],
        PUMP_FUN_PROGRAM,
    )
    sharing_config, _ = Pubkey.find_program_address(
        [b"sharing-config", bytes(mint)],
        PUMP_FEE_PROGRAM,
    )
    fee_config, _ = Pubkey.find_program_address(
        [b"fee_config", bytes(PUMP_FUN_PROGRAM)],
        PUMP_FEE_PROGRAM,
    )
    fee_recipient = random.choice(
        RESERVED_FEE_RECIPIENTS if curve.is_mayhem_mode else NORMAL_FEE_RECIPIENTS,
    )
    buyback_fee_recipient = random.choice(BUYBACK_FEE_RECIPIENTS)

    def ata(owner: Pubkey, token_mint: Pubkey, program: Pubkey) -> Pubkey:
        return derive_associated_token_account(owner, token_mint, program)[0]

    accounts = [
        (PUMP_GLOBAL, False, False),
        (mint, False, False),
        (quote_mint, False, False),
        (token_program, False, False),
        (TOKEN_PROGRAM, False, False),
        (ASSOCIATED_TOKEN_PROGRAM, False, False),
        (fee_recipient, False, True),
        (ata(fee_recipient, quote_mint, TOKEN_PROGRAM), False, True),
        (buyback_fee_recipient, False, True),
        (ata(buyback_fee_recipient, quote_mint, TOKEN_PROGRAM), False, True),
        (bonding_curve, False, True),
        (associated_base_curve, False, True),
        (associated_quote_curve, False, True),
        (user, True, True),
        (user_ata, False, True),
        (ata(user, quote_mint, TOKEN_PROGRAM), False, True),
        (creator_vault, False, True),
        (ata(creator_vault, quote_mint, TOKEN_PROGRAM), False, True),
        (sharing_config, False, False),
    ]
    if is_buy:
        accounts.append((global_volume, False, False))
    accounts.extend(
        [
            (user_volume, False, True),
            (ata(user_volume, quote_mint, TOKEN_PROGRAM), False, True),
            (fee_config, False, False),
            (PUMP_FEE_PROGRAM, False, False),
            (SYSTEM_PROGRAM, False, False),
            (PUMP_EVENT_AUTHORITY, False, False),
            (PUMP_FUN_PROGRAM, False, False),
        ],
    )
    return Instruction(
        PUMP_FUN_PROGRAM,
        discriminator + struct.pack("<QQ", amount, limit),
        [AccountMeta(pubkey, signer, writable) for pubkey, signer, writable in accounts],
    )
