from __future__ import annotations

import asyncio
import base64
import json
import struct

import httpx
import pytest
from solders.keypair import Keypair
from solders.pubkey import Pubkey

from src.chain.jito import JITO_TIP_ACCOUNTS, JitoBlockEngineClient
from src.chain.pumpfun import (
    BONDING_CURVE_DISCRIMINATOR,
    DEFAULT_PUBKEY,
    TOKEN_2022_PROGRAM,
    WSOL_MINT,
    calculate_buy_amount,
    calculate_sell_amount,
    decode_bonding_curve,
    derive_associated_bonding_curve,
    derive_bonding_curve,
    maximum_input,
    minimum_output,
)
from src.chain.pumpfun_tx import (
    BUY_V2_DISCRIMINATOR,
    BUYBACK_FEE_RECIPIENTS,
    NORMAL_FEE_RECIPIENTS,
    SELL_V2_DISCRIMINATOR,
    build_buy_instructions,
    build_sell_instruction,
)
from src.execution.direct import DirectExecutor, _token_delta

MINT = Pubkey.from_string("CU7nUQaJ4beyYjC3xAUrh5RiSjw14fhU6oWTwRBse8gj")
CREATOR = Pubkey.from_string("5wyFsNExysbXf2hTtcn8Tqd3urs9Nv85Zx1zNdAfTMmX")


def _curve_data(*, complete: bool = False) -> bytes:
    data = bytearray(151)
    data[:8] = BONDING_CURVE_DISCRIMINATOR
    struct.pack_into("<QQQQQ", data, 8, 10_000, 1_000, 9_000, 900, 1_000_000)
    data[48] = complete
    data[49:81] = bytes(CREATOR)
    data[83:115] = bytes(DEFAULT_PUBKEY)
    return bytes(data)


def test_curve_decode_preserves_current_fields_and_sol_aliases() -> None:
    curve = decode_bonding_curve(_curve_data())

    assert curve.virtual_token_reserves == 10_000
    assert curve.virtual_quote_reserves == curve.virtual_sol_reserves == 1_000
    assert curve.real_quote_reserves == curve.real_sol_reserves == 900
    assert curve.creator == CREATOR
    assert curve.quote_mint == WSOL_MINT
    assert curve.is_sol_paired is True


def test_curve_decode_rejects_truncated_or_wrong_data() -> None:
    with pytest.raises(ValueError, match="truncated"):
        decode_bonding_curve(b"short")
    with pytest.raises(ValueError, match="discriminator"):
        decode_bonding_curve(b"x" * 151)


def test_pda_derivation_is_local_and_ata_depends_on_token_program() -> None:
    curve, bump = derive_bonding_curve(MINT)
    ata, _ = derive_associated_bonding_curve(MINT, curve, TOKEN_2022_PROGRAM)

    assert bump >= 0
    assert str(curve) == "7TW1gobQyM7WoigqNa4Dvc2SoPep1XwmPpG5xRcGzQTC"
    assert ata != curve


def test_constant_product_quotes_and_slippage() -> None:
    assert calculate_buy_amount(100, 1_000, 10_000) == 910
    assert calculate_sell_amount(1_000, 1_000, 10_000) == 91
    assert minimum_output(10_000, 100) == 9_900
    assert maximum_input(10_000, 100) == 10_100


def test_v2_buy_and_sell_instructions_match_current_layout() -> None:
    curve = decode_bonding_curve(_curve_data())
    user = Keypair().pubkey()
    buy = build_buy_instructions(
        mint=MINT,
        token_program=TOKEN_2022_PROGRAM,
        curve=curve,
        user=user,
        amount=500,
        max_sol_cost=1_000,
    )[-1]
    sell = build_sell_instruction(
        mint=MINT,
        token_program=TOKEN_2022_PROGRAM,
        curve=curve,
        user=user,
        amount=500,
        min_sol_output=50,
    )

    assert len(buy.accounts) == 27
    assert len(sell.accounts) == 26
    assert buy.data[:8] == BUY_V2_DISCRIMINATOR
    assert sell.data[:8] == SELL_V2_DISCRIMINATOR
    assert struct.unpack("<QQ", buy.data[8:]) == (500, 1_000)
    assert struct.unpack("<QQ", sell.data[8:]) == (500, 50)
    assert buy.accounts[6].pubkey in NORMAL_FEE_RECIPIENTS
    assert buy.accounts[8].pubkey in BUYBACK_FEE_RECIPIENTS
    assert [meta.pubkey for meta in buy.accounts if meta.is_signer] == [user]


def test_transaction_fill_uses_wallet_owned_token_balances_only() -> None:
    meta = {
        "preTokenBalances": [
            {"mint": "mint", "owner": "wallet", "uiTokenAmount": {"amount": "5"}},
        ],
        "postTokenBalances": [
            {"mint": "mint", "owner": "wallet", "uiTokenAmount": {"amount": "11"}},
            {"mint": "mint", "owner": "other", "uiTokenAmount": {"amount": "99"}},
        ],
    }
    assert _token_delta(meta, "mint", "wallet") == 6


def test_jito_tip_accounts_are_current_valid_pubkeys() -> None:
    assert len(JITO_TIP_ACCOUNTS) == 8
    assert all(Pubkey.from_string(account) for account in JITO_TIP_ACCOUNTS)


def test_direct_buy_falls_back_to_rpc_after_jito_rejection() -> None:
    async def run() -> None:
        calls = {"token_balance": 0, "jito": 0, "rpc_send": 0}
        mint_data = bytearray(82)
        mint_data[44] = 6
        keypair = Keypair()

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.host == "mainnet.block-engine.jito.wtf":
                calls["jito"] += 1
                return httpx.Response(503, json={"error": "busy"})
            method = json.loads(request.content)["method"]
            if method == "getMultipleAccounts":
                return httpx.Response(
                    200,
                    json={
                        "result": {
                            "value": [
                                {"data": [base64.b64encode(_curve_data()).decode(), "base64"]},
                                {
                                    "owner": str(TOKEN_2022_PROGRAM),
                                    "data": [base64.b64encode(mint_data).decode(), "base64"],
                                },
                            ],
                        },
                    },
                )
            if method == "getTokenAccountsByOwner":
                calls["token_balance"] += 1
                accounts = []
                if calls["token_balance"] == 2:
                    accounts = [
                        {
                            "account": {
                                "data": {
                                    "parsed": {"info": {"tokenAmount": {"amount": "500"}}},
                                },
                            },
                        },
                    ]
                return httpx.Response(200, json={"result": {"value": accounts}})
            if method == "getLatestBlockhash":
                return httpx.Response(
                    200,
                    json={"result": {"value": {"blockhash": "11111111111111111111111111111111"}}},
                )
            if method == "sendTransaction":
                calls["rpc_send"] += 1
                return httpx.Response(200, json={"result": "rpc-signature"})
            if method == "getSignatureStatuses":
                return httpx.Response(
                    200,
                    json={
                        "result": {
                            "value": [{"err": None, "confirmationStatus": "confirmed", "slot": 7}],
                        },
                    },
                )
            if method == "getTransaction":
                return httpx.Response(
                    200,
                    json={
                        "result": {
                            "meta": {
                                "preTokenBalances": [],
                                "postTokenBalances": [
                                    {
                                        "mint": str(MINT),
                                        "owner": str(keypair.pubkey()),
                                        "uiTokenAmount": {"amount": "500"},
                                    },
                                ],
                                "preBalances": [2_000_000],
                                "postBalances": [995_000],
                                "fee": 5_000,
                            },
                        },
                    },
                )
            raise AssertionError(method)

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        jito = JitoBlockEngineClient(http_client=client)
        executor = DirectExecutor(
            rpc_url="https://rpc.example",
            keypair=keypair,
            http_client=client,
            jito_client=jito,
            use_jito=True,
            poll_interval_s=0.001,
        )
        try:
            trade = await executor.buy(str(MINT), 0.001)
        finally:
            await executor.close()
            await client.aclose()

        assert calls == {"token_balance": 1, "jito": 1, "rpc_send": 1}
        assert trade.tx_signature == "rpc-signature"
        assert trade.token_amount == pytest.approx(0.0005)
        assert trade.metadata["jito"] is False
        assert trade.metadata["transaction_size_bytes"] < 1_232

    asyncio.run(run())
