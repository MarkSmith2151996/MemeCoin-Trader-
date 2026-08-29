"""Coverage: Jupiter Swap API live client (quote, sign, send, confirm, retry, reconcile).

All tests use httpx.MockTransport — no real network calls.
"""

from __future__ import annotations

import asyncio
import base64
import json

import httpx
import pytest
from solders.hash import Hash
from solders.instruction import CompiledInstruction
from solders.keypair import Keypair
from solders.message import MessageHeader, MessageV0, to_bytes_versioned
from solders.pubkey import Pubkey
from solders.signature import Signature
from solders.transaction import VersionedTransaction

from src.chain.jupiter import SOL_MINT
from src.chain.jupiter_swap import (
    MAX_JITO_TIP_LAMPORTS,
    MIN_JITO_TIP_LAMPORTS,
    JupiterSwapClient,
    JupiterSwapQuote,
)

TOKEN_MINT = "tok12345678901234567890123456789012"
WSOL_MINT = SOL_MINT
RPC = "https://rpc.example"
DECIMALS = 6
RPC_DECIMALS_BODY = {"jsonrpc": "2.0", "id": 1, "result": {"value": {"decimals": DECIMALS}}}
RPC_DECIMALS_9_BODY = {"jsonrpc": "2.0", "id": 1, "result": {"value": {"decimals": 9}}}
RPC_BALANCE_BODY = {"jsonrpc": "2.0", "id": 1, "result": {"value": 49000000}}


def _make_keypair() -> Keypair:
    return Keypair()


def _signed_tx_b64(keypair: Keypair) -> str:
    ix = CompiledInstruction(program_id_index=1, accounts=bytes([0]), data=b"")
    msg = MessageV0(
        header=MessageHeader(
            num_required_signatures=1,
            num_readonly_signed_accounts=0,
            num_readonly_unsigned_accounts=1,
        ),
        account_keys=[keypair.pubkey(), Pubkey.from_string(WSOL_MINT)],
        recent_blockhash=Hash.from_string("11111111111111111111111111111111"),
        instructions=[ix],
        address_table_lookups=[],
    )
    unsigned = VersionedTransaction.populate(msg, [keypair.sign_message(to_bytes_versioned(msg))])
    return base64.b64encode(bytes(unsigned)).decode()


def _quote_response(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "inputMint": WSOL_MINT,
        "outputMint": TOKEN_MINT,
        "inAmount": "50000000",
        "outAmount": "1000000",
        "priceImpactPct": "0.001",
        "routePlan": [{"swapInfo": {"marketInfos": []}, "percent": 100}],
    }
    payload.update(overrides)
    return payload


def _make_client(
    handler,
    keypair: Keypair | None = None,
    *,
    priority_fee_callback=None,
) -> JupiterSwapClient:
    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    return JupiterSwapClient(
        base_url="https://api.jup.ag",
        solana_rpc_url=RPC,
        http_client=client,
        keypair=keypair or _make_keypair(),
        api_key="test-key",
        confirm_timeout_s=0.5,
        poll_interval_s=0.01,
        max_retries=2,
        priority_fee_callback=priority_fee_callback,
        use_jito_bundles=False,
    )


def test_get_wallet_holdings_returns_positive_balances_by_mint() -> None:
    def account(mint: str, amount: str, decimals: int) -> dict:
        return {
            "account": {
                "data": {
                    "parsed": {
                        "info": {
                            "mint": mint,
                            "tokenAmount": {"amount": amount, "decimals": decimals},
                        },
                    },
                },
            },
        }

    requested_programs: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        assert payload["method"] == "getTokenAccountsByOwner"
        program_id = payload["params"][1]["programId"]
        requested_programs.append(program_id)
        values = (
            [
                account(TOKEN_MINT, "1250000", 6),
                account(TOKEN_MINT, "250000", 6),
                account("zero-mint", "0", 6),
            ]
            if program_id == "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
            else [account("token-2022-mint", "2000000", 6)]
        )
        return httpx.Response(
            200,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "result": {
                    "value": values,
                },
            },
        )

    async def run() -> None:
        client = _make_client(handler)
        try:
            holdings = await client.get_wallet_holdings()
        finally:
            await client.close()
        assert holdings == {TOKEN_MINT: 1.5, "token-2022-mint": 2.0}
        assert requested_programs == [
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
        ]

    asyncio.run(run())


def test_get_token_accounts_includes_classic_and_token_2022() -> None:
    def account(address: str, mint: str, amount: str) -> dict:
        return {
            "pubkey": address,
            "account": {
                "data": {
                    "parsed": {
                        "info": {
                            "mint": mint,
                            "tokenAmount": {"amount": amount, "decimals": 6},
                        },
                    },
                },
            },
        }

    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        program_id = payload["params"][1]["programId"]
        value = (
            [account("classic-account", TOKEN_MINT, "1000000")]
            if program_id == "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
            else [account("token-2022-account", "token-2022-mint", "2000000")]
        )
        return httpx.Response(
            200,
            json={"jsonrpc": "2.0", "id": 1, "result": {"value": value}},
        )

    async def run() -> None:
        client = _make_client(handler)
        try:
            accounts = await client.get_token_accounts()
        finally:
            await client.close()
        assert accounts is not None
        assert [(item.address, item.mint, item.program_id) for item in accounts] == [
            (
                "classic-account",
                TOKEN_MINT,
                "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            ),
            (
                "token-2022-account",
                "token-2022-mint",
                "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
            ),
        ]

    asyncio.run(run())


def test_quote_happy_path_sends_api_key() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json=RPC_DECIMALS_BODY)
        captured["url"] = str(request.url)
        captured["headers"] = dict(request.headers)
        return httpx.Response(200, json=_quote_response())

    client = _make_client(handler)

    quote = asyncio.run(client.get_quote(WSOL_MINT, TOKEN_MINT, 50_000_000))

    assert quote is not None
    assert quote.in_amount == 50_000_000
    assert quote.out_amount == 1_000_000
    assert quote.price_impact_pct == pytest.approx(0.001)
    assert quote.token_decimals == 6
    assert quote.input_mint == WSOL_MINT
    assert quote.output_mint == TOKEN_MINT
    url = str(captured["url"])
    assert "/swap/v1/quote" in url
    assert "inputMint=So11111111111111111111111111111111111111112" in url
    assert "amount=50000000" in url
    assert "slippageBps=100" in url
    assert "dynamicSlippage=true" in url
    headers = captured["headers"]
    assert headers.get("x-api-key") == "test-key"


def test_quote_price_sol_buy_direction() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json=RPC_DECIMALS_BODY)
        return httpx.Response(200, json=_quote_response())

    client = _make_client(handler)

    quote = asyncio.run(client.get_quote(WSOL_MINT, TOKEN_MINT, 50_000_000))

    assert quote is not None
    # 0.05 SOL in -> 1,000,000 (6-dec) tokens out => 0.05 SOL per 1.0 token
    assert quote.price_sol == pytest.approx(0.05)


def test_quote_falls_back_from_quicknode_metis_to_public_jupiter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    keypair = _make_keypair()
    monkeypatch.setenv("QUICKNODE_RPC_URL", "https://quicknode.example")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "quicknode.example":
            assert request.url.path == "/quote"
            return httpx.Response(503, json={"error": "unavailable"})
        if request.url.host == "public.jupiterapi.com":
            assert request.url.path == "/quote"
            return httpx.Response(200, json=_quote_response())
        payload = json.loads(request.content)
        assert payload["method"] == "getTokenSupply"
        return httpx.Response(200, json=RPC_DECIMALS_BODY)

    client = JupiterSwapClient(
        solana_rpc_url=RPC,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        keypair=keypair,
        api_key="test-key",
        use_jito_bundles=False,
    )
    quote = asyncio.run(client.get_quote(WSOL_MINT, TOKEN_MINT, 50_000_000))
    asyncio.run(client.close())

    assert quote is not None
    assert quote.swap_api_base_url == "https://public.jupiterapi.com"


def test_quote_sell_direction_price() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json=RPC_DECIMALS_BODY)
        return httpx.Response(
            200,
            json={
                "inputMint": TOKEN_MINT,
                "outputMint": WSOL_MINT,
                "inAmount": "1000000",
                "outAmount": "50000000",
                "priceImpactPct": "0.001",
                "routePlan": [],
            },
        )

    client = _make_client(handler)

    quote = asyncio.run(client.get_quote(TOKEN_MINT, WSOL_MINT, 1_000_000))

    assert quote is not None
    assert quote.price_sol == pytest.approx(0.05)


def test_quote_failures_return_none() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if request.method == "POST":
            return httpx.Response(200, json=RPC_DECIMALS_BODY)
        return httpx.Response(429, text="rate limited")

    client = _make_client(handler)
    assert asyncio.run(client.get_quote(WSOL_MINT, TOKEN_MINT, 50_000_000)) is None
    assert calls["n"] == 2


def test_quote_non_positive_amount_returns_none() -> None:
    client = _make_client(lambda request: httpx.Response(500))
    assert asyncio.run(client.get_quote(WSOL_MINT, TOKEN_MINT, 0)) is None


def test_priority_fee_callback_feeds_buy_and_sell_swap_requests() -> None:
    calls: list[str] = []
    fee_bodies: list[object] = []

    async def priority_fee() -> int:
        calls.append("priority_fee")
        return 75_000

    def handler(request: httpx.Request) -> httpx.Response:
        fee_bodies.append(json.loads(request.content)["prioritizationFeeLamports"])
        return httpx.Response(
            200,
            json={"swapTransaction": "transaction", "lastValidBlockHeight": 1},
        )

    client = _make_client(handler, priority_fee_callback=priority_fee)
    buy_quote = JupiterSwapQuote(
        input_mint=WSOL_MINT,
        output_mint=TOKEN_MINT,
        in_amount=20_000_000,
        out_amount=1_000_000,
        price_impact_pct=0.0,
        slippage_bps=100,
        token_decimals=6,
        price_sol=None,
        raw={},
        swap_api_base_url="http://jupiter.test",
        swap_api_legacy_paths=True,
    )
    sell_quote = JupiterSwapQuote(
        input_mint=TOKEN_MINT,
        output_mint=WSOL_MINT,
        in_amount=1_000_000,
        out_amount=20_000_000,
        price_impact_pct=0.0,
        slippage_bps=300,
        token_decimals=6,
        price_sol=None,
        raw={},
        swap_api_base_url="http://jupiter.test",
        swap_api_legacy_paths=True,
    )

    async def run() -> None:
        try:
            await client._request_swap_transaction(buy_quote)
            await client._request_swap_transaction(sell_quote)
        finally:
            await client.close()

    asyncio.run(run())

    assert calls == ["priority_fee", "priority_fee"]
    assert fee_bodies == [75_000, 75_000]


def test_execute_swap_confirmed_happy_path() -> None:
    keypair = _make_keypair()
    tx_b64 = _signed_tx_b64(keypair)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/swap/v1/quote":
            return httpx.Response(200, json=_quote_response())
        if request.url.path == "/swap/v1/swap":
            body = request.content.decode()
            assert '"dynamicSlippage":true' in body
            assert '"priorityLevelWithMaxLamports"' in body
            assert '"veryHigh"' in body
            assert '"maxLamports":1000000' in body
            assert f'"userPublicKey":"{keypair.pubkey()}"' in body
            assert '"wrapAndUnwrapSol":true' in body
            return httpx.Response(
                200,
                json={
                    "swapTransaction": tx_b64,
                    "lastValidBlockHeight": 12345,
                    "prioritizationFeeLamports": 5000,
                },
            )
        payload = json.loads(request.content)
        method = payload["method"]
        if method == "getTokenSupply":
            return httpx.Response(200, json=RPC_DECIMALS_BODY)
        if method == "sendTransaction":
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": "sig-confirmed"})
        if method == "getSignatureStatuses":
            return httpx.Response(
                200,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {"value": [{"slot": 42, "confirmations": 1, "err": None,
                                "confirmationStatus": "confirmed"}]},
                },
            )
        if method == "getBlockHeight":
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": 12346})
        if method == "getTokenAccountsByOwner":
            return httpx.Response(
                200,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {"value": [{"account": {"data": {"parsed": {"info":
                                {"tokenAmount": {"amount": "1000000"}}}}}}]},
                },
            )
        raise AssertionError(f"unexpected RPC method {method}")

    client = _make_client(handler, keypair=keypair)

    quote = asyncio.run(client.get_quote(WSOL_MINT, TOKEN_MINT, 50_000_000))
    assert quote is not None
    result = asyncio.run(client.execute_swap(quote))

    assert result.ok is True
    assert result.signature == "sig-confirmed"
    assert result.confirmation_status == "confirmed"
    assert result.slot == 42
    assert result.attempts == 1
    assert result.price_sol == pytest.approx(0.05)
    assert result.token_balance_after == pytest.approx(1.0)


def test_execute_swap_retries_on_expiry_then_confirms() -> None:
    keypair = _make_keypair()
    tx_b64 = _signed_tx_b64(keypair)
    send_calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/swap/v1/quote":
            return httpx.Response(200, json=_quote_response())
        if request.url.path == "/swap/v1/swap":
            return httpx.Response(
                200,
                json={"swapTransaction": tx_b64, "lastValidBlockHeight": 12345},
            )
        payload = json.loads(request.content)
        method = payload["method"]
        if method == "getTokenSupply":
            return httpx.Response(200, json=RPC_DECIMALS_BODY)
        if method == "sendTransaction":
            send_calls["n"] += 1
            return httpx.Response(
                200,
                json={"jsonrpc": "2.0", "id": 1, "result": f"sig-{send_calls['n']}"},
            )
        if method == "getSignatureStatuses":
            if send_calls["n"] == 1:
                return httpx.Response(
                200,
                json={"jsonrpc": "2.0", "id": 1, "result": {"value": [None]}},
            )
            return httpx.Response(
                200,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {"value": [{"slot": 99, "confirmations": 0, "err": None,
                                "confirmationStatus": "confirmed"}]},
                },
            )
        if method == "getBlockHeight":
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": 12346})
        if method == "getTokenAccountsByOwner":
            return httpx.Response(
                200,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {"value": [{"account": {"data": {"parsed": {"info":
                                {"tokenAmount": {"amount": "1000000"}}}}}}]},
                },
            )
        raise AssertionError(f"unexpected RPC method {method}")

    client = _make_client(handler, keypair=keypair)

    quote = asyncio.run(client.get_quote(WSOL_MINT, TOKEN_MINT, 50_000_000))
    assert quote is not None
    result = asyncio.run(client.execute_swap(quote))

    assert result.ok is True
    assert result.attempts == 2
    assert result.signature == "sig-2"
    assert result.balance_before_last_attempt == pytest.approx(1.0)
    assert send_calls["n"] == 2


def test_execute_swap_gives_up_after_max_retries() -> None:
    keypair = _make_keypair()
    tx_b64 = _signed_tx_b64(keypair)
    send_calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/swap/v1/quote":
            return httpx.Response(200, json=_quote_response())
        if request.url.path == "/swap/v1/swap":
            return httpx.Response(
                200,
                json={"swapTransaction": tx_b64, "lastValidBlockHeight": 12345},
            )
        payload = json.loads(request.content)
        method = payload["method"]
        if method == "getTokenSupply":
            return httpx.Response(200, json=RPC_DECIMALS_BODY)
        if method == "sendTransaction":
            send_calls["n"] += 1
            return httpx.Response(
                200,
                json={"jsonrpc": "2.0", "id": 1, "result": f"sig-{send_calls['n']}"},
            )
        if method == "getSignatureStatuses":
            return httpx.Response(
                200,
                json={"jsonrpc": "2.0", "id": 1, "result": {"value": [None]}},
            )
        if method == "getBlockHeight":
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": 12346})
        if method == "getTokenAccountsByOwner":
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": {"value": []}})
        raise AssertionError(f"unexpected RPC method {method}")

    client = _make_client(handler, keypair=keypair)

    quote = asyncio.run(client.get_quote(WSOL_MINT, TOKEN_MINT, 50_000_000))
    assert quote is not None
    result = asyncio.run(client.execute_swap(quote))

    assert result.ok is False
    assert result.confirmation_status == "expired"
    assert send_calls["n"] == 3  # initial + 2 retries
    assert "attempt_1" in result.diagnostics
    assert "attempt_3" in result.diagnostics


def test_execute_swap_timeout_without_proven_expiry_does_not_resubmit() -> None:
    keypair = _make_keypair()
    tx_b64 = _signed_tx_b64(keypair)
    send_calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/swap/v1/quote":
            return httpx.Response(200, json=_quote_response())
        if request.url.path == "/swap/v1/swap":
            return httpx.Response(
                200,
                json={"swapTransaction": tx_b64, "lastValidBlockHeight": 12345},
            )
        payload = json.loads(request.content)
        method = payload["method"]
        if method == "getTokenSupply":
            return httpx.Response(200, json=RPC_DECIMALS_BODY)
        if method == "sendTransaction":
            send_calls["n"] += 1
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": "sig-pending"})
        if method == "getSignatureStatuses":
            return httpx.Response(
                200,
                json={"jsonrpc": "2.0", "id": 1, "result": {"value": [None]}},
            )
        if method == "getBlockHeight":
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": 12345})
        if method == "getTokenAccountsByOwner":
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": {"value": []}})
        raise AssertionError(f"unexpected RPC method {method}")

    client = _make_client(handler, keypair=keypair)
    quote = asyncio.run(client.get_quote(WSOL_MINT, TOKEN_MINT, 50_000_000))
    assert quote is not None
    result = asyncio.run(client.execute_swap(quote))

    assert result.ok is False
    assert result.confirmation_status == "unknown"
    assert result.signature == "sig-pending"
    assert send_calls["n"] == 1


def test_execute_swap_failed_transaction_returns_error() -> None:
    keypair = _make_keypair()
    tx_b64 = _signed_tx_b64(keypair)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/swap/v1/quote":
            return httpx.Response(200, json=_quote_response())
        if request.url.path == "/swap/v1/swap":
            return httpx.Response(
                200,
                json={"swapTransaction": tx_b64, "lastValidBlockHeight": 12345},
            )
        payload = json.loads(request.content)
        method = payload["method"]
        if method == "getTokenSupply":
            return httpx.Response(200, json=RPC_DECIMALS_BODY)
        if method == "sendTransaction":
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": "sig-failed"})
        if method == "getSignatureStatuses":
            return httpx.Response(
                200,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {"value": [{"slot": None, "confirmations": None,
                            "err": {"InstructionError": [0, "Slippage"]},
                            "confirmationStatus": "finalized"}]},
                },
            )
        if method == "getTokenAccountsByOwner":
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": {"value": []}})
        raise AssertionError(f"unexpected RPC method {method}")

    client = _make_client(handler, keypair=keypair)

    quote = asyncio.run(client.get_quote(WSOL_MINT, TOKEN_MINT, 50_000_000))
    assert quote is not None
    result = asyncio.run(client.execute_swap(quote))

    assert result.ok is False
    assert result.confirmation_status == "failed"
    assert "Slippage" in (result.error or "")


def test_processed_signature_with_slot_is_not_confirmed() -> None:
    keypair = _make_keypair()

    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        assert payload["method"] == "getSignatureStatuses"
        return httpx.Response(
            200,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "result": {
                    "value": [{"slot": 42, "confirmations": 1, "err": None,
                               "confirmationStatus": "processed"}],
                },
            },
        )

    client = _make_client(handler, keypair=keypair)

    assert asyncio.run(client._signature_status("sig-processed")) is None


def test_wallet_pubkey_from_keypair() -> None:
    keypair = _make_keypair()
    client = _make_client(lambda request: httpx.Response(500), keypair=keypair)
    assert client.wallet_pubkey == str(keypair.pubkey())


def test_missing_api_key_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("JUPITER_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="JUPITER_API_KEY"):
        JupiterSwapClient(
            solana_rpc_url=RPC,
            http_client=httpx.AsyncClient(),
            keypair=_make_keypair(),
        )


def test_missing_wallet_key_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JUPITER_API_KEY", "test-key")
    monkeypatch.delenv("WALLET_PRIVATE_KEY", raising=False)
    with pytest.raises(RuntimeError, match="WALLET_PRIVATE_KEY"):
        JupiterSwapClient(
            solana_rpc_url=RPC,
            http_client=httpx.AsyncClient(),
            api_key="test-key",
        )


def test_bad_base64_wallet_key_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JUPITER_API_KEY", "test-key")
    monkeypatch.setenv("WALLET_PRIVATE_KEY", "!!!not-base64!!!")
    with pytest.raises(RuntimeError, match="not valid base64"):
        JupiterSwapClient(
            solana_rpc_url=RPC,
            http_client=httpx.AsyncClient(),
            api_key="test-key",
        )


def test_wrong_length_wallet_key_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JUPITER_API_KEY", "test-key")
    monkeypatch.setenv("WALLET_PRIVATE_KEY", base64.b64encode(b"short").decode())
    with pytest.raises(RuntimeError, match="64 bytes"):
        JupiterSwapClient(
            solana_rpc_url=RPC,
            http_client=httpx.AsyncClient(),
            api_key="test-key",
        )


def test_reconcile_sol_balance_after_swap() -> None:
    keypair = _make_keypair()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/swap/v1/quote":
            return httpx.Response(200, json=_quote_response())
        if request.url.path == "/swap/v1/swap":
            return httpx.Response(
                200,
                json={"swapTransaction": _signed_tx_b64(keypair), "lastValidBlockHeight": 12345},
            )
        payload = json.loads(request.content)
        method = payload["method"]
        if method == "getTokenSupply":
            return httpx.Response(200, json=RPC_DECIMALS_9_BODY)
        if method == "sendTransaction":
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": "sig-sol"})
        if method == "getSignatureStatuses":
            return httpx.Response(
                200,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {"value": [{"slot": 7, "confirmations": 0, "err": None,
                                "confirmationStatus": "confirmed"}]},
                },
            )
        if method == "getBalance":
            return httpx.Response(200, json=RPC_BALANCE_BODY)
        raise AssertionError(f"unexpected RPC method {method}")

    client = _make_client(handler, keypair=keypair)

    quote = asyncio.run(client.get_quote(TOKEN_MINT, WSOL_MINT, 1_000_000))
    assert quote is not None
    result = asyncio.run(client.execute_swap(quote))

    assert result.ok is True
    assert result.token_balance_after == pytest.approx(0.049)


# ── MT-589: Jito bundle routing ──────────────────────────────────────

def test_execute_swap_submits_jito_bundle_when_enabled() -> None:
    keypair = _make_keypair()
    tx_b64 = _signed_tx_b64(keypair)
    jito_calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/swap/v1/quote":
            return httpx.Response(200, json=_quote_response())
        if request.url.path == "/swap/v1/swap":
            return httpx.Response(
                200,
                json={"swapTransaction": tx_b64, "lastValidBlockHeight": 12345},
            )
        if request.url.path == "/api/v1/bundles":
            payload = json.loads(request.content)
            assert payload["method"] == "sendBundle"
            transactions = payload["params"][0]
            # Bundle = [tip transfer, swap].
            assert len(transactions) == 2
            assert transactions[1] == tx_b64
            jito_calls["n"] += 1
            return httpx.Response(
                200,
                json={"jsonrpc": "2.0", "id": 1, "result": "bundle-jito-1"},
            )
        payload = json.loads(request.content)
        method = payload["method"]
        if method == "getTipFloor":
            return httpx.Response(
                200,
                json={"result": [{"ema_landed_tips_50th_percentile": 0.00001}]},
            )
        if method == "getTokenSupply":
            return httpx.Response(200, json=RPC_DECIMALS_BODY)
        if method == "getSignatureStatuses":
            return httpx.Response(
                200,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {"value": [{"slot": 42, "confirmations": 1, "err": None,
                                "confirmationStatus": "confirmed"}]},
                },
            )
        if method == "getTokenAccountsByOwner":
            return httpx.Response(
                200,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {"value": [{"account": {"data": {"parsed": {"info":
                                {"tokenAmount": {"amount": "1000000"}}}}}}]},
                },
            )
        raise AssertionError(f"unexpected RPC method {method}")

    client = _make_client(handler, keypair=keypair)
    client._use_jito_bundles = True
    client._jito_min_size_sol = 0.0

    quote = asyncio.run(client.get_quote(WSOL_MINT, TOKEN_MINT, 50_000_000))
    assert quote is not None
    result = asyncio.run(client.execute_swap(quote))

    # The bundle carried the tip transfer + swap; RPC sendTransaction never ran.
    assert jito_calls["n"] == 1
    assert result.ok is True
    assert result.confirmation_status == "confirmed"
    # The swap signature is the wallet's signature over the swap message.
    expected_sig = str(Signature.from_bytes(base64.b64decode(tx_b64)[:64]))
    assert result.signature == expected_sig


def test_jito_is_skipped_below_position_size_threshold() -> None:
    client = _make_client(lambda request: httpx.Response(500))
    client._use_jito_bundles = True
    client._jito_min_size_sol = 0.1

    small_buy = JupiterSwapQuote(
        input_mint=WSOL_MINT,
        output_mint=TOKEN_MINT,
        in_amount=20_000_000,
        out_amount=1,
        price_impact_pct=0.0,
        slippage_bps=100,
        token_decimals=6,
        price_sol=None,
        raw={},
    )
    large_sell = JupiterSwapQuote(
        input_mint=TOKEN_MINT,
        output_mint=WSOL_MINT,
        in_amount=1,
        out_amount=400_000_000,
        price_impact_pct=0.0,
        slippage_bps=100,
        token_decimals=6,
        price_sol=None,
        raw={},
    )

    assert client._should_use_jito_bundles(small_buy) is False
    assert client._should_use_jito_bundles(large_sell) is True


def test_jito_min_size_defaults_to_point_one_sol(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("JITO_MIN_SIZE_SOL", raising=False)
    client = _make_client(lambda request: httpx.Response(500))
    assert client._jito_min_size_sol == pytest.approx(0.1)


def test_oversized_swap_rebuilds_with_compact_jupiter_route() -> None:
    client = _make_client(lambda request: httpx.Response(500))
    original_quote = JupiterSwapQuote(
        input_mint=WSOL_MINT,
        output_mint=TOKEN_MINT,
        in_amount=20_000_000,
        out_amount=1,
        price_impact_pct=0.0,
        slippage_bps=100,
        token_decimals=6,
        price_sol=None,
        raw={},
    )
    compact_quote = JupiterSwapQuote(
        input_mint=WSOL_MINT,
        output_mint=TOKEN_MINT,
        in_amount=20_000_000,
        out_amount=2,
        price_impact_pct=0.0,
        slippage_bps=100,
        token_decimals=6,
        price_sol=None,
        raw={},
    )
    oversized_tx = base64.b64encode(b"x" * 1660).decode()
    compact_tx = base64.b64encode(b"x" * 1232).decode()

    async def request_swap(quote: JupiterSwapQuote) -> tuple[str, int, int]:
        return (oversized_tx, 1, 1) if quote is original_quote else (compact_tx, 2, 2)

    async def get_compact_quote(
        *args,
        max_accounts: int | None = None,
        **kwargs,
    ) -> JupiterSwapQuote:
        assert max_accounts == 32
        return compact_quote

    client._request_swap_transaction = request_swap  # type: ignore[method-assign]
    client.get_quote = get_compact_quote  # type: ignore[method-assign]

    quote, transaction, last_valid, fees = asyncio.run(
        client._request_sized_swap_transaction(original_quote),
    )

    assert quote is compact_quote
    assert transaction == compact_tx
    assert last_valid == 2
    assert fees == 2


def test_execute_swap_falls_back_to_rpc_when_jito_fails() -> None:
    keypair = _make_keypair()
    tx_b64 = _signed_tx_b64(keypair)
    jito_calls = {"n": 0}
    send_calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/swap/v1/quote":
            return httpx.Response(200, json=_quote_response())
        if request.url.path == "/swap/v1/swap":
            return httpx.Response(
                200,
                json={"swapTransaction": tx_b64, "lastValidBlockHeight": 12345},
            )
        if request.url.path == "/api/v1/bundles":
            jito_calls["n"] += 1
            return httpx.Response(503, json={"error": "busy"})
        payload = json.loads(request.content)
        method = payload["method"]
        if method == "getTipFloor":
            return httpx.Response(
                200,
                json={"result": [{"ema_landed_tips_50th_percentile": 0.00001}]},
            )
        if method == "getTokenSupply":
            return httpx.Response(200, json=RPC_DECIMALS_BODY)
        if method == "sendTransaction":
            send_calls["n"] += 1
            return httpx.Response(
                200,
                json={"jsonrpc": "2.0", "id": 1, "result": "sig-rpc-fallback"},
            )
        if method == "getSignatureStatuses":
            return httpx.Response(
                200,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {"value": [{"slot": 7, "confirmations": 0, "err": None,
                                "confirmationStatus": "confirmed"}]},
                },
            )
        if method == "getTokenAccountsByOwner":
            return httpx.Response(
                200,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {"value": [{"account": {"data": {"parsed": {"info":
                                {"tokenAmount": {"amount": "1000000"}}}}}}]},
                },
            )
        raise AssertionError(f"unexpected RPC method {method}")

    client = _make_client(handler, keypair=keypair)
    client._use_jito_bundles = True
    client._jito_min_size_sol = 0.0

    quote = asyncio.run(client.get_quote(WSOL_MINT, TOKEN_MINT, 50_000_000))
    assert quote is not None
    result = asyncio.run(client.execute_swap(quote))

    assert jito_calls["n"] == 1
    assert send_calls["n"] == 1
    assert result.ok is True
    assert result.signature == "sig-rpc-fallback"


def test_jito_tip_uses_tip_floor_and_caps_at_half_milli_sol() -> None:
    async def run() -> tuple[int, int]:
        def high_handler(request: httpx.Request) -> httpx.Response:
            assert json.loads(request.content)["method"] == "getTipFloor"
            return httpx.Response(
                200,
                json={"result": [{"ema_landed_tips_50th_percentile": 0.001}]},
            )

        def empty_handler(request: httpx.Request) -> httpx.Response:
            assert json.loads(request.content)["method"] == "getTipFloor"
            return httpx.Response(200, json={"result": []})

        high_client = JupiterSwapClient(
            solana_rpc_url=RPC,
            http_client=httpx.AsyncClient(
                transport=httpx.MockTransport(high_handler),
            ),
            keypair=_make_keypair(),
            api_key="test-key",
        )
        none_client = JupiterSwapClient(
            solana_rpc_url=RPC,
            http_client=httpx.AsyncClient(
                transport=httpx.MockTransport(empty_handler),
            ),
            keypair=_make_keypair(),
            api_key="test-key",
        )
        try:
            high = await high_client._jito_tip_lamports()
            low = await none_client._jito_tip_lamports()
        finally:
            await high_client.close()
            await none_client.close()
        return high, low

    high, low = asyncio.run(run())
    assert high == MAX_JITO_TIP_LAMPORTS
    assert low == MIN_JITO_TIP_LAMPORTS
