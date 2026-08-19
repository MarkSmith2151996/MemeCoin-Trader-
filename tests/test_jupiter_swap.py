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
from src.chain.jupiter_swap import MIN_JITO_TIP_LAMPORTS, JupiterSwapClient

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


def _make_client(handler, keypair: Keypair | None = None) -> JupiterSwapClient:
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
    )


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
    assert calls["n"] == 1


def test_quote_non_positive_amount_returns_none() -> None:
    client = _make_client(lambda request: httpx.Response(500))
    assert asyncio.run(client.get_quote(WSOL_MINT, TOKEN_MINT, 0)) is None


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

    quote = asyncio.run(client.get_quote(WSOL_MINT, TOKEN_MINT, 50_000_000))
    assert quote is not None
    result = asyncio.run(client.execute_swap(quote))

    assert jito_calls["n"] == 1
    assert send_calls["n"] == 1
    assert result.ok is True
    assert result.signature == "sig-rpc-fallback"


def test_jito_tip_uses_dynamic_fee_with_minimum_floor() -> None:
    async def run() -> tuple[int, int]:
        async def callback_high() -> int | None:
            return 5_000_000

        async def callback_none() -> int | None:
            return None

        high_client = JupiterSwapClient(
            solana_rpc_url=RPC,
            http_client=httpx.AsyncClient(
                transport=httpx.MockTransport(lambda request: httpx.Response(200)),
            ),
            keypair=_make_keypair(),
            api_key="test-key",
            priority_fee_callback=callback_high,
        )
        none_client = JupiterSwapClient(
            solana_rpc_url=RPC,
            http_client=httpx.AsyncClient(
                transport=httpx.MockTransport(lambda request: httpx.Response(200)),
            ),
            keypair=_make_keypair(),
            api_key="test-key",
            priority_fee_callback=callback_none,
        )
        try:
            high = await high_client._jito_tip_lamports()
            low = await none_client._jito_tip_lamports()
        finally:
            await high_client.close()
            await none_client.close()
        return high, low

    high, low = asyncio.run(run())
    assert high == 5_000_000
    assert low == MIN_JITO_TIP_LAMPORTS
