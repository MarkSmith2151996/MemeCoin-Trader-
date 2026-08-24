"""Live execution adapter backed by the Jupiter Swap API.

Implements the same ``ExecutionAdapter`` contract as the paper adapter
(``src/execution/paper.py``) plus explicit ``buy``/``sell`` helpers. When
``EXECUTION_MODE=live``, Strategy B instantiates this adapter and every entry
becomes a real signed swap; shadow mode (paper + Jupiter quotes) is untouched.

Safety gates before any swap:
- the token must not be in the banned list,
- the wallet must hold enough SOL for the notional plus a reserve,
- the quote price impact must stay below ``max_price_impact_pct`` (default 5%),
- the circuit breaker must be clear (MT-546) — a tripped breaker blocks new
  buys only; sells are never blocked so open positions stay manageable.

Every step — quote, price impact, signed transaction, send result,
confirmation, and fill price versus the reference paper mark — is logged.
A failed or crashed sell trips the breaker (``src/execution/safety_controls.py``);
reset it with ``scripts/reset_breaker.py`` after investigating.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

from src.chain.jupiter import LAMPORTS_PER_SOL, SOL_MINT
from src.chain.jupiter_swap import JupiterSwapClient, JupiterSwapQuote
from src.core.models import Side, SwapQuote, Trade
from src.execution.base import ExecutionAdapter
from src.execution.price_provider import PriceProvider
from src.execution.safety_controls import CircuitBreaker

log = logging.getLogger("live_execution")

MAX_PRICE_IMPACT_PCT = 5.0
WALLET_RESERVE_SOL = 0.01
POST_SELL_BALANCE_ATTEMPTS = 3
BALANCE_RECONCILIATION_RETRY_S = 10.0


def is_jupiter_slippage_error(error: object) -> bool:
    """Return whether a Jupiter/Anchor error reports exceeded slippage."""
    message = str(error).lower()
    return "6001" in message or "0x1771" in message


class LiveExecutionAdapter(ExecutionAdapter):
    """Real-swap execution adapter using the Jupiter Swap API."""

    def __init__(
        self,
        client: JupiterSwapClient | None = None,
        *,
        banned_tokens: set[str] | None = None,
        max_price_impact_pct: float = MAX_PRICE_IMPACT_PCT,
        wallet_reserve_sol: float = WALLET_RESERVE_SOL,
        reference_price_provider: PriceProvider | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        balance_reconciliation_retry_s: float = BALANCE_RECONCILIATION_RETRY_S,
    ) -> None:
        self._client = client if client is not None else JupiterSwapClient()
        self._banned_tokens = set(banned_tokens or ())
        self._max_price_impact_pct = max_price_impact_pct
        self._wallet_reserve_sol = wallet_reserve_sol
        self._reference_price_provider = reference_price_provider
        self._circuit_breaker = circuit_breaker if circuit_breaker is not None else CircuitBreaker()
        self._balance_reconciliation_retry_s = balance_reconciliation_retry_s
        self._closed = False

    @property
    def mode(self) -> str:
        return "live"

    async def buy(self, mint_address: str, amount_sol: float, slippage_bps: int = 100) -> Trade:
        """Buy ``amount_sol`` worth of ``mint_address`` through Jupiter."""
        self._ensure_open()
        self._check_circuit_breaker()
        await self._check_token_allowed(mint_address)
        await self._check_sol_balance(amount_sol)

        amount_lamports = int(amount_sol * LAMPORTS_PER_SOL)
        quote = await self._quote_or_raise(SOL_MINT, mint_address, amount_lamports, slippage_bps)
        await self._check_price_impact(mint_address, quote)

        log.info(
            "LIVE BUY %s: %s SOL → %.8f tokens @ %.10f SOL impact=%.4f%%",
            mint_address[:16], amount_sol, quote.out_amount / 10**quote.token_decimals,
            quote.price_sol or 0.0, quote.price_impact_pct * 100,
        )

        result = await self._client.execute_swap(quote)
        await self._log_swap_result("BUY", mint_address, result)
        if not result.ok:
            raise RuntimeError(
                f"live buy failed ({result.confirmation_status}): {result.error or 'unknown'}",
            )

        token_amount = result.out_amount / (10**quote.token_decimals)
        return Trade(
            mint_address=mint_address,
            side=Side.BUY,
            amount_sol=amount_sol,
            token_amount=token_amount,
            price_sol=result.price_sol or quote.price_sol,
            slippage_bps=slippage_bps,
            tx_signature=result.signature,
            mode=self.mode,
            status=result.confirmation_status,
            metadata={
                "provider": "jupiter",
                "quote_in_amount": result.in_amount,
                "quote_out_amount": result.out_amount,
                "price_impact_pct": quote.price_impact_pct,
                "fees_lamports": result.fees_lamports,
                "confirmation_status": result.confirmation_status,
                "slot": result.slot,
                "token_balance_after": result.token_balance_after,
            },
        )

    async def sell(self, mint_address: str, token_amount: float, slippage_bps: int = 100) -> Trade:
        """Sell the wallet's full balance of ``mint_address`` through Jupiter."""
        self._ensure_open()
        await self._check_token_allowed(mint_address)
        if token_amount <= 0:
            raise RuntimeError(f"live sell rejected non-positive token amount {token_amount}")

        decimals = await self._client.get_token_decimals(mint_address)
        wallet_balance = await self._wait_for_positive_token_balance(mint_address, decimals)
        token_lamports = int(wallet_balance * 10**decimals)

        if abs(wallet_balance - token_amount) > 1 / 10**decimals:
            log.warning(
                "LIVE SELL %s position_amount=%.8f wallet_amount=%.8f; selling wallet balance",
                mint_address[:16], token_amount, wallet_balance,
            )

        quote = await self._quote_or_raise(mint_address, SOL_MINT, token_lamports, slippage_bps)
        await self._check_price_impact(mint_address, quote)

        log.info(
            "LIVE SELL %s: %.8f tokens → %s SOL @ %.10f SOL impact=%.4f%%",
            mint_address[:16], wallet_balance, quote.out_amount / LAMPORTS_PER_SOL,
            quote.price_sol or 0.0, quote.price_impact_pct * 100,
        )

        try:
            result = await self._client.execute_swap(quote)
        except Exception as exc:
            if not is_jupiter_slippage_error(exc):
                # A crashed swap execution leaves the outcome unknown, so
                # block new buys until an operator reviews the failure.
                self._circuit_breaker.trip(
                    mint=mint_address,
                    error=f"sell crash: {exc}",
                    reason="sell_failure",
                )
            raise
        await self._log_swap_result("SELL", mint_address, result)
        if not result.ok:
            error = result.error or f"sell {result.confirmation_status}"
            if not is_jupiter_slippage_error(error):
                # A failed sell (RPC error, expiry, or confirmation timeout)
                # blocks new buys until an operator resets the breaker.
                self._circuit_breaker.trip(
                    mint=mint_address,
                    signature_attempt=result.signature,
                    error=error,
                    reason="sell_failure",
                )
            raise RuntimeError(
                f"live sell failed ({result.confirmation_status}): {error}",
            )

        token_balance_after = await self._verify_token_balance_cleared(mint_address, decimals)

        sol_out = result.out_amount / LAMPORTS_PER_SOL
        return Trade(
            mint_address=mint_address,
            side=Side.SELL,
            amount_sol=sol_out,
            token_amount=wallet_balance,
            price_sol=result.price_sol or quote.price_sol,
            slippage_bps=slippage_bps,
            tx_signature=result.signature,
            mode=self.mode,
            status=result.confirmation_status,
            metadata={
                "provider": "jupiter",
                "quote_in_amount": result.in_amount,
                "quote_out_amount": result.out_amount,
                "price_impact_pct": quote.price_impact_pct,
                "fees_lamports": result.fees_lamports,
                "confirmation_status": result.confirmation_status,
                "slot": result.slot,
                "sol_balance_after": result.token_balance_after,
                "token_balance_after": token_balance_after,
            },
        )

    async def get_token_balance(self, mint_address: str) -> float | None:
        """Return the wallet balance for a mint without attempting a swap."""
        self._ensure_open()
        return await self._client.get_token_balance(mint_address)

    async def get_wallet_holdings(self) -> dict[str, float] | None:
        """Return all positive SPL-token balances without attempting a swap."""
        self._ensure_open()
        return await self._client.get_wallet_holdings()

    async def get_sol_balance(self) -> float | None:
        """Return the wallet SOL balance without attempting a swap."""
        self._ensure_open()
        return await self._client.get_sol_balance()

    def trip_circuit_breaker(
        self,
        *,
        error: str,
        mint: str | None = None,
        signature_attempt: str | None = None,
        reason: str,
    ) -> None:
        """Block new live buys after an external consistency failure."""
        self._circuit_breaker.trip(
            error=error,
            mint=mint,
            signature_attempt=signature_attempt,
            reason=reason,
        )

    async def execute_swap(
        self,
        mint_address: str,
        side: Side,
        amount_sol: float,
        slippage_bps: int = 300,
    ) -> Trade:
        """Base-contract entry point. Buys take ``amount_sol``; sells use the
        wallet's current token balance for the mint when the caller cannot
        supply a token amount."""
        self._ensure_open()
        if side == Side.BUY:
            return await self.buy(mint_address, amount_sol, slippage_bps)
        balance = await self._client.get_token_balance(mint_address)
        if balance is None or balance <= 0:
            raise RuntimeError(
                f"live sell requires a token amount; wallet holds no {mint_address[:16]}",
            )
        return await self.sell(mint_address, balance, slippage_bps)

    async def get_quote(
        self,
        mint_address: str,
        side: Side,
        amount_sol: float,
        slippage_bps: int = 300,
    ) -> SwapQuote:
        """Quote the swap in SOL terms (compatible with the paper adapter)."""
        self._ensure_open()
        if side == Side.BUY:
            amount_lamports = int(amount_sol * LAMPORTS_PER_SOL)
            quote = await self._client.get_quote(
                SOL_MINT, mint_address, amount_lamports, slippage_bps,
            )
            estimated_out = quote.out_amount / (10**quote.token_decimals) if quote else 0.0
        else:
            decimals = await self._client.get_token_decimals(mint_address)
            token_lamports = int(amount_sol * (10**decimals)) if decimals else int(amount_sol)
            quote = await self._client.get_quote(
                mint_address, SOL_MINT, token_lamports, slippage_bps,
            )
            estimated_out = quote.out_amount / LAMPORTS_PER_SOL if quote else 0.0
        return SwapQuote(
            mint_address=mint_address,
            side=side,
            amount_sol=amount_sol,
            estimated_out_amount=estimated_out,
            price_sol=quote.price_sol if quote else None,
            price_impact_pct=quote.price_impact_pct if quote else 0.0,
            slippage_bps=slippage_bps,
            provider="live",
            expires_at=datetime.now(UTC) + timedelta(seconds=30),
        )

    async def get_current_price(self, mint_address: str) -> float | None:
        self._ensure_open()
        if self._reference_price_provider is not None:
            try:
                price = await self._reference_price_provider.get_current_price(mint_address)
                if price is not None and price > 0:
                    return price
            except Exception as exc:
                log.debug("reference price lookup failed for %s: %s", mint_address[:16], exc)
        quote = await self._client.get_quote(
            SOL_MINT, mint_address, 10_000_000, 100,
        )
        return quote.price_sol if quote else None

    async def close(self) -> None:
        if not self._closed:
            await self._client.close()
            self._closed = True

    # ── Pre-swap gates ───────────────────────────────────────────────

    async def _check_token_allowed(self, mint_address: str) -> None:
        if mint_address in self._banned_tokens:
            raise RuntimeError(f"token {mint_address[:16]} is banned")

    async def _check_sol_balance(self, amount_sol: float) -> None:
        sol_balance = await self._client.get_sol_balance()
        if sol_balance is None:
            raise RuntimeError("cannot verify wallet SOL balance — refusing live buy")
        required = amount_sol + self._wallet_reserve_sol
        if sol_balance < required:
            raise RuntimeError(
                f"insufficient SOL balance: {sol_balance:.4f} SOL needed >= {required:.4f}",
            )

    async def _check_price_impact(self, mint_address: str, quote: JupiterSwapQuote) -> None:
        impact_pct = quote.price_impact_pct * 100
        if impact_pct >= self._max_price_impact_pct:
            raise RuntimeError(
                f"price impact {impact_pct:.2f}% exceeds limit "
                f"{self._max_price_impact_pct:.2f}% for {mint_address[:16]}",
            )

    def _check_circuit_breaker(self) -> None:
        """Block new buys while the sell-failure breaker is tripped.

        Sells are never blocked — open positions must keep being manageable.
        Paper mode never reaches this adapter, so the breaker is live-only.
        """
        state = self._circuit_breaker.status()
        if state.tripped:
            raise RuntimeError(
                "circuit breaker tripped (reason="
                f"{state.reason or 'sell_failure'} mint={state.mint or '-'} "
                f"at={state.tripped_at or '-'} error={state.error or '-'}) — "
                "new buys blocked; reset via scripts/reset_breaker.py",
            )

    async def _quote_or_raise(
        self,
        input_mint: str,
        output_mint: str,
        amount_lamports: int,
        slippage_bps: int,
    ) -> JupiterSwapQuote:
        quote = await self._client.get_quote(input_mint, output_mint, amount_lamports, slippage_bps)
        if quote is None:
            raise RuntimeError(
                f"quote failed {input_mint[:16]}→{output_mint[:16]} "
                f"amount={amount_lamports} slip={slippage_bps}",
            )
        if quote.out_amount <= 0:
            raise RuntimeError(f"quote returned zero output for {output_mint[:16]}")
        return quote

    async def _verify_token_balance_cleared(self, mint_address: str, decimals: int) -> float:
        """Require a confirmed sell to leave no spendable balance for its mint."""
        dust = 1 / 10**decimals
        last_balance: float | None = None
        for attempt in range(1, POST_SELL_BALANCE_ATTEMPTS + 1):
            balance = await self._client.get_token_balance(mint_address)
            last_balance = balance
            if balance is not None and balance <= dust:
                log.info(
                    "LIVE SELL BALANCE %s cleared after attempt=%d balance=%.12f",
                    mint_address[:16], attempt, balance,
                )
                return balance
            if attempt < POST_SELL_BALANCE_ATTEMPTS:
                log.warning(
                    "LIVE SELL BALANCE %s still=%s after attempt=%d; retrying",
                    mint_address[:16], balance, attempt,
                )
                await asyncio.sleep(self._balance_reconciliation_retry_s)
        error = (
            f"sell confirmed but token balance not cleared after "
            f"{POST_SELL_BALANCE_ATTEMPTS} checks: {last_balance}"
        )
        self._circuit_breaker.trip(mint=mint_address, error=error, reason="sell_failure")
        raise RuntimeError(error)

    async def _wait_for_positive_token_balance(self, mint_address: str, decimals: int) -> float:
        """Allow the RPC token-account indexer time to expose a fresh buy fill."""
        dust = 1 / 10**decimals
        last_balance: float | None = None
        for attempt in range(1, POST_SELL_BALANCE_ATTEMPTS + 1):
            balance = await self._client.get_token_balance(mint_address)
            last_balance = balance
            if balance is not None and balance > dust:
                return balance
            if attempt < POST_SELL_BALANCE_ATTEMPTS:
                log.warning(
                    "LIVE SELL BALANCE %s unavailable after attempt=%d; retrying",
                    mint_address[:16], attempt,
                )
                await asyncio.sleep(self._balance_reconciliation_retry_s)
        raise RuntimeError(
            f"cannot verify a positive wallet token balance for {mint_address[:16]}: {last_balance}",
        )

    async def _log_swap_result(self, side: str, mint_address: str, result) -> None:
        """Log the send result, confirmation, and fill vs paper reference price."""
        paper_price = None
        if self._reference_price_provider is not None:
            try:
                paper_price = await self._reference_price_provider.get_current_price(mint_address)
            except Exception:
                paper_price = None
        vs_paper = ""
        if paper_price is not None and result.price_sol is not None and paper_price > 0:
            diff_pct = (result.price_sol - paper_price) / paper_price * 100
            vs_paper = f" fill_vs_paper={diff_pct:+.2f}% (paper={paper_price:.10f})"
        log.info(
            "LIVE %s RESULT %s: ok=%s sig=%s status=%s slot=%s fees=%s%s",
            side, mint_address[:16], result.ok, result.signature or "-",
            result.confirmation_status, result.slot or "-",
            result.fees_lamports or "-", vs_paper,
        )
        for diag in result.diagnostics:
            log.info("LIVE %s %s: %s", side, mint_address[:16], diag)

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("live execution adapter is closed")
