"""Funding-source analysis for buyer sybil and bundle detection."""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from src.core.models import CheckResult


DEFAULT_FUNDING_WINDOW = timedelta(minutes=15)
DEFAULT_BUNDLED_BUYER_THRESHOLD_PCT = 50.0


@dataclass(frozen=True, slots=True)
class InboundTransfer:
    """A recent inbound SOL transfer into one buyer wallet."""

    source_wallet: str | None
    observed_at: datetime | None = None
    amount_sol: float | None = None
    signature: str | None = None


class FundingTransferProvider(Protocol):
    """Provider abstraction for recent inbound funding lookups."""

    async def get_recent_inbound_transfers(self, wallet: str) -> Sequence[InboundTransfer] | None:
        """Return recent inbound transfers for the given wallet."""


@dataclass(frozen=True, slots=True)
class FundingAnalysisResult:
    """Aggregate funding-source analysis outcome for one buyer set."""

    funding_sybil_check: CheckResult
    bundled_buyer_pct: float
    largest_common_funder_group_size: int
    buyers_with_known_funders: int
    buyers_with_unknown_funders: int
    total_buyers: int
    flagged: bool
    dominant_funder: str | None = None
    provider_failures: int = 0
    analysis_window_minutes: int = 15


async def analyze_buyer_funding(
    buyer_wallets: Sequence[str],
    provider: FundingTransferProvider,
    *,
    as_of: datetime | None = None,
    funding_window: timedelta = DEFAULT_FUNDING_WINDOW,
    bundled_buyer_threshold_pct: float = DEFAULT_BUNDLED_BUYER_THRESHOLD_PCT,
) -> FundingAnalysisResult:
    """Group buyers by recent funder and flag likely bundled launches."""

    normalized_wallets = [wallet.strip() for wallet in buyer_wallets if wallet.strip()]
    if not normalized_wallets:
        return FundingAnalysisResult(
            funding_sybil_check=CheckResult.UNKNOWN,
            bundled_buyer_pct=0.0,
            largest_common_funder_group_size=0,
            buyers_with_known_funders=0,
            buyers_with_unknown_funders=0,
            total_buyers=0,
            flagged=False,
            analysis_window_minutes=int(funding_window.total_seconds() // 60),
        )

    reference_time = as_of.astimezone(UTC) if as_of is not None else datetime.now(UTC)
    funder_counts: Counter[str] = Counter()
    unknown_funders = 0
    provider_failures = 0

    for wallet in normalized_wallets:
        try:
            transfers = await provider.get_recent_inbound_transfers(wallet)
        except Exception:
            provider_failures += 1
            unknown_funders += 1
            continue

        funder = _resolve_recent_funder(
            transfers or [],
            as_of=reference_time,
            funding_window=funding_window,
        )
        if funder is None:
            unknown_funders += 1
            continue
        funder_counts[funder] += 1

    total_buyers = len(normalized_wallets)
    known_funders = sum(funder_counts.values())
    dominant_funder, dominant_group_size = _largest_funder_group(funder_counts)
    bundled_buyer_pct = round((dominant_group_size / total_buyers) * 100, 2) if total_buyers else 0.0
    flagged = bundled_buyer_pct > bundled_buyer_threshold_pct

    if flagged:
        funding_sybil_check = CheckResult.FAIL
    elif known_funders == 0:
        funding_sybil_check = CheckResult.UNKNOWN
    else:
        funding_sybil_check = CheckResult.PASS

    return FundingAnalysisResult(
        funding_sybil_check=funding_sybil_check,
        bundled_buyer_pct=bundled_buyer_pct,
        largest_common_funder_group_size=dominant_group_size,
        buyers_with_known_funders=known_funders,
        buyers_with_unknown_funders=unknown_funders,
        total_buyers=total_buyers,
        flagged=flagged,
        dominant_funder=dominant_funder,
        provider_failures=provider_failures,
        analysis_window_minutes=int(funding_window.total_seconds() // 60),
    )


def _resolve_recent_funder(
    transfers: Sequence[InboundTransfer],
    *,
    as_of: datetime,
    funding_window: timedelta,
) -> str | None:
    eligible_transfers = [
        transfer
        for transfer in transfers
        if _is_eligible_transfer(transfer, as_of=as_of, funding_window=funding_window)
    ]
    if not eligible_transfers:
        return None

    most_recent_transfer = max(
        eligible_transfers,
        key=lambda transfer: transfer.observed_at or datetime.min.replace(tzinfo=UTC),
    )
    assert most_recent_transfer.source_wallet is not None
    return most_recent_transfer.source_wallet.strip()


def _is_eligible_transfer(
    transfer: InboundTransfer,
    *,
    as_of: datetime,
    funding_window: timedelta,
) -> bool:
    if transfer.source_wallet is None or not transfer.source_wallet.strip():
        return False
    if transfer.observed_at is None:
        return True

    observed_at = transfer.observed_at.astimezone(UTC)
    if observed_at > as_of:
        return False
    return as_of - observed_at <= funding_window


def _largest_funder_group(funder_counts: Counter[str]) -> tuple[str | None, int]:
    if not funder_counts:
        return None, 0
    dominant_funder, dominant_group_size = max(
        funder_counts.items(),
        key=lambda item: (item[1], item[0]),
    )
    return dominant_funder, dominant_group_size
