import asyncio
from datetime import UTC, datetime, timedelta

from src.core.models import CheckResult
from src.risk.funding_analysis import InboundTransfer, analyze_buyer_funding


class FakeFundingProvider:
    def __init__(self, transfers_by_wallet: dict[str, list[InboundTransfer] | None], failures: set[str] | None = None) -> None:
        self._transfers_by_wallet = transfers_by_wallet
        self._failures = failures or set()
        self.calls: list[str] = []

    async def get_recent_inbound_transfers(self, wallet: str) -> list[InboundTransfer] | None:
        self.calls.append(wallet)
        if wallet in self._failures:
            raise RuntimeError(f"lookup failed for {wallet}")
        return self._transfers_by_wallet.get(wallet, [])


def test_shared_funder_majority_flags_bundled_launch() -> None:
    as_of = datetime(2026, 7, 7, 12, 0, tzinfo=UTC)
    buyers = [f"buyer-{index}" for index in range(40)]
    transfers_by_wallet = {
        wallet: [InboundTransfer(source_wallet="shared-funder", observed_at=as_of - timedelta(minutes=3))]
        for wallet in buyers[:35]
    }
    for wallet in buyers[35:]:
        transfers_by_wallet[wallet] = [InboundTransfer(source_wallet=f"funder-{wallet}", observed_at=as_of - timedelta(minutes=4))]

    result = asyncio.run(analyze_buyer_funding(buyers, FakeFundingProvider(transfers_by_wallet), as_of=as_of))

    assert result.funding_sybil_check == CheckResult.FAIL
    assert result.flagged is True
    assert result.bundled_buyer_pct == 87.5
    assert result.largest_common_funder_group_size == 35
    assert result.buyers_with_known_funders == 40
    assert result.buyers_with_unknown_funders == 0


def test_diverse_funders_pass() -> None:
    as_of = datetime(2026, 7, 7, 12, 0, tzinfo=UTC)
    buyers = [f"buyer-{index}" for index in range(8)]
    transfers_by_wallet = {
        wallet: [InboundTransfer(source_wallet=f"funder-{index}", observed_at=as_of - timedelta(minutes=2))]
        for index, wallet in enumerate(buyers)
    }

    result = asyncio.run(analyze_buyer_funding(buyers, FakeFundingProvider(transfers_by_wallet), as_of=as_of))

    assert result.funding_sybil_check == CheckResult.PASS
    assert result.flagged is False
    assert result.bundled_buyer_pct == 12.5
    assert result.largest_common_funder_group_size == 1
    assert result.buyers_with_known_funders == 8
    assert result.buyers_with_unknown_funders == 0


def test_missing_and_failed_provider_data_degrades_gracefully() -> None:
    as_of = datetime(2026, 7, 7, 12, 0, tzinfo=UTC)
    buyers = ["known-1", "known-2", "missing", "failed"]
    provider = FakeFundingProvider(
        {
            "known-1": [InboundTransfer(source_wallet="funder-a", observed_at=as_of - timedelta(minutes=1))],
            "known-2": [InboundTransfer(source_wallet="funder-b", observed_at=as_of - timedelta(minutes=1))],
            "missing": None,
        },
        failures={"failed"},
    )

    result = asyncio.run(analyze_buyer_funding(buyers, provider, as_of=as_of))

    assert result.funding_sybil_check == CheckResult.PASS
    assert result.flagged is False
    assert result.buyers_with_known_funders == 2
    assert result.buyers_with_unknown_funders == 2
    assert result.provider_failures == 1
    assert provider.calls == buyers


def test_empty_buyer_list_returns_neutral_unknown_result() -> None:
    provider = FakeFundingProvider({})

    result = asyncio.run(analyze_buyer_funding([], provider))

    assert result.funding_sybil_check == CheckResult.UNKNOWN
    assert result.flagged is False
    assert result.total_buyers == 0
    assert result.bundled_buyer_pct == 0.0
    assert result.buyers_with_known_funders == 0
    assert result.buyers_with_unknown_funders == 0
    assert provider.calls == []


def test_time_window_filtering_ignores_stale_funding() -> None:
    as_of = datetime(2026, 7, 7, 12, 0, tzinfo=UTC)
    buyers = ["buyer-1", "buyer-2", "buyer-3"]
    provider = FakeFundingProvider(
        {
            "buyer-1": [InboundTransfer(source_wallet="shared", observed_at=as_of - timedelta(minutes=4))],
            "buyer-2": [InboundTransfer(source_wallet="shared", observed_at=as_of - timedelta(minutes=16))],
            "buyer-3": [InboundTransfer(source_wallet="shared", observed_at=as_of - timedelta(minutes=2))],
        }
    )

    result = asyncio.run(
        analyze_buyer_funding(
            buyers,
            provider,
            as_of=as_of,
            funding_window=timedelta(minutes=15),
        )
    )

    assert result.funding_sybil_check == CheckResult.FAIL
    assert result.buyers_with_known_funders == 2
    assert result.buyers_with_unknown_funders == 1
    assert result.largest_common_funder_group_size == 2
    assert result.bundled_buyer_pct == 66.67


def test_all_unknown_funders_return_unknown_without_network_calls() -> None:
    buyers = ["buyer-1", "buyer-2", "buyer-3"]
    provider = FakeFundingProvider(
        {
            wallet: [InboundTransfer(source_wallet=None)]
            for wallet in buyers
        }
    )

    result = asyncio.run(analyze_buyer_funding(buyers, provider))

    assert result.funding_sybil_check == CheckResult.UNKNOWN
    assert result.flagged is False
    assert result.buyers_with_known_funders == 0
    assert result.buyers_with_unknown_funders == 3
    assert provider.calls == buyers
