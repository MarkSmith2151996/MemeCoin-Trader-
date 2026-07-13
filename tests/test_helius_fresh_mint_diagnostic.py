import pytest

from src.signals.helius_fresh_mint_diagnostic import (
    DEFAULT_SOURCE_LABEL,
    FreshMintDiagnosticRequest,
    FreshMintDiagnosticState,
    run_fresh_mint_diagnostic,
)


def test_fake_transport_reports_aggregate_mint_counts_and_bad_records() -> None:
    calls: list[FreshMintDiagnosticRequest] = []

    def transport(request: FreshMintDiagnosticRequest) -> list[object]:
        calls.append(request)
        return [
            {"mint": "mint-a"},
            {"mint": "mint-a"},
            {"mint": "mint-b"},
            {"mint": " "},
            "not-a-record",
        ]

    request = FreshMintDiagnosticRequest(contract_id="fake-contract")
    report = run_fresh_mint_diagnostic(request, transport)

    assert calls == [request]
    assert report.state == FreshMintDiagnosticState.OK
    assert report.source_label == DEFAULT_SOURCE_LABEL
    assert report.total_records == 5
    assert report.unique_mints == 2
    assert report.invalid_records == 2
    assert report.duplicate_rate == pytest.approx(1 / 3)


def test_fake_transport_record_cap_is_bounded_to_five() -> None:
    def transport(_: FreshMintDiagnosticRequest) -> list[object]:
        return [{"mint": f"mint-{index}"} for index in range(8)]

    report = run_fresh_mint_diagnostic(
        FreshMintDiagnosticRequest(contract_id="fake-contract", max_records=99),
        transport,
    )

    assert report.state == FreshMintDiagnosticState.OK
    assert report.total_records == 5
    assert report.unique_mints == 5
    assert report.duplicate_rate == 0.0


def test_blank_contract_does_not_call_transport() -> None:
    def transport(_: FreshMintDiagnosticRequest) -> list[object]:
        raise AssertionError("transport must not run without a contract")

    report = run_fresh_mint_diagnostic(
        FreshMintDiagnosticRequest(contract_id=" "),
        transport,
    )

    assert report.state == FreshMintDiagnosticState.UNCONFIGURED
    assert report.total_records == 0
    assert report.duplicate_rate is None


def test_transport_error_returns_sanitized_aggregate_only_report() -> None:
    def transport(_: FreshMintDiagnosticRequest) -> list[object]:
        raise RuntimeError("https://helius.example/?api-key=secret")

    report = run_fresh_mint_diagnostic(
        FreshMintDiagnosticRequest(contract_id="fake-contract"),
        transport,
    )

    assert report.state == FreshMintDiagnosticState.PROVIDER_ERROR
    assert report.total_records == 0
    assert report.source_label == DEFAULT_SOURCE_LABEL
