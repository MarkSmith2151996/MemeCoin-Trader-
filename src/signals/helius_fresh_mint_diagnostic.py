"""Fake-transport scaffold for bounded Helius fresh-mint diagnostics."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import StrEnum
from itertools import islice


MAX_DIAGNOSTIC_RECORDS = 5
DEFAULT_SOURCE_LABEL = "helius_fresh_mint_diagnostic"
FreshMintTransport = Callable[["FreshMintDiagnosticRequest"], Iterable[object]]


class FreshMintDiagnosticState(StrEnum):
    OK = "ok"
    UNCONFIGURED = "unconfigured"
    PROVIDER_ERROR = "provider_error"


@dataclass(frozen=True)
class FreshMintDiagnosticRequest:
    """Explicit contract identifier and bounded record count for a fake transport."""

    contract_id: str
    max_records: int = MAX_DIAGNOSTIC_RECORDS
    source_label: str = DEFAULT_SOURCE_LABEL


@dataclass(frozen=True)
class FreshMintDiagnosticReport:
    """Aggregate-only result that intentionally excludes raw provider details."""

    state: FreshMintDiagnosticState
    source_label: str
    total_records: int
    unique_mints: int
    invalid_records: int
    duplicate_rate: float | None


def run_fresh_mint_diagnostic(
    request: FreshMintDiagnosticRequest,
    transport: FreshMintTransport,
) -> FreshMintDiagnosticReport:
    """Run an injected fake transport and return bounded aggregate mint counts."""

    contract_id = request.contract_id.strip()
    source_label = request.source_label.strip() or DEFAULT_SOURCE_LABEL
    if not contract_id:
        return _report(
            FreshMintDiagnosticState.UNCONFIGURED,
            source_label,
            0,
            0,
            0,
            0,
        )

    try:
        records = list(islice(transport(request), _record_cap(request.max_records)))
    except Exception:
        return _report(
            FreshMintDiagnosticState.PROVIDER_ERROR,
            source_label,
            0,
            0,
            0,
            0,
        )

    seen_mints: set[str] = set()
    invalid_records = 0
    duplicate_records = 0
    for record in records:
        mint = _mint_from_record(record)
        if mint is None:
            invalid_records += 1
            continue
        if mint in seen_mints:
            duplicate_records += 1
            continue
        seen_mints.add(mint)

    return _report(
        FreshMintDiagnosticState.OK,
        source_label,
        len(records),
        len(seen_mints),
        invalid_records,
        duplicate_records,
    )


def _record_cap(max_records: int) -> int:
    return min(max(max_records, 0), MAX_DIAGNOSTIC_RECORDS)


def _mint_from_record(record: object) -> str | None:
    if not isinstance(record, dict):
        return None
    mint = record.get("mint")
    if not isinstance(mint, str) or not mint.strip():
        return None
    return mint.strip()


def _report(
    state: FreshMintDiagnosticState,
    source_label: str,
    total_records: int,
    unique_mints: int,
    invalid_records: int,
    duplicate_records: int,
) -> FreshMintDiagnosticReport:
    valid_records = unique_mints + duplicate_records
    duplicate_rate = duplicate_records / valid_records if valid_records else None
    return FreshMintDiagnosticReport(
        state=state,
        source_label=source_label,
        total_records=total_records,
        unique_mints=unique_mints,
        invalid_records=invalid_records,
        duplicate_rate=duplicate_rate,
    )
