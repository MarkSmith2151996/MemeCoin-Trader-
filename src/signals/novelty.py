"""Pure novelty summaries for caller-supplied raw signals."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from src.core.models import Signal


@dataclass(frozen=True)
class SourceNoveltySummary:
    """Raw and session-novelty counts for one observed signal source."""

    total_signals: int
    unique_mints: int
    novel_signals: int
    duplicate_signals: int
    invalid_mint_signals: int
    invalid_wallet_origin_signals: int


@dataclass(frozen=True)
class SignalOrigin:
    """One source and optional tracked-wallet label observed for a mint."""

    source: str
    wallet_label: str | None = None


@dataclass(frozen=True)
class NoveltySummary:
    """Session-scoped duplicate and source-origin diagnostics."""

    total_signals: int
    unique_mints: int
    novel_signals: int
    duplicate_signals: int
    invalid_mint_signals: int
    invalid_wallet_origin_signals: int
    source_mix: dict[str, SourceNoveltySummary]
    origins_by_mint: dict[str, tuple[SignalOrigin, ...]]


@dataclass
class _SourceCounts:
    total_signals: int = 0
    novel_signals: int = 0
    duplicate_signals: int = 0
    invalid_mint_signals: int = 0
    invalid_wallet_origin_signals: int = 0


def summarize_novelty(signals: Iterable[Signal]) -> NoveltySummary:
    """Summarize raw signals without polling, ranking, filtering, or persistence.

    Novelty is limited to the supplied sequence: the first nonblank occurrence of
    a mint is novel and later occurrences are duplicates. Solana mint casing is
    preserved because it is part of the address identity.
    """

    seen_mints: set[str] = set()
    source_mints: dict[str, set[str]] = {}
    source_counts: dict[str, _SourceCounts] = {}
    origins: dict[str, list[SignalOrigin]] = {}
    total_signals = 0
    duplicate_signals = 0
    invalid_mint_signals = 0
    invalid_wallet_origin_signals = 0

    for signal in signals:
        total_signals += 1
        source = signal.source.value
        counts = source_counts.setdefault(source, _SourceCounts())
        counts.total_signals += 1

        wallet_label, has_invalid_wallet_origin = _wallet_origin(signal)
        if has_invalid_wallet_origin:
            invalid_wallet_origin_signals += 1
            counts.invalid_wallet_origin_signals += 1

        mint = signal.mint_address.strip()
        if not mint:
            invalid_mint_signals += 1
            counts.invalid_mint_signals += 1
            continue

        source_mints.setdefault(source, set()).add(mint)
        origin = SignalOrigin(source=source, wallet_label=wallet_label)
        mint_origins = origins.setdefault(mint, [])
        if origin not in mint_origins:
            mint_origins.append(origin)

        if mint in seen_mints:
            duplicate_signals += 1
            counts.duplicate_signals += 1
            continue

        seen_mints.add(mint)
        counts.novel_signals += 1

    return NoveltySummary(
        total_signals=total_signals,
        unique_mints=len(seen_mints),
        novel_signals=len(seen_mints),
        duplicate_signals=duplicate_signals,
        invalid_mint_signals=invalid_mint_signals,
        invalid_wallet_origin_signals=invalid_wallet_origin_signals,
        source_mix={
            source: SourceNoveltySummary(
                total_signals=counts.total_signals,
                unique_mints=len(source_mints.get(source, set())),
                novel_signals=counts.novel_signals,
                duplicate_signals=counts.duplicate_signals,
                invalid_mint_signals=counts.invalid_mint_signals,
                invalid_wallet_origin_signals=counts.invalid_wallet_origin_signals,
            )
            for source, counts in source_counts.items()
        },
        origins_by_mint={mint: tuple(mint_origins) for mint, mint_origins in origins.items()},
    )


def _wallet_origin(signal: Signal) -> tuple[str | None, bool]:
    """Return an existing whale label without interpreting arbitrary payload data."""

    if signal.source.value != "WHALE_TRACKER":
        return None, False

    tracked_wallet = signal.payload.get("tracked_wallet")
    if not isinstance(tracked_wallet, dict):
        return None, True

    label = tracked_wallet.get("label")
    if not isinstance(label, str) or not label.strip():
        return None, True
    return label.strip(), False
