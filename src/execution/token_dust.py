"""Shared raw-token dust limits for live clearance and reconciliation."""

from __future__ import annotations

# A confirmed sell may leave a few base units due to token-program rounding.
# Reconciliation must accept exactly the same residue the exit path accepts.
RAW_TOKEN_DUST_TOLERANCE = 10
WALLET_ONLY_DUST_VALUE_SOL = 0.001
