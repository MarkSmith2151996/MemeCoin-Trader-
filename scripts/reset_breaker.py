#!/usr/bin/env python3
"""Manually reset (or inspect) the live circuit breaker flag (MT-546).

The breaker trips automatically when a live sell fails. Until it is reset,
new live buys are blocked. Use this only after investigating the failed sell.

Run:
    python3 scripts/reset_breaker.py          # show state, no changes
    python3 scripts/reset_breaker.py --reset  # clear the trip flag
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.execution.safety_controls import CircuitBreaker  # noqa: E402

BREAKER_PATH = ROOT / "data" / "circuit_breaker.json"


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect or reset the circuit breaker flag")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Clear the trip flag after investigating the failed sell",
    )
    args = parser.parse_args()

    breaker = CircuitBreaker(flag_path=BREAKER_PATH)
    state = breaker.status()

    if not state.tripped:
        print(f"Circuit breaker: CLEAR ({BREAKER_PATH})")
        if args.reset:
            print("Nothing to reset.")
        return 0

    print(f"Circuit breaker: TRIPPED ({BREAKER_PATH})")
    print(f"  reason:      {state.reason or '-'}")
    print(f"  mint:        {state.mint or '-'}")
    print(f"  signature:   {state.signature_attempt or '-'}")
    print(f"  error:       {state.error or '-'}")
    print(f"  tripped at:  {state.tripped_at or '-'}")

    if not args.reset:
        print()
        print("New live buys are blocked. Run with --reset after investigating the failed sell.")
        return 0

    breaker.reset()
    print()
    print("Circuit breaker RESET — new live buys are enabled again.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
