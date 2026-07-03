#!/usr/bin/env python
"""Create a minimal token record and run local risk scoring."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.core.models import TokenInfo  # noqa: E402
from src.risk.scorer import assess_token  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Risk-check a token mint with available local data")
    parser.add_argument("mint_address")
    args = parser.parse_args()

    token = TokenInfo(mint_address=args.mint_address)
    assessment = assess_token(token)
    print(assessment.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
