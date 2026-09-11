#!/usr/bin/env python3
"""Freeze and verify immutable post-hoc replay trade-log references."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_MANIFEST = REPO_ROOT / "config" / "golden_reference.json"


def stable_hash(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def csv_entry_count(path: Path) -> int:
    with path.open(newline="", encoding="utf-8") as source:
        return sum(1 for _ in csv.DictReader(source))


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "entries": []}
    manifest = load_json(path)
    if not isinstance(manifest.get("entries"), list):
        raise ValueError(f"{path} must contain an entries list")
    return manifest


def find_entry(manifest: dict[str, Any], name: str) -> dict[str, Any]:
    matches = [entry for entry in manifest["entries"] if entry.get("name") == name]
    if len(matches) != 1:
        raise ValueError(
            f"Expected exactly one manifest entry named {name!r}; found {len(matches)}"
        )
    return matches[0]


def diff_entry(expected: dict[str, Any], actual: dict[str, Any]) -> list[str]:
    return [
        f"{key}: expected {expected.get(key)!r}, got {actual.get(key)!r}"
        for key in sorted(set(expected) | set(actual))
        if expected.get(key) != actual.get(key)
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--freeze", action="store_true")
    modes.add_argument("--verify", action="store_true")
    parser.add_argument("--name", required=True)
    parser.add_argument("--trade-log", type=Path, required=True)
    parser.add_argument(
        "--config", type=Path, required=True, help="Claimed replay configuration JSON."
    )
    parser.add_argument(
        "--pnl", type=float, required=True, help="Headline PnL in SOL at full precision."
    )
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--status", default="active", help="Reference status recorded by --freeze.")
    return parser.parse_args()


def observed_entry(args: argparse.Namespace) -> dict[str, Any]:
    if not args.trade_log.is_file():
        raise FileNotFoundError(f"Trade log not found: {args.trade_log}")
    if not args.config.is_file():
        raise FileNotFoundError(f"Configuration not found: {args.config}")
    config = load_json(args.config)
    return {
        "config_sha256": stable_hash(config),
        "trade_log_sha256": file_hash(args.trade_log),
        "entry_count": csv_entry_count(args.trade_log),
        "headline_pnl_sol": args.pnl,
    }


def main() -> None:
    args = parse_args()
    observed = observed_entry(args)
    manifest = load_manifest(args.manifest)
    if args.freeze:
        entry = {"name": args.name, "status": args.status, **observed}
        existing = [item for item in manifest["entries"] if item.get("name") != args.name]
        manifest["entries"] = sorted([*existing, entry], key=lambda item: str(item["name"]))
        args.manifest.parent.mkdir(parents=True, exist_ok=True)
        args.manifest.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        print(f"Frozen {args.name}: {observed['entry_count']:,} entries, {args.pnl:.15f} SOL")
        return

    expected = find_entry(manifest, args.name)
    comparison = {key: expected.get(key) for key in observed}
    differences = diff_entry(comparison, observed)
    if differences:
        message = "Golden reference mismatch:\n" + "\n".join(
            f"- {item}" for item in differences
        )
        raise SystemExit(message)
    print(f"Verified {args.name}: {observed['entry_count']:,} entries, {args.pnl:.15f} SOL")


if __name__ == "__main__":
    main()
