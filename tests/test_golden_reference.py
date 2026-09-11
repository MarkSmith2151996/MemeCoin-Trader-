from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "golden_reference.py"


def command(*args: str) -> list[str]:
    return [sys.executable, str(SCRIPT), *args]


def test_freeze_and_verify_detects_config_drift(tmp_path: Path) -> None:
    log = tmp_path / "trades.csv"
    config = tmp_path / "config.json"
    manifest = tmp_path / "manifest.json"
    log.write_text("mint,pnl\na,1\nb,2\n", encoding="utf-8")
    config.write_text(json.dumps({"max_open": 5, "position_size_sol": 0.02}), encoding="utf-8")

    frozen = subprocess.run(
        command(
            "--freeze", "--name", "baseline", "--trade-log", str(log), "--config", str(config),
            "--pnl", "-1.25", "--manifest", str(manifest),
        ),
        check=False,
        capture_output=True,
        text=True,
    )
    assert frozen.returncode == 0, frozen.stderr
    assert subprocess.run(
        command(
            "--verify", "--name", "baseline", "--trade-log", str(log), "--config", str(config),
            "--pnl", "-1.25", "--manifest", str(manifest),
        ),
        check=False,
    ).returncode == 0

    config.write_text(json.dumps({"max_open": 3, "position_size_sol": 0.02}), encoding="utf-8")
    mismatched = subprocess.run(
        command(
            "--verify", "--name", "baseline", "--trade-log", str(log), "--config", str(config),
            "--pnl", "-1.25", "--manifest", str(manifest),
        ),
        check=False,
        capture_output=True,
        text=True,
    )
    assert mismatched.returncode != 0
    assert "config_sha256" in mismatched.stderr
