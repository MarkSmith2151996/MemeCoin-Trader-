"""Cross-process singleton coverage for Strategy B."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_second_strategy_b_process_is_rejected(tmp_path: Path) -> None:
    lock_path = tmp_path / "strategy_b.lock"
    acquire = (
        "from pathlib import Path; "
        "from scripts.run_strategy_b import _acquire_singleton_lock; "
        "_acquire_singleton_lock(Path(sys.argv[1]))"
    )
    holder_command = f"import sys, time; {acquire}; print('locked', flush=True); time.sleep(30)"
    holder = subprocess.Popen(
        [sys.executable, "-c", holder_command, str(lock_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "locked"
        second = subprocess.run(
            [sys.executable, "-c", f"import sys; {acquire}", str(lock_path)],
            capture_output=True,
            text=True,
            check=False,
        )
    finally:
        holder.terminate()
        holder.wait(timeout=5)

    assert second.returncode == 1
    assert "FATAL: another Strategy B instance is running — exiting" in second.stdout
