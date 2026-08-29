"""V2 collector/executor logging timestamp coverage."""

from __future__ import annotations

import logging

from services.logging_utils import UtcLogFormatter


def test_utc_log_formatter_matches_z_suffix() -> None:
    record = logging.LogRecord("test", logging.INFO, __file__, 1, "message", (), None)
    record.created = 0
    formatter = UtcLogFormatter(
        "%(asctime)sZ %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    assert formatter.format(record) == "1970-01-01T00:00:00Z INFO message"
