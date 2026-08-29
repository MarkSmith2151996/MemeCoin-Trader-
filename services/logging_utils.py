"""Shared logging setup for V2 service entry points."""

from __future__ import annotations

import logging
import sys
import time


class UtcLogFormatter(logging.Formatter):
    """Render an ISO-like UTC timestamp before appending the ``Z`` marker."""

    converter = time.gmtime


def configure_service_logging() -> None:
    """Configure the collector and executor with truthful UTC timestamps."""

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        UtcLogFormatter(
            "%(asctime)sZ %(levelname)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    )
    logging.basicConfig(level=logging.INFO, handlers=[handler])
