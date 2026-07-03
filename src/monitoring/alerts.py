"""Alert dispatch boundary."""

from __future__ import annotations


async def send_alert(message: str) -> None:
    if not message:
        raise ValueError("message cannot be empty")
    return None
