"""Safe diagnostic helpers for provider and RPC errors."""

from __future__ import annotations

import re
from urllib.parse import urlparse


_URL_RE = re.compile(r"https?://[^\s'\"<>]+")
_SECRET_PARAM_RE = re.compile(r"(?i)\b(api[-_]?key|token|authorization|password|secret)=([^\s&;,]+)")
_BASIC_AUTH_RE = re.compile(r"(?i)\bbasic\s+[^\s,;]+")


def rpc_label(url: str | None) -> str | None:
    """Return a host-only RPC label, never URL credentials or query data."""
    if not url:
        return None
    parsed = urlparse(url)
    if parsed.hostname:
        return parsed.hostname
    return "configured"


def sanitize_provider_error(error: object) -> str:
    """Remove URLs, query secrets, and Basic credentials from exception text."""
    text = str(error)

    def replace_url(match: re.Match[str]) -> str:
        parsed = urlparse(match.group(0))
        return f"{parsed.scheme}://{parsed.hostname or 'configured'}"

    text = _URL_RE.sub(replace_url, text)
    text = _SECRET_PARAM_RE.sub(r"\1=[REDACTED]", text)
    return _BASIC_AUTH_RE.sub("Basic [REDACTED]", text)
