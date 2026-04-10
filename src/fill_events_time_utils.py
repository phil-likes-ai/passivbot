from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional


def format_ms(ts: Optional[int]) -> str:
    if ts is None:
        return "None"
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def day_key(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
