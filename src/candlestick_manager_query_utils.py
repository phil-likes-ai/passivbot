from __future__ import annotations

import asyncio
import time

import numpy as np


async def get_last_prices(self, symbols: list[str], max_age_ms: int = 10_000) -> dict[str, float]:
    """Return latest close for current minute per symbol."""
    out: dict[str, float] = {}
    if not symbols:
        return out

    async def one(sym: str) -> float:
        try:
            val = await self.get_current_close(sym, max_age_ms=max_age_ms)
            return float(val) if isinstance(val, (int, float)) else 0.0
        except Exception:
            return 0.0

    tasks = {s: asyncio.create_task(one(s)) for s in symbols}
    for s, t in tasks.items():
        out[s] = await t
    return out


async def get_ema_bounds_many(
    self,
    items: list[tuple[str, float, float]],
    *,
    max_age_ms: int | None = 60_000,
    timeframe: str | None = None,
    tf: str | None = None,
) -> dict[str, tuple[float, float]]:
    """Return EMA bounds per symbol for a list of `(symbol, span_0, span_1)` tuples."""
    out: dict[str, tuple[float, float]] = {}
    if not items:
        return out

    async def one(sym: str, s0: float, s1: float) -> tuple[float, float]:
        try:
            lo, hi = await self.get_ema_bounds(sym, s0, s1, max_age_ms=max_age_ms, timeframe=timeframe, tf=tf)
            lo = float(lo) if isinstance(lo, (int, float)) else float("nan")
            hi = float(hi) if isinstance(hi, (int, float)) else float("nan")
            if not (np.isfinite(lo) and np.isfinite(hi)):
                return (0.0, 0.0)
            return (lo, hi)
        except Exception:
            return (0.0, 0.0)

    tasks = {sym: asyncio.create_task(one(sym, s0, s1)) for (sym, s0, s1) in items}
    for sym, t in tasks.items():
        out[sym] = await t
    return out


async def get_latest_ema_log_range_many(
    self,
    items: list[tuple[str, float]],
    *,
    max_age_ms: int | None = 600_000,
    timeframe: str | None = None,
    tf: str | None = "1h",
) -> dict[str, float]:
    """Return latest log-range EMA for each `(symbol, span)` pair."""
    out: dict[str, float] = {}
    if not items:
        return out

    async def one(sym: str, span: float) -> float:
        try:
            val = await self.get_latest_ema_log_range(sym, span, max_age_ms=max_age_ms, timeframe=timeframe, tf=tf)
            return float(val) if np.isfinite(val) else 0.0
        except Exception:
            return 0.0

    tasks = {sym: asyncio.create_task(one(sym, span)) for (sym, span) in items}
    for sym, t in tasks.items():
        out[sym] = await t
    return out


def set_current_close(self, symbol: str, price: float, timestamp_ms: int) -> None:
    """Inject a price into the current-close cache."""
    self._current_close_cache[symbol] = (float(price), int(timestamp_ms))


def is_rate_limited(self) -> bool:
    """Return True if a global rate-limit backoff is active."""
    return self._rate_limit_until > time.time()
