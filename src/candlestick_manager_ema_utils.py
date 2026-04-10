from __future__ import annotations

import math
from importlib import import_module
from typing import Optional

import numpy as np


def _cm_helpers():
    cm = import_module("candlestick_manager")
    return cm.ONE_MIN_MS, cm._utc_now_ms, cm._tf_to_ms


def ema(self, values: np.ndarray, span: float) -> float:
    return float(self._ema_series(values, span)[-1])


def ema_series(self, values: np.ndarray, span: float) -> np.ndarray:
    """Return bias-corrected EMA (pandas ewm adjust=True) over `values`."""
    n = int(values.shape[0])
    if n == 0:
        return np.empty((0,), dtype=np.float64)
    span = float(span)
    alpha = 2.0 / (span + 1.0)
    one_minus = 1.0 - alpha
    out = np.empty((n,), dtype=np.float64)
    num = float(values[0])
    den = 1.0
    out[0] = num / den
    for i in range(1, n):
        v = float(values[i])
        if not np.isfinite(v):
            out[i] = out[i - 1]
            continue
        num = alpha * v + one_minus * num
        den = alpha + one_minus * den
        if den <= np.finfo(np.float64).tiny:
            num = alpha * v
            den = alpha
        out[i] = num / den
    return out


async def latest_finalized_range(self, span: float, *, period_ms: int | None = None):
    one_min_ms, utc_now_ms, _ = _cm_helpers()
    if period_ms is None:
        period_ms = one_min_ms
    span_candles = max(1, int(math.ceil(float(span))))
    now = utc_now_ms()
    end_floor = (int(now) // int(period_ms)) * int(period_ms)
    end_ts = int(end_floor - period_ms)
    start_ts = int(end_ts - period_ms * (span_candles - 1))
    return start_ts, end_ts


async def get_latest_ema_volume(
    self,
    symbol: str,
    span: float,
    max_age_ms: Optional[int] = None,
    *,
    timeframe: Optional[str] = None,
    tf: Optional[str] = None,
) -> float:
    return await self._get_latest_ema_generic(
        symbol,
        span,
        max_age_ms,
        timeframe,
        tf=tf,
        metric_key="volume",
        series_fn=lambda a: np.asarray(a["bv"], dtype=np.float64),
    )


async def get_latest_ema_quote_volume(
    self,
    symbol: str,
    span: float,
    max_age_ms: Optional[int] = None,
    *,
    timeframe: Optional[str] = None,
    tf: Optional[str] = None,
) -> float:
    """Return latest EMA of quote volume over last `span` finalized candles."""
    return await self._get_latest_ema_generic(
        symbol,
        span,
        max_age_ms,
        timeframe,
        tf=tf,
        metric_key="qv",
        series_fn=lambda a: (
            np.asarray(a["bv"], dtype=np.float64)
            * (np.asarray(a["h"], dtype=np.float64) + np.asarray(a["l"], dtype=np.float64) + np.asarray(a["c"], dtype=np.float64))
            / 3.0
        ),
    )


async def get_latest_ema_generic(
    self,
    symbol: str,
    span: float,
    max_age_ms: Optional[int],
    timeframe: Optional[str],
    *,
    tf: Optional[str] = None,
    metric_key: str,
    series_fn,
) -> float:
    """Shared implementation for EMA helpers over a derived series."""
    _, utc_now_ms, tf_to_ms = _cm_helpers()
    out_tf = timeframe if timeframe is not None else tf
    period_ms = tf_to_ms(out_tf)
    start_ts, end_ts = await self._latest_finalized_range(span, period_ms=period_ms)
    now = utc_now_ms()
    tf_key = str(period_ms)
    key = (metric_key, float(span), tf_key)
    cache = self._ema_cache.setdefault(symbol, {})
    if max_age_ms is not None and max_age_ms > 0 and key in cache:
        val, cached_end_ts, computed_at = cache[key]
        if int(cached_end_ts) == int(end_ts) and (now - int(computed_at)) <= int(max_age_ms):
            return float(val)
    arr = await self.get_candles(symbol, start_ts=start_ts, end_ts=end_ts, max_age_ms=max_age_ms, timeframe=out_tf)
    if arr.size == 0:
        return float("nan")
    series = series_fn(arr)
    res = float(self._ema(series, span))
    cache[key] = (res, int(end_ts), int(now))
    return res
