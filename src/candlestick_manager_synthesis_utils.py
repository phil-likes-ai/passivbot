from __future__ import annotations

import math

import numpy as np


def linear_interpolate(value0: float, value1: float, ratio: float) -> float:
    return float(value0 + (value1 - value0) * ratio)


def ohlcv_xm_to_1m(candle: np.void, minutes: int, *, candle_dtype, one_min_ms: int) -> np.ndarray:
    if minutes <= 0:
        raise ValueError(f"minutes must be > 0, got {minutes}")

    ts = int(candle["ts"])
    o = float(candle["o"])
    h = float(candle["h"])
    l = float(candle["l"])
    c = float(candle["c"])
    bv = float(candle["bv"])

    if not all(math.isfinite(x) for x in (o, h, l, c, bv)):
        raise ValueError("all OHLCV values must be finite")
    if h < l:
        h, l = l, h
    o = min(max(o, l), h)
    c = min(max(c, l), h)

    out = np.zeros(minutes, dtype=candle_dtype)
    out["ts"] = np.arange(ts, ts + minutes * one_min_ms, one_min_ms, dtype=np.int64)
    out["bv"] = float(bv / minutes)

    last_idx = minutes - 1
    if last_idx == 0:
        out[0]["o"] = o
        out[0]["h"] = h
        out[0]["l"] = l
        out[0]["c"] = c
        return out

    pivot_a = min(last_idx, max(1, minutes // 3))
    pivot_b = min(last_idx, max(pivot_a + 1, (2 * minutes) // 3))

    if c >= o:
        waypoints = [(0, o), (pivot_a, l), (pivot_b, h), (last_idx, c)]
        low_idx = pivot_a
        high_idx = pivot_b
    else:
        waypoints = [(0, o), (pivot_a, h), (pivot_b, l), (last_idx, c)]
        high_idx = pivot_a
        low_idx = pivot_b

    deduped = [waypoints[0]]
    for idx, value in waypoints[1:]:
        if idx > deduped[-1][0]:
            deduped.append((idx, value))
        else:
            deduped[-1] = (idx, value)

    close_path = np.empty(minutes, dtype=np.float64)
    close_path[0] = o
    for (i0, v0), (i1, v1) in zip(deduped, deduped[1:]):
        span = max(1, i1 - i0)
        for minute_idx in range(i0, i1 + 1):
            ratio = 0.0 if i1 == i0 else (minute_idx - i0) / span
            close_path[minute_idx] = min(max(linear_interpolate(v0, v1, ratio), l), h)

    prev_close = o
    for minute_idx in range(minutes):
        minute_open = prev_close
        minute_close = float(close_path[minute_idx])
        minute_high = max(minute_open, minute_close)
        minute_low = min(minute_open, minute_close)
        if minute_idx == high_idx:
            minute_high = max(minute_high, h)
        if minute_idx == low_idx:
            minute_low = min(minute_low, l)
        out[minute_idx]["o"] = minute_open
        out[minute_idx]["h"] = minute_high
        out[minute_idx]["l"] = minute_low
        out[minute_idx]["c"] = minute_close
        prev_close = minute_close

    return out


def ohlcv_5m_to_1m(candle: np.void, *, candle_dtype, one_min_ms: int) -> np.ndarray:
    return ohlcv_xm_to_1m(candle, 5, candle_dtype=candle_dtype, one_min_ms=one_min_ms)


def ohlcv_15m_to_1m(candle: np.void, *, candle_dtype, one_min_ms: int) -> np.ndarray:
    return ohlcv_xm_to_1m(candle, 15, candle_dtype=candle_dtype, one_min_ms=one_min_ms)


def synthesize_1m_from_higher_tf(
    candles: np.ndarray,
    tf_minutes: int,
    *,
    ensure_dtype_fn,
    candle_dtype,
    one_min_ms: int,
) -> np.ndarray:
    arr = ensure_dtype_fn(candles)
    if arr.size == 0:
        return np.empty((0,), dtype=candle_dtype)
    if tf_minutes == 5:
        expanded = [ohlcv_5m_to_1m(row, candle_dtype=candle_dtype, one_min_ms=one_min_ms) for row in arr]
    elif tf_minutes == 15:
        expanded = [ohlcv_15m_to_1m(row, candle_dtype=candle_dtype, one_min_ms=one_min_ms) for row in arr]
    else:
        raise ValueError(f"unsupported tf_minutes={tf_minutes}")
    if not expanded:
        return np.empty((0,), dtype=candle_dtype)
    return np.sort(np.concatenate(expanded), order="ts")
