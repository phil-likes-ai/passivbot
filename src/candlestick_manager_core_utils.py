from __future__ import annotations

import numpy as np


def floor_minute(ms: int, one_min_ms: int) -> int:
    return (int(ms) // one_min_ms) * one_min_ms


def ensure_dtype(a: np.ndarray, candle_dtype) -> np.ndarray:
    if a.dtype != candle_dtype:
        return a.astype(candle_dtype, copy=False)
    return a


def ts_index(a: np.ndarray) -> np.ndarray:
    if a.size == 0:
        return np.empty((0,), dtype=np.int64)
    return np.asarray(a["ts"], dtype=np.int64)


def sanitize_symbol(symbol: str) -> str:
    return symbol.replace("/", "_")
