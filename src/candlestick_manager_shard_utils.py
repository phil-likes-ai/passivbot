from __future__ import annotations

import os

import numpy as np


def load_shard(self, path: str):
    from candlestick_manager import CANDLE_DTYPE, _ensure_dtype

    if not os.path.exists(path):
        self.log.debug(f"Shard not found (expected for pre-inception): {path}")
        return np.empty((0,), dtype=CANDLE_DTYPE)
    try:
        with open(path, "rb") as f:
            arr = np.load(f, allow_pickle=False)
        if isinstance(arr, np.ndarray) and arr.dtype == CANDLE_DTYPE:
            return arr
        if isinstance(arr, np.ndarray) and arr.ndim == 2 and arr.shape[1] >= 6:
            raw = np.asarray(arr[:, :6], dtype=np.float64)
            out = np.empty((raw.shape[0],), dtype=CANDLE_DTYPE)
            out["ts"] = raw[:, 0].astype(np.int64)
            out["o"] = raw[:, 1].astype(np.float32)
            out["h"] = raw[:, 2].astype(np.float32)
            out["l"] = raw[:, 3].astype(np.float32)
            out["c"] = raw[:, 4].astype(np.float32)
            out["bv"] = raw[:, 5].astype(np.float32)
            return out
        return _ensure_dtype(arr)
    except Exception as e:
        self.log.warning(f"Failed loading shard {path}: {e}")
        return np.empty((0,), dtype=CANDLE_DTYPE)
