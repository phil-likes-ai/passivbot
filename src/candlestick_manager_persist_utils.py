from __future__ import annotations

import math

import numpy as np


def update_persist_cache(
    self,
    symbol: str,
    arr: np.ndarray,
    *,
    tf_norm: str,
    merge_cache: bool,
    last_refresh_ms: int | None,
    skip_memory_retention: bool,
) -> None:
    if not (merge_cache or tf_norm == "1m"):
        return
    merged_cache = self._merge_overwrite(self._ensure_symbol_cache(symbol), arr)
    self._cache[symbol] = merged_cache
    if not skip_memory_retention:
        try:
            self._enforce_memory_retention(symbol)
        except Exception:
            pass
    if last_refresh_ms is not None and merged_cache.size:
        self._set_last_refresh_meta(
            symbol,
            last_refresh_ms=last_refresh_ms,
            last_final_ts=int(merged_cache[-1]["ts"]),
        )
    self._check_synthetic_replacement(symbol, arr)


def notify_persist_observer(observer, symbol: str, tf_norm: str, arr: np.ndarray) -> None:
    if observer is None:
        return
    try:
        observer(symbol, tf_norm, arr)
    except Exception:
        return


def check_synthetic_replacement(self, symbol: str, real_data: np.ndarray) -> None:
    if symbol not in self._synthetic_timestamps or not self._synthetic_timestamps[symbol]:
        return
    if real_data.size == 0:
        return

    real_ts_set = set(real_data["ts"].astype(np.int64, copy=False).tolist())
    replaced = self._synthetic_timestamps[symbol] & real_ts_set
    if not replaced:
        return

    self._synthetic_timestamps[symbol] -= replaced
    self._invalidate_ema_cache(symbol)
    count = len(replaced)
    if self._candle_replace_batch_mode:
        self._candle_replace_batch[symbol] = self._candle_replace_batch.get(symbol, 0) + count
    else:
        self.log.debug(
            "[candle] %s: real data replaced %d synthetic candle%s, EMA cache invalidated",
            symbol,
            count,
            "s" if count > 1 else "",
        )


def track_synthetic_timestamps(
    self, symbol: str, timestamps: list[int], *, utc_now_ms_fn, one_min_ms: int
) -> None:
    if not symbol or not timestamps:
        return
    ts_set = {int(ts) for ts in timestamps if int(ts) > 0}
    if not ts_set:
        return
    if symbol not in self._synthetic_timestamps:
        self._synthetic_timestamps[symbol] = set()
    self._synthetic_timestamps[symbol].update(ts_set)
    cutoff = utc_now_ms_fn() - 7 * 24 * 60 * one_min_ms
    self._synthetic_timestamps[symbol] = {
        ts for ts in self._synthetic_timestamps[symbol] if ts > cutoff
    }


def plan_runtime_synthetic_gap(
    arr: np.ndarray,
    through_ts: int,
    *,
    one_min_ms: int,
    max_memory_candles_per_symbol: int,
) -> tuple[int, float] | None:
    if arr.size == 0 or through_ts <= 0:
        return None

    arr = np.sort(arr, order="ts")
    ts_arr = arr["ts"].astype(np.int64, copy=False)
    idx = int(np.searchsorted(ts_arr, through_ts, side="right")) - 1
    if idx < 0:
        return None

    last_ts = int(ts_arr[idx])
    if last_ts >= through_ts:
        return None

    max_synth = max(1, min(max_memory_candles_per_symbol, 24 * 60))
    first_synth_ts = max(last_ts + one_min_ms, through_ts - (max_synth - 1) * one_min_ms)
    if first_synth_ts > through_ts:
        return None

    prev_close = float(arr[idx]["c"])
    if not math.isfinite(prev_close):
        return None

    return first_synth_ts, prev_close


def build_runtime_synthetic_gap(
    first_synth_ts: int,
    through_ts: int,
    prev_close: float,
    *,
    one_min_ms: int,
    candle_dtype,
) -> np.ndarray:
    synth_ts = np.arange(first_synth_ts, through_ts + one_min_ms, one_min_ms, dtype=np.int64)
    synth = np.empty((synth_ts.shape[0],), dtype=candle_dtype)
    synth["ts"] = synth_ts
    synth["o"] = prev_close
    synth["h"] = prev_close
    synth["l"] = prev_close
    synth["c"] = prev_close
    synth["bv"] = 0.0
    return synth


def apply_runtime_synthetic_gap(
    self,
    symbol: str,
    arr: np.ndarray,
    synth: np.ndarray,
    *,
    track_synthetic_timestamps_fn,
) -> int:
    synth_ts = synth["ts"].astype(np.int64, copy=False)
    if synth_ts.size == 0:
        return 0

    merged = self._merge_overwrite(arr, synth)
    self._cache[symbol] = merged
    try:
        self._enforce_memory_retention(symbol)
    except Exception as exc:
        self._log(
            "debug",
            "runtime_synthetic_retention_enforcement_failed",
            symbol=symbol,
            error=str(exc),
        )
    track_synthetic_timestamps_fn(symbol, synth_ts.tolist())
    return int(synth_ts.shape[0])


def log_runtime_synthetic_gap(self, symbol: str, synthesized: int, first_ts: int, last_ts: int, seed_last_real_ts: int) -> None:
    self._log(
        "debug",
        "runtime_synthetic_gap_materialized",
        symbol=symbol,
        synthesized=synthesized,
        first_ts=first_ts,
        last_ts=last_ts,
        seed_last_real_ts=seed_last_real_ts,
    )


def invalidate_ema_cache(self, symbol: str) -> None:
    if symbol in self._ema_cache:
        del self._ema_cache[symbol]


def needs_ema_recompute(self, symbol: str) -> bool:
    return symbol not in self._ema_cache or not self._ema_cache[symbol]


def clear_synthetic_tracking(self, symbol: str | None = None) -> None:
    if symbol is None:
        self._synthetic_timestamps.clear()
    elif symbol in self._synthetic_timestamps:
        del self._synthetic_timestamps[symbol]
