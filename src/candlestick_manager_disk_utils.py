from __future__ import annotations

import time
from typing import Any, Dict, List, Tuple

import numpy as np


def plan_disk_load(
    symbol: str,
    tf_norm: str,
    days: Dict[str, Tuple[int, int]],
    shard_paths: Dict[str, str],
    legacy_paths: Dict[str, str],
    legacy_day_is_complete_fn,
) -> Tuple[List[Tuple[str, str]], Dict[str, Dict[str, Any]], int, int, int]:
    load_keys: List[Tuple[str, str]] = []
    day_ctx: Dict[str, Dict[str, Any]] = {}
    legacy_hits = 0
    primary_hits = 0
    merged_hits = 0

    for key, (day_start, day_end) in days.items():
        primary_path = shard_paths.get(key)
        legacy_path = legacy_paths.get(key)
        if primary_path is None and legacy_path is None:
            continue

        chosen_path = None
        chosen_source = ""

        if tf_norm == "1m" and legacy_path is not None:
            legacy_complete = False
            try:
                legacy_complete = legacy_day_is_complete_fn(symbol, tf_norm, key)
            except Exception:
                legacy_complete = False

            if legacy_complete:
                chosen_path = legacy_path
                chosen_source = "legacy"
                legacy_hits += 1
            elif primary_path is not None:
                chosen_path = legacy_path
                chosen_source = "merge"
                merged_hits += 1
            else:
                chosen_path = legacy_path
                chosen_source = "legacy"
                legacy_hits += 1
        else:
            if primary_path is not None:
                chosen_path = primary_path
                chosen_source = "primary"
                primary_hits += 1
            else:
                chosen_path = legacy_path
                chosen_source = "legacy"
                legacy_hits += 1

        if chosen_path is not None:
            load_keys.append((key, chosen_path))
            day_ctx[key] = {
                "day_start": int(day_start),
                "day_end": int(day_end),
                "source": chosen_source,
                "primary_path": primary_path,
                "legacy_path": legacy_path,
            }

    return load_keys, day_ctx, legacy_hits, primary_hits, merged_hits


def execute_disk_load(
    load_keys: List[Tuple[str, str]],
    day_ctx: Dict[str, Dict[str, Any]],
    tf_norm: str,
    *,
    load_shard_fn,
    merge_overwrite_fn,
    candle_dtype,
    progress_cb,
) -> Tuple[List[np.ndarray], float]:
    arrays: List[np.ndarray] = []
    t0 = time.monotonic()
    last_progress_log = t0
    total = len(load_keys)
    for i, (day_key, path) in enumerate(sorted(load_keys), start=1):
        ctx = day_ctx.get(day_key, {})
        src = str(ctx.get("source") or "")
        if tf_norm == "1m" and src == "merge":
            legacy_arr = load_shard_fn(path)
            primary_arr = np.empty((0,), dtype=candle_dtype)
            try:
                primary_path = ctx.get("primary_path")
                if primary_path:
                    primary_arr = load_shard_fn(str(primary_path))
            except Exception:
                primary_arr = np.empty((0,), dtype=candle_dtype)
            arr = merge_overwrite_fn(primary_arr, legacy_arr)
        else:
            arr = load_shard_fn(path)

        arrays.append(arr)
        now = time.monotonic()
        if now - last_progress_log >= 5.0 or i == total:
            last_progress_log = now
            progress_cb(i, total, day_key, now - t0)

    arrays = [arr for arr in arrays if arr.size]
    return arrays, t0


def finalize_disk_arrays(arrays: List[np.ndarray]) -> np.ndarray | None:
    if not arrays:
        return None
    return np.sort(np.concatenate(arrays), order="ts")


def bucket_rows_by_date_key(arr: np.ndarray, date_key_fn) -> List[Tuple[str, np.ndarray]]:
    buckets: List[Tuple[str, np.ndarray]] = []
    current_key: str | None = None
    bucket_rows: List[tuple] = []
    for row in arr:
        key = date_key_fn(int(row["ts"]))
        if current_key is None:
            current_key = key
        if key != current_key:
            if bucket_rows:
                buckets.append((str(current_key), np.array(bucket_rows, dtype=arr.dtype)))
            bucket_rows = []
            current_key = key
        bucket_rows.append(tuple(row.tolist()))
    if bucket_rows and current_key is not None:
        buckets.append((current_key, np.array(bucket_rows, dtype=arr.dtype)))
    return buckets


def save_incremental_buckets(
    symbol: str,
    buckets: List[Tuple[str, np.ndarray]],
    shard_paths: Dict[str, str],
    *,
    tf_norm: str,
    defer_index: bool,
    candle_dtype,
    load_shard_fn,
    merge_overwrite_fn,
    save_shard_fn,
    shard_path_fn,
) -> List[str]:
    shards_saved: List[str] = []
    total = len(buckets)
    for i, (key, chunk) in enumerate(buckets):
        existing = np.empty((0,), dtype=candle_dtype)
        path = shard_paths.get(key)
        if path:
            try:
                existing = load_shard_fn(path)
            except Exception:
                existing = np.empty((0,), dtype=candle_dtype)
        merged = merge_overwrite_fn(existing, chunk)
        should_defer = defer_index or i != total - 1
        save_shard_fn(symbol, key, merged, tf=tf_norm, defer_index=should_defer)
        shard_paths[key] = shard_path_fn(symbol, key, tf=tf_norm)
        shards_saved.append(key)
    return shards_saved
