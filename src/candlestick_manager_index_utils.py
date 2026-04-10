from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

import numpy as np
import zlib


def symbol_dir(self, symbol: str, timeframe: Optional[str] = None, *, tf: Optional[str] = None) -> str:
    from candlestick_manager import _sanitize_symbol

    sym = _sanitize_symbol(symbol)
    tf_dir = self._normalize_timeframe_arg(timeframe, tf)
    return str(Path(self.cache_dir) / "ohlcv" / self.exchange_name / tf_dir / sym)


def index_path(self, symbol: str, timeframe: Optional[str] = None, *, tf: Optional[str] = None) -> str:
    return str(Path(self._symbol_dir(symbol, timeframe=timeframe, tf=tf)) / "index.json")


def shard_path(
    self,
    symbol: str,
    date_key: str,
    timeframe: Optional[str] = None,
    *,
    tf: Optional[str] = None,
) -> str:
    return str(Path(self._symbol_dir(symbol, timeframe=timeframe, tf=tf)) / f"{date_key}.npy")


def prune_missing_shards_from_index(self, idx: dict) -> int:
    """Remove shard entries whose files are missing; refresh derived meta fields."""
    del self
    try:
        shards = idx.get("shards", {})
        if not isinstance(shards, dict) or not shards:
            return 0
        removed = 0
        for day_key, shard_meta in list(shards.items()):
            if not isinstance(shard_meta, dict):
                continue
            path = shard_meta.get("path")
            if not path:
                continue
            if not os.path.exists(str(path)):
                shards.pop(day_key, None)
                removed += 1
        if not removed:
            return 0
        idx["shards"] = shards
        meta = idx.setdefault("meta", {})
        try:
            last_ts = 0
            inception_ts: Optional[int] = None
            for shard_meta in shards.values():
                if not isinstance(shard_meta, dict):
                    continue
                mt = shard_meta.get("max_ts")
                if mt is not None:
                    last_ts = max(last_ts, int(mt))
                mi = shard_meta.get("min_ts")
                if mi is not None:
                    inception_ts = int(mi) if inception_ts is None else min(inception_ts, int(mi))
            meta["last_final_ts"] = int(last_ts)
            meta["inception_ts"] = inception_ts
        except Exception:
            meta["last_final_ts"] = 0
            meta["inception_ts"] = None
        return int(removed)
    except Exception:
        return 0


def ensure_symbol_index(self, symbol: str, timeframe: Optional[str] = None, *, tf: Optional[str] = None) -> dict:
    tf_norm = self._normalize_timeframe_arg(timeframe, tf)
    key = f"{symbol}::{tf_norm}"
    idx_path = self._index_path(symbol, timeframe=timeframe, tf=tf_norm)
    existing = self._index.get(key)
    cached_mtime = self._index_mtime.get(key)
    try:
        current_mtime = os.path.getmtime(idx_path)
    except FileNotFoundError:
        current_mtime = None
    except Exception:
        current_mtime = None

    if existing is None or cached_mtime != current_mtime:
        idx = {"shards": {}, "meta": {}}
        if current_mtime is not None:
            try:
                with open(idx_path, "r", encoding="utf-8") as f:
                    idx = json.load(f)
            except FileNotFoundError:
                pass
            except Exception as e:
                self._log(
                    "warning",
                    "index_load_failed",
                    symbol=symbol,
                    timeframe=tf_norm,
                    error=str(e),
                )
        if not isinstance(idx, dict):
            idx = {"shards": {}, "meta": {}}
        idx.setdefault("shards", {})
        meta = idx.setdefault("meta", {})
        meta.setdefault("known_gaps", [])
        meta.setdefault("last_refresh_ms", 0)
        meta.setdefault("last_final_ts", 0)
        meta.setdefault("inception_ts", None)
        meta.setdefault("inception_ts_probe_ms", 0)
        meta.setdefault("inception_ts_probe_end_ts", 0)

        removed = self._prune_missing_shards_from_index(idx)
        if removed:
            self._log(
                "warning",
                "index_pruned_missing_shards",
                symbol=symbol,
                timeframe=tf_norm,
                removed=removed,
            )
        self._index[key] = idx
        self._index_mtime[key] = current_mtime
        self._log(
            "debug",
            "index_reload",
            symbol=symbol,
            timeframe=tf_norm,
            mtime=current_mtime,
            cache_hit=existing is not None,
        )
        return idx

    idx = existing
    idx.setdefault("shards", {})
    meta = idx.setdefault("meta", {})
    meta.setdefault("known_gaps", [])
    meta.setdefault("last_refresh_ms", 0)
    meta.setdefault("last_final_ts", 0)
    meta.setdefault("inception_ts", None)

    removed = self._prune_missing_shards_from_index(idx)
    if removed:
        self._log(
            "warning",
            "index_pruned_missing_shards",
            symbol=symbol,
            timeframe=tf_norm,
            removed=removed,
        )
    self._index[key] = idx
    self._index_mtime[key] = current_mtime
    if current_mtime is not None:
        self._log("debug", "index_cached", symbol=symbol, timeframe=tf_norm, mtime=current_mtime)
    return idx


def set_persist_batch_observer(self, observer) -> None:
    self._persist_batch_observer = observer


def rebuild_index_shards_for_days(
    day_ranges: dict,
    shard_paths: dict,
    shards: dict,
    *,
    range_start: int,
    range_end: int,
    load_shard_fn,
    ensure_dtype_fn,
    candle_dtype,
) -> tuple[dict, int, int, int]:
    updated = 0
    removed = 0
    scanned = 0
    for day_key, (day_start, day_end) in day_ranges.items():
        if day_end < range_start or day_start > range_end:
            continue
        path = shard_paths.get(day_key)
        if path is None or not os.path.exists(path):
            if day_key in shards:
                shards.pop(day_key, None)
                removed += 1
            continue
        try:
            arr = ensure_dtype_fn(load_shard_fn(path))
        except Exception:
            arr = np.empty((0,), dtype=candle_dtype)
        if arr.size == 0:
            if day_key in shards:
                shards.pop(day_key, None)
                removed += 1
            continue
        arr = np.sort(arr, order="ts")
        crc = int(zlib.crc32(arr.tobytes()) & 0xFFFFFFFF)
        shards[day_key] = {
            "path": path,
            "min_ts": int(arr[0]["ts"]),
            "max_ts": int(arr[-1]["ts"]),
            "count": int(arr.shape[0]),
            "crc32": crc,
        }
        updated += 1
        scanned += 1
    return shards, updated, removed, scanned


def normalize_future_refresh(meta: dict, *, now_ms: int, one_min_ms: int) -> tuple[bool, int]:
    try:
        last_refresh = int(meta.get("last_refresh_ms", 0) or 0)
    except Exception:
        last_refresh = 0
    if last_refresh > now_ms + one_min_ms:
        meta["last_refresh_ms"] = 0
        return True, last_refresh
    return False, last_refresh


def get_last_refresh_ms(idx: dict) -> int:
    try:
        return int(idx.get("meta", {}).get("last_refresh_ms", 0))
    except Exception:
        return 0


def get_last_final_ts(idx: dict) -> int:
    try:
        return int(idx.get("meta", {}).get("last_final_ts", 0))
    except Exception:
        return 0


def get_inception_ts(idx: dict) -> int | None:
    try:
        val = idx.get("meta", {}).get("inception_ts")
        return int(val) if val is not None else None
    except Exception:
        return None


def set_last_refresh_meta(idx: dict, last_refresh_ms: int, last_final_ts: int | None = None) -> dict:
    meta = idx.setdefault("meta", {})
    meta["last_refresh_ms"] = int(last_refresh_ms)
    if last_final_ts is not None:
        meta["last_final_ts"] = int(last_final_ts)
    return idx


def set_inception_ts(idx: dict, ts: int) -> tuple[dict, object, bool]:
    meta = idx.setdefault("meta", {})
    current = meta.get("inception_ts")
    changed = current is None or int(ts) < int(current)
    if changed:
        meta["inception_ts"] = int(ts)
    return idx, current, changed


def get_inception_probe_meta(idx: dict) -> tuple[int, int]:
    meta = idx.get("meta", {})
    try:
        last_probe_ms = int(meta.get("inception_ts_probe_ms", 0) or 0)
        last_probe_end_ts = int(meta.get("inception_ts_probe_end_ts", 0) or 0)
        return last_probe_ms, last_probe_end_ts
    except Exception:
        return 0, 0


def set_inception_probe_meta(idx: dict, probe_ms: int, probe_end_ts: int) -> dict:
    meta = idx.setdefault("meta", {})
    meta["inception_ts_probe_ms"] = int(probe_ms)
    meta["inception_ts_probe_end_ts"] = int(probe_end_ts)
    return idx


def get_min_shard_ts_from_index(idx: dict) -> int | None:
    try:
        shards = idx.get("shards") or {}
        if isinstance(shards, dict):
            min_ts: int | None = None
            for shard_meta in shards.values():
                if not isinstance(shard_meta, dict):
                    continue
                shard_min = shard_meta.get("min_ts")
                if shard_min is None:
                    continue
                ts = int(shard_min)
                min_ts = ts if min_ts is None else min(min_ts, ts)
            return min_ts
    except Exception:
        return None
    return None


def get_min_shard_ts_from_filenames(shard_dir: str, date_range_of_key_fn) -> int | None:
    try:
        if not os.path.isdir(shard_dir):
            return None
        day_keys = [name[:-4] for name in os.listdir(shard_dir) if name.endswith(".npy")]
        if not day_keys:
            return None
        day_keys.sort()
        start_ts, _ = date_range_of_key_fn(day_keys[0])
        return int(start_ts)
    except Exception:
        return None


def maybe_update_inception_ts(self, symbol: str, arr: np.ndarray, *, save: bool = True) -> bool:
    if arr.size == 0:
        return False
    first_ts = int(arr[0]["ts"]) if arr.ndim else int(arr["ts"])
    current = self._get_inception_ts(symbol)
    if current is None or first_ts < current:
        self._set_inception_ts(symbol, first_ts, save=save)
        return True
    return False
