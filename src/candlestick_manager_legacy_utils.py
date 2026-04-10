from __future__ import annotations

import os

import numpy as np


def legacy_day_is_complete(self, symbol: str, tf: str, date_key: str) -> bool:
    """Return True if legacy has a continuous shard for this day."""
    from candlestick_manager import ONE_MIN_MS

    cache_key = (str(symbol), str(tf), str(date_key))
    cached = self._legacy_day_quality_cache.get(cache_key)
    if cached is not None:
        return bool(cached)
    ok = False
    try:
        legacy_paths = self._get_legacy_shard_paths(symbol, tf)
        legacy_path = legacy_paths.get(date_key)
        if not legacy_path or not os.path.exists(str(legacy_path)):
            ok = False
        else:
            arr = self._load_shard(str(legacy_path))
            if arr.size == 0:
                ok = False
            else:
                day_start, day_end = self._date_range_of_key(str(date_key))
                expected_len = int((day_end - day_start) // ONE_MIN_MS) + 1
                if int(arr.shape[0]) != int(expected_len):
                    ok = False
                else:
                    ts = np.sort(arr["ts"].astype(np.int64, copy=False))
                    if int(ts[0]) != int(day_start) or int(ts[-1]) != int(day_end):
                        ok = False
                    else:
                        diffs = np.diff(ts)
                        ok = bool(
                            diffs.size
                            and int(diffs.min()) == ONE_MIN_MS
                            and int(diffs.max()) == ONE_MIN_MS
                        )
    except Exception:
        ok = False
    self._legacy_day_quality_cache[cache_key] = bool(ok)
    return bool(ok)
