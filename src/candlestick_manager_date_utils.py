from __future__ import annotations

import calendar
import time
from typing import Dict, Tuple


def date_range_of_key(date_key: str, one_min_ms: int) -> Tuple[int, int]:
    y, m, d = map(int, date_key.split("-"))
    tm = time.struct_time((y, m, d, 0, 0, 0, 0, 0, 0))
    start = int(calendar.timegm(tm)) * 1000
    end = start + 24 * 60 * 60 * 1000 - one_min_ms
    return start, end


def date_key(ts_ms: int) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(int(ts_ms) / 1000.0))


def date_keys_between(start_ts: int, end_ts: int, one_min_ms: int) -> Dict[str, Tuple[int, int]]:
    first_key = date_key(start_ts)
    y, m, d = map(int, first_key.split("-"))
    tm = time.struct_time((y, m, d, 0, 0, 0, 0, 0, 0))
    day_start = int(calendar.timegm(tm)) * 1000
    res: Dict[str, Tuple[int, int]] = {}
    t = day_start
    while t <= end_ts:
        key = date_key(t)
        ds, de = date_range_of_key(key, one_min_ms)
        res[key] = (ds, de)
        t = de + one_min_ms
    return res
