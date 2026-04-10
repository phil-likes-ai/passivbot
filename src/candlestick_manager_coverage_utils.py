from __future__ import annotations

from typing import Any

import numpy as np


def missing_spans(arr: np.ndarray, start_ts: int, end_ts: int, *, one_min_ms: int) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    if start_ts > end_ts:
        return spans
    if arr.size == 0:
        return [(start_ts, end_ts)]
    ts = np.asarray(arr["ts"], dtype=np.int64)
    ts = ts[(ts >= start_ts) & (ts <= end_ts)]
    if ts.size == 0:
        return [(start_ts, end_ts)]
    if ts[0] > start_ts:
        spans.append((start_ts, int(ts[0] - one_min_ms)))
    for i in range(len(ts) - 1):
        if ts[i + 1] - ts[i] > one_min_ms:
            spans.append((int(ts[i] + one_min_ms), int(ts[i + 1] - one_min_ms)))
    if ts[-1] < end_ts:
        spans.append((int(ts[-1] + one_min_ms), end_ts))
    return spans


def missing_spans_step(
    arr: np.ndarray, start_ts: int, end_ts: int, step_ms: int
) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    if start_ts > end_ts or step_ms <= 0:
        return spans
    if arr.size == 0:
        return [(start_ts, end_ts)]
    ts = np.asarray(arr["ts"], dtype=np.int64)
    ts = ts[(ts >= start_ts) & (ts <= end_ts)]
    if ts.size == 0:
        return [(start_ts, end_ts)]
    ts = np.sort(ts)
    head_end = int(ts[0] - step_ms)
    if head_end >= start_ts:
        spans.append((int(start_ts), head_end))
    for i in range(len(ts) - 1):
        gap_start = int(ts[i] + step_ms)
        gap_end = int(ts[i + 1] - step_ms)
        if gap_end >= gap_start:
            spans.append((gap_start, gap_end))
    tail_start = int(ts[-1] + step_ms)
    if tail_start <= end_ts:
        spans.append((tail_start, int(end_ts)))
    return spans


def summarize_disk_coverage(
    arr: np.ndarray | None,
    s_ts: int,
    e_ts: int,
    *,
    tf_norm: str,
    step_ms: int,
    one_min_ms: int,
    slice_ts_range_fn,
) -> dict[str, Any]:
    if arr is None or arr.size == 0:
        missing = [(s_ts, e_ts)]
        loaded_rows = 0
    else:
        sub = slice_ts_range_fn(arr, s_ts, e_ts)
        missing = (
            missing_spans(sub, s_ts, e_ts, one_min_ms=one_min_ms)
            if step_ms == one_min_ms
            else missing_spans_step(sub, s_ts, e_ts, step_ms)
        )
        loaded_rows = int(sub.shape[0]) if sub is not None else 0

    missing_candles = int(sum((end - start) // step_ms + 1 for start, end in missing)) if missing else 0
    return {
        "ok": len(missing) == 0,
        "missing_spans": missing,
        "missing_candles": missing_candles,
        "loaded_rows": loaded_rows,
        "timeframe": tf_norm,
    }


def format_missing_span_summary(missing: list[tuple[int, int]], max_span_log: int, fmt_ts_fn) -> str:
    top_parts = [f"{fmt_ts_fn(int(start))} to {fmt_ts_fn(int(end))}" for start, end in missing[: max(1, int(max_span_log))]]
    top_str = ", ".join(top_parts)
    if len(missing) > max_span_log:
        top_str = f"{top_str} (+{len(missing) - max_span_log} more)"
    return top_str
