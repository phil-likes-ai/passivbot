from importlib import import_module

import numpy as np


cm_coverage_utils = import_module("candlestick_manager_coverage_utils")


def test_missing_spans_and_missing_spans_step_detect_gaps():
    arr = np.array([(60_000,), (180_000,)], dtype=[("ts", np.int64)])

    assert cm_coverage_utils.missing_spans(arr, 0, 240_000, one_min_ms=60_000) == [
        (0, 0),
        (120_000, 120_000),
        (240_000, 240_000),
    ]
    assert cm_coverage_utils.missing_spans_step(arr, 0, 240_000, 120_000) == []


def test_summarize_disk_coverage_and_format_missing_span_summary():
    arr = np.array([(0,), (120_000,)], dtype=[("ts", np.int64)])
    summary = cm_coverage_utils.summarize_disk_coverage(
        arr,
        0,
        180_000,
        tf_norm="1m",
        step_ms=60_000,
        one_min_ms=60_000,
        slice_ts_range_fn=lambda data, start, end: data,
    )

    assert summary["ok"] is False
    assert summary["missing_candles"] == 2
    assert cm_coverage_utils.format_missing_span_summary(
        summary["missing_spans"], 2, lambda ts: f"T{ts}"
    ) == "T60000 to T60000, T180000 to T180000"
