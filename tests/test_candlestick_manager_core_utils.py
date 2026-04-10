from importlib import import_module

import numpy as np

from candlestick_manager import CANDLE_DTYPE, ONE_MIN_MS


cm_core_utils = import_module("candlestick_manager_core_utils")


def test_floor_minute_rounds_down_to_minute_boundary():
    assert cm_core_utils.floor_minute(ONE_MIN_MS + 1234, ONE_MIN_MS) == ONE_MIN_MS


def test_ensure_dtype_casts_only_when_needed():
    arr = np.array([(1, 1, 1, 1, 1, 1)], dtype=CANDLE_DTYPE)
    same = cm_core_utils.ensure_dtype(arr, CANDLE_DTYPE)
    assert same.dtype == CANDLE_DTYPE

    raw = np.array([(1, 1, 1, 1, 1, 1)], dtype=[("ts", "int64"), ("o", "float64"), ("h", "float64"), ("l", "float64"), ("c", "float64"), ("bv", "float64")])
    casted = cm_core_utils.ensure_dtype(raw, CANDLE_DTYPE)
    assert casted.dtype == CANDLE_DTYPE


def test_ts_index_and_sanitize_symbol_behave_as_expected():
    arr = np.array([(1, 1, 1, 1, 1, 1), (2, 2, 2, 2, 2, 2)], dtype=CANDLE_DTYPE)
    assert cm_core_utils.ts_index(arr).tolist() == [1, 2]
    assert cm_core_utils.ts_index(np.empty((0,), dtype=CANDLE_DTYPE)).tolist() == []
    assert cm_core_utils.sanitize_symbol("BTC/USDT") == "BTC_USDT"
