from importlib import import_module

import numpy as np
import pytest

from candlestick_manager import CANDLE_DTYPE, ONE_MIN_MS


cm_synthesis_utils = import_module("candlestick_manager_synthesis_utils")


def test_ohlcv_xm_to_1m_expands_higher_tf_candle():
    candle = np.array([(0, 10.0, 14.0, 9.0, 13.0, 5.0)], dtype=CANDLE_DTYPE)[0]

    result = cm_synthesis_utils.ohlcv_xm_to_1m(
        candle, 5, candle_dtype=CANDLE_DTYPE, one_min_ms=ONE_MIN_MS
    )

    assert result.shape[0] == 5
    assert int(result[0]["ts"]) == 0
    assert int(result[-1]["ts"]) == 4 * ONE_MIN_MS
    assert float(result[0]["o"]) == 10.0
    assert float(result[-1]["c"]) == 13.0
    assert pytest.approx(float(result[0]["bv"]) * 5, rel=1e-6) == 5.0


def test_ohlcv_xm_to_1m_rejects_non_finite_values():
    candle = np.array([(0, 10.0, np.inf, 9.0, 13.0, 5.0)], dtype=CANDLE_DTYPE)[0]

    with pytest.raises(ValueError):
        cm_synthesis_utils.ohlcv_xm_to_1m(
            candle, 5, candle_dtype=CANDLE_DTYPE, one_min_ms=ONE_MIN_MS
        )


def test_synthesize_1m_from_higher_tf_expands_and_sorts():
    candles = np.array(
        [
            (5 * ONE_MIN_MS, 10.0, 12.0, 9.0, 11.0, 5.0),
            (0, 8.0, 9.0, 7.0, 8.5, 5.0),
        ],
        dtype=CANDLE_DTYPE,
    )

    result = cm_synthesis_utils.synthesize_1m_from_higher_tf(
        candles,
        5,
        ensure_dtype_fn=lambda arr: arr,
        candle_dtype=CANDLE_DTYPE,
        one_min_ms=ONE_MIN_MS,
    )

    assert result.shape[0] == 10
    assert list(result["ts"]) == sorted(result["ts"].tolist())


def test_synthesize_1m_from_higher_tf_rejects_unsupported_tf():
    candles = np.array([(0, 1.0, 1.0, 1.0, 1.0, 1.0)], dtype=CANDLE_DTYPE)

    with pytest.raises(ValueError):
        cm_synthesis_utils.synthesize_1m_from_higher_tf(
            candles,
            3,
            ensure_dtype_fn=lambda arr: arr,
            candle_dtype=CANDLE_DTYPE,
            one_min_ms=ONE_MIN_MS,
        )
