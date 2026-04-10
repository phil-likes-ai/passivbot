from importlib import import_module
import types

import numpy as np
import pytest


cm_ema_utils = import_module("candlestick_manager_ema_utils")


def test_ema_series_handles_nonfinite_values():
    arr = np.asarray([1.0, float("nan"), 3.0], dtype=np.float64)

    out = cm_ema_utils.ema_series(object(), arr, 3.0)

    assert out.shape == (3,)
    assert out[1] == out[0]


@pytest.mark.asyncio
async def test_latest_finalized_range_aligns_to_previous_bucket(monkeypatch):
    monkeypatch.setattr(cm_ema_utils, "_cm_helpers", lambda: (60_000, lambda: 180_000, lambda tf: 60_000))

    start_ts, end_ts = await cm_ema_utils.latest_finalized_range(object(), 3.0, period_ms=60_000)

    assert (start_ts, end_ts) == (0, 120_000)


@pytest.mark.asyncio
async def test_get_latest_ema_generic_uses_cache_when_fresh(monkeypatch):
    monkeypatch.setattr(cm_ema_utils, "_cm_helpers", lambda: (60_000, lambda: 10_000, lambda tf: 60_000))

    async def latest_finalized_range(span, period_ms=60_000):
        return (0, 9_000)

    cm = types.SimpleNamespace(
        _latest_finalized_range=latest_finalized_range,
        _ema_cache={"BTC/USDT:USDT": {("volume", 10.0, "60000"): (7.5, 9000, 9500)}},
    )

    result = await cm_ema_utils.get_latest_ema_generic(
        cm,
        "BTC/USDT:USDT",
        10.0,
        1000,
        None,
        tf="1m",
        metric_key="volume",
        series_fn=lambda arr: np.asarray([1.0]),
    )

    assert result == 7.5

