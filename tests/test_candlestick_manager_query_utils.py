from importlib import import_module
import types

import pytest


cm_query_utils = import_module("candlestick_manager_query_utils")


@pytest.mark.asyncio
async def test_get_last_prices_returns_zero_on_failures():
    async def get_current_close(symbol, max_age_ms=10_000):
        if symbol == "bad":
            raise RuntimeError("boom")
        return 123.0

    cm = types.SimpleNamespace(get_current_close=get_current_close)

    result = await cm_query_utils.get_last_prices(cm, ["ok", "bad"])

    assert result == {"ok": 123.0, "bad": 0.0}


@pytest.mark.asyncio
async def test_get_ema_bounds_many_returns_zero_pair_on_nonfinite():
    async def get_ema_bounds(symbol, s0, s1, max_age_ms=None, timeframe=None, tf=None):
        return (float("nan"), float("nan"))

    cm = types.SimpleNamespace(get_ema_bounds=get_ema_bounds)

    result = await cm_query_utils.get_ema_bounds_many(cm, [("BTC/USDT:USDT", 10.0, 20.0)])

    assert result == {"BTC/USDT:USDT": (0.0, 0.0)}


@pytest.mark.asyncio
async def test_get_latest_ema_log_range_many_returns_zero_on_exception():
    async def get_latest_ema_log_range(symbol, span, max_age_ms=None, timeframe=None, tf=None):
        raise RuntimeError("boom")

    cm = types.SimpleNamespace(get_latest_ema_log_range=get_latest_ema_log_range)

    result = await cm_query_utils.get_latest_ema_log_range_many(cm, [("BTC/USDT:USDT", 10.0)])

    assert result == {"BTC/USDT:USDT": 0.0}


def test_set_current_close_updates_cache():
    cm = types.SimpleNamespace(_current_close_cache={})

    cm_query_utils.set_current_close(cm, "BTC/USDT:USDT", 101.5, 1234)

    assert cm._current_close_cache == {"BTC/USDT:USDT": (101.5, 1234)}


def test_is_rate_limited_checks_until_timestamp(monkeypatch):
    monkeypatch.setattr(cm_query_utils.time, "time", lambda: 10.0)
    cm = types.SimpleNamespace(_rate_limit_until=11.0)
    assert cm_query_utils.is_rate_limited(cm) is True

    cm._rate_limit_until = 9.0
    assert cm_query_utils.is_rate_limited(cm) is False
