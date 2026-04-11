import logging
from importlib import import_module
import types


pb_logging_utils = import_module("passivbot_logging_utils")


def test_log_ema_gating_logs_once_when_meaningfully_gated(monkeypatch, caplog):
    monkeypatch.setattr(pb_logging_utils, "utc_ms", lambda: 1_000_000)
    calls = []
    monkeypatch.setattr(pb_logging_utils.logging, "info", lambda *args: calls.append(args))
    bot = types.SimpleNamespace(
        PB_modes={"BTC/USDT:USDT": {"long": "normal"}},
        positions={"BTC/USDT:USDT": {"long": {"size": 0.0}}},
        _ema_gating_last_log_ms={},
        bp=lambda pside, key, symbol: {
            "ema_span_0": 10.0,
            "ema_span_1": 40.0,
            "entry_initial_ema_dist": 0.01,
        }[key],
    )

    pb_logging_utils.log_ema_gating(
        bot,
        ideal_orders={},
        m1_close_emas={"BTC/USDT:USDT": {10.0: 100.0, 40.0: 102.0, 20.0: 101.0}},
        last_prices={"BTC/USDT:USDT": 110.0},
        symbols=["BTC/USDT:USDT"],
    )

    assert calls
    assert "entry gated" in calls[0][0]


def test_log_ema_gating_throttles_and_logs_debug_on_calc_error(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    monkeypatch.setattr(pb_logging_utils, "utc_ms", lambda: 1_000)
    bot = types.SimpleNamespace(
        PB_modes={"BTC/USDT:USDT": {"long": "normal"}},
        positions={"BTC/USDT:USDT": {"long": {"size": 0.0}}},
        _ema_gating_last_log_ms={"BTC/USDT:USDT:long": 900},
        bp=lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("bad bp")),
    )

    pb_logging_utils.log_ema_gating(
        bot,
        ideal_orders={},
        m1_close_emas={},
        last_prices={"BTC/USDT:USDT": 110.0},
        symbols=["BTC/USDT:USDT"],
    )

    assert "failed EMA gating log" in caplog.text
    assert any(record.exc_info for record in caplog.records)


def test_maybe_log_ema_debug_is_disabled_by_default(monkeypatch):
    calls = []
    monkeypatch.setattr(pb_logging_utils.logging, "info", lambda *args: calls.append(args))
    bot = types.SimpleNamespace(bp=lambda *args, **kwargs: 1)

    pb_logging_utils.maybe_log_ema_debug(
        bot,
        ema_bounds_long={"BTC/USDT:USDT": (1.0, 2.0)},
        ema_bounds_short={},
        entry_volatility_logrange_ema_1h={},
    )

    assert calls == []


def test_maybe_log_ema_debug_logs_span_lookup_failures_with_exc_info(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    monkeypatch.setattr(pb_logging_utils, "EMA_DEBUG_LOGGING_ENABLED", True)
    monkeypatch.setattr(pb_logging_utils, "utc_ms", lambda: 30_001)
    bot = types.SimpleNamespace(
        bp=lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("bad bp")),
        _last_ema_debug_log_ms=-100_000,
    )

    pb_logging_utils.maybe_log_ema_debug(
        bot,
        ema_bounds_long={"BTC/USDT:USDT": (1.0, 2.0)},
        ema_bounds_short={},
        entry_volatility_logrange_ema_1h={},
    )

    assert any(record.exc_info for record in caplog.records)
    assert "EMA debug | long -> BTC/USDT:USDT spans=(?, ?) lower=1 upper=2" in caplog.text
