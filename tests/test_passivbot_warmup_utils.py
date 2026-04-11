import logging
from importlib import import_module


compute_live_warmup_windows = import_module("passivbot_warmup_utils").compute_live_warmup_windows


def test_compute_live_warmup_windows_respects_fixed_window_and_caps():
    wins, h1_hours, skip = compute_live_warmup_windows(
        {"long": {"BTC/USDT:USDT"}, "short": set()},
        lambda pside, key, sym: 0.0,
        window_candles=180,
        max_warmup_minutes=120,
    )

    assert wins == {"BTC/USDT:USDT": 120}
    assert h1_hours == {"BTC/USDT:USDT": 2}
    assert skip == {"BTC/USDT:USDT": True}


def test_compute_live_warmup_windows_uses_forager_spans_when_enabled():
    values = {
        ("long", "ema_span_0", "BTC/USDT:USDT"): 10.0,
        ("long", "ema_span_1", "BTC/USDT:USDT"): 20.0,
        ("long", "forager_volume_ema_span", "BTC/USDT:USDT"): 300.0,
        ("long", "forager_volatility_ema_span", "BTC/USDT:USDT"): 150.0,
        ("long", "entry_volatility_ema_span_hours", "BTC/USDT:USDT"): 36.0,
    }

    wins, h1_hours, skip = compute_live_warmup_windows(
        {"long": {"BTC/USDT:USDT"}, "short": set()},
        lambda pside, key, sym: values.get((pside, key, sym), 0.0),
        forager_enabled={"long": True, "short": False},
        warmup_ratio=0.5,
    )

    assert wins == {"BTC/USDT:USDT": 450}
    assert h1_hours == {"BTC/USDT:USDT": 54}
    assert skip == {"BTC/USDT:USDT": True}


def test_compute_live_warmup_windows_logs_invalid_inputs_and_preserves_fallbacks(caplog):
    def bad_lookup(pside, key, sym):
        if key == "ema_span_0":
            return "bad"
        raise RuntimeError("boom")

    with caplog.at_level(logging.DEBUG):
        wins, h1_hours, skip = compute_live_warmup_windows(
            {"long": {"BTC/USDT:USDT"}, "short": set()},
            bad_lookup,
            warmup_ratio="bad",
            max_warmup_minutes="bad",
        )

    assert wins == {"BTC/USDT:USDT": 1}
    assert h1_hours == {"BTC/USDT:USDT": 0}
    assert skip == {"BTC/USDT:USDT": True}
    assert any(record.exc_info for record in caplog.records)
    assert any("invalid warmup_ratio" in record.getMessage() for record in caplog.records)
    assert any("invalid max_warmup_minutes" in record.getMessage() for record in caplog.records)


def test_compute_live_warmup_windows_logs_bp_lookup_failures_and_preserves_fallbacks(caplog):
    def failing_lookup(pside, key, sym):
        raise RuntimeError(f"boom for {pside}/{key}/{sym}")

    with caplog.at_level(logging.DEBUG):
        wins, h1_hours, skip = compute_live_warmup_windows(
            {"long": {"BTC/USDT:USDT"}, "short": set()},
            failing_lookup,
        )

    assert wins == {"BTC/USDT:USDT": 1}
    assert h1_hours == {"BTC/USDT:USDT": 0}
    assert skip == {"BTC/USDT:USDT": True}
    assert any(record.exc_info for record in caplog.records)
    assert any("bp lookup failed" in record.getMessage() for record in caplog.records)
