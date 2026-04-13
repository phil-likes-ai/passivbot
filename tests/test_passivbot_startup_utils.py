import logging
import types
from importlib import import_module

import pytest


pb_startup_utils = import_module("passivbot_startup_utils")


def test_log_startup_banner_includes_mode_positions_and_twel(caplog, monkeypatch):
    class FakeNow:
        def strftime(self, fmt):
            return "2026-01-01 00:00:00 UTC"

    class FakeDateTime:
        @staticmethod
        def now(tz):
            return FakeNow()

    monkeypatch.setattr(pb_startup_utils, "datetime", FakeDateTime)
    caplog.set_level(logging.INFO)

    values = {
        ("long", "total_wallet_exposure_limit"): 0.5,
        ("short", "total_wallet_exposure_limit"): 0.25,
        ("long", "n_positions"): 3,
        ("short", "n_positions"): 1,
    }
    bot = types.SimpleNamespace(
        user="alice",
        exchange="bybit",
        bot_value=lambda pside, key: values[(pside, key)],
    )

    pb_startup_utils.log_startup_banner(bot)

    text = caplog.text
    assert "PASSIVBOT" in text
    assert "bybit:alice" in text
    assert "Mode: LONG + SHORT" in text
    assert "Positions: 3L/1S" in text
    assert "TWEL: L:50% S:25%" in text


def test_log_startup_banner_raises_for_missing_total_wallet_exposure_limit():
    values = {
        ("long", "total_wallet_exposure_limit"): 0.5,
        ("long", "n_positions"): 3,
        ("short", "n_positions"): 1,
    }
    bot = types.SimpleNamespace(
        user="alice",
        exchange="bybit",
        bot_value=lambda pside, key: values[(pside, key)],
    )

    with pytest.raises(ValueError, match="short total_wallet_exposure_limit"):
        pb_startup_utils.log_startup_banner(bot)


def test_log_startup_banner_raises_for_invalid_total_wallet_exposure_limit():
    values = {
        ("long", "total_wallet_exposure_limit"): "invalid",
        ("short", "total_wallet_exposure_limit"): 0.25,
        ("long", "n_positions"): 3,
        ("short", "n_positions"): 1,
    }
    bot = types.SimpleNamespace(
        user="alice",
        exchange="bybit",
        bot_value=lambda pside, key: values[(pside, key)],
    )

    with pytest.raises(ValueError, match="long total_wallet_exposure_limit"):
        pb_startup_utils.log_startup_banner(bot)


@pytest.mark.asyncio
async def test_maybe_apply_boot_stagger_uses_configured_value(monkeypatch):
    sleep_calls = []
    monkeypatch.setattr(pb_startup_utils.random, "uniform", lambda a, b: 1.5)

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(pb_startup_utils.asyncio, "sleep", fake_sleep)
    bot = types.SimpleNamespace(config={"live": {"boot_stagger_seconds": 5}}, exchange="binance")

    await pb_startup_utils.maybe_apply_boot_stagger(bot)

    assert sleep_calls == [1.5]


@pytest.mark.asyncio
async def test_maybe_apply_boot_stagger_defaults_for_hyperliquid(monkeypatch):
    sleep_calls = []
    monkeypatch.setattr(pb_startup_utils.random, "uniform", lambda a, b: 2.0)

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(pb_startup_utils.asyncio, "sleep", fake_sleep)
    bot = types.SimpleNamespace(config={"live": {}}, exchange="hyperliquid")

    await pb_startup_utils.maybe_apply_boot_stagger(bot)

    assert sleep_calls == [2.0]


@pytest.mark.asyncio
async def test_maybe_apply_boot_stagger_logs_exc_info_and_skips_sleep_for_invalid_value(caplog, monkeypatch):
    sleep_calls = []

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(pb_startup_utils.asyncio, "sleep", fake_sleep)
    caplog.set_level(logging.DEBUG)
    bot = types.SimpleNamespace(config={"live": {"boot_stagger_seconds": {"bad": "value"}}}, exchange="binance")

    await pb_startup_utils.maybe_apply_boot_stagger(bot)

    assert sleep_calls == []
    matching_records = [
        record
        for record in caplog.records
        if "invalid boot_stagger_seconds config; defaulting to 0.0" in record.getMessage()
    ]
    assert matching_records
    assert matching_records[0].exc_info is not None


@pytest.mark.asyncio
async def test_finalize_startup_ready_marks_ready_and_emits_event(monkeypatch):
    monkeypatch.setattr(pb_startup_utils, "utc_ms", lambda: 12345)
    events = []
    flushes = []
    runs = []
    bot = types.SimpleNamespace(
        _bot_ready=False,
        debug_mode=False,
        _monitor_record_event=lambda name, tags, payload, ts=None: events.append((name, tags, payload, ts)),
        _monitor_flush_snapshot=lambda force=False, ts=None: _async_capture(flushes, (force, ts)),
        run_execution_loop=lambda: _async_capture(runs, "ran"),
    )

    await pb_startup_utils.finalize_startup_ready(bot)

    assert bot._bot_ready is True
    assert events == [("bot.ready", ("bot", "lifecycle", "ready"), {"debug_mode": False}, 12345)]
    assert flushes == [(True, 12345)]
    assert runs == ["ran"]


async def _async_capture(lst, value):
    lst.append(value)


@pytest.mark.asyncio
async def test_handle_startup_error_records_and_emits_stop(monkeypatch):
    monkeypatch.setattr(pb_startup_utils, "utc_ms", lambda: 54321)
    errors = []
    flushes = []
    stops = []
    bot = types.SimpleNamespace(
        _monitor_record_error=lambda name, exc, tags=(), payload=None, ts=None: errors.append((name, type(exc).__name__, tags, payload, ts)),
        _monitor_flush_snapshot=lambda force=False, ts=None: _async_capture(flushes, (force, ts)),
        _monitor_emit_stop=lambda reason, ts=None, payload=None: stops.append((reason, ts, payload)),
    )

    await pb_startup_utils.handle_startup_error(bot, RuntimeError("boom"), "warmup")

    assert errors == [(
        "error.bot",
        "RuntimeError",
        ("error", "bot", "startup"),
        {"source": "start_bot", "stage": "warmup"},
        54321,
    )]
    assert flushes == [(True, 54321)]
    assert stops == [(
        "startup_error",
        54321,
        {"stage": "warmup", "error_type": "RuntimeError"},
    )]


@pytest.mark.asyncio
async def test_run_startup_preloop_runs_stages_and_returns_true(monkeypatch):
    monkeypatch.setattr(pb_startup_utils.asyncio, "sleep", lambda seconds: _async_capture([], seconds))
    monkeypatch.setattr(pb_startup_utils, "format_approved_ignored_coins", lambda config, exchange, quote=None: _async_capture([], (exchange, quote)))
    monkeypatch.setattr(pb_startup_utils, "utc_ms", lambda: 111)
    stages = []
    flushes = []
    bot = types.SimpleNamespace(
        config={},
        user_info={"exchange": "bybit"},
        quote="USDT",
        init_markets=lambda: _async_capture([], "init"),
        _monitor_flush_snapshot=lambda force=False, ts=None: _async_capture(flushes, (force, ts)),
        warmup_candles_staggered=lambda: _async_capture([], "warmup"),
        _equity_hard_stop_enabled=lambda: False,
        _log_memory_snapshot=lambda: None,
        start_data_maintainers=lambda: _async_capture([], "maintainers"),
    )

    result = await pb_startup_utils.run_startup_preloop(bot, stages.append)

    assert result is True
    assert stages == [
        "format_approved_ignored_coins",
        "init_markets",
        "warmup_candles_staggered",
        "post_init_sleep",
        "start_data_maintainers",
    ]
    assert flushes == [(True, 111)]


@pytest.mark.asyncio
async def test_run_startup_preloop_raises_on_warmup_failure_and_stops_before_later_stages(monkeypatch):
    monkeypatch.setattr(pb_startup_utils.asyncio, "sleep", lambda seconds: _async_capture([], seconds))
    monkeypatch.setattr(pb_startup_utils, "format_approved_ignored_coins", lambda config, exchange, quote=None: _async_capture([], (exchange, quote)))
    monkeypatch.setattr(pb_startup_utils, "utc_ms", lambda: 333)
    stages = []
    flushes = []
    later_stage_calls = []

    async def fail_warmup():
        raise RuntimeError("warmup boom")

    bot = types.SimpleNamespace(
        config={},
        user_info={"exchange": "bybit"},
        quote="USDT",
        init_markets=lambda: _async_capture([], "init"),
        _monitor_flush_snapshot=lambda force=False, ts=None: _async_capture(flushes, (force, ts)),
        warmup_candles_staggered=fail_warmup,
        _equity_hard_stop_enabled=lambda: False,
        _log_memory_snapshot=lambda: later_stage_calls.append("memory"),
        start_data_maintainers=lambda: _async_capture(later_stage_calls, "maintainers"),
    )

    with pytest.raises(RuntimeError, match="warmup boom"):
        await pb_startup_utils.run_startup_preloop(bot, stages.append)

    assert stages == [
        "format_approved_ignored_coins",
        "init_markets",
        "warmup_candles_staggered",
    ]
    assert flushes == [(True, 333)]
    assert later_stage_calls == []


@pytest.mark.asyncio
async def test_run_startup_preloop_can_abort_after_equity_hard_stop(monkeypatch):
    monkeypatch.setattr(pb_startup_utils, "format_approved_ignored_coins", lambda config, exchange, quote=None: _async_capture([], None))
    monkeypatch.setattr(pb_startup_utils, "utc_ms", lambda: 222)
    stops = []
    stages = []
    bot = types.SimpleNamespace(
        config={},
        user_info={"exchange": "bybit"},
        quote="USDT",
        init_markets=lambda: _async_capture([], None),
        _monitor_flush_snapshot=lambda force=False, ts=None: _async_capture([], (force, ts)),
        warmup_candles_staggered=lambda: _async_capture([], None),
        _equity_hard_stop_enabled=lambda: True,
        _equity_hard_stop_initialize_from_history=lambda: _async_capture([], None),
        stop_signal_received=True,
        _monitor_emit_stop=lambda reason, ts=None, payload=None: stops.append((reason, ts, payload)),
    )

    result = await pb_startup_utils.run_startup_preloop(bot, stages.append)

    assert result is False
    assert "equity_hard_stop_initialize_from_history" in stages
    assert stops == [(
        "startup_aborted",
        222,
        {"stage": "equity_hard_stop_initialize_from_history", "stop_signal_received": True},
    )]
