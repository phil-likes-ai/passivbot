import types
from importlib import import_module
import logging


pb_runtime_ops = import_module("passivbot_runtime_ops")


def test_get_fetch_delay_seconds_uses_config_or_exchange_default():
    bot = types.SimpleNamespace(config={"live": {"warmup_fetch_delay_ms": "150"}}, exchange="bybit")
    assert pb_runtime_ops.get_fetch_delay_seconds(bot) == 0.15

    bot2 = types.SimpleNamespace(config={"live": {}}, exchange="hyperliquid")
    assert pb_runtime_ops.get_fetch_delay_seconds(bot2) == 0.2

    bot3 = types.SimpleNamespace(config={"live": {}}, exchange="binance")
    assert pb_runtime_ops.get_fetch_delay_seconds(bot3) == 0.0


def test_set_log_silence_watchdog_context_updates_selected_fields():
    bot = types.SimpleNamespace(_log_silence_watchdog_phase="old", _log_silence_watchdog_stage="old")

    pb_runtime_ops.set_log_silence_watchdog_context(bot, phase="boot")
    assert bot._log_silence_watchdog_phase == "boot"
    assert bot._log_silence_watchdog_stage == "old"

    pb_runtime_ops.set_log_silence_watchdog_context(bot, stage="warmup")
    assert bot._log_silence_watchdog_phase == "boot"
    assert bot._log_silence_watchdog_stage == "warmup"


def test_maybe_log_silence_watchdog_logs_when_threshold_exceeded(monkeypatch, caplog):
    monkeypatch.setattr(pb_runtime_ops, "utc_ms", lambda: 10_000)
    caplog.set_level(logging.INFO)
    bot = types.SimpleNamespace(
        _log_silence_watchdog_seconds=5.0,
        get_last_log_activity_monotonic=lambda: 0.0,
        _log_silence_watchdog_phase="runtime",
        _log_silence_watchdog_stage="loop",
        _health_start_ms=5_000,
        _last_loop_duration_ms=2500,
        _format_duration=lambda ms: "5s",
    )

    assert pb_runtime_ops.maybe_log_silence_watchdog(bot, now_monotonic=10.0) is True
    assert "silence watchdog: no logs for 10s" in caplog.text


def test_start_and_stop_log_silence_watchdog_manage_task(monkeypatch):
    created = []

    class FakeTask:
        def __init__(self):
            self.cancelled = False

        def done(self):
            return False

        def cancel(self):
            self.cancelled = True

        def __await__(self):
            if False:
                yield None
            return None

    monkeypatch.setattr(pb_runtime_ops.asyncio, "create_task", lambda coro: created.append(FakeTask()) or created[-1])
    bot = types.SimpleNamespace(_log_silence_watchdog_seconds=5.0, _run_log_silence_watchdog=lambda: None)

    pb_runtime_ops.start_log_silence_watchdog(bot)
    assert len(created) == 1
    assert bot._log_silence_watchdog_task is created[0]

    import asyncio as _asyncio

    async def run_stop():
        await pb_runtime_ops.stop_log_silence_watchdog(bot)

    _asyncio.run(run_stop())
    assert created[0].cancelled is True
    assert bot._log_silence_watchdog_task is None


def test_stop_data_maintainers_cancels_all_known_tasks():
    class Task:
        def __init__(self, result):
            self.result = result

        def cancel(self):
            return self.result

    bot = types.SimpleNamespace(
        maintainers={"a": Task(True), "b": Task(False)},
        WS_ohlcvs_1m_tasks={"ws": Task(True)},
    )

    result = pb_runtime_ops.stop_data_maintainers(bot, verbose=False)

    assert result == {"a": True, "b": False}


def test_maybe_log_health_summary_respects_interval(monkeypatch):
    monkeypatch.setattr(pb_runtime_ops, "utc_ms", lambda: 10_000)
    calls = []
    bot = types.SimpleNamespace(
        _health_last_summary_ms=9_500,
        _health_summary_interval_ms=1_000,
        _log_health_summary=lambda: calls.append("logged"),
    )

    pb_runtime_ops.maybe_log_health_summary(bot)
    assert calls == []

    bot._health_last_summary_ms = 8_000
    pb_runtime_ops.maybe_log_health_summary(bot)

    assert calls == ["logged"]
    assert bot._health_last_summary_ms == 10_000


def test_log_health_summary_includes_core_fields(monkeypatch, caplog):
    monkeypatch.setattr(pb_runtime_ops, "utc_ms", lambda: 10_000)
    caplog.set_level(logging.INFO)
    bot = types.SimpleNamespace(
        _health_start_ms=5_000,
        _format_duration=lambda ms: "5s",
        positions={
            "BTC/USDT:USDT": {"long": {"size": 1.0}, "short": {"size": 0.0}},
            "ETH/USDT:USDT": {"long": {"size": 0.0}, "short": {"size": 2.0}},
        },
        get_raw_balance=lambda: 100.0,
        get_hysteresis_snapped_balance=lambda: 99.0,
        quote="USDT",
        _health_fills=2,
        _health_pnl=3.5,
        _last_loop_duration_ms=1200,
        error_counts=[9_500],
        _health_orders_placed=4,
        _health_orders_cancelled=1,
        _health_ws_reconnects=2,
        _health_rate_limits=3,
    )

    pb_runtime_ops.log_health_summary(bot)

    text = caplog.text
    assert "[health] uptime=5s" in text
    assert "positions=1 long, 1 short" in text
    assert "balance=100.00 USDT (snap 99.00)" in text
    assert "orders=+4/-1" in text
    assert "fills=2 (pnl=+3.50)" in text
    assert "errors=1/10" in text


def test_log_memory_snapshot_returns_when_rss_unavailable():
    bot = types.SimpleNamespace()

    assert pb_runtime_ops.log_memory_snapshot(bot, get_process_rss_bytes=lambda: None) is None


def test_log_memory_snapshot_logs_summary_and_updates_prev(monkeypatch):
    calls = []
    monkeypatch.setattr(pb_runtime_ops.logging, "info", lambda msg: calls.append(msg))

    class FakeTask:
        def done(self):
            return False

        def get_coro(self):
            class C:
                __qualname__ = "demo_coro"

            return C()

    monkeypatch.setattr(pb_runtime_ops.asyncio, "get_running_loop", lambda: object())
    monkeypatch.setattr(pb_runtime_ops.asyncio, "all_tasks", lambda loop: {FakeTask()})

    arr = types.SimpleNamespace(nbytes=1024, shape=(10,))
    bot = types.SimpleNamespace(cm=types.SimpleNamespace(_cache={"BTC": arr}, _tf_range_cache={}), _mem_log_prev=None)

    pb_runtime_ops.log_memory_snapshot(bot, now_ms=123, get_process_rss_bytes=lambda: 1024 * 1024)

    assert calls
    assert "[memory] rss=1.00 MiB" in calls[0]
    assert "cm_cache=" in calls[0]
    assert "tasks=1 pending=1" in calls[0]
    assert bot._mem_log_prev["timestamp"] == 123


def test_get_exchange_time_returns_utc_ms(monkeypatch):
    monkeypatch.setattr(pb_runtime_ops, "utc_ms", lambda: 555)

    assert pb_runtime_ops.get_exchange_time(object()) == 555
