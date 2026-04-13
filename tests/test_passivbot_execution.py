import asyncio
import logging
import types
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

import passivbot as pb_mod
from passivbot import Passivbot


class _FailingPnlsManager:
    def __init__(self, refresh_error: Exception):
        self.history_scope = "window"
        self._refresh_error = refresh_error

    def get_events(self):
        return []

    def get_history_scope(self):
        return self.history_scope

    def set_history_scope(self, scope):
        self.history_scope = scope

    async def refresh(self, **kwargs):
        del kwargs
        raise self._refresh_error

    async def refresh_latest(self, **kwargs):
        del kwargs
        raise AssertionError("refresh_latest should not be called in this scenario")


@pytest.mark.asyncio
async def test_execute_order_uses_extracted_helper_pipeline():
    class DummyCCA:
        def __init__(self):
            self.create_order = AsyncMock(return_value={"id": "abc"})

    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.cca = DummyCCA()
    bot._build_order_params = lambda order: {"reduceOnly": True}

    order = {
        "symbol": "BTC/USDT:USDT",
        "side": "buy",
        "qty": 0.5,
        "price": 50000.0,
        "type": "limit",
    }

    execute_order = getattr(Passivbot, "execute_order")
    result = await execute_order(bot, order)

    assert result == {"id": "abc"}
    cast(Any, bot.cca).create_order.assert_awaited_once_with(
        symbol="BTC/USDT:USDT",
        type="limit",
        side="buy",
        amount=0.5,
        price=50000.0,
        params={"reduceOnly": True},
    )


@pytest.mark.asyncio
async def test_execute_order_raises_on_missing_type():
    class DummyCCA:
        def __init__(self):
            self.create_order = AsyncMock(return_value={"id": "abc"})

    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.cca = DummyCCA()
    bot._build_order_params = lambda order: {"reduceOnly": True}

    order = {
        "symbol": "BTC/USDT:USDT",
        "side": "buy",
        "qty": 0.5,
        "price": 50000.0,
    }

    execute_order = getattr(Passivbot, "execute_order")

    with pytest.raises(KeyError, match="missing required order field 'type' for BTC/USDT:USDT"):
        await execute_order(bot, order)

    cast(Any, bot.cca).create_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_execute_orders_parent_returns_empty_list_when_no_orders():
    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.live_value = lambda key: 5
    bot.add_to_recent_order_executions = lambda order: None
    bot.log_order_action = lambda *args, **kwargs: None
    bot._log_order_action_summary = lambda *args, **kwargs: None

    async def execute_orders(orders):
        assert orders == []
        return []

    bot.execute_orders = execute_orders

    result = await Passivbot.execute_orders_parent(bot, [])

    assert result == []


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_result", [None, False])
async def test_execute_orders_parent_raises_on_invalid_result(invalid_result):
    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.live_value = lambda key: 5
    bot.add_to_recent_order_executions = lambda order: None
    bot.log_order_action = lambda *args, **kwargs: None
    bot._log_order_action_summary = lambda *args, **kwargs: None

    async def execute_orders(orders):
        assert len(orders) == 1
        return invalid_result

    bot.execute_orders = execute_orders
    order = {"symbol": "BTC/USDT:USDT", "side": "buy"}

    with pytest.raises(RuntimeError, match="execute_orders returned invalid result"):
        await Passivbot.execute_orders_parent(bot, [order])


@pytest.mark.asyncio
async def test_execute_orders_parent_raises_on_mismatched_execution_count():
    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.live_value = lambda key: 5
    bot.add_to_recent_order_executions = lambda order: None
    bot.log_order_action = lambda *args, **kwargs: None
    bot._log_order_action_summary = lambda *args, **kwargs: None

    async def execute_orders(_orders):
        return [{"id": "abc"}]

    bot.execute_orders = execute_orders
    orders = [
        {"symbol": "BTC/USDT:USDT", "side": "buy"},
        {"symbol": "ETH/USDT:USDT", "side": "sell"},
    ]

    with pytest.raises(RuntimeError, match="execute_orders returned 1 executions for 2 orders"):
        await Passivbot.execute_orders_parent(bot, orders)


@pytest.mark.asyncio
async def test_execute_orders_parent_raises_on_unacknowledged_execution():
    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.live_value = lambda key: 5
    bot.add_to_recent_order_executions = lambda order: None
    bot.log_order_action = lambda *args, **kwargs: None
    bot._log_order_action_summary = lambda *args, **kwargs: None

    async def execute_orders(_orders):
        return [{"status": "unknown"}]

    bot.execute_orders = execute_orders
    bot.did_create_order = lambda executed: False
    order = {"symbol": "BTC/USDT:USDT", "side": "buy"}

    with pytest.raises(
        RuntimeError, match="execute_orders returned unacknowledged result for BTC/USDT:USDT"
    ):
        await Passivbot.execute_orders_parent(bot, [order])


@pytest.mark.asyncio
async def test_execute_cancellations_parent_returns_empty_list_when_no_orders():
    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.live_value = lambda key: 5
    bot.add_to_recent_order_cancellations = lambda order: None
    bot.log_order_action = lambda *args, **kwargs: None
    bot._log_order_action_summary = lambda *args, **kwargs: None

    async def execute_cancellations(orders):
        assert orders == []
        return []

    bot.execute_cancellations = execute_cancellations

    result = await Passivbot.execute_cancellations_parent(bot, [])

    assert result == []


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_result", [None, False])
async def test_execute_cancellations_parent_raises_on_invalid_result(invalid_result):
    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.live_value = lambda key: 5
    bot.add_to_recent_order_cancellations = lambda order: None
    bot.log_order_action = lambda *args, **kwargs: None
    bot._log_order_action_summary = lambda *args, **kwargs: None

    async def execute_cancellations(orders):
        assert len(orders) == 1
        return invalid_result

    bot.execute_cancellations = execute_cancellations
    order = {"symbol": "BTC/USDT:USDT", "side": "sell", "reduce_only": True}

    with pytest.raises(RuntimeError, match="execute_cancellations returned invalid result"):
        await Passivbot.execute_cancellations_parent(bot, [order])


@pytest.mark.asyncio
async def test_execute_cancellations_parent_raises_on_mismatched_execution_count():
    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.live_value = lambda key: 5
    bot.add_to_recent_order_cancellations = lambda order: None
    bot.log_order_action = lambda *args, **kwargs: None
    bot._log_order_action_summary = lambda *args, **kwargs: None
    bot.state_change_detected_by_symbol = set()

    async def execute_cancellations(_orders):
        return [{"id": "abc"}]

    bot.execute_cancellations = execute_cancellations
    orders = [
        {"symbol": "BTC/USDT:USDT", "side": "sell", "reduce_only": True},
        {"symbol": "ETH/USDT:USDT", "side": "buy", "reduce_only": True},
    ]

    with pytest.raises(
        RuntimeError, match="execute_cancellations returned 1 executions for 2 orders"
    ):
        await Passivbot.execute_cancellations_parent(bot, orders)

    assert bot.state_change_detected_by_symbol == {"BTC/USDT:USDT", "ETH/USDT:USDT"}


@pytest.mark.asyncio
async def test_execute_cancellations_parent_raises_on_unacknowledged_execution():
    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.live_value = lambda key: 5
    bot.add_to_recent_order_cancellations = lambda order: None
    bot.log_order_action = lambda *args, **kwargs: None
    bot._log_order_action_summary = lambda *args, **kwargs: None
    bot.state_change_detected_by_symbol = set()

    async def execute_cancellations(_orders):
        return [{"status": "unknown"}]

    bot.execute_cancellations = execute_cancellations
    bot.did_cancel_order = lambda executed, order=None: False
    order = {"symbol": "BTC/USDT:USDT", "side": "sell", "reduce_only": True}

    with pytest.raises(
        RuntimeError,
        match="execute_cancellations returned unacknowledged result for BTC/USDT:USDT",
    ):
        await Passivbot.execute_cancellations_parent(bot, [order])

    assert bot.state_change_detected_by_symbol == {"BTC/USDT:USDT"}


@pytest.mark.asyncio
async def test_execute_cancellation_returns_empty_dict_for_already_gone(caplog):
    class DummyCCA:
        def __init__(self):
            self.cancel_order = AsyncMock(side_effect=Exception("order does not exist"))

    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.cca = DummyCCA()

    execute_cancellation = getattr(Passivbot, "execute_cancellation")

    with caplog.at_level(logging.INFO):
        result = await execute_cancellation(bot, {"symbol": "BTC/USDT:USDT", "id": "abc123def456"})

    assert result == {}
    assert "[order] cancel skipped: BTC/USDT:USDT abc123def456 - order likely already filled or cancelled" in caplog.text


@pytest.mark.asyncio
async def test_execute_cancellation_returns_empty_dict_and_logs_exception_info(caplog):
    class DummyCCA:
        def __init__(self):
            self.cancel_order = AsyncMock(side_effect=RuntimeError("exchange timeout"))

    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.cca = DummyCCA()

    execute_cancellation = getattr(Passivbot, "execute_cancellation")

    with caplog.at_level(logging.ERROR):
        result = await execute_cancellation(bot, {"symbol": "BTC/USDT:USDT", "id": "abc123def456"})

    assert result == {}
    assert len(caplog.records) == 1
    assert caplog.records[0].exc_info is not None
    assert "[order] cancel failed: BTC/USDT:USDT abc123def456" in caplog.text


@pytest.mark.asyncio
async def test_fetch_market_prices_raises_on_invalid_required_price():
    bot = cast(Any, Passivbot.__new__(Passivbot))

    async def bad_close(symbol, max_age_ms=None):
        assert symbol == "BTC/USDT:USDT"
        assert max_age_ms == 10_000
        return None

    bot.cm = types.SimpleNamespace(get_current_close=bad_close)

    with pytest.raises(
        RuntimeError,
        match="failed fetching market prices for order sorting: BTC/USDT:USDT: invalid market price",
    ):
        await Passivbot._fetch_market_prices(bot, {"BTC/USDT:USDT"})


@pytest.mark.asyncio
async def test_sort_orders_by_market_diff_raises_when_market_price_fetch_fails():
    bot = cast(Any, Passivbot.__new__(Passivbot))

    async def bad_close(symbol, max_age_ms=None):
        raise RuntimeError(f"cm unavailable for {symbol}")

    bot.cm = types.SimpleNamespace(get_current_close=bad_close)
    orders = [{"symbol": "BTC/USDT:USDT", "side": "buy", "price": 100.0}]

    with pytest.raises(
        RuntimeError,
        match="failed fetching market prices for order sorting: BTC/USDT:USDT: cm unavailable for BTC/USDT:USDT",
    ):
        await Passivbot._sort_orders_by_market_diff(bot, orders, "to_create")


def test_register_signal_handlers_registers_sigint(monkeypatch):
    calls = []

    def fake_signal(sig, handler):
        calls.append((sig, handler))

    monkeypatch.setattr(pb_mod.signal, "signal", fake_signal)

    pb_mod.register_signal_handlers()

    assert calls == [(pb_mod.signal.SIGINT, pb_mod.signal_handler)]


def test_signal_handler_logs_and_stops_loop_when_no_bot(monkeypatch, caplog):
    calls = []

    class DummyLoop:
        def stop(self):
            calls.append("stop")

        def call_soon_threadsafe(self, callback):
            calls.append(callback)

    monkeypatch.delitem(pb_mod.__dict__, "bot", raising=False)
    monkeypatch.setattr(pb_mod.asyncio, "get_event_loop", lambda: DummyLoop())

    with caplog.at_level(logging.INFO):
        pb_mod.signal_handler(None, None)

    assert "Received shutdown signal. Stopping bot..." in caplog.text
    assert len(calls) == 1
    calls[0]()
    assert calls == [calls[0], "stop"]


@pytest.mark.asyncio
async def test_shutdown_bot_logs_shutdown_sequence(caplog):
    bot = cast(Any, types.SimpleNamespace())
    bot.stop_data_maintainers = lambda: None
    bot.close = AsyncMock(return_value=None)

    with caplog.at_level(logging.INFO):
        await pb_mod.shutdown_bot(bot)

    assert "Shutting down bot..." in caplog.text
    bot.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_shutdown_bot_logs_timeout(monkeypatch, caplog):
    bot = cast(Any, types.SimpleNamespace())
    bot.stop_data_maintainers = lambda: None
    bot.close = AsyncMock(return_value=None)

    async def raise_timeout(awaitable, timeout):
        del timeout
        awaitable.close()
        raise asyncio.TimeoutError

    monkeypatch.setattr(pb_mod.asyncio, "wait_for", raise_timeout)

    with caplog.at_level(logging.WARNING):
        await pb_mod.shutdown_bot(bot)

    assert "Shutdown timed out after 3 seconds. Forcing exit." in caplog.text


@pytest.mark.asyncio
async def test_shutdown_bot_logs_exception_info_and_continues(caplog):
    bot = cast(Any, types.SimpleNamespace())
    bot.stop_data_maintainers = lambda: None
    bot.close = AsyncMock(side_effect=RuntimeError("close failed"))

    with caplog.at_level(logging.ERROR):
        await pb_mod.shutdown_bot(bot)

    assert len(caplog.records) == 1
    assert caplog.records[0].exc_info is not None
    assert "Error during shutdown: close failed" in caplog.text
    bot.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_update_pos_oos_pnls_ohlcvs_raises_with_context_when_open_orders_update_fails():
    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.stop_signal_received = False
    bot.update_positions_and_balance = AsyncMock(return_value=(True, True))
    bot.update_open_orders = AsyncMock(side_effect=RuntimeError("open orders boom"))
    bot.update_pnls = AsyncMock(return_value=True)
    bot.update_ohlcvs_1m_for_actives = AsyncMock(return_value=None)

    with pytest.raises(RuntimeError, match="update_open_orders failed during update_pos_oos_pnls_ohlcvs"):
        await Passivbot.update_pos_oos_pnls_ohlcvs(bot)

    bot.update_ohlcvs_1m_for_actives.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_pos_oos_pnls_ohlcvs_can_succeed_on_rerun_after_failed_open_orders_update():
    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.stop_signal_received = False
    bot.update_positions_and_balance = AsyncMock(return_value=(True, True))
    bot.update_ohlcvs_1m_for_actives = AsyncMock(return_value=None)

    state = {"fail_open_orders_once": True}

    async def update_open_orders():
        if state["fail_open_orders_once"]:
            state["fail_open_orders_once"] = False
            raise RuntimeError("open orders boom")
        return True

    bot.update_open_orders = AsyncMock(side_effect=update_open_orders)
    bot.update_pnls = AsyncMock(return_value=True)

    with pytest.raises(RuntimeError, match="update_open_orders failed during update_pos_oos_pnls_ohlcvs"):
        await Passivbot.update_pos_oos_pnls_ohlcvs(bot)

    result = await Passivbot.update_pos_oos_pnls_ohlcvs(bot)

    assert result is True
    bot.update_ohlcvs_1m_for_actives.assert_awaited_once()


@pytest.mark.asyncio
async def test_update_open_orders_raises_unexpected_fetch_error_and_preserves_existing_cache():
    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.stop_signal_received = False
    bot._health_rate_limits = 0
    bot.open_orders = {"BTC/USDT:USDT": [{"id": "old", "symbol": "BTC/USDT:USDT"}]}

    async def fetch_open_orders():
        raise RuntimeError("fetch open orders boom")

    bot.fetch_open_orders = fetch_open_orders

    with pytest.raises(RuntimeError, match="fetch open orders boom"):
        await Passivbot.update_open_orders(bot)

    assert bot.open_orders == {"BTC/USDT:USDT": [{"id": "old", "symbol": "BTC/USDT:USDT"}]}
    assert bot._health_rate_limits == 0


@pytest.mark.asyncio
async def test_update_open_orders_rate_limit_returns_false_and_increments_health_counter():
    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.stop_signal_received = False
    bot._health_rate_limits = 0
    bot.open_orders = {}

    async def fetch_open_orders():
        raise pb_mod.RateLimitExceeded("too many requests")

    bot.fetch_open_orders = fetch_open_orders

    result = await Passivbot.update_open_orders(bot)

    assert result is False
    assert bot._health_rate_limits == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_result", [None, False])
async def test_update_open_orders_raises_on_invalid_fetch_result_and_preserves_existing_cache(
    invalid_result,
):
    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.stop_signal_received = False
    bot._health_rate_limits = 0
    bot.open_orders = {"BTC/USDT:USDT": [{"id": "old", "symbol": "BTC/USDT:USDT"}]}

    async def fetch_open_orders():
        return invalid_result

    bot.fetch_open_orders = fetch_open_orders

    with pytest.raises(RuntimeError, match="fetch_open_orders returned invalid result"):
        await Passivbot.update_open_orders(bot)

    assert bot.open_orders == {"BTC/USDT:USDT": [{"id": "old", "symbol": "BTC/USDT:USDT"}]}
    assert bot._health_rate_limits == 0


@pytest.mark.asyncio
async def test_update_pnls_raises_unexpected_refresh_error_after_monitoring():
    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.stop_signal_received = False
    bot.init_pnls = AsyncMock()
    bot.live_value = lambda key: "all" if key == "pnls_max_lookback_days" else None
    bot.get_exchange_time = lambda: 1_700_000_060_000
    bot._log_new_fill_events = lambda new_events: None
    bot.logging_level = 0
    bot._health_rate_limits = 0
    monitor_errors = []
    bot._monitor_record_event = lambda *args, **kwargs: None
    bot._monitor_record_error = lambda *args, **kwargs: monitor_errors.append((args, kwargs))
    bot._pnls_manager = _FailingPnlsManager(RuntimeError("refresh boom"))

    with pytest.raises(RuntimeError, match="refresh boom"):
        await Passivbot.update_pnls(bot)

    assert len(monitor_errors) == 1
    args, kwargs = monitor_errors[0]
    assert args[0] == "error.exchange"
    assert isinstance(args[1], RuntimeError)
    assert kwargs["payload"] == {"source": "update_pnls"}


@pytest.mark.asyncio
async def test_update_pnls_rate_limit_returns_false_and_records_event():
    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.stop_signal_received = False
    bot.init_pnls = AsyncMock()
    bot.live_value = lambda key: "all" if key == "pnls_max_lookback_days" else None
    bot.get_exchange_time = lambda: 1_700_000_060_000
    bot._log_new_fill_events = lambda new_events: None
    bot.logging_level = 0
    bot._health_rate_limits = 0
    monitor_events = []
    bot._monitor_record_error = lambda *args, **kwargs: None
    bot._monitor_record_event = lambda *args, **kwargs: monitor_events.append((args, kwargs))
    bot._pnls_manager = _FailingPnlsManager(pb_mod.RateLimitExceeded("too many requests"))

    result = await Passivbot.update_pnls(bot)

    assert result is False
    assert bot._health_rate_limits == 1
    assert len(monitor_events) == 1
    args, _kwargs = monitor_events[0]
    assert args[2] == {"source": "update_pnls", "message": "rate limit exceeded"}


@pytest.mark.asyncio
async def test_update_pnls_raises_when_init_does_not_materialize_manager_and_records_monitoring():
    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.stop_signal_received = False
    bot.live_value = lambda key: "all" if key == "pnls_max_lookback_days" else None
    bot.get_exchange_time = lambda: 1_700_000_060_000
    bot._log_new_fill_events = lambda new_events: None
    bot.logging_level = 0
    bot._health_rate_limits = 0
    monitor_errors = []
    bot._monitor_record_event = lambda *args, **kwargs: None
    bot._monitor_record_error = lambda *args, **kwargs: monitor_errors.append((args, kwargs))
    bot._pnls_manager = None

    async def init_pnls():
        bot._pnls_manager = None

    bot.init_pnls = init_pnls

    with pytest.raises(RuntimeError, match="FillEventsManager unavailable after init_pnls"):
        await Passivbot.update_pnls(bot)

    assert len(monitor_errors) == 1
    args, kwargs = monitor_errors[0]
    assert args[0] == "error.exchange"
    assert isinstance(args[1], RuntimeError)
    assert kwargs["payload"] == {"source": "update_pnls"}


@pytest.mark.asyncio
async def test_update_pnls_raises_init_failure_after_monitoring():
    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.stop_signal_received = False
    bot.live_value = lambda key: "all" if key == "pnls_max_lookback_days" else None
    bot.get_exchange_time = lambda: 1_700_000_060_000
    bot._log_new_fill_events = lambda new_events: None
    bot.logging_level = 0
    bot._health_rate_limits = 0
    monitor_errors = []
    bot._pnls_manager = None
    bot._monitor_record_event = lambda *args, **kwargs: None
    bot._monitor_record_error = lambda *args, **kwargs: monitor_errors.append((args, kwargs))

    async def init_pnls():
        raise RuntimeError("init pnls boom")

    bot.init_pnls = init_pnls

    with pytest.raises(RuntimeError, match="init pnls boom"):
        await Passivbot.update_pnls(bot)

    assert len(monitor_errors) == 1
    args, kwargs = monitor_errors[0]
    assert args[0] == "error.exchange"
    assert isinstance(args[1], RuntimeError)
    assert kwargs["payload"] == {"source": "update_pnls"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failing_attr", "expected_message"),
    [
        ("get_max_n_positions", "failed to resolve max position count during warmup for long"),
        (
            "get_current_n_positions",
            "failed to resolve current position count during warmup for long",
        ),
    ],
)
async def test_warmup_candles_staggered_raises_on_slot_count_lookup_failure(
    failing_attr, expected_message
):
    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.approved_coins_minus_ignored_coins = {"long": set(), "short": set()}

    def fail(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("lookup boom")

    bot.get_max_n_positions = fail if failing_attr == "get_max_n_positions" else lambda pside: 1
    bot.get_current_n_positions = (
        fail if failing_attr == "get_current_n_positions" else lambda pside: 0
    )

    with pytest.raises(RuntimeError, match=expected_message):
        await Passivbot.warmup_candles_staggered(bot)


@pytest.mark.asyncio
async def test_warmup_candles_staggered_raises_on_per_symbol_candle_failure_after_flushing_batches(
    monkeypatch,
):
    class DummyCM:
        default_window_candles = 120

        def __init__(self):
            self.started = []
            self.flushed = []

        def start_synth_candle_batch(self):
            self.started.append("synth")

        def start_candle_replace_batch(self):
            self.started.append("replace")

        def flush_synth_candle_batch(self):
            self.flushed.append("synth")

        def flush_candle_replace_batch(self):
            self.flushed.append("replace")

        async def get_candles(self, symbol, **kwargs):
            if symbol == "BTC/USDT:USDT" and kwargs.get("timeframe", "1m") == "1m":
                raise RuntimeError("candle fetch boom")

    monkeypatch.setattr(
        pb_mod,
        "compute_live_warmup_windows",
        lambda *args, **kwargs: (
            {"BTC/USDT:USDT": 5},
            {"BTC/USDT:USDT": 0},
            {"BTC/USDT:USDT": True},
        ),
    )
    monkeypatch.setattr(pb_mod, "utc_ms", lambda: 1_700_000_060_000)
    monkeypatch.setattr(pb_mod.asyncio, "sleep", AsyncMock(return_value=None))

    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.approved_coins_minus_ignored_coins = {"long": {"BTC/USDT:USDT"}, "short": set()}
    bot.get_max_n_positions = lambda pside: 1
    bot.get_current_n_positions = lambda pside: 0
    bot.is_forager_mode = lambda pside=None: False
    bot.get_symbols_approved_or_has_pos = lambda pside: ["BTC/USDT:USDT"]
    bot.get_symbols_with_pos = lambda pside: []
    bot.config = {"live": {}}
    bot.exchange = "bybit"
    bot.cm = DummyCM()
    bot.bp = lambda pside, key, sym: 1.0
    bot.rebuild_required_candle_indices = AsyncMock(return_value=None)
    bot._get_fetch_delay_seconds = lambda: 0.0

    with pytest.raises(
        RuntimeError,
        match="warmup_candles_staggered failed for required symbol/timeframe fetches: 1m:BTC/USDT:USDT:RuntimeError:candle fetch boom",
    ):
        await Passivbot.warmup_candles_staggered(bot)

    assert bot.cm.started == ["synth", "replace"]
    assert bot.cm.flushed == ["synth", "replace"]


@pytest.mark.asyncio
async def test_warmup_candles_staggered_raises_on_required_candle_index_rebuild_failure(
    monkeypatch,
):
    monkeypatch.setattr(
        pb_mod,
        "compute_live_warmup_windows",
        lambda *args, **kwargs: (
            {"BTC/USDT:USDT": 5},
            {"BTC/USDT:USDT": 0},
            {"BTC/USDT:USDT": True},
        ),
    )
    monkeypatch.setattr(pb_mod, "utc_ms", lambda: 1_700_000_060_000)

    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.approved_coins_minus_ignored_coins = {"long": {"BTC/USDT:USDT"}, "short": set()}
    bot.get_max_n_positions = lambda pside: 1
    bot.get_current_n_positions = lambda pside: 0
    bot.is_forager_mode = lambda pside=None: False
    bot.get_symbols_approved_or_has_pos = lambda pside: ["BTC/USDT:USDT"]
    bot.get_symbols_with_pos = lambda pside: []
    bot.config = {"live": {}}
    bot.exchange = "bybit"
    bot.cm = object()
    bot.bp = lambda pside, key, sym: 1.0
    bot.rebuild_required_candle_indices = AsyncMock(side_effect=RuntimeError("index rebuild boom"))

    with pytest.raises(
        RuntimeError,
        match="failed to rebuild required candle indices during warmup",
    ):
        await Passivbot.warmup_candles_staggered(bot)


@pytest.mark.asyncio
async def test_close_bot_clients_stops_maintainers_and_closes_clients():
    class DummyClient:
        def __init__(self):
            self.close = AsyncMock()

    bot = cast(Any, types.SimpleNamespace())
    bot.cca = DummyClient()
    bot.ccp = DummyClient()
    bot.stop_data_maintainers = AsyncMock(side_effect=None)
    bot.stop_data_maintainers = lambda: None

    close_bot_clients = getattr(pb_mod, "close_bot_clients")
    await close_bot_clients(bot)

    bot.ccp.close.assert_awaited_once()
    bot.cca.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_close_bot_clients_logs_exception_info_and_returns_none(caplog):
    class DummyClient:
        def __init__(self, side_effect=None):
            self.close = AsyncMock(side_effect=side_effect)

    bot = cast(Any, types.SimpleNamespace())
    bot.ccp = DummyClient(side_effect=RuntimeError("close failed"))
    bot.cca = DummyClient()
    bot.stop_data_maintainers = lambda: None

    close_bot_clients = getattr(pb_mod, "close_bot_clients")

    with caplog.at_level(logging.ERROR):
        result = await close_bot_clients(bot)

    assert result is None
    assert len(caplog.records) == 1
    assert caplog.records[0].exc_info is not None
    assert "error while closing bot clients during restart loop" in caplog.text
    bot.ccp.close.assert_awaited_once()
    bot.cca.close.assert_not_awaited()
