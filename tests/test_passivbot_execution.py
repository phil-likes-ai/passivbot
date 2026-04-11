import asyncio
import sys
import types
import logging
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest


if "passivbot_rust" not in sys.modules:
    stub = types.ModuleType("passivbot_rust")
    setattr(stub, "qty_to_cost", lambda *args, **kwargs: 0.0)
    setattr(stub, "round_dynamic", lambda x, y=None: x)
    setattr(stub, "calc_order_price_diff", lambda *args, **kwargs: 0.0)
    setattr(stub, "hysteresis", lambda x, y, z: x)
    sys.modules["passivbot_rust"] = stub

import passivbot as pb_mod
from passivbot import Passivbot


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
