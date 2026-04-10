import sys
import types
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


def test_register_signal_handlers_registers_sigint(monkeypatch):
    calls = []

    def fake_signal(sig, handler):
        calls.append((sig, handler))

    monkeypatch.setattr(pb_mod.signal, "signal", fake_signal)

    pb_mod.register_signal_handlers()

    assert calls == [(pb_mod.signal.SIGINT, pb_mod.signal_handler)]


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
