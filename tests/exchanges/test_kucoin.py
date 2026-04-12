import pytest
from unittest.mock import AsyncMock

from exchanges.kucoin import KucoinBot


class _DummyKucoin(KucoinBot):
    pass


def test_get_balance_raises_when_margin_balance_is_non_finite():
    bot = _DummyKucoin.__new__(_DummyKucoin)
    bot.exchange = "kucoin"
    bot.quote = "USDT"

    with pytest.raises(ValueError, match="non-finite marginBalance"):
        bot._get_balance({"info": {"data": {"marginBalance": float("nan")}}})


def test_get_balance_raises_when_margin_balance_is_boolean():
    bot = _DummyKucoin.__new__(_DummyKucoin)
    bot.exchange = "kucoin"
    bot.quote = "USDT"

    with pytest.raises(TypeError, match="invalid boolean marginBalance"):
        bot._get_balance({"info": {"data": {"marginBalance": True}}})


@pytest.mark.asyncio
async def test_update_exchange_config_enables_hedge_mode():
    bot = _DummyKucoin.__new__(_DummyKucoin)
    bot.exchange = "kucoin"
    bot.cca = AsyncMock()
    bot.cca.set_position_mode = AsyncMock(return_value={"ok": True})

    await bot.update_exchange_config()

    bot.cca.set_position_mode.assert_awaited_once_with(True)


@pytest.mark.asyncio
async def test_update_exchange_config_by_symbols_runs_margin_then_leverage_passes(monkeypatch):
    bot = _DummyKucoin.__new__(_DummyKucoin)
    bot.exchange = "kucoin"
    bot.EXCHANGE_CONFIG_PACING_SECONDS = 0.25
    bot._get_margin_mode_for_symbol = lambda symbol: "isolated" if symbol == "BTC/USDT:USDT" else "cross"
    bot._calc_leverage_for_symbol = lambda symbol: 7 if symbol == "BTC/USDT:USDT" else 3

    calls = []
    sleep_calls = []

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)

    bot.cca = AsyncMock()

    async def set_margin_mode(**params):
        calls.append(("margin", params))
        return {"status": "ok"}

    async def set_leverage(**params):
        calls.append(("leverage", params))
        return {"status": "ok"}

    bot.cca.set_margin_mode = set_margin_mode
    bot.cca.set_leverage = set_leverage
    monkeypatch.setattr("exchanges.kucoin.asyncio.sleep", fake_sleep)

    await bot.update_exchange_config_by_symbols(["BTC/USDT:USDT", "ETH/USDT:USDT"])

    assert [kind for kind, _ in calls] == ["margin", "margin", "leverage", "leverage"]
    assert calls[0][1] == {"marginMode": "isolated", "symbol": "BTC/USDT:USDT"}
    assert calls[2][1] == {
        "leverage": 7,
        "symbol": "BTC/USDT:USDT",
        "params": {"marginMode": "isolated"},
    }
    assert sleep_calls == [0.25, 0.25]


@pytest.mark.asyncio
async def test_update_exchange_config_by_symbols_raises_first_failure_after_both_passes(monkeypatch):
    bot = _DummyKucoin.__new__(_DummyKucoin)
    bot.exchange = "kucoin"
    bot.EXCHANGE_CONFIG_PACING_SECONDS = 0.0
    bot._get_margin_mode_for_symbol = lambda symbol: "isolated"
    bot._calc_leverage_for_symbol = lambda symbol: 5
    bot.cca = AsyncMock()
    bot.cca.set_margin_mode = AsyncMock(side_effect=RuntimeError("margin failed"))
    bot.cca.set_leverage = AsyncMock(return_value={"status": "ok"})
    monkeypatch.setattr("exchanges.kucoin.asyncio.sleep", AsyncMock())

    with pytest.raises(RuntimeError, match="kucoin exchange config failed for BTC/USDT:USDT set_margin_mode"):
        await bot.update_exchange_config_by_symbols(["BTC/USDT:USDT"])

    bot.cca.set_margin_mode.assert_awaited_once()
    bot.cca.set_leverage.assert_awaited_once()


@pytest.mark.asyncio
async def test_update_exchange_config_by_symbols_raises_leverage_failure(monkeypatch):
    bot = _DummyKucoin.__new__(_DummyKucoin)
    bot.exchange = "kucoin"
    bot.EXCHANGE_CONFIG_PACING_SECONDS = 0.0
    bot._get_margin_mode_for_symbol = lambda symbol: "cross"
    bot._calc_leverage_for_symbol = lambda symbol: 9
    bot.cca = AsyncMock()
    bot.cca.set_margin_mode = AsyncMock(return_value={"status": "ok"})
    bot.cca.set_leverage = AsyncMock(side_effect=RuntimeError("leverage failed"))
    monkeypatch.setattr("exchanges.kucoin.asyncio.sleep", AsyncMock())

    with pytest.raises(RuntimeError, match="kucoin exchange config failed for BTC/USDT:USDT set_leverage"):
        await bot.update_exchange_config_by_symbols(["BTC/USDT:USDT"])

    bot.cca.set_margin_mode.assert_awaited_once()
    bot.cca.set_leverage.assert_awaited_once()
