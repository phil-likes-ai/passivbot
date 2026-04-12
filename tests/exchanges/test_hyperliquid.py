import pytest
from unittest.mock import AsyncMock

from exchanges.hyperliquid import HyperliquidBot


class _DummyHyperliquid(HyperliquidBot):
    pass


def test_normalize_ccxt_position_raises_when_side_missing():
    bot = _DummyHyperliquid.__new__(_DummyHyperliquid)
    bot.exchange = "hyperliquid"

    with pytest.raises(KeyError, match="missing side"):
        bot._normalize_ccxt_position({"symbol": "BTC/USDC:USDC", "contracts": 1, "entryPrice": 100.0})


def test_normalize_ccxt_position_raises_when_contracts_is_boolean():
    bot = _DummyHyperliquid.__new__(_DummyHyperliquid)
    bot.exchange = "hyperliquid"

    with pytest.raises(TypeError, match="invalid boolean contracts"):
        bot._normalize_ccxt_position(
            {"symbol": "BTC/USDC:USDC", "side": "long", "contracts": True, "entryPrice": 100.0}
        )


def test_normalize_ccxt_position_raises_when_entry_price_is_non_positive_for_open_position():
    bot = _DummyHyperliquid.__new__(_DummyHyperliquid)
    bot.exchange = "hyperliquid"

    with pytest.raises(ValueError, match="non-positive entryPrice"):
        bot._normalize_ccxt_position(
            {"symbol": "BTC/USDC:USDC", "side": "long", "contracts": 1, "entryPrice": 0.0}
        )


def test_normalize_ccxt_position_preserves_negative_size_for_short():
    bot = _DummyHyperliquid.__new__(_DummyHyperliquid)
    bot.exchange = "hyperliquid"

    normalized = bot._normalize_ccxt_position(
        {"symbol": "BTC/USDC:USDC", "side": "short", "contracts": 2, "entryPrice": 100.0}
    )

    assert normalized["position_side"] == "short"
    assert normalized["size"] == -2.0
    assert normalized["price"] == 100.0


@pytest.mark.asyncio
async def test_update_exchange_config_is_noop():
    bot = _DummyHyperliquid.__new__(_DummyHyperliquid)
    bot.exchange = "hyperliquid"

    assert await bot.update_exchange_config() is None


@pytest.mark.asyncio
async def test_update_exchange_config_by_symbols_uses_vault_address_when_needed(monkeypatch):
    bot = _DummyHyperliquid.__new__(_DummyHyperliquid)
    bot.exchange = "hyperliquid"
    bot.user_info = {"is_vault": True, "wallet_address": "0xabc"}
    bot._calc_leverage_for_symbol = lambda symbol: 9
    bot._get_margin_mode_for_symbol = lambda symbol: "cross"
    bot.cca = AsyncMock()
    bot.cca.set_margin_mode = AsyncMock(return_value={"status": "ok"})
    monkeypatch.setattr("exchanges.hyperliquid.asyncio.sleep", AsyncMock())

    await bot.update_exchange_config_by_symbols(["BTC/USDC:USDC"])

    bot.cca.set_margin_mode.assert_awaited_once_with(
        "cross",
        symbol="BTC/USDC:USDC",
        params={"leverage": 9, "vaultAddress": "0xabc"},
    )


@pytest.mark.asyncio
async def test_update_exchange_config_by_symbols_tolerates_unchanged_margin_error(monkeypatch):
    bot = _DummyHyperliquid.__new__(_DummyHyperliquid)
    bot.exchange = "hyperliquid"
    bot.user_info = {"is_vault": False, "wallet_address": "0xabc"}
    bot._calc_leverage_for_symbol = lambda symbol: 3
    bot._get_margin_mode_for_symbol = lambda symbol: "isolated"
    bot.cca = AsyncMock()
    bot.cca.set_margin_mode = AsyncMock(side_effect=RuntimeError('{"code":"59107"} unchanged'))
    monkeypatch.setattr("exchanges.hyperliquid.asyncio.sleep", AsyncMock())

    await bot.update_exchange_config_by_symbols(["ETH/USDC:USDC"])

    bot.cca.set_margin_mode.assert_awaited_once()


@pytest.mark.asyncio
async def test_update_exchange_config_by_symbols_propagates_non_tolerated_margin_error(monkeypatch):
    bot = _DummyHyperliquid.__new__(_DummyHyperliquid)
    bot.exchange = "hyperliquid"
    bot.user_info = {"is_vault": False, "wallet_address": "0xabc"}
    bot._calc_leverage_for_symbol = lambda symbol: 3
    bot._get_margin_mode_for_symbol = lambda symbol: "isolated"
    bot.cca = AsyncMock()
    bot.cca.set_margin_mode = AsyncMock(side_effect=RuntimeError("margin failed"))
    monkeypatch.setattr("exchanges.hyperliquid.asyncio.sleep", AsyncMock())

    with pytest.raises(
        RuntimeError, match=r"hyperliquid: set_margin_mode failed for ETH/USDC:USDC \(isolated\)"
    ):
        await bot.update_exchange_config_by_symbols(["ETH/USDC:USDC"])

    bot.cca.set_margin_mode.assert_awaited_once()


@pytest.mark.asyncio
async def test_update_exchange_config_by_symbols_sleeps_between_symbols(monkeypatch):
    bot = _DummyHyperliquid.__new__(_DummyHyperliquid)
    bot.exchange = "hyperliquid"
    bot.user_info = {"is_vault": False, "wallet_address": "0xabc"}
    bot._calc_leverage_for_symbol = lambda symbol: 2
    bot._get_margin_mode_for_symbol = lambda symbol: "cross"
    bot.cca = AsyncMock()
    bot.cca.set_margin_mode = AsyncMock(return_value={"status": "ok"})
    sleep_mock = AsyncMock()
    monkeypatch.setattr("exchanges.hyperliquid.asyncio.sleep", sleep_mock)

    await bot.update_exchange_config_by_symbols(["BTC/USDC:USDC", "ETH/USDC:USDC"])

    assert bot.cca.set_margin_mode.await_count == 2
    sleep_mock.assert_awaited_once_with(0.2)
