import pytest
from unittest.mock import AsyncMock

from exchanges.bybit import BybitBot


@pytest.mark.asyncio
async def test_fetch_open_orders_paginates_and_deduplicates():
    bot = BybitBot.__new__(BybitBot)
    bot.exchange = "bybit"
    bot.cca = AsyncMock()
    bot._record_live_margin_mode_from_payload = lambda payload, symbol=None: None

    pages = [
        [
            {
                "id": "1",
                "symbol": "BTC/USDT:USDT",
                "side": "buy",
                "amount": 0.1,
                "timestamp": 1000,
                "info": {"positionSide": "LONG", "nextPageCursor": "abc"},
            }
        ],
        [
            {
                "id": "1",
                "symbol": "BTC/USDT:USDT",
                "side": "buy",
                "amount": 0.1,
                "timestamp": 1000,
                "info": {"positionSide": "LONG"},
            },
            {
                "id": "2",
                "symbol": "ETH/USDT:USDT",
                "side": "sell",
                "amount": 0.2,
                "timestamp": 2000,
                "info": {},
            },
        ],
    ]

    async def fetch_open_orders(symbol=None, limit=None, params=None):
        if params and params.get("cursor") == "abc":
            return pages[1]
        return pages[0]

    bot.cca.fetch_open_orders = fetch_open_orders

    orders = await bot.fetch_open_orders()

    assert [o["id"] for o in orders] == ["1", "2"]
    assert orders[0]["position_side"] == "long"
    assert orders[1]["qty"] == 0.2


@pytest.mark.asyncio
async def test_fetch_positions_paginates_and_deduplicates():
    bot = BybitBot.__new__(BybitBot)
    bot.exchange = "bybit"
    bot.cca = AsyncMock()
    bot._record_live_margin_mode = lambda symbol, margin_mode: None

    pages = [
        [
            {
                "symbol": "BTC/USDT:USDT",
                "side": "long",
                "contracts": 1,
                "entryPrice": 50000,
                "info": {"nextPageCursor": "next"},
            }
        ],
        [
            {
                "symbol": "BTC/USDT:USDT",
                "side": "long",
                "contracts": 1,
                "entryPrice": 50000,
                "info": {},
            },
            {
                "symbol": "ETH/USDT:USDT",
                "side": "short",
                "contracts": 2,
                "entryPrice": 3000,
                "info": {},
            },
        ],
    ]

    async def fetch_positions(params=None):
        if params and params.get("cursor") == "next":
            return pages[1]
        return pages[0]

    bot.cca.fetch_positions = fetch_positions

    positions = await bot.fetch_positions()

    assert len(positions) == 2
    assert {p["symbol"] for p in positions} == {"BTC/USDT:USDT", "ETH/USDT:USDT"}


@pytest.mark.asyncio
async def test_fetch_positions_raises_when_side_missing():
    bot = BybitBot.__new__(BybitBot)
    bot.exchange = "bybit"
    bot.cca = AsyncMock()
    bot._record_live_margin_mode = lambda symbol, margin_mode: None

    async def fetch_positions(params=None):
        return [{"symbol": "BTC/USDT:USDT", "contracts": 1, "entryPrice": 50000, "info": {}}]

    bot.cca.fetch_positions = fetch_positions

    with pytest.raises(KeyError, match="missing side"):
        await bot.fetch_positions()


@pytest.mark.asyncio
async def test_fetch_positions_raises_when_entry_price_is_non_positive_for_open_position():
    bot = BybitBot.__new__(BybitBot)
    bot.exchange = "bybit"
    bot.cca = AsyncMock()
    bot._record_live_margin_mode = lambda symbol, margin_mode: None

    async def fetch_positions(params=None):
        return [
            {
                "symbol": "BTC/USDT:USDT",
                "side": "long",
                "contracts": 1,
                "entryPrice": 0,
                "info": {},
            }
        ]

    bot.cca.fetch_positions = fetch_positions

    with pytest.raises(ValueError, match="non-positive entryPrice"):
        await bot.fetch_positions()
