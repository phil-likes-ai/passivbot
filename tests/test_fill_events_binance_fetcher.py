import asyncio
from importlib import import_module
import types


fill_events_manager = import_module("fill_events_manager")


def test_binance_fetch_enrichment_handles_individual_failures():
    fetcher = fill_events_manager.BinanceFetcher(
        api=types.SimpleNamespace(),
        symbol_resolver=lambda value: value,
        positions_provider=lambda: [],
        open_orders_provider=lambda: [],
    )

    async def fetch_income(since_ms, until_ms):
        return [
            {
                "id": "e1",
                "timestamp": 10,
                "symbol": "BTC/USDT:USDT",
                "side": "sell",
                "qty": 0.0,
                "price": 0.0,
                "pnl": 1.0,
                "fees": None,
                "pb_order_type": "",
                "position_side": "long",
                "client_order_id": "",
                "order_id": "o1",
            },
            {
                "id": "e2",
                "timestamp": 20,
                "symbol": "ETH/USDT:USDT",
                "side": "sell",
                "qty": 0.0,
                "price": 0.0,
                "pnl": 2.0,
                "fees": None,
                "pb_order_type": "",
                "position_side": "long",
                "client_order_id": "",
                "order_id": "o2",
            },
        ]

    async def get_market_symbols():
        return {"BTC/USDT:USDT", "ETH/USDT:USDT"}

    async def enrich(order_id, symbol):
        if order_id == "o1":
            return ("cid-1", "close_long")
        raise RuntimeError("boom")

    fetcher._fetch_income = fetch_income
    fetcher._collect_symbols = lambda provider: []
    fetcher._get_market_symbols = get_market_symbols
    fetcher._fetch_symbol_trades = lambda symbol, since_ms, until_ms: asyncio.sleep(0, result=[])
    fetcher._enrich_with_order_details = enrich

    result = asyncio.run(fetcher.fetch(None, None, {}, None))

    assert [event["id"] for event in result] == ["e1", "e2"]
    assert result[0]["client_order_id"] == "cid-1"
    assert result[0]["pb_order_type"] == "close_long"
    assert result[1]["client_order_id"] == ""
    assert result[1]["pb_order_type"] == ""
