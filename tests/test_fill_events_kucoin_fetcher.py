from importlib import import_module
import types

import pytest


fill_events_manager = import_module("fill_events_manager")


@pytest.mark.asyncio
async def test_enrich_with_order_details_bulk_applies_cache_fetch_and_unknown(monkeypatch):
    fetcher = fill_events_manager.KucoinFetcher(api=types.SimpleNamespace())

    async def enrich(order_id, symbol):
        if order_id == "o2":
            return ("cid-2", "close_long")
        if order_id == "o3":
            raise RuntimeError("boom")
        return None

    fetcher._enrich_with_order_details = enrich
    monkeypatch.setattr(fill_events_manager.kucoin_utils, "ensure_order_detail_defaults", lambda events: [ev.setdefault("pb_order_type", "unknown") if not ev.get("pb_order_type") else None for ev in events])

    events = [
        {"id": "t1", "order_id": "o1", "symbol": "BTC/USDT:USDT", "client_order_id": "", "pb_order_type": ""},
        {"id": "t2", "order_id": "o1", "symbol": "BTC/USDT:USDT", "client_order_id": "", "pb_order_type": ""},
        {"id": "t3", "order_id": "o2", "symbol": "ETH/USDT:USDT", "client_order_id": "", "pb_order_type": ""},
        {"id": "t4", "order_id": "o3", "symbol": "SOL/USDT:USDT", "client_order_id": "", "pb_order_type": ""},
    ]
    detail_cache = {"t1": ("cached", "entry_long")}

    await fetcher._enrich_with_order_details_bulk(events, detail_cache)

    assert events[0]["client_order_id"] == "cached"
    assert events[1]["pb_order_type"] == "entry_long"
    assert events[2]["client_order_id"] == "cid-2"
    assert events[2]["pb_order_type"] == "close_long"
    assert events[3]["pb_order_type"] == "unknown"
    assert detail_cache["t2"] == ("cached", "entry_long")
    assert detail_cache["t3"] == ("cid-2", "close_long")
