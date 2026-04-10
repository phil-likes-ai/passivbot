import asyncio
from importlib import import_module
import types

fill_events_manager = import_module("fill_events_manager")


def test_fetch_from_endpoint_collects_batches_and_stops_on_short_page():
    class API:
        def __init__(self):
            self.calls = []

        async def private_get_trade_fills(self, params):
            self.calls.append(params)
            if len(self.calls) == 1:
                return {
                    "data": [
                        {"tradeId": "t1", "ts": 20, "billId": "b1", "clOrdId": "cid1"},
                        {"tradeId": "t2", "ts": 10, "billId": "b2", "clOrdId": "cid2"},
                    ]
                }
            return {"data": [{"tradeId": "t3", "ts": 5, "billId": "b3", "clOrdId": "cid3"}]}

    api = API()
    fetcher = fill_events_manager.OkxFetcher(api=api, trade_limit=2)
    fetcher._normalize_fill = lambda raw: {
        "id": raw["tradeId"],
        "timestamp": raw["ts"],
        "client_order_id": raw.get("clOrdId", ""),
        "pb_order_type": "",
    }
    batches = []

    async def run_test():
        return await fetcher._fetch_from_endpoint(
            endpoint="recent",
            since_ms=0,
            until_ms=30,
            collected={},
            max_fetches=10,
            start_fetch_count=0,
            on_batch=batches.append,
            detail_cache={},
        )

    fetch_count, collected = asyncio.run(run_test())

    assert fetch_count == 2
    assert sorted(collected) == ["t1", "t2", "t3"]
    assert [event["id"] for event in batches[0]] == ["t1", "t2"]
    assert api.calls[1]["after"] == "b2"


def test_fetch_from_endpoint_uses_history_endpoint():
    class API:
        def __init__(self):
            self.history_calls = 0

        async def private_get_trade_fills_history(self, params):
            self.history_calls += 1
            return {"data": []}

    api = API()
    fetcher = fill_events_manager.OkxFetcher(api=api, trade_limit=2)

    async def run_test():
        return await fetcher._fetch_from_endpoint(
            endpoint="history",
            since_ms=0,
            until_ms=30,
            collected={},
            max_fetches=10,
            start_fetch_count=0,
            on_batch=None,
            detail_cache={},
        )

    fetch_count, collected = asyncio.run(run_test())

    assert fetch_count == 1
    assert collected == {}
    assert api.history_calls == 1
