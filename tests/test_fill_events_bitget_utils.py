from importlib import import_module
import asyncio
import types

import pytest


bitget_utils = import_module("fill_events_bitget_utils")


def test_resolve_symbol_uses_resolver_or_falls_back(monkeypatch):
    fetcher = types.SimpleNamespace(_symbol_resolver=lambda value: "BTC/USDT:USDT" if value == "BTCUSDT" else None)
    assert bitget_utils.resolve_symbol(fetcher, "BTCUSDT") == "BTC/USDT:USDT"
    assert bitget_utils.resolve_symbol(fetcher, "ETHUSDT") == "ETHUSDT"

    warnings = []
    broken = types.SimpleNamespace(_symbol_resolver=lambda value: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(bitget_utils.logger, "warning", lambda *args: warnings.append(args))
    assert bitget_utils.resolve_symbol(broken, "XRPUSDT") == "XRPUSDT"
    assert warnings


def test_normalize_fill_builds_expected_payload(monkeypatch):
    monkeypatch.setattr(bitget_utils, "ts_to_date", lambda ts: f"T{ts}")
    fetcher = types.SimpleNamespace(_symbol_resolver=lambda value: "BTC/USDT:USDT")

    result = bitget_utils.normalize_fill(
        fetcher,
        {
            "tradeId": "t1",
            "orderId": "o1",
            "cTime": 1000,
            "symbol": "BTCUSDT",
            "baseVolume": 2.0,
            "price": 10.0,
            "profit": 1.5,
            "feeDetail": {"fee": 0.1},
        },
        lambda raw: ("buy", "long"),
    )

    assert result["id"] == "t1"
    assert result["datetime"] == "T1000"
    assert result["symbol"] == "BTC/USDT:USDT"
    assert result["position_side"] == "long"


def test_apply_detail_result_updates_event_and_cache():
    event = {"id": "t1", "client_order_id": "", "pb_order_type": ""}
    cache = {}

    count = bitget_utils.apply_detail_result(event, cache, ("cid", "entry_long"))

    assert count == 1
    assert event["client_order_id"] == "cid"
    assert cache["t1"] == ("cid", "entry_long")


def test_oldest_and_next_end_time_helpers_cover_empty_and_short_batches():
    fill_list = [{"cTime": 20}, {"cTime": 10}]

    assert bitget_utils.oldest_fill_timestamp(fill_list) == 10
    assert bitget_utils.next_end_time_for_empty_batch(100, None, 10) is None
    assert bitget_utils.next_end_time_for_empty_batch(100, 50, 10) == 90
    assert bitget_utils.next_end_time_for_short_batch(fill_list, 100, None, 10) is None
    assert bitget_utils.next_end_time_for_short_batch(fill_list, 55, 50, 10) is None
    assert bitget_utils.next_end_time_for_short_batch(fill_list, 100, 5, 10) == 9


def test_build_batch_events_and_process_fill_batch():
    class Fetcher:
        detail_concurrency = 2

        def _normalize_fill(self, raw):
            return {
                "id": raw["tradeId"],
                "client_order_id": raw.get("client_order_id", ""),
                "pb_order_type": raw.get("pb_order_type", ""),
            }

        async def _enrich_with_details(self, event, cache):
            if event["id"] == "t2":
                event["client_order_id"] = "cid2"
                event["pb_order_type"] = "kind2"
                cache[event["id"]] = ("cid2", "kind2")
                return 1
            return 0

        async def _flush_detail_tasks(self, tasks):
            if not tasks:
                return 0
            results = await asyncio.gather(*tasks)
            tasks.clear()
            return sum(results)

    async def run_test():
        events = {}
        cache = {"t1": ("cid1", "kind1")}
        return await bitget_utils.process_fill_batch(
            Fetcher(),
            [{"tradeId": "t1"}, {"tradeId": "t2"}],
            cache,
            events,
        ), events, cache

    (batch_ids, detail_hits, detail_fetches), events, cache = asyncio.run(run_test())

    assert batch_ids == ["t1", "t2"]
    assert detail_hits == 1
    assert detail_fetches == 1
    assert bitget_utils.build_batch_events(events, batch_ids)[0]["client_order_id"] == "cid1"
    assert cache["t2"] == ("cid2", "kind2")


def test_normalize_fill_raises_for_missing_qty():
    fetcher = types.SimpleNamespace(_symbol_resolver=lambda value: "BTC/USDT:USDT")

    with pytest.raises(ValueError, match=r"Bitget fill missing required qty source 'baseVolume'"):
        bitget_utils.normalize_fill(
            fetcher,
            {
                "tradeId": "t1",
                "orderId": "o1",
                "cTime": 1000,
                "symbol": "BTCUSDT",
                "price": 10.0,
            },
            lambda raw: ("buy", "long"),
        )


def test_normalize_fill_raises_for_missing_price():
    fetcher = types.SimpleNamespace(_symbol_resolver=lambda value: "BTC/USDT:USDT")

    with pytest.raises(ValueError, match=r"Bitget fill missing required price source 'price'"):
        bitget_utils.normalize_fill(
            fetcher,
            {
                "tradeId": "t1",
                "orderId": "o1",
                "cTime": 1000,
                "symbol": "BTCUSDT",
                "baseVolume": 2.0,
            },
            lambda raw: ("buy", "long"),
        )


def test_normalize_fill_valid_path_unchanged(monkeypatch):
    monkeypatch.setattr(bitget_utils, "ts_to_date", lambda ts: f"T{ts}")
    fetcher = types.SimpleNamespace(_symbol_resolver=lambda value: "BTC/USDT:USDT")

    result = bitget_utils.normalize_fill(
        fetcher,
        {
            "tradeId": "t1",
            "orderId": "o1",
            "cTime": 1000,
            "symbol": "BTCUSDT",
            "baseVolume": 2.0,
            "price": 10.0,
            "profit": 1.5,
            "feeDetail": {"fee": 0.1},
        },
        lambda raw: ("buy", "long"),
    )

    assert result["id"] == "t1"
    assert result["qty"] == 2.0
    assert result["price"] == 10.0
