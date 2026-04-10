import asyncio
from importlib import import_module
import types


fill_events_manager = import_module("fill_events_manager")


def test_enrich_with_details_returns_zero_on_fetch_failure():
    class API:
        async def private_mix_get_v2_mix_order_detail(self, params):
            raise RuntimeError("boom")

    fetcher = fill_events_manager.BitgetFetcher(
        api=API(),
        symbol_resolver=lambda value: value,
    )

    event = {"id": "t1", "order_id": "o1", "symbol_external": "BTCUSDT", "datetime": "T1"}
    result = asyncio.run(fetcher._enrich_with_details(event, {}))

    assert result == 0


def test_flush_detail_tasks_sums_successful_results():
    fetcher = fill_events_manager.BitgetFetcher(
        api=types.SimpleNamespace(),
        symbol_resolver=lambda value: value,
    )

    async def ok(value):
        return value

    async def run_test():
        tasks = [asyncio.create_task(ok(1)), asyncio.create_task(ok(2))]
        total = await fetcher._flush_detail_tasks(tasks)
        return total, tasks

    total, tasks = asyncio.run(run_test())

    assert total == 3
    assert tasks == []
