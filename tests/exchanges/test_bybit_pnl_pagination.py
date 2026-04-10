import pytest

from exchanges.bybit import BybitBot


@pytest.mark.asyncio
async def test_fetch_pnls_sub_spans_all_required_weeks():
    bot = BybitBot.__new__(BybitBot)
    week = 1000 * 60 * 60 * 24 * 7
    start_time = 0
    end_time = week * 60

    calls = []

    async def fetch_pnl(start_time: int | None = None, end_time: int | None = None, limit=None):
        calls.append((start_time, end_time))
        return [{"timestamp": start_time, "orderId": f"{start_time}"}]

    bot.fetch_pnl = fetch_pnl

    result = await bot.fetch_pnls_sub(start_time=start_time, end_time=end_time)

    assert len(calls) == 60
    assert calls[0] == (week * 59, week * 60)
    assert calls[-1] == (0, week)
    assert len(result) == 60


@pytest.mark.asyncio
async def test_fetch_pnls_sub_uses_exchange_time_when_end_missing():
    bot = BybitBot.__new__(BybitBot)
    week = 1000 * 60 * 60 * 24 * 7
    start_time = week * 2
    fake_now = week * 5

    calls = []

    async def fetch_pnl(start_time: int | None = None, end_time: int | None = None, limit=None):
        calls.append((start_time, end_time))
        return []

    bot.fetch_pnl = fetch_pnl
    bot.get_exchange_time = lambda: fake_now

    await bot.fetch_pnls_sub(start_time=start_time, end_time=None)

    assert calls[0][1] == fake_now + 1000 * 60 * 60 * 24
    assert calls[-1][0] == start_time
