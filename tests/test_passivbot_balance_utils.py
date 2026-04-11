import types
from importlib import import_module

import pytest

balance_utils = import_module("passivbot_balance_utils")


def test_get_hysteresis_snapped_balance_raises_for_missing_balance():
    bot = types.SimpleNamespace()

    with pytest.raises(AttributeError, match="balance"):
        balance_utils.get_hysteresis_snapped_balance(bot)


def test_get_raw_balance_prefers_valid_balance_raw():
    bot = types.SimpleNamespace(balance=10.0, balance_raw=25.0)

    assert balance_utils.get_raw_balance(bot) == 25.0


def test_get_raw_balance_falls_back_to_snapped_when_balance_raw_missing():
    bot2 = types.SimpleNamespace(balance=11.0)
    bot2.get_hysteresis_snapped_balance = lambda: balance_utils.get_hysteresis_snapped_balance(bot2)

    assert balance_utils.get_raw_balance(bot2) == 11.0


@pytest.mark.parametrize("value", [None, "", float("nan"), float("inf"), float("-inf")])
def test_get_raw_balance_raises_for_invalid_balance_raw(value):
    bot = types.SimpleNamespace(balance=10.0, balance_raw=value)

    with pytest.raises((TypeError, ValueError), match="balance_raw"):
        balance_utils.get_raw_balance(bot)


def test_calc_effective_min_cost_at_price_uses_qty_step_when_min_qty_missing(monkeypatch):
    stub = types.SimpleNamespace(qty_to_cost=lambda qty, price, c_mult: qty * price * c_mult)
    monkeypatch.setattr(balance_utils, "_get_pbr", lambda: stub)

    bot = types.SimpleNamespace(
        qty_steps={"SOL/USDT:USDT": 1.0},
        min_qtys={"SOL/USDT:USDT": 0.0},
        min_costs={"SOL/USDT:USDT": 0.1},
        c_mults={"SOL/USDT:USDT": 1.0},
    )

    result = balance_utils.calc_effective_min_cost_at_price(bot, "SOL/USDT:USDT", 88.165)

    assert result == 88.165


@pytest.mark.asyncio
async def test_handle_balance_update_emits_event_and_sets_execution_flag(monkeypatch):
    monkeypatch.setattr(balance_utils.time, "time", lambda: 1000.0)
    events = []
    bot = types.SimpleNamespace(
        get_raw_balance=lambda: 100.0,
        get_hysteresis_snapped_balance=lambda: 99.0,
        calc_upnl_sum=lambda: _async_value(5.0),
        _monitor_record_event=lambda name, tags, payload: events.append((name, tags, payload)),
        execution_scheduled=False,
    )

    await balance_utils.handle_balance_update(bot, source="REST")

    assert bot._monitor_last_equity == 105.0
    assert bot.execution_scheduled is True
    assert bot._previous_balance_raw == 100.0
    assert bot._previous_balance_snapped == 99.0
    assert events[0][0] == "account.balance"
    assert events[0][2]["equity"] == 105.0


@pytest.mark.asyncio
async def test_handle_balance_update_updates_raw_only_log_time(monkeypatch):
    monkeypatch.setattr(balance_utils.time, "time", lambda: 1000.0)
    bot = types.SimpleNamespace(
        _previous_balance_raw=90.0,
        _previous_balance_snapped=100.0,
        _last_raw_only_log_time=0.0,
        get_raw_balance=lambda: 100.0,
        get_hysteresis_snapped_balance=lambda: 100.0,
        calc_upnl_sum=lambda: _async_value(0.0),
        _monitor_record_event=lambda *args, **kwargs: None,
        execution_scheduled=False,
    )

    await balance_utils.handle_balance_update(bot)

    assert bot._last_raw_only_log_time == 1000.0


async def _async_value(value):
    return value


@pytest.mark.asyncio
async def test_calc_upnl_sum_aggregates_positions():
    bot = types.SimpleNamespace(
        fetched_positions=[
            {"symbol": "BTC/USDT:USDT", "position_side": "long", "price": 100.0, "size": 1.0},
            {"symbol": "ETH/USDT:USDT", "position_side": "short", "price": 50.0, "size": 2.0},
        ],
        cm=types.SimpleNamespace(get_last_prices=lambda symbols, max_age_ms=60_000: _async_value({"BTC/USDT:USDT": 110.0, "ETH/USDT:USDT": 45.0})),
        calc_pnl=lambda pside, entry, close, qty, inverse, c_mult: (close - entry) * qty if pside == "long" else (entry - close) * qty,
        inverse=False,
        c_mults={"BTC/USDT:USDT": 1.0, "ETH/USDT:USDT": 1.0},
    )

    result = await balance_utils.calc_upnl_sum(bot)

    assert result == 20.0


@pytest.mark.asyncio
async def test_calc_upnl_sum_returns_zero_on_error_and_logs_exception(caplog):
    bot = types.SimpleNamespace(
        fetched_positions=[{"symbol": "BTC/USDT:USDT", "position_side": "long", "price": 100.0, "size": 1.0}],
        cm=types.SimpleNamespace(get_last_prices=lambda symbols, max_age_ms=60_000: _async_value({})),
        calc_pnl=lambda *args, **kwargs: 1.0,
        inverse=False,
        c_mults={"BTC/USDT:USDT": 1.0},
    )

    with caplog.at_level("ERROR"):
        result = await balance_utils.calc_upnl_sum(bot)

    assert result == 0.0
    assert len(caplog.records) == 1
    assert caplog.records[0].exc_info is not None
    assert "[balance] calc_upnl_sum failed symbol=BTC/USDT:USDT" in caplog.text


@pytest.mark.asyncio
async def test_update_effective_min_cost_updates_all_symbols():
    bot = types.SimpleNamespace(
        get_symbols_approved_or_has_pos=lambda: {"BTC/USDT:USDT", "ETH/USDT:USDT"},
        cm=types.SimpleNamespace(get_last_prices=lambda symbols, max_age_ms=600_000: _async_value({s: 10.0 for s in symbols})),
        _calc_effective_min_cost_at_price=lambda symbol, price: 5.0 if symbol.startswith("BTC") else 7.0,
    )

    await balance_utils.update_effective_min_cost(bot)

    assert bot.effective_min_cost == {"BTC/USDT:USDT": 5.0, "ETH/USDT:USDT": 7.0}


@pytest.mark.asyncio
async def test_update_effective_min_cost_single_symbol():
    bot = types.SimpleNamespace(
        effective_min_cost={"BTC/USDT:USDT": 1.0},
        cm=types.SimpleNamespace(get_last_prices=lambda symbols, max_age_ms=600_000: _async_value({"ETH/USDT:USDT": 20.0})),
        _calc_effective_min_cost_at_price=lambda symbol, price: 9.0,
    )

    await balance_utils.update_effective_min_cost(bot, "ETH/USDT:USDT")

    assert bot.effective_min_cost == {"BTC/USDT:USDT": 1.0, "ETH/USDT:USDT": 9.0}
