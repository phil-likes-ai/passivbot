from importlib import import_module
import types
from unittest.mock import AsyncMock

import pytest


pb_market_init_utils = import_module("passivbot_market_init_utils")


@pytest.mark.asyncio
async def test_ensure_exchange_config_ready_retries_then_succeeds(monkeypatch):
    sleep_calls = []

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(pb_market_init_utils.asyncio, "sleep", fake_sleep)
    bot = types.SimpleNamespace(update_exchange_config=AsyncMock(
        side_effect=[pb_market_init_utils.NetworkError("boom"), None]
    ))

    await pb_market_init_utils.ensure_exchange_config_ready_for_market_init(bot)

    assert bot.update_exchange_config.await_count == 2
    assert sleep_calls == [5]


@pytest.mark.asyncio
async def test_ensure_exchange_config_ready_raises_after_final_attempt(monkeypatch):
    sleep_calls = []

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(pb_market_init_utils.asyncio, "sleep", fake_sleep)
    bot = types.SimpleNamespace(update_exchange_config=AsyncMock(
        side_effect=[
            pb_market_init_utils.NetworkError("boom1"),
            pb_market_init_utils.RequestTimeout("boom2"),
            pb_market_init_utils.NetworkError("boom3"),
        ]
    ))

    with pytest.raises(pb_market_init_utils.NetworkError):
        await pb_market_init_utils.ensure_exchange_config_ready_for_market_init(bot)

    assert bot.update_exchange_config.await_count == 3
    assert sleep_calls == [5, 10]


@pytest.mark.asyncio
async def test_apply_post_market_load_setup_runs_expected_steps():
    calls = []
    async def record(label):
        calls.append(label)

    bot = types.SimpleNamespace(
        init_coin_overrides=lambda: calls.append("init_coin_overrides"),
        refresh_approved_ignored_coins_lists=lambda: calls.append("refresh_approved"),
        set_wallet_exposure_limits=lambda: calls.append("set_wel"),
        update_positions_and_balance=lambda: record("update_positions_and_balance"),
        update_open_orders=lambda: record("update_open_orders"),
        _assert_supported_live_state=lambda: calls.append("assert_supported"),
        update_effective_min_cost=lambda: record("update_effective_min_cost"),
        is_forager_mode=lambda: True,
        update_first_timestamps=lambda: record("update_first_timestamps"),
    )

    await pb_market_init_utils.apply_post_market_load_setup(bot)

    assert calls == [
        "init_coin_overrides",
        "refresh_approved",
        "set_wel",
        "update_positions_and_balance",
        "update_open_orders",
        "assert_supported",
        "update_effective_min_cost",
        "update_first_timestamps",
    ]


def test_apply_loaded_markets_sets_symbols_and_padding():
    calls = []
    bot = types.SimpleNamespace(
        sym_padding=2,
        set_market_specific_settings=lambda: calls.append("set_market_specific_settings"),
    )

    pb_market_init_utils.apply_loaded_markets(
        bot,
        {"BTC/USDT:USDT": {}, "ETH/USDT:USDT": {}},
        ["BTC/USDT:USDT"],
        {"ETH/USDT:USDT": "inactive"},
    )

    assert bot.markets_dict == {"BTC/USDT:USDT": {}, "ETH/USDT:USDT": {}}
    assert bot.eligible_symbols == {"BTC/USDT:USDT"}
    assert bot.ineligible_symbols == {"ETH/USDT:USDT": "inactive"}
    assert bot.max_len_symbol == len("BTC/USDT:USDT")
    assert bot.sym_padding == len("BTC/USDT:USDT") + 1
    assert calls == ["set_market_specific_settings"]
