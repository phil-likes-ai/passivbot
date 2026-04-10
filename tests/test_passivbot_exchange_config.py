import types
from importlib import import_module
from unittest.mock import AsyncMock

import pytest
from ccxt.base.errors import RateLimitExceeded


pb_exchange_config = import_module("passivbot_exchange_config")


def test_is_rate_limit_like_exception_detects_known_patterns():
    bot = types.SimpleNamespace()

    assert pb_exchange_config.is_rate_limit_like_exception(bot, RateLimitExceeded("nope"))
    assert pb_exchange_config.is_rate_limit_like_exception(bot, RuntimeError("too many requests"))
    assert not pb_exchange_config.is_rate_limit_like_exception(bot, RuntimeError("boom"))


def test_exchange_config_backoff_and_pause_depend_on_exchange(monkeypatch):
    monkeypatch.setattr(pb_exchange_config.random, "uniform", lambda a, b: 0.0)

    assert pb_exchange_config.exchange_config_backoff_seconds(
        types.SimpleNamespace(exchange="bybit"), 2
    ) == 10.0
    assert pb_exchange_config.exchange_config_backoff_seconds(
        types.SimpleNamespace(exchange="binance"), 2
    ) == 4.0
    assert pb_exchange_config.exchange_config_success_pause_seconds(
        types.SimpleNamespace(exchange="kucoin")
    ) == 0.2
    assert pb_exchange_config.exchange_config_success_pause_seconds(
        types.SimpleNamespace(exchange="binance")
    ) == 0.05


@pytest.mark.asyncio
async def test_update_exchange_configs_records_retry_state_on_failure(monkeypatch):
    monkeypatch.setattr(pb_exchange_config, "utc_ms", lambda: 1_000)
    monkeypatch.setattr(pb_exchange_config.random, "uniform", lambda a, b: 0.0)

    bot = types.SimpleNamespace(
        active_symbols=["BTC/USDT:USDT"],
        exchange="binance",
        _health_rate_limits=0,
        update_exchange_config_by_symbols=AsyncMock(side_effect=RuntimeError("boom")),
    )
    bot._exchange_config_backoff_seconds = lambda attempt: pb_exchange_config.exchange_config_backoff_seconds(
        bot, attempt
    )
    bot._exchange_config_success_pause_seconds = lambda: pb_exchange_config.exchange_config_success_pause_seconds(
        bot
    )
    bot._is_rate_limit_like_exception = lambda exc: pb_exchange_config.is_rate_limit_like_exception(bot, exc)

    await pb_exchange_config.update_exchange_configs(bot)

    assert bot._exchange_config_retry_attempts["BTC/USDT:USDT"] == 1
    assert bot._exchange_config_retry_after_ms["BTC/USDT:USDT"] == 3_000


@pytest.mark.asyncio
async def test_update_exchange_configs_marks_symbol_done_and_applies_pause(monkeypatch):
    monkeypatch.setattr(pb_exchange_config, "utc_ms", lambda: 1_000)
    sleep_calls = []

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(pb_exchange_config.asyncio, "sleep", fake_sleep)

    bot = types.SimpleNamespace(
        active_symbols=["BTC/USDT:USDT"],
        exchange="kucoin",
        _health_rate_limits=0,
        update_exchange_config_by_symbols=AsyncMock(return_value=None),
    )
    bot._exchange_config_backoff_seconds = lambda attempt: pb_exchange_config.exchange_config_backoff_seconds(
        bot, attempt
    )
    bot._exchange_config_success_pause_seconds = lambda: pb_exchange_config.exchange_config_success_pause_seconds(
        bot
    )
    bot._is_rate_limit_like_exception = lambda exc: pb_exchange_config.is_rate_limit_like_exception(bot, exc)

    await pb_exchange_config.update_exchange_configs(bot)

    assert "BTC/USDT:USDT" in bot.already_updated_exchange_config_symbols
    assert sleep_calls == [0.2]
