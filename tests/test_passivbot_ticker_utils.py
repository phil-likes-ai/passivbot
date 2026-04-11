import types
from importlib import import_module
import logging
from unittest.mock import AsyncMock

import pytest


pb_ticker_utils = import_module("passivbot_ticker_utils")


def test_normalize_tickers_fills_last_from_bid_ask_midpoint():
    tickers = {
        "BTC/USDT:USDT": {"bid": 100.0, "ask": 102.0, "last": None},
    }

    result = pb_ticker_utils.normalize_tickers(tickers)

    assert result["BTC/USDT:USDT"]["last"] == 101.0


def test_normalize_tickers_fills_missing_bid_ask_from_last():
    tickers = {
        "BTC/USDT:USDT": {"bid": None, "ask": None, "last": 100.0},
    }

    result = pb_ticker_utils.normalize_tickers(tickers)

    assert result["BTC/USDT:USDT"]["bid"] == 100.0
    assert result["BTC/USDT:USDT"]["ask"] == 100.0


@pytest.mark.asyncio
async def test_update_tickers_fetches_and_normalizes():
    bot = types.SimpleNamespace(
        cca=types.SimpleNamespace(
            fetch_tickers=AsyncMock(
                return_value={"BTC/USDT:USDT": {"bid": 100.0, "ask": 102.0, "last": None}}
            )
        )
    )

    await pb_ticker_utils.update_tickers(bot)

    assert bot.tickers["BTC/USDT:USDT"]["last"] == 101.0


@pytest.mark.asyncio
async def test_update_tickers_logs_exception_and_preserves_existing_state_on_fetch_failure(caplog):
    existing_tickers = {"BTC/USDT:USDT": {"bid": 99.0, "ask": 101.0, "last": 100.0}}
    bot = types.SimpleNamespace(
        tickers=existing_tickers,
        cca=types.SimpleNamespace(fetch_tickers=AsyncMock(side_effect=RuntimeError("boom"))),
    )

    with caplog.at_level(logging.ERROR):
        await pb_ticker_utils.update_tickers(bot)

    assert bot.tickers is existing_tickers
    assert any(record.exc_info for record in caplog.records)
