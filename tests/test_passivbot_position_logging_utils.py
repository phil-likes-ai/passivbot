from importlib import import_module
import logging
import types

import pytest


pb_position_logging_utils = import_module("passivbot_position_logging_utils")


@pytest.mark.asyncio
async def test_log_position_changes_noop_when_positions_unchanged():
    bot = types.SimpleNamespace()
    positions = [{"symbol": "BTC/USDT:USDT", "position_side": "long", "size": 1.0, "price": 100.0}]

    assert await pb_position_logging_utils.log_position_changes(bot, positions, positions) is None


@pytest.mark.asyncio
async def test_log_position_changes_logs_debug_when_pprice_diff_and_upnl_fallback(monkeypatch, caplog):
    class FakePbr:
        @staticmethod
        def qty_to_cost(size, price, c_mult):
            return abs(size) * price * c_mult

        @staticmethod
        def calc_pprice_diff_int(pside_int, price, last_price):
            raise RuntimeError("pprice diff failed")

        @staticmethod
        def round_dynamic(value, digits):
            return round(value, digits)

    async def get_current_close(symbol, max_age_ms=60_000):
        return 110.0

    def calc_pnl_raises(*args, **kwargs):
        raise RuntimeError("upnl failed")

    bot = types.SimpleNamespace(
        c_mults={"BTC/USDT:USDT": 1.0},
        pside_int_map={"long": 1},
        inverse=False,
        cm=types.SimpleNamespace(get_current_close=get_current_close),
        get_raw_balance=lambda: 1000.0,
        bp=lambda pside, key, symbol: 1.0 if key == "wallet_exposure_limit" else 0.0,
        bot_value=lambda pside, key: 1.0,
    )
    positions_old = [{"symbol": "BTC/USDT:USDT", "position_side": "long", "size": 0.0, "price": 0.0}]
    positions_new = [{"symbol": "BTC/USDT:USDT", "position_side": "long", "size": 1.0, "price": 100.0}]

    monkeypatch.setattr(pb_position_logging_utils, "_get_pbr", lambda: FakePbr())
    monkeypatch.setattr(pb_position_logging_utils, "calc_pnl", calc_pnl_raises)
    caplog.set_level(logging.DEBUG)

    assert await pb_position_logging_utils.log_position_changes(bot, positions_old, positions_new) is None
    assert any(record.exc_info for record in caplog.records)
    assert any("failed to calculate pprice diff" in record.getMessage() for record in caplog.records)
    assert any("failed to calculate upnl" in record.getMessage() for record in caplog.records)
    assert any(
        "PA dist: 0.0" in record.getMessage() and "upnl: 0" in record.getMessage()
        for record in caplog.records
    )
