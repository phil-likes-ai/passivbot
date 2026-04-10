from importlib import import_module
import types

import pytest


pb_position_logging_utils = import_module("passivbot_position_logging_utils")


@pytest.mark.asyncio
async def test_log_position_changes_noop_when_positions_unchanged():
    bot = types.SimpleNamespace()
    positions = [{"symbol": "BTC/USDT:USDT", "position_side": "long", "size": 1.0, "price": 100.0}]

    assert await pb_position_logging_utils.log_position_changes(bot, positions, positions) is None
