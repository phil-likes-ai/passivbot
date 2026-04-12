from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from passivbot import Passivbot


@pytest.mark.asyncio
async def test_update_positions_and_balance_raises_with_context_when_balance_update_fails():
    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.update_balance = AsyncMock(side_effect=RuntimeError("balance boom"))
    bot._fetch_and_apply_positions = AsyncMock(return_value=(True, {"old": 1}, {"new": 1}))
    bot.log_position_changes = AsyncMock(return_value=None)
    bot.handle_balance_update = AsyncMock(return_value=None)

    with pytest.raises(RuntimeError, match="update_balance failed during update_positions_and_balance"):
        await Passivbot.update_positions_and_balance(bot)

    bot.handle_balance_update.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_positions_and_balance_raises_with_context_when_positions_fetch_fails():
    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.update_balance = AsyncMock(return_value=True)
    bot._fetch_and_apply_positions = AsyncMock(side_effect=RuntimeError("positions boom"))
    bot.log_position_changes = AsyncMock(return_value=None)
    bot.handle_balance_update = AsyncMock(return_value=None)

    with pytest.raises(
        RuntimeError, match="_fetch_and_apply_positions failed during update_positions_and_balance"
    ):
        await Passivbot.update_positions_and_balance(bot)

    bot.handle_balance_update.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_positions_and_balance_can_succeed_on_rerun_after_balance_failure():
    bot = cast(Any, Passivbot.__new__(Passivbot))
    state = {"fail_balance_once": True}

    async def update_balance():
        if state["fail_balance_once"]:
            state["fail_balance_once"] = False
            raise RuntimeError("balance boom")
        return True

    bot.update_balance = AsyncMock(side_effect=update_balance)
    bot._fetch_and_apply_positions = AsyncMock(return_value=(True, {"old": 1}, {"new": 2}))
    bot.log_position_changes = AsyncMock(return_value=None)
    bot.handle_balance_update = AsyncMock(return_value=None)

    with pytest.raises(RuntimeError, match="update_balance failed during update_positions_and_balance"):
        await Passivbot.update_positions_and_balance(bot)

    result = await Passivbot.update_positions_and_balance(bot)

    assert result == (True, True)
    bot.log_position_changes.assert_awaited_once_with({"old": 1}, {"new": 2})
    bot.handle_balance_update.assert_awaited_once_with(source="REST")
