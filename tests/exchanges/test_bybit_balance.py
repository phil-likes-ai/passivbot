import pytest
from unittest.mock import AsyncMock

from exchanges.bybit import BybitBot


@pytest.mark.asyncio
async def test_fetch_balance_unified_counts_only_enabled_collateral():
    bot = BybitBot.__new__(BybitBot)
    bot.quote = "USDT"
    bot.cca = AsyncMock()
    bot.cca.fetch_balance = AsyncMock(
        return_value={
            "info": {
                "result": {
                    "list": [
                        {
                            "accountType": "UNIFIED",
                            "coin": [
                                {
                                    "marginCollateral": True,
                                    "collateralSwitch": True,
                                    "usdValue": "100.5",
                                    "unrealisedPnl": "2.5",
                                },
                                {
                                    "marginCollateral": True,
                                    "collateralSwitch": False,
                                    "usdValue": "999",
                                    "unrealisedPnl": "999",
                                },
                            ],
                        }
                    ]
                }
            }
        }
    )

    balance = await bot.fetch_balance()

    assert balance == 103.0


@pytest.mark.asyncio
async def test_fetch_balance_unified_handles_string_flags_correctly():
    bot = BybitBot.__new__(BybitBot)
    bot.quote = "USDT"
    bot.cca = AsyncMock()
    bot.cca.fetch_balance = AsyncMock(
        return_value={
            "info": {
                "result": {
                    "list": [
                        {
                            "accountType": "UNIFIED",
                            "coin": [
                                {
                                    "marginCollateral": "1",
                                    "collateralSwitch": "1",
                                    "usdValue": "50",
                                    "unrealisedPnl": "5",
                                },
                                {
                                    "marginCollateral": "0",
                                    "collateralSwitch": "1",
                                    "usdValue": "500",
                                    "unrealisedPnl": "500",
                                },
                                {
                                    "marginCollateral": "true",
                                    "collateralSwitch": "false",
                                    "usdValue": "700",
                                    "unrealisedPnl": "700",
                                },
                            ],
                        }
                    ]
                }
            }
        }
    )

    balance = await bot.fetch_balance()

    assert balance == 55.0


@pytest.mark.asyncio
async def test_fetch_balance_non_unified_uses_quote_total():
    bot = BybitBot.__new__(BybitBot)
    bot.quote = "USDT"
    bot.cca = AsyncMock()
    bot.cca.fetch_balance = AsyncMock(
        return_value={
            "info": {"result": {"list": [{"accountType": "CONTRACT"}]}},
            "USDT": {"total": 321.0},
        }
    )

    balance = await bot.fetch_balance()

    assert balance == 321.0


@pytest.mark.asyncio
async def test_fetch_balance_raises_on_malformed_unified_payload():
    bot = BybitBot.__new__(BybitBot)
    bot.quote = "USDT"
    bot.cca = AsyncMock()
    bot.cca.fetch_balance = AsyncMock(
        return_value={
            "info": {"result": {"list": [{"accountType": "UNIFIED", "coin": None}]}}
        }
    )

    with pytest.raises(KeyError, match="coin list"):
        await bot.fetch_balance()
