import types
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

import passivbot as pb_mod
import passivbot_candle_refresh_utils as pb_candle_refresh_utils
from passivbot import Passivbot


class _ForagerRefreshCM:
    def __init__(self, *, default_window_candles: int, last_final_by_symbol: dict[str, int]):
        self.default_window_candles = default_window_candles
        self.last_final_by_symbol = last_final_by_symbol
        self.get_candles = AsyncMock(return_value=None)

    def get_last_final_ts(self, symbol: str) -> int:
        return self.last_final_by_symbol.get(symbol, 0)


@pytest.mark.asyncio
async def test_refresh_forager_candidate_candles_refreshes_only_stale_non_active_candidates(
    monkeypatch,
):
    now_ms = 1_700_000_120_000
    monkeypatch.setattr(pb_mod, "utc_ms", lambda: now_ms)
    monkeypatch.setattr(pb_candle_refresh_utils, "utc_ms", lambda: now_ms)

    cm = _ForagerRefreshCM(
        default_window_candles=20,
        last_final_by_symbol={
            "ACTIVE/USDT:USDT": now_ms - 300_000,
            "STALE/USDT:USDT": now_ms - 200_000,
            "FRESH/USDT:USDT": now_ms - 30_000,
        },
    )

    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.config = {
        "live": {
            "max_ohlcv_fetches_per_minute": 10,
            "warmup_ratio": 0.5,
            "max_warmup_minutes": 50,
        }
    }
    bot.cm = cm
    bot.start_time_ms = now_ms - 600_000
    bot.candle_refresh_log_boot_delay_ms = 0
    bot._forager_refresh_log_last_ms = 0
    bot.active_symbols = {"ACTIVE/USDT:USDT"}
    bot.approved_coins_minus_ignored_coins = {
        "long": {"ACTIVE/USDT:USDT", "STALE/USDT:USDT", "FRESH/USDT:USDT"},
        "short": set(),
    }
    bot.is_forager_mode = lambda pside=None: pside in (None, "long")
    bot.get_max_n_positions = lambda pside: 2
    bot.get_current_n_positions = lambda pside: 1
    bot.get_symbols_with_pos = lambda pside: set()
    bot._forager_refresh_budget = lambda max_calls: 2
    bot._forager_target_staleness_ms = lambda n_symbols, max_calls: 90_000
    bot._get_fetch_delay_seconds = lambda: 0.0

    def bp(pside, key, symbol):
        assert pside == "long"
        spans = {
            ("forager_volume_ema_span", "STALE/USDT:USDT"): 40.0,
            ("forager_volatility_ema_span", "STALE/USDT:USDT"): 10.0,
            ("forager_volume_ema_span", "FRESH/USDT:USDT"): 5.0,
            ("forager_volatility_ema_span", "FRESH/USDT:USDT"): 3.0,
        }
        return spans[(key, symbol)]

    bot.bp = bp

    await Passivbot._refresh_forager_candidate_candles(bot)

    expected_end_ts = (now_ms // 60_000) * 60_000 - 60_000
    cm.get_candles.assert_awaited_once()
    args, kwargs = cm.get_candles.await_args
    assert args == ("STALE/USDT:USDT",)
    assert kwargs == {
        "start_ts": expected_end_ts - (60_000 * 50),
        "end_ts": expected_end_ts,
        "max_age_ms": 0,
        "strict": False,
        "max_lookback_candles": 50,
    }


@pytest.mark.asyncio
async def test_update_ohlcvs_1m_for_actives_prioritizes_positions_and_breaks_on_rate_limit(
    monkeypatch,
):
    now_ms = 1_700_000_120_000
    monkeypatch.setattr(pb_mod, "utc_ms", lambda: now_ms)
    monkeypatch.setattr(pb_candle_refresh_utils, "utc_ms", lambda: now_ms)

    def reverse_shuffle(items):
        items.reverse()

    monkeypatch.setattr(pb_mod.random, "shuffle", reverse_shuffle)
    monkeypatch.setattr(pb_candle_refresh_utils.random, "shuffle", reverse_shuffle)

    state = {"rate_limited": False, "calls": []}

    async def get_candles(symbol, **kwargs):
        state["calls"].append((symbol, kwargs))
        state["rate_limited"] = True

    cm = types.SimpleNamespace(
        default_window_candles=30,
        get_candles=get_candles,
        is_rate_limited=lambda: state["rate_limited"],
    )

    refresh_forager = AsyncMock(return_value=None)
    refresh_logs = []

    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.cm = cm
    bot.active_symbols = {"ALPHA/USDT:USDT", "BETA/USDT:USDT", "POS/USDT:USDT"}
    bot.has_position = lambda symbol=None, pside=None: symbol == "POS/USDT:USDT"
    bot._maybe_log_candle_refresh = (
        lambda label, symbols, **kwargs: refresh_logs.append((label, list(symbols), kwargs))
    )
    bot._refresh_forager_candidate_candles = refresh_forager
    bot._get_fetch_delay_seconds = lambda: 0.0

    await Passivbot.update_ohlcvs_1m_for_actives(bot)

    expected_end_ts = (now_ms // 60_000) * 60_000 - 60_000
    assert [call[0] for call in state["calls"]] == ["POS/USDT:USDT"]
    assert state["calls"][0][1] == {
        "start_ts": expected_end_ts - (60_000 * 30),
        "end_ts": expected_end_ts,
        "max_age_ms": 60_000,
        "strict": False,
        "max_lookback_candles": 30,
    }
    assert refresh_logs == [
        (
            "active refresh",
            ["ALPHA/USDT:USDT", "BETA/USDT:USDT", "POS/USDT:USDT"],
            {"target_age_ms": 60_000, "refreshed": 3, "throttle_ms": 60_000},
        )
    ]
    refresh_forager.assert_awaited_once_with()
