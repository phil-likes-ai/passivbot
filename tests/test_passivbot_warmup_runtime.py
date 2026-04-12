import types
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

import passivbot as pb_mod
from passivbot import Passivbot


class _DummyWarmupCM:
    default_window_candles = 120

    def __init__(self):
        self.started: list[str] = []
        self.flushed: list[str] = []

    def start_synth_candle_batch(self):
        self.started.append("synth")

    def start_candle_replace_batch(self):
        self.started.append("replace")

    def flush_synth_candle_batch(self):
        self.flushed.append("synth")

    def flush_candle_replace_batch(self):
        self.flushed.append("replace")

    async def get_candles(self, symbol, **kwargs):
        del symbol, kwargs
        return None


def _make_warmup_bot(config: dict[str, Any]) -> Any:
    bot = cast(Any, Passivbot.__new__(Passivbot))
    bot.approved_coins_minus_ignored_coins = {"long": {"BTC/USDT:USDT"}, "short": set()}
    bot.get_max_n_positions = lambda pside: 1
    bot.get_current_n_positions = lambda pside: 0
    bot.is_forager_mode = lambda pside=None: False
    bot.get_symbols_approved_or_has_pos = lambda pside: ["BTC/USDT:USDT"]
    bot.get_symbols_with_pos = lambda pside: []
    bot.config = config
    bot.exchange = "bybit"
    bot.cm = _DummyWarmupCM()
    bot.bp = lambda pside, key, sym: 1.0
    bot.rebuild_required_candle_indices = AsyncMock(return_value=None)
    bot._get_fetch_delay_seconds = lambda: 0.0
    return bot


@pytest.mark.asyncio
async def test_warmup_candles_staggered_raises_on_invalid_warmup_concurrency(monkeypatch):
    monkeypatch.setattr(
        pb_mod,
        "compute_live_warmup_windows",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not be reached")),
    )
    bot = _make_warmup_bot({"live": {"warmup_concurrency": {"bad": "value"}}})

    with pytest.raises(RuntimeError, match=r"invalid live\.warmup_concurrency during warmup"):
        await Passivbot.warmup_candles_staggered(bot)

    assert bot.cm.started == []
    assert bot.cm.flushed == []


@pytest.mark.asyncio
async def test_warmup_candles_staggered_raises_on_invalid_warmup_jitter(monkeypatch):
    monkeypatch.setattr(
        pb_mod,
        "compute_live_warmup_windows",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not be reached")),
    )
    bot = _make_warmup_bot({"live": {"warmup_jitter_seconds": "bad"}})

    with pytest.raises(RuntimeError, match=r"invalid live\.warmup_jitter_seconds during warmup"):
        await Passivbot.warmup_candles_staggered(bot)

    assert bot.cm.started == []
    assert bot.cm.flushed == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("config_key", "bad_value"),
    [
        ("warmup_ratio", "bad"),
        ("max_warmup_minutes", "bad"),
    ],
)
async def test_warmup_candles_staggered_raises_on_invalid_window_config(
    monkeypatch,
    config_key,
    bad_value,
):
    monkeypatch.setattr(pb_mod, "utc_ms", lambda: 1_700_000_060_000)
    monkeypatch.setattr(
        pb_mod,
        "compute_live_warmup_windows",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not be reached")),
    )
    bot = _make_warmup_bot({"live": {"warmup_jitter_seconds": 0, config_key: bad_value}})

    with pytest.raises(RuntimeError, match=rf"invalid live\.{config_key} during warmup"):
        await Passivbot.warmup_candles_staggered(bot)

    assert bot.cm.started == []
    assert bot.cm.flushed == []
