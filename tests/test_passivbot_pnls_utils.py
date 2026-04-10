import types
from importlib import import_module

import pytest


pb_pnls_utils = import_module("passivbot_pnls_utils")


@pytest.mark.asyncio
async def test_init_pnls_is_noop_when_already_initialized():
    bot = types.SimpleNamespace(_pnls_initialized=True)

    assert await pb_pnls_utils.init_pnls(bot) is None


@pytest.mark.asyncio
async def test_init_pnls_runs_bybit_doctor_by_default(monkeypatch, tmp_path):
    reports = []

    class FakeManager:
        def __init__(self, **kwargs):
            self._events = [1, 2]

        async def ensure_loaded(self):
            return None

        async def run_doctor(self, auto_repair):
            reports.append(auto_repair)
            return {"anomaly_events": 3, "repaired": True}

    monkeypatch.setattr(pb_pnls_utils, "FillEventsManager", FakeManager)
    monkeypatch.setattr(pb_pnls_utils, "_extract_symbol_pool", lambda config, runtime: {"BTC/USDT:USDT"})
    monkeypatch.setattr(pb_pnls_utils, "_build_fetcher_for_bot", lambda bot, pool: object())
    monkeypatch.setattr(pb_pnls_utils.os, "getenv", lambda key, default="": "")

    bot = types.SimpleNamespace(
        _pnls_initialized=False,
        config={},
        exchange="bybit",
        user="alice",
    )

    await pb_pnls_utils.init_pnls(bot)

    assert isinstance(bot._pnls_manager, FakeManager)
    assert bot._pnls_initialized is True
    assert reports == [True]


@pytest.mark.asyncio
async def test_init_pnls_respects_non_bybit_doctor_mode(monkeypatch):
    reports = []

    class FakeManager:
        def __init__(self, **kwargs):
            self._events = []

        async def ensure_loaded(self):
            return None

        async def run_doctor(self, auto_repair):
            reports.append(auto_repair)
            return {"anomaly_events": 0, "repaired": False}

    monkeypatch.setattr(pb_pnls_utils, "FillEventsManager", FakeManager)
    monkeypatch.setattr(pb_pnls_utils, "_extract_symbol_pool", lambda config, runtime: set())
    monkeypatch.setattr(pb_pnls_utils, "_build_fetcher_for_bot", lambda bot, pool: object())
    monkeypatch.setattr(pb_pnls_utils.os, "getenv", lambda key, default="": "repair")

    bot = types.SimpleNamespace(
        _pnls_initialized=False,
        config={},
        exchange="binance",
        user="alice",
    )

    await pb_pnls_utils.init_pnls(bot)

    assert reports == [True]
