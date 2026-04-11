import types
from importlib import import_module
import logging


pb_trailing_utils = import_module("passivbot_trailing_utils")


def test_get_last_position_changes_returns_empty_when_no_manager():
    bot = types.SimpleNamespace(_pnls_manager=None)

    result = pb_trailing_utils.get_last_position_changes(bot)

    assert dict(result) == {}


def test_get_last_position_changes_prefers_latest_matching_event(monkeypatch):
    monkeypatch.setattr(pb_trailing_utils, "utc_ms", lambda: 10_000)
    events = [
        types.SimpleNamespace(symbol="BTC/USDT:USDT", position_side="long", timestamp=100),
        types.SimpleNamespace(symbol="BTC/USDT:USDT", position_side="long", timestamp=200),
        types.SimpleNamespace(symbol="BTC/USDT:USDT", position_side="short", timestamp=300),
    ]
    bot = types.SimpleNamespace(
        _pnls_manager=types.SimpleNamespace(get_events=lambda: events),
        positions={"BTC/USDT:USDT": {"long": {"size": 1.0}, "short": {"size": 1.0}}},
        has_position=lambda pside, symbol: True,
        is_trailing=lambda symbol, pside: True,
    )

    result = pb_trailing_utils.get_last_position_changes(bot)

    assert result["BTC/USDT:USDT"]["long"] == 200
    assert result["BTC/USDT:USDT"]["short"] == 300


def test_get_last_position_changes_uses_default_when_no_event_found(monkeypatch):
    monkeypatch.setattr(pb_trailing_utils, "utc_ms", lambda: 10_000)
    bot = types.SimpleNamespace(
        _pnls_manager=types.SimpleNamespace(get_events=lambda: []),
        positions={"BTC/USDT:USDT": {"long": {"size": 1.0}, "short": {"size": 0.0}}},
        has_position=lambda pside, symbol: pside == "long",
        is_trailing=lambda symbol, pside: True,
    )

    result = pb_trailing_utils.get_last_position_changes(bot)

    assert result["BTC/USDT:USDT"]["long"] == 10_000 - 1000 * 60 * 60 * 24 * 7


def test_get_last_position_changes_logs_exception_and_uses_default_for_malformed_event(
    monkeypatch, caplog
):
    monkeypatch.setattr(pb_trailing_utils, "utc_ms", lambda: 10_000)
    malformed_event = types.SimpleNamespace(position_side="long", timestamp=123)
    bot = types.SimpleNamespace(
        _pnls_manager=types.SimpleNamespace(get_events=lambda: [malformed_event]),
        positions={"BTC/USDT:USDT": {"long": {"size": 1.0}}},
        has_position=lambda pside, symbol: pside == "long",
        is_trailing=lambda symbol, pside: True,
    )

    with caplog.at_level(logging.ERROR):
        result = pb_trailing_utils.get_last_position_changes(bot)

    assert result["BTC/USDT:USDT"]["long"] == 10_000 - 1000 * 60 * 60 * 24 * 7
    assert len(caplog.records) == 1
    assert caplog.records[0].exc_info is not None
