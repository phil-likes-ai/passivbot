import types
from importlib import import_module


pb_client_utils = import_module("passivbot_client_utils")


def test_build_ccxt_options_parses_recv_window_and_merges_overrides():
    bot = types.SimpleNamespace(config={"live": {"recv_window_ms": "5000"}})

    result = pb_client_utils.build_ccxt_options(bot, {"custom": True})

    assert result == {
        "adjustForTimeDifference": True,
        "recvWindow": 5000,
        "custom": True,
    }


def test_build_ccxt_options_ignores_invalid_recv_window():
    bot = types.SimpleNamespace(config={"live": {"recv_window_ms": "banana"}})

    result = pb_client_utils.build_ccxt_options(bot)

    assert result == {"adjustForTimeDifference": True}


def test_apply_endpoint_override_calls_override_helper(monkeypatch):
    calls = []

    def fake_apply(client, override):
        calls.append((client, override))

    monkeypatch.setattr(pb_client_utils, "apply_rest_overrides_to_ccxt", fake_apply)

    client = object()
    override = object()
    bot = types.SimpleNamespace(endpoint_override=override)

    pb_client_utils.apply_endpoint_override(bot, client)

    assert calls == [(client, override)]
