from importlib import import_module
import types


pb_debug_utils = import_module("passivbot_debug_utils")


def test_log_once_logs_only_first_time(monkeypatch):
    calls = []
    monkeypatch.setattr(pb_debug_utils.logging, "info", lambda msg: calls.append(msg))
    bot = types.SimpleNamespace()

    pb_debug_utils.log_once(bot, "hello")
    pb_debug_utils.log_once(bot, "hello")

    assert calls == ["hello"]


def test_debug_print_only_prints_in_debug_mode(monkeypatch):
    calls = []
    monkeypatch.setattr("builtins.print", lambda *args: calls.append(args))

    bot = types.SimpleNamespace(debug_mode=False)
    pb_debug_utils.debug_print(bot, "x")
    assert calls == []

    bot.debug_mode = True
    pb_debug_utils.debug_print(bot, "x", 1)
    assert calls == [("x", 1)]
