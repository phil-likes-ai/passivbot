import types
from importlib import import_module


pb_order_update_utils = import_module("passivbot_order_update_utils")


def test_add_and_remove_order_are_noops():
    bot = types.SimpleNamespace()

    assert pb_order_update_utils.add_new_order(bot, {"id": "x"}) is None
    assert pb_order_update_utils.remove_order(bot, {"id": "x"}) is None


def test_handle_order_update_sets_execution_flag_only_when_updates_exist():
    bot = types.SimpleNamespace(execution_scheduled=False)

    pb_order_update_utils.handle_order_update(bot, [])
    assert bot.execution_scheduled is False

    pb_order_update_utils.handle_order_update(bot, [{"id": "x"}])
    assert bot.execution_scheduled is True


def test_recent_order_cancellation_tracking_and_age_window(monkeypatch):
    times = iter([1000, 1000, 2000])
    monkeypatch.setattr(pb_order_update_utils, "utc_ms", lambda: next(times))
    bot = types.SimpleNamespace(recent_order_cancellations=[])
    order = {
        "symbol": "BTC/USDT:USDT",
        "side": "buy",
        "position_side": "long",
        "qty": 1.0,
        "price": 100.0,
    }

    pb_order_update_utils.add_to_recent_order_cancellations(bot, order)
    remaining = pb_order_update_utils.order_was_recently_cancelled(bot, order, max_age_ms=1500)

    assert remaining == 500.0


def test_recent_order_execution_tracking_and_age_window(monkeypatch):
    times = iter([1000, 1000, 2500])
    monkeypatch.setattr(pb_order_update_utils, "utc_ms", lambda: next(times))
    bot = types.SimpleNamespace(recent_order_executions=[])
    order = {
        "symbol": "BTC/USDT:USDT",
        "side": "buy",
        "position_side": "long",
        "qty": 1.0,
        "price": 100.0,
    }

    pb_order_update_utils.add_to_recent_order_executions(bot, order)
    remaining = pb_order_update_utils.order_was_recently_updated(bot, order, max_age_ms=3000)

    assert remaining == 1500.0
