import types
from importlib import import_module


pb_mode_utils = import_module("passivbot_mode_utils")


def test_get_max_n_positions_caps_by_approved_symbol_count():
    bot = types.SimpleNamespace(
        bot_value=lambda pside, key: 5,
        approved_coins_minus_ignored_coins={"long": {"BTC/USDT:USDT", "ETH/USDT:USDT"}},
    )

    assert pb_mode_utils.get_max_n_positions(bot, "long") == 2


def test_get_forced_pb_mode_prefers_runtime_and_inactive_market_fallback():
    bot = types.SimpleNamespace(
        _equity_hard_stop_enabled=lambda pside: False,
        _runtime_forced_modes={"long": {"BTC/USDT:USDT": "panic"}},
        config_get=lambda path, symbol=None: None,
        markets_dict={"ETH/USDT:USDT": {"active": False}},
    )

    assert pb_mode_utils.get_forced_PB_mode(bot, "long", "BTC/USDT:USDT") == "panic"
    assert pb_mode_utils.get_forced_PB_mode(bot, "long", "ETH/USDT:USDT") == "tp_only"


def test_get_forced_pb_mode_uses_configured_mode():
    bot = types.SimpleNamespace(
        _equity_hard_stop_enabled=lambda pside: False,
        config_get=lambda path, symbol=None: "manual",
        markets_dict={"BTC/USDT:USDT": {"active": True}},
    )

    assert pb_mode_utils.get_forced_PB_mode(bot, "long", "BTC/USDT:USDT") == "manual"


def test_get_current_n_positions_counts_nonzero_positions():
    bot = types.SimpleNamespace(
        positions={
            "BTC/USDT:USDT": {"long": {"size": 1.0}},
            "ETH/USDT:USDT": {"long": {"size": 0.0}},
            "SOL/USDT:USDT": {"long": {"size": 2.0}},
        },
        get_forced_PB_mode=lambda pside, symbol=None: None,
    )

    assert pb_mode_utils.get_current_n_positions(bot, "long") == 2


def test_is_forager_mode_checks_side_and_global_state():
    values = {
        ("long", "total_wallet_exposure_limit"): 0.5,
        ("short", "total_wallet_exposure_limit"): 0.0,
    }
    bot = types.SimpleNamespace(
        bot_value=lambda pside, key: values[(pside, key)],
        live_value=lambda key: None,
        get_max_n_positions=lambda pside: 2,
        approved_coins_minus_ignored_coins={"long": {"BTC", "ETH", "SOL"}, "short": set()},
    )
    bot.is_forager_mode = lambda pside=None: pb_mode_utils.is_forager_mode(bot, pside)

    assert bot.is_forager_mode("long") is True
    assert bot.is_forager_mode("short") is False
    assert bot.is_forager_mode() is True


def test_mode_override_to_orchestrator_mode_normalizes_values():
    assert pb_mode_utils.mode_override_to_orchestrator_mode(object(), None) is None
    assert pb_mode_utils.mode_override_to_orchestrator_mode(object(), "tp_only_with_active_entry_cancellation") == "tp_only"
    assert pb_mode_utils.mode_override_to_orchestrator_mode(object(), "panic") == "panic"
    assert pb_mode_utils.mode_override_to_orchestrator_mode(object(), "weird") == "manual"


def test_python_mode_from_orchestrator_state_prefers_override_then_active_flag():
    bot = types.SimpleNamespace(PB_mode_stop={"long": "graceful_stop"})

    assert pb_mode_utils.python_mode_from_orchestrator_state(bot, "long", "BTC/USDT:USDT", {"active": True}, "manual") == "manual"
    assert pb_mode_utils.python_mode_from_orchestrator_state(bot, "long", "BTC/USDT:USDT", {"active": True}, None) == "normal"
    assert pb_mode_utils.python_mode_from_orchestrator_state(bot, "long", "BTC/USDT:USDT", {"active": False}, None) == "graceful_stop"


def test_build_orchestrator_mode_overrides_fallback_uses_mode_converter():
    bot = types.SimpleNamespace(
        PB_modes={"long": {"BTC/USDT:USDT": "tp_only_with_active_entry_cancellation"}, "short": {}},
        _mode_override_to_orchestrator_mode=lambda mode: pb_mode_utils.mode_override_to_orchestrator_mode(None, mode),
    )

    result = pb_mode_utils.build_orchestrator_mode_overrides_fallback(bot, ["BTC/USDT:USDT", "ETH/USDT:USDT"])

    assert result == {
        "long": {"BTC/USDT:USDT": "tp_only", "ETH/USDT:USDT": None},
        "short": {"BTC/USDT:USDT": None, "ETH/USDT:USDT": None},
    }


def test_pside_blocks_new_entries_checks_forced_modes():
    bot = types.SimpleNamespace(get_forced_PB_mode=lambda pside: "panic")
    assert pb_mode_utils.pside_blocks_new_entries(bot, "long") is True

    bot.get_forced_PB_mode = lambda pside: "normal"
    assert pb_mode_utils.pside_blocks_new_entries(bot, "long") is False


def test_build_live_symbol_universe_merges_runtime_and_approved_symbols():
    bot = types.SimpleNamespace(
        positions={"BTC/USDT:USDT": {}},
        open_orders={"ETH/USDT:USDT": {}},
        coin_overrides={"SOL/USDT:USDT": {}},
        approved_coins_minus_ignored_coins={"long": {"XRP/USDT:USDT"}, "short": {"DOGE/USDT:USDT"}},
        _pside_blocks_new_entries=lambda pside: pside == "short",
        is_approved=lambda pside, symbol: True,
    )

    result = pb_mode_utils.build_live_symbol_universe(bot)

    assert result == ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "XRP/USDT:USDT"]
