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


def test_get_current_n_positions_counts_nonzero_positions_without_forced_mode_stub():
    bot = types.SimpleNamespace(
        positions={
            "BTC/USDT:USDT": {"long": {"size": 1.0}},
            "ETH/USDT:USDT": {"long": {"size": 0.0}},
            "SOL/USDT:USDT": {"long": {"size": 2.0}},
        },
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


def test_forager_refresh_budget_uses_token_bucket_state(monkeypatch):
    bot = types.SimpleNamespace()
    ticks = iter([60_000, 90_000])
    monkeypatch.setattr(pb_mode_utils, "utc_ms", lambda: next(ticks))

    first = pb_mode_utils._forager_refresh_budget(bot, 4)
    second = pb_mode_utils._forager_refresh_budget(bot, 4)

    assert first == 4
    assert second == 2
    assert bot._forager_refresh_state["last_ms"] == 90_000


def test_split_forager_budget_by_side_round_robins_remainder():
    bot = types.SimpleNamespace(_forager_budget_rr=0)

    first = pb_mode_utils._split_forager_budget_by_side(bot, 3, ["long", "short"])
    second = pb_mode_utils._split_forager_budget_by_side(bot, 3, ["long", "short"])

    assert first == {"long": 2, "short": 1}
    assert second == {"long": 1, "short": 2}


def test_forager_target_staleness_falls_back_to_ttl_for_invalid_values():
    bot = types.SimpleNamespace(inactive_coin_candle_ttl_ms=123_000)
    assert pb_mode_utils._forager_target_staleness_ms(bot, "bad", 0) == 123_000


def test_maybe_log_candle_refresh_logs_debug_with_exc_info_on_failure():
    bot = types.SimpleNamespace(candle_refresh_log_boot_delay_ms=0, start_time_ms=0)
    calls = []
    old_debug = pb_mode_utils.logging.debug
    try:
        pb_mode_utils.logging.debug = lambda *args, **kwargs: calls.append((args, kwargs))
        pb_mode_utils._maybe_log_candle_refresh(bot, "forager", None)
    finally:
        pb_mode_utils.logging.debug = old_debug

    assert calls
    assert calls[-1][0][0] == "[candle] failed to emit candle refresh summary"
    assert calls[-1][1].get("exc_info") is True
