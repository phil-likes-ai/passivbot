from importlib import import_module


pb_hook_utils = import_module("passivbot_hook_utils")


def test_filter_approved_symbols_is_passthrough_by_default():
    symbols = {"BTC/USDT:USDT", "ETH/USDT:USDT"}

    assert pb_hook_utils.filter_approved_symbols(object(), "long", symbols) == symbols


def test_assert_supported_live_state_is_noop_by_default():
    assert pb_hook_utils.assert_supported_live_state(object()) is None


def test_symbol_is_eligible_is_true_by_default():
    assert pb_hook_utils.symbol_is_eligible(object(), "BTC/USDT:USDT") is True
