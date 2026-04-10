import sys
import types
from importlib import import_module


stub = sys.modules.get("passivbot_rust")
if stub is None:
    stub = types.ModuleType("passivbot_rust")
    sys.modules["passivbot_rust"] = stub
setattr(stub, "calc_pnl_long", lambda entry, close, qty, c_mult: 11.0)
setattr(stub, "calc_pnl_short", lambda entry, close, qty, c_mult: -7.0)
setattr(stub, "calc_order_price_diff", lambda side, order_price, market_price: 0.123)

pb_utils = import_module("passivbot_utils")


def test_clip_by_timestamp_slices_sorted_rows():
    rows = [{"timestamp": 10}, {"timestamp": 20}, {"timestamp": 30}]

    assert pb_utils.clip_by_timestamp(rows, 15, 25) == [{"timestamp": 20}]


def test_calc_pnl_delegates_by_side():
    assert pb_utils.calc_pnl("long", 1, 2, 3, False, 1.0) == 11.0
    assert pb_utils.calc_pnl("short", 1, 2, 3, False, 1.0) == -7.0
    assert pb_utils.calc_pnl(None, 1, 2, 3, False, 1.0) == 11.0


def test_order_market_diff_delegates_to_rust_helper():
    assert pb_utils.order_market_diff("buy", 100.0, 101.0) == 0.123


def test_get_function_and_caller_names_are_safe():
    def outer():
        def inner():
            return pb_utils.get_function_name(), pb_utils.get_caller_name()

        return inner()

    function_name, caller_name = outer()

    assert function_name == "inner"
    assert caller_name == "outer"


def test_or_default_returns_default_on_exception():
    assert pb_utils.or_default(lambda: 5, default=1) == 5
    assert pb_utils.or_default(lambda: 1 / 0, default=7) == 7


def test_orders_matching_respects_tolerances():
    base = {
        "symbol": "BTC/USDT:USDT",
        "side": "buy",
        "position_side": "long",
        "price": 100.0,
        "qty": 1.0,
    }
    near = {**base, "price": 100.1, "qty": 1.005}
    far = {**base, "price": 101.0}

    assert pb_utils.orders_matching(base, near, tolerance_qty=0.01, tolerance_price=0.002)
    assert not pb_utils.orders_matching(base, far, tolerance_qty=0.01, tolerance_price=0.002)


def test_order_has_match_returns_first_match_or_false():
    target = {
        "symbol": "BTC/USDT:USDT",
        "side": "buy",
        "position_side": "long",
        "price": 100.0,
        "qty": 1.0,
    }
    orders = [
        {**target, "price": 100.05},
        {**target, "price": 100.08},
    ]

    assert pb_utils.order_has_match(target, orders)["price"] == 100.05
    assert pb_utils.order_has_match(target, [{**target, "side": "sell"}]) is False
