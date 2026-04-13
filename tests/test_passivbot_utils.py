import logging
import sys
import types
from importlib import import_module
from types import SimpleNamespace

pb_utils = import_module("passivbot_utils")


def _fake_pbr():
    return SimpleNamespace(
        calc_pnl_long=lambda entry, close, qty, c_mult: 11.0,
        calc_pnl_short=lambda entry, close, qty, c_mult: -7.0,
        calc_order_price_diff=lambda side, order_price, market_price: 0.123,
    )


def test_clip_by_timestamp_slices_sorted_rows():
    rows = [{"timestamp": 10}, {"timestamp": 20}, {"timestamp": 30}]

    assert pb_utils.clip_by_timestamp(rows, 15, 25) == [{"timestamp": 20}]


def test_calc_pnl_delegates_by_side(monkeypatch):
    monkeypatch.setattr(pb_utils, "_get_pbr", _fake_pbr)
    assert pb_utils.calc_pnl("long", 1, 2, 3, False, 1.0) == 11.0
    assert pb_utils.calc_pnl("short", 1, 2, 3, False, 1.0) == -7.0
    assert pb_utils.calc_pnl(None, 1, 2, 3, False, 1.0) == 11.0


def test_order_market_diff_delegates_to_rust_helper(monkeypatch):
    monkeypatch.setattr(pb_utils, "_get_pbr", _fake_pbr)
    assert pb_utils.order_market_diff("buy", 100.0, 101.0) == 0.123


def test_get_function_and_caller_names_are_safe():
    def outer():
        def inner():
            return pb_utils.get_function_name(), pb_utils.get_caller_name()

        return inner()

    function_name, caller_name = outer()

    assert function_name == "inner"
    assert caller_name == "outer"


def test_or_default_returns_default_on_exception_with_debug_log(caplog):
    assert pb_utils.or_default(lambda: 5, default=1) == 5

    with caplog.at_level(logging.DEBUG):
        result = pb_utils.or_default(lambda: 1 / 0, default=7)

    assert result == 7
    assert "or_default falling back to default" in caplog.text
    assert any(record.exc_info for record in caplog.records)


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


def test_get_process_rss_bytes_falls_back_from_psutil_to_resource_with_debug_log(monkeypatch, caplog):
    class FailingPsutil:
        class Process:
            def __init__(self, pid):
                raise RuntimeError(f"psutil failed for pid {pid}")

    class ResourceStub:
        RUSAGE_SELF = object()

        @staticmethod
        def getrusage(who):
            assert who is ResourceStub.RUSAGE_SELF
            return types.SimpleNamespace(ru_maxrss=123)

    monkeypatch.setattr(pb_utils, "psutil", FailingPsutil)
    monkeypatch.setattr(pb_utils, "resource", ResourceStub)

    with caplog.at_level(logging.DEBUG):
        result = pb_utils.get_process_rss_bytes()

    expected = 123 * 1024 if sys.platform.startswith("linux") else 123
    assert result == expected
    assert "psutil RSS lookup failed" in caplog.text


def test_get_process_rss_bytes_returns_none_when_resource_lookup_fails_with_debug_log(monkeypatch, caplog):
    class ResourceStub:
        RUSAGE_SELF = object()

        @staticmethod
        def getrusage(who):
            raise RuntimeError(f"resource failed for {who!r}")

    monkeypatch.setattr(pb_utils, "psutil", None)
    monkeypatch.setattr(pb_utils, "resource", ResourceStub)

    with caplog.at_level(logging.DEBUG):
        result = pb_utils.get_process_rss_bytes()

    assert result is None
    assert "resource RSS lookup failed" in caplog.text
