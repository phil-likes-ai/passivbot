import sys
import types
from importlib import import_module

import numpy as np


stub = sys.modules.get("passivbot_rust")
if stub is None:
    stub = types.ModuleType("passivbot_rust")
    sys.modules["passivbot_rust"] = stub
setattr(stub, "order_type_id_to_snake", lambda type_id: f"type_{type_id}")
setattr(stub, "trailing_bundle_default_py", lambda: (1.0, 2.0, 3.0, 4.0))
setattr(
    stub,
    "update_trailing_bundle_py",
    lambda highs, lows, closes, bundle=None: (float(lows.min()), 9.0, float(highs.max()), 5.0),
)

order_utils = import_module("passivbot_order_utils")
custom_id_to_snake = order_utils.custom_id_to_snake
has_open_unstuck_order = order_utils.has_open_unstuck_order
order_to_order_tuple = order_utils.order_to_order_tuple
trailing_bundle_default_dict = order_utils.trailing_bundle_default_dict
trailing_bundle_from_arrays = order_utils.trailing_bundle_from_arrays
try_decode_type_id_from_custom_id = order_utils.try_decode_type_id_from_custom_id


def test_try_decode_type_id_from_custom_id_supports_marker_and_leading_hex():
    assert try_decode_type_id_from_custom_id("abc0x00ffxyz") == 255
    assert try_decode_type_id_from_custom_id("00ff_extra") == 255
    assert try_decode_type_id_from_custom_id("no-marker") is None


def test_custom_id_to_snake_returns_unknown_for_invalid_id(caplog):
    result = custom_id_to_snake("invalid")

    assert result == "unknown"
    assert "failed to convert custom_id" in caplog.text


def test_trailing_bundle_helpers_return_expected_dicts():
    assert trailing_bundle_default_dict() == {
        "min_since_open": 1.0,
        "max_since_min": 2.0,
        "max_since_open": 3.0,
        "min_since_max": 4.0,
    }

    result = trailing_bundle_from_arrays(
        np.asarray([4.0, 6.0]), np.asarray([2.0, 3.0]), np.asarray([3.0, 5.0])
    )

    assert result == {
        "min_since_open": 2.0,
        "max_since_min": 9.0,
        "max_since_open": 6.0,
        "min_since_max": 5.0,
    }


def test_order_to_order_tuple_normalizes_qty_and_price():
    result = order_to_order_tuple(
        object(),
        {
            "symbol": "BTC/USDT:USDT",
            "side": "buy",
            "position_side": "long",
            "qty": "1.2345678901234",
            "price": "101.2345678901234",
        },
    )

    assert result == (
        "BTC/USDT:USDT",
        "buy",
        "long",
        round(1.2345678901234, 12),
        round(101.2345678901234, 12),
    )


def test_has_open_unstuck_order_detects_unstuck_custom_ids(monkeypatch):
    monkeypatch.setattr(order_utils, "snake_of", lambda type_id: "close_unstuck_long")
    bot = types.SimpleNamespace(open_orders={"BTC/USDT:USDT": [{"custom_id": "0x00ff_demo"}]})

    assert has_open_unstuck_order(bot) is True
