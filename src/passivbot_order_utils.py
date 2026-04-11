from __future__ import annotations

import logging
import re

import numpy as np

import passivbot_rust as pbr


_TYPE_MARKER_RE = re.compile(r"0x([0-9a-fA-F]{4})", re.IGNORECASE)
_LEADING_HEX4_RE = re.compile(r"^(?:0x)?([0-9a-fA-F]{4})", re.IGNORECASE)


def try_decode_type_id_from_custom_id(custom_id: str) -> int | None:
    """Extract the 16-bit order type id encoded in a custom order id string."""
    m = _TYPE_MARKER_RE.search(custom_id)
    if m:
        return int(m.group(1), 16)

    m = _LEADING_HEX4_RE.match(custom_id)
    if m:
        return int(m.group(1), 16)

    return None


def order_type_id_to_hex4(type_id: int) -> str:
    """Return the four-hex-digit representation of an order type id."""
    return f"{type_id:04x}"


def type_token(type_id: int, with_marker: bool = True) -> str:
    """Return the printable order type marker, optionally prefixed with `0x`."""
    h4 = order_type_id_to_hex4(type_id)
    return ("0x" + h4) if with_marker else h4


def snake_of(type_id: int) -> str:
    """Map an order type id to its snake_case string representation."""
    try:
        return pbr.order_type_id_to_snake(type_id)
    except Exception:
        logging.debug(
            "[order] failed to map order type id to snake_case; type_id=%s",
            type_id,
            exc_info=True,
        )
        return "unknown"


def custom_id_to_snake(custom_id) -> str:
    """Translate a broker custom id into the snake_case order type name."""
    type_id = try_decode_type_id_from_custom_id(custom_id)
    if type_id is None:
        logging.error("[order] order type decode failed; custom_id=%s; reason=invalid_custom_id", custom_id)
        return "unknown"
    return snake_of(type_id)


def trailing_bundle_tuple_to_dict(bundle_tuple: tuple[float, float, float, float]) -> dict:
    min_since_open, max_since_min, max_since_open, min_since_max = bundle_tuple
    return {
        "min_since_open": float(min_since_open),
        "max_since_min": float(max_since_min),
        "max_since_open": float(max_since_open),
        "min_since_max": float(min_since_max),
    }


def trailing_bundle_default_dict() -> dict:
    return trailing_bundle_tuple_to_dict(pbr.trailing_bundle_default_py())


def trailing_bundle_from_arrays(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray) -> dict:
    if highs.size == 0:
        return trailing_bundle_default_dict()
    bundle_tuple = pbr.update_trailing_bundle_py(
        np.asarray(highs, dtype=np.float64),
        np.asarray(lows, dtype=np.float64),
        np.asarray(closes, dtype=np.float64),
        bundle=None,
    )
    return trailing_bundle_tuple_to_dict(bundle_tuple)


def order_to_order_tuple(self, order):
    """Convert an order dictionary into a normalized tuple for comparisons."""
    return (
        order["symbol"],
        order["side"],
        order["position_side"],
        round(float(order["qty"]), 12),
        round(float(order["price"]), 12),
    )


def has_open_unstuck_order(self) -> bool:
    """Return True if an unstuck order is currently live on the exchange."""
    for orders in getattr(self, "open_orders", {}).values():
        for order in orders or []:
            custom_id = order.get("custom_id") if isinstance(order, dict) else None
            if not custom_id:
                continue
            type_id = try_decode_type_id_from_custom_id(custom_id)
            if type_id is None:
                continue
            order_type = snake_of(type_id)
            if order_type in {"close_unstuck_long", "close_unstuck_short"}:
                return True
    return False
