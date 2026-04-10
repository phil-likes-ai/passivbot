from __future__ import annotations

import bisect
import inspect
import os
import sys
from importlib import import_module
from typing import Optional

try:
    import psutil
except Exception:
    psutil = None

try:
    import resource
except Exception:
    resource = None


def _get_pbr():
    return import_module("passivbot_rust")


def get_process_rss_bytes() -> Optional[int]:
    """Return current process RSS in bytes or None if unavailable."""
    try:
        if psutil is not None:
            return int(psutil.Process(os.getpid()).memory_info().rss)
    except Exception:
        pass
    if resource is not None:
        try:
            getrusage = getattr(resource, "getrusage", None)
            rusage_self = getattr(resource, "RUSAGE_SELF", None)
            if getrusage is None or rusage_self is None:
                return None
            usage = getrusage(rusage_self).ru_maxrss
            if sys.platform.startswith("linux"):
                usage = int(usage) * 1024
            else:
                usage = int(usage)
            return int(usage)
        except Exception:
            pass
    return None


def clip_by_timestamp(xs, start_ts, end_ts):
    """Slice a timestamp-sorted event list between start/end timestamps."""
    timestamps = [x["timestamp"] for x in xs]
    i0 = bisect.bisect_left(timestamps, start_ts) if start_ts else 0
    i1 = bisect.bisect_right(timestamps, end_ts) if end_ts else len(xs)
    return xs[i0:i1]


def calc_pnl(position_side, entry_price, close_price, qty, inverse, c_mult):
    """Calculate trade PnL by delegating to the appropriate Rust helper."""
    del inverse
    pbr = _get_pbr()
    if isinstance(position_side, str):
        if position_side == "long":
            return pbr.calc_pnl_long(entry_price, close_price, qty, c_mult)
        return pbr.calc_pnl_short(entry_price, close_price, qty, c_mult)
    return pbr.calc_pnl_long(entry_price, close_price, qty, c_mult)


def order_market_diff(side: str, order_price: float, market_price: float) -> float:
    """Return side-aware relative price diff between order and market."""
    pbr = _get_pbr()
    return float(pbr.calc_order_price_diff(side, float(order_price), float(market_price)))


def get_function_name() -> str:
    """Return the caller function name one frame above the current scope."""
    frame = inspect.currentframe()
    if frame is None or frame.f_back is None:
        return "<unknown>"
    return frame.f_back.f_code.co_name


def get_caller_name() -> str:
    """Return the caller name two frames above the current scope."""
    frame = inspect.currentframe()
    if frame is None or frame.f_back is None or frame.f_back.f_back is None:
        return "<unknown>"
    return frame.f_back.f_back.f_code.co_name


def or_default(f, *args, default=None, **kwargs):
    """Execute `f` safely, returning `default` if an exception is raised."""
    try:
        return f(*args, **kwargs)
    except Exception:
        return default


def orders_matching(o0, o1, tolerance_qty=0.01, tolerance_price=0.002):
    """Return True if two orders are equivalent within the supplied tolerances."""
    for k in ["symbol", "side", "position_side"]:
        if o0[k] != o1[k]:
            return False
    if tolerance_price:
        if abs(o0["price"] - o1["price"]) / o0["price"] > tolerance_price:
            return False
    else:
        if o0["price"] != o1["price"]:
            return False
    if tolerance_qty:
        if abs(o0["qty"] - o1["qty"]) / o0["qty"] > tolerance_qty:
            return False
    else:
        if o0["qty"] != o1["qty"]:
            return False
    return True


def order_has_match(order, orders, tolerance_qty=0.01, tolerance_price=0.002):
    """Return the first matching order in `orders` or False if none match."""
    for elm in orders:
        if orders_matching(order, elm, tolerance_qty, tolerance_price):
            return elm
    return False
