from __future__ import annotations

import math
import re
from typing import Callable


def build_ema_pairs(ema_map: dict) -> list[list[float]]:
    return [[float(k), float(v)] for k, v in sorted(ema_map.items())]


def build_side_input(
    *,
    pside: str,
    symbol: str,
    mode_overrides: dict[str, dict[str, str | None]],
    positions: dict,
    trailing_prices: dict,
    bot_params_to_rust_dict_fn: Callable[[str, str | None], dict],
    mode_override_to_orchestrator_mode_fn: Callable[[str | None], str],
    trailing_bundle_default_fn: Callable[[], dict],
) -> dict:
    mode = mode_override_to_orchestrator_mode_fn(mode_overrides[pside].get(symbol))
    pos = positions.get(symbol, {}).get(pside, {"size": 0.0, "price": 0.0})
    trailing = trailing_prices.get(symbol, {}).get(pside)
    if not trailing:
        trailing = trailing_bundle_default_fn()
    else:
        trailing = dict(trailing)
    return {
        "mode": mode,
        "position": {"size": float(pos["size"]), "price": float(pos["price"])},
        "trailing": {
            "min_since_open": float(trailing.get("min_since_open", 0.0)),
            "max_since_min": float(trailing.get("max_since_min", 0.0)),
            "max_since_open": float(trailing.get("max_since_open", 0.0)),
            "min_since_max": float(trailing.get("min_since_max", 0.0)),
        },
        "bot_params": bot_params_to_rust_dict_fn(pside, symbol),
    }


def build_symbol_input(
    *,
    symbol: str,
    idx: int,
    mprice: float,
    active: bool,
    qty_step: float,
    price_step: float,
    min_qty: float,
    min_cost: float,
    c_mult: float,
    maker_fee: float,
    taker_fee: float,
    effective_min_cost: float,
    m1_close_emas: dict,
    m1_volume_emas: dict,
    m1_log_range_emas: dict,
    h1_log_range_emas: dict,
    side_input_fn: Callable[[str], dict],
) -> dict:
    return {
        "symbol_idx": int(idx),
        "order_book": {"bid": float(mprice), "ask": float(mprice)},
        "exchange": {
            "qty_step": float(qty_step),
            "price_step": float(price_step),
            "min_qty": float(min_qty),
            "min_cost": float(min_cost),
            "c_mult": float(c_mult),
            "maker_fee": float(maker_fee),
            "taker_fee": float(taker_fee),
        },
        "tradable": bool(active),
        "next_candle": None,
        "effective_min_cost": float(effective_min_cost),
        "emas": {
            "m1": {
                "close": build_ema_pairs(m1_close_emas),
                "log_range": build_ema_pairs(m1_log_range_emas),
                "volume": build_ema_pairs(m1_volume_emas),
            },
            "h1": {
                "close": [],
                "log_range": build_ema_pairs(h1_log_range_emas),
                "volume": [],
            },
        },
        "long": side_input_fn("long"),
        "short": side_input_fn("short"),
    }


def get_required_market_fee(*, markets_dict: dict, symbol: str, fee_side: str) -> float:
    market = markets_dict.get(symbol)
    if market is None:
        raise KeyError(f"missing market metadata for {symbol} while building orchestrator input")
    if fee_side not in market:
        raise KeyError(f"missing required {fee_side}_fee for {symbol}")
    fee = float(market[fee_side])
    if not math.isfinite(fee):
        raise ValueError(f"invalid {fee_side}_fee for {symbol}: {market[fee_side]}")
    return fee


def build_orchestrator_input_base(
    *,
    balance: float,
    balance_raw: float,
    filter_by_min_effective_cost: bool,
    market_orders_allowed: bool,
    market_order_near_touch_threshold: float,
    panic_close_market: bool,
    unstuck_allowance_long: float,
    unstuck_allowance_short: float,
    max_realized_loss_pct: float,
    realized_pnl_cumsum_max: float,
    realized_pnl_cumsum_last: float,
    global_bp: dict,
    effective_hedge_mode: bool,
) -> dict:
    return {
        "balance": float(balance),
        "balance_raw": float(balance_raw),
        "global": {
            "filter_by_min_effective_cost": bool(filter_by_min_effective_cost),
            "market_orders_allowed": bool(market_orders_allowed),
            "market_order_near_touch_threshold": float(market_order_near_touch_threshold),
            "panic_close_market": bool(panic_close_market),
            "unstuck_allowance_long": float(unstuck_allowance_long),
            "unstuck_allowance_short": float(unstuck_allowance_short),
            "max_realized_loss_pct": float(max_realized_loss_pct),
            "realized_pnl_cumsum_max": float(realized_pnl_cumsum_max),
            "realized_pnl_cumsum_last": float(realized_pnl_cumsum_last),
            "sort_global": True,
            "global_bot_params": global_bp,
            "hedge_mode": bool(effective_hedge_mode),
        },
        "symbols": [],
        "peek_hints": None,
    }


def build_ideal_orders_by_symbol(
    *,
    orders: list,
    idx_to_symbol: dict[int, str],
    order_type_snake_to_id_fn: Callable[[str], int],
) -> dict[str, list]:
    ideal_orders: dict[str, list] = {}
    for order in orders:
        symbol = idx_to_symbol.get(int(order["symbol_idx"]))
        if symbol is None:
            continue
        order_type = str(order["order_type"])
        order_type_id = int(order_type_snake_to_id_fn(order_type))
        execution_type = str(order.get("execution_type", "limit"))
        tup = (
            float(order["qty"]),
            float(order["price"]),
            order_type,
            order_type_id,
            execution_type,
        )
        ideal_orders.setdefault(symbol, []).append(tup)
    return ideal_orders


def extract_unstuck_log_payload(
    *,
    orders: list,
    idx_to_symbol: dict[int, str],
    positions: dict,
    last_prices: dict,
    unstuck_allowances: dict,
) -> dict | None:
    for order in orders:
        order_type_str = order.get("order_type", "")
        if "close_unstuck" not in order_type_str:
            continue
        symbol = idx_to_symbol.get(int(order.get("symbol_idx", -1)))
        if not symbol:
            return None
        pside = "long" if "long" in order_type_str else "short"
        pos = positions.get(symbol, {}).get(pside, {})
        entry_price = float(pos.get("price", 0.0) or 0.0)
        current_price = float(last_prices.get(symbol, 0.0) or 0.0)
        if entry_price > 0.0 and current_price > 0.0:
            price_diff_pct = (current_price / entry_price - 1.0) * 100.0
            sign = "+" if price_diff_pct >= 0.0 else ""
        else:
            price_diff_pct = 0.0
            sign = ""
        coin = symbol.split("/")[0] if "/" in symbol else symbol
        allowance = float(unstuck_allowances.get(pside, 0.0) or 0.0)
        return {
            "coin": coin,
            "pside": pside,
            "entry_price": entry_price,
            "current_price": current_price,
            "price_diff_pct": price_diff_pct,
            "sign": sign,
            "allowance": allowance,
        }
    return None


def log_missing_ema_error(*, error: Exception, idx_to_symbol: dict[int, str], logger) -> None:
    msg = str(error)
    if "MissingEma" not in msg:
        return
    match = re.search(r"symbol_idx\s*:\s*(\d+)", msg)
    if not match:
        return
    idx = int(match.group(1))
    symbol = idx_to_symbol.get(idx)
    if symbol:
        logger.error("[ema] Missing EMA for %s (symbol_idx=%d)", symbol, idx)
