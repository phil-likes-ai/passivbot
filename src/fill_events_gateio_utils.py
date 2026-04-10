from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Tuple

import asyncio
import logging

from ccxt.base.errors import RateLimitExceeded

from pure_funcs import ts_to_date


logger = logging.getLogger("fill_events_manager")


def normalize_raw_trade(raw: Dict[str, object]) -> Dict[str, object]:
    """Convert raw Gate.io my_trades_timerange response to CCXT-like format."""
    create_time = raw.get("create_time", 0)
    timestamp_ms = int(float(create_time) * 1000) if create_time else 0
    contract = str(raw.get("contract") or "")
    symbol = contract.replace("_", "/") + ":USDT" if contract else ""
    size = float(raw.get("size") or 0)
    side = "buy" if size >= 0 else "sell"
    fee_cost = float(raw.get("fee") or 0)
    fee = {"cost": fee_cost, "currency": "USDT"} if fee_cost else None
    return {
        "id": str(raw.get("trade_id") or raw.get("id") or ""),
        "order": str(raw.get("order_id") or ""),
        "timestamp": timestamp_ms,
        "symbol": symbol,
        "side": side,
        "amount": abs(size),
        "price": float(raw.get("price") or 0),
        "fee": fee,
        "info": raw,
    }


def merge_trades_with_orders(fetcher, trades: List[Dict[str, object]], orders_by_id: Dict[str, Dict[str, object]], detail_cache: Dict[str, Tuple[str, str]]) -> List[Dict[str, object]]:
    """Merge trades with order-level PnL, distributing proportionally."""
    trades_by_order: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for t in trades:
        oid = str(t.get("order") or t.get("info", {}).get("order_id") or "")
        trades_by_order[oid].append(t)

    events = []
    for order_id, order_trades in trades_by_order.items():
        order = orders_by_id.get(order_id, {})
        order_info = order.get("info", {}) if order else {}
        order_pnl = float(order_info.get("pnl") or 0.0)
        total_qty = sum(abs(float(t.get("amount", 0))) for t in order_trades)
        for t in order_trades:
            event = fetcher._normalize_trade(t, order, order_pnl, total_qty, detail_cache)
            events.append(event)
    return events


def determine_position_side(side: str, is_close: bool) -> str:
    side = side.lower()
    if is_close:
        if side == "buy":
            return "short"
        if side == "sell":
            return "long"
    else:
        if side == "buy":
            return "long"
        if side == "sell":
            return "short"
    return "long"


def normalize_trade(fetcher, trade: Dict[str, object], order: Dict[str, object], order_pnl: float, total_qty: float, detail_cache: Dict[str, Tuple[str, str]]) -> Dict[str, object]:
    info = trade.get("info", {}) or {}
    order_info = order.get("info", {}) if order else {}

    trade_id = str(trade.get("id") or info.get("trade_id") or "")
    order_id = str(trade.get("order") or info.get("order_id") or "")

    ts_raw = trade.get("timestamp") or info.get("create_time") or 0
    try:
        timestamp = int(fetcher.ensure_millis(float(ts_raw)))
    except Exception:
        timestamp = int(float(ts_raw)) if ts_raw else 0

    symbol = str(trade.get("symbol") or info.get("contract") or "")
    side = str(trade.get("side") or info.get("side") or "").lower()
    qty = abs(float(trade.get("amount") or info.get("size") or 0.0))
    price = float(trade.get("price") or info.get("price") or 0.0)
    fee = trade.get("fee")

    proportion = qty / total_qty if total_qty > 0 else 0
    pnl = order_pnl * proportion

    client_order_id = str(
        info.get("text") or order.get("clientOrderId") or order_info.get("text") or ""
    )

    if trade_id and trade_id in detail_cache:
        client_order_id, pb_type = detail_cache[trade_id]
    else:
        from fill_events_parse_utils import custom_id_to_snake

        pb_type = custom_id_to_snake(client_order_id) if client_order_id else "unknown"
        if trade_id and client_order_id:
            detail_cache[trade_id] = (client_order_id, pb_type)

    close_size = float(info.get("close_size", 0))
    is_reduce_only = order.get("reduceOnly", False) or order_info.get("is_reduce_only", False)
    is_close = close_size > 0 or is_reduce_only or abs(order_pnl) > 0
    position_side = determine_position_side(side, is_close)

    return {
        "id": trade_id,
        "order_id": order_id,
        "timestamp": timestamp,
        "datetime": ts_to_date(timestamp) if timestamp else "",
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "price": price,
        "pnl": pnl,
        "fees": fee,
        "pb_order_type": pb_type or "unknown",
        "position_side": position_side,
        "client_order_id": client_order_id,
        "raw": [{"source": "my_trades_timerange", "data": dict(trade)}],
    }


async def fetch_orders_for_pnl(fetcher, order_ids: set[str]) -> Dict[str, Dict[str, object]]:
    """Fetch closed orders to get PnL data."""
    orders_by_id: Dict[str, Dict[str, object]] = {}
    max_fetches = 400
    fetch_count = 0
    params: Dict[str, object] = {"status": "finished", "limit": 100, "offset": 0}

    while fetch_count < max_fetches:
        fetch_count += 1
        try:
            batch = await fetcher.api.fetch_closed_orders(params=params)
        except RateLimitExceeded as exc:
            logger.debug("GateioFetcher._fetch_orders_for_pnl: rate-limited (%s); sleeping", exc)
            await asyncio.sleep(1.0)
            continue

        if not batch:
            break

        for order in batch:
            oid = str(order.get("id") or "")
            if oid:
                orders_by_id[oid] = order

        if order_ids and order_ids.issubset(orders_by_id.keys()):
            break
        if len(batch) < 100:
            break

        params["offset"] = int(params.get("offset", 0)) + 100

    return orders_by_id
