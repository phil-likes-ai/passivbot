from __future__ import annotations

from pure_funcs import ts_to_date


def normalize_trade(trade: dict) -> dict:
    info = trade.get("info", {}) or {}
    trade_id = str(trade.get("id") or info.get("hash") or info.get("tid") or "")
    order_id = str(trade.get("order") or info.get("oid") or "")
    timestamp = int(
        trade.get("timestamp")
        or info.get("time")
        or info.get("tradeTime")
        or info.get("updatedTime")
        or 0
    )
    symbol_raw = trade.get("symbol") or info.get("symbol") or info.get("coin")
    side = str(trade.get("side") or info.get("side") or "").lower()
    qty_source = trade.get("amount") or info.get("sz")
    if qty_source is None:
        raise ValueError(f"Hyperliquid fill missing required qty: trade keys {list(trade.keys())}, info keys {list(info.keys())}")
    qty = abs(float(qty_source))
    price_source = trade.get("price") or info.get("px")
    if price_source is None:
        raise ValueError(f"Hyperliquid fill missing required price: trade keys {list(trade.keys())}, info keys {list(info.keys())}")
    price = float(price_source)
    pnl = float(trade.get("pnl") or info.get("closedPnl") or 0.0)
    fee = trade.get("fee") or {"currency": info.get("feeToken"), "cost": info.get("fee")}
    client_order_id = trade.get("clientOrderId") or info.get("cloid") or info.get("clOrdId") or ""
    direction = str(info.get("dir", "")).lower()
    if "short" in direction:
        position_side = "short"
    elif "long" in direction:
        position_side = "long"
    else:
        position_side = "long" if side == "buy" else "short"
    return {
        "id": trade_id,
        "order_id": order_id,
        "timestamp": timestamp,
        "datetime": ts_to_date(timestamp) if timestamp else "",
        "symbol": str(symbol_raw or ""),
        "side": side,
        "qty": qty,
        "price": price,
        "pnl": pnl,
        "fees": fee,
        "pb_order_type": "",
        "position_side": position_side,
        "client_order_id": str(client_order_id or ""),
        "raw": [{"source": "fetch_my_trades", "data": trade}],
        "c_mult": float(info.get("contractMultiplier") or info.get("multiplier") or 1.0),
    }
