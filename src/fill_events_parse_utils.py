from __future__ import annotations

from typing import Dict, List, Optional, Tuple


def normalize_raw_field(raw: object) -> List[Dict[str, object]]:
    """Normalize raw field to List[Dict] format."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return [dict(item) if isinstance(item, dict) else {"data": item} for item in raw]
    if isinstance(raw, dict):
        return [{"source": "legacy", "data": raw}]
    return [{"source": "unknown", "data": str(raw)}]


def extract_source_ids(raw: object, fallback_id: Optional[object]) -> List[str]:
    """Extract stable source IDs from raw payloads, with fallback to event id."""
    ids: set[str] = set()
    raw_items = normalize_raw_field(raw)
    for item in raw_items:
        data = item.get("data") if isinstance(item, dict) else item
        if isinstance(data, dict):
            for key in ("id", "tradeId", "trade_id", "execId"):
                val = data.get(key)
                if val:
                    ids.add(str(val))
            info = data.get("info")
            if isinstance(info, dict):
                for key in ("tid", "id", "tradeId", "trade_id", "execId"):
                    val = info.get(key)
                    if val:
                        ids.add(str(val))
    if not ids and fallback_id:
        ids.add(str(fallback_id))
    return sorted(ids)


def bybit_trade_dedupe_key(trade: Dict[str, object]) -> Optional[Tuple[object, ...]]:
    """Build a stable dedupe key for Bybit fetch_my_trades rows."""
    info = trade.get("info")
    info = info if isinstance(info, dict) else {}
    exec_id = trade.get("id") or info.get("execId")
    if exec_id:
        return ("exec_id", str(exec_id))
    timestamp = int(trade.get("timestamp") or info.get("execTime") or 0)
    symbol = str(trade.get("symbol") or info.get("symbol") or "")
    side = str(trade.get("side") or info.get("side") or "").lower()
    order_id = str(trade.get("order") or info.get("orderId") or "")
    amount = float(trade.get("amount") or info.get("execQty") or 0.0)
    price = float(trade.get("price") or info.get("execPrice") or 0.0)
    if timestamp <= 0 or not symbol or not side or not order_id or amount <= 0.0 or price <= 0.0:
        return None
    return ("fallback", timestamp, symbol, side, order_id, amount, price)


def bybit_trade_qty_abs(trade: Dict[str, object]) -> float:
    info = trade.get("info")
    info = info if isinstance(info, dict) else {}
    return abs(float(trade.get("amount") or info.get("execQty") or 0.0))


def bybit_trade_qty_signed(trade: Dict[str, object]) -> float:
    info = trade.get("info")
    info = info if isinstance(info, dict) else {}
    side = str(trade.get("side") or info.get("side") or "").lower()
    qty = bybit_trade_qty_abs(trade)
    if side == "sell":
        return -qty
    return qty


def bybit_event_group_key(event) -> Tuple[int, str, str, str, str]:
    return (
        int(event.timestamp),
        str(event.symbol),
        str(event.pb_order_type),
        str(event.side).lower(),
        str(event.position_side).lower(),
    )


def custom_id_to_snake(client_oid: str) -> str:
    """Import helper from passivbot when available; otherwise preserve raw id."""
    try:
        from passivbot import custom_id_to_snake as real

        return real(client_oid)
    except Exception:
        return client_oid or ""


def deduce_side_pside(elm: dict) -> Tuple[str, str]:
    """Import helper from exchanges.bitget when available; fallback sanely."""
    try:
        from exchanges.bitget import deduce_side_pside as real

        return real(elm)
    except Exception:
        side = str(elm.get("side", "buy")).lower()
        return side or "buy", "long"
