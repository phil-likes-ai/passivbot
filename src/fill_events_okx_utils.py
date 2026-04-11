from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from pure_funcs import ts_to_date


def normalize_inst_id(inst_id: str) -> str:
    symbol = inst_id
    if "-SWAP" in inst_id:
        parts = inst_id.replace("-SWAP", "").split("-")
        if len(parts) == 2:
            base, quote = parts
            symbol = f"{base}/{quote}:{quote}"
    elif "-" in inst_id:
        parts = inst_id.split("-")
        if len(parts) >= 2:
            base, quote = parts[0], parts[1]
            symbol = f"{base}/{quote}:{quote}"
    return symbol


def determine_position_side(side: str, pos_side_raw: str, pnl: float) -> str:
    side = side.lower()
    pos_side_raw = pos_side_raw.lower()
    if pos_side_raw == "net":
        if pnl != 0:
            return "short" if side == "buy" else "long"
        return "long" if side == "buy" else "short"
    if pos_side_raw in ("long", "short"):
        return pos_side_raw
    return "long" if side == "buy" else "short"


def _require_okx_fill_float(
    raw: dict, key: str, field_name: str, trade_id: str, order_id: str
) -> float:
    value = raw.get(key)
    if value in (None, ""):
        raise ValueError(
            f"OKX fill missing required {field_name} source '{key}' "
            f"for trade_id='{trade_id}' order_id='{order_id}'"
        )
    return float(value)


def normalize_fill(raw: dict) -> dict:
    trade_id = str(raw.get("tradeId") or "")
    order_id = str(raw.get("ordId") or "")
    timestamp = int(raw.get("ts") or raw.get("fillTime") or 0)
    side = str(raw.get("side") or "").lower()
    pnl = float(raw.get("fillPnl") or 0.0)
    fee_ccy = str(raw.get("feeCcy") or "")
    fee_amt = float(raw.get("fee") or 0.0)
    qty = abs(_require_okx_fill_float(raw, "fillSz", "qty", trade_id, order_id))
    price = _require_okx_fill_float(raw, "fillPx", "price", trade_id, order_id)

    return {
        "id": trade_id,
        "order_id": order_id,
        "timestamp": timestamp,
        "datetime": ts_to_date(timestamp) if timestamp else "",
        "symbol": normalize_inst_id(str(raw.get("instId") or "")),
        "side": side,
        "qty": qty,
        "price": price,
        "pnl": pnl,
        "fees": {"currency": fee_ccy, "cost": abs(fee_amt)} if fee_ccy else None,
        "pb_order_type": "",
        "position_side": determine_position_side(side, str(raw.get("posSide") or ""), pnl),
        "client_order_id": str(raw.get("clOrdId") or ""),
        "raw": [{"source": "okx_fills", "data": raw}],
        "c_mult": 1.0,
    }


def apply_order_detail_cache(
    event: dict, detail_cache: dict, custom_id_to_snake_fn
) -> None:
    event_id = event.get("id")
    cache_entry = detail_cache.get(event_id)
    if cache_entry:
        event["client_order_id"], event["pb_order_type"] = cache_entry
    else:
        client_oid = str(event.get("client_order_id") or "")
        pb_type = str(event.get("pb_order_type") or "")
        if not pb_type and client_oid:
            pb_type = custom_id_to_snake_fn(client_oid)
        if not pb_type:
            pb_type = "unknown"
        event["client_order_id"] = client_oid
        event["pb_order_type"] = pb_type
        if event_id and client_oid:
            detail_cache[event_id] = (client_oid, pb_type)


def process_fill_batch(
    fills: List[dict],
    normalize_fill_fn,
    detail_cache: Dict[str, Tuple[str, str]],
    custom_id_to_snake_fn,
    collected: Dict[str, dict],
    since_ms: Optional[int],
    until_ms: Optional[int],
) -> Tuple[List[dict], Optional[int]]:
    batch_events: List[dict] = []
    oldest_ts: Optional[int] = None
    for raw in fills:
        event = normalize_fill_fn(raw)
        event_id = event["id"]
        if not event_id:
            continue

        apply_order_detail_cache(event, detail_cache, custom_id_to_snake_fn)

        ts = event["timestamp"]
        if since_ms is not None and ts < since_ms:
            continue
        if until_ms is not None and ts > until_ms:
            continue

        if oldest_ts is None or ts < oldest_ts:
            oldest_ts = ts

        if event_id in detail_cache:
            event["client_order_id"], event["pb_order_type"] = detail_cache[event_id]

        collected[event_id] = event
        batch_events.append(event)

    return batch_events, oldest_ts


def next_after_cursor(fills: List[dict]) -> Optional[str]:
    if not fills:
        return None
    cursor = fills[-1].get("billId")
    return str(cursor) if cursor else None


def finalize_events(
    collected: Dict[str, dict],
    detail_cache: Dict[str, Tuple[str, str]],
    custom_id_to_snake_fn,
    coalesce_events_fn,
    since_ms: Optional[int],
    until_ms: Optional[int],
) -> List[dict]:
    events = sorted(collected.values(), key=lambda ev: ev["timestamp"])
    if since_ms is not None:
        events = [ev for ev in events if ev["timestamp"] >= since_ms]
    if until_ms is not None:
        events = [ev for ev in events if ev["timestamp"] <= until_ms]
    events = coalesce_events_fn(events)
    for event in events:
        apply_order_detail_cache(event, detail_cache, custom_id_to_snake_fn)
    return events


def build_fetch_params(
    inst_type: str,
    trade_limit: int,
    since_ms: Optional[int],
    until_ms: Optional[int],
    after_cursor: Optional[str],
) -> Dict[str, object]:
    params: Dict[str, object] = {
        "instType": inst_type,
        "limit": str(trade_limit),
    }
    if since_ms is not None:
        params["begin"] = str(since_ms)
    if until_ms is not None:
        params["end"] = str(until_ms)
    if after_cursor:
        params["after"] = after_cursor
    return params


def reached_since_boundary(oldest_ts: Optional[int], since_ms: Optional[int]) -> bool:
    return since_ms is not None and oldest_ts is not None and oldest_ts <= since_ms


def short_batch(fills: List[dict], trade_limit: int) -> bool:
    return len(fills) < trade_limit
