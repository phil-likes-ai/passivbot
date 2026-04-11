from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from pure_funcs import ensure_millis, ts_to_date


def _as_str(value: object) -> str:
    return str(value or "")


def _as_float(value: object) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        return float(value) if value else 0.0
    return 0.0


def _get_required_float(trade: dict, info: dict, *keys: str) -> float:
    for key in keys:
        value = trade.get(key)
        if value not in (None, ""):
            return float(value)
        value = info.get(key)
        if value not in (None, ""):
            return float(value)
    raise ValueError(
        f"KuCoin trade missing required qty/price: trade keys {keys} and info keys {keys} are all empty "
        f"for trade_id={trade.get('id') or info.get('tradeId') or info.get('id')}"
    )


def _as_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        return int(value) if value else 0
    return 0


def match_pnls(
    closes: List[Dict[str, object]],
    positions: List[Dict[str, object]],
    events: Dict[str, Dict[str, object]],
) -> List[Dict[str, object]]:
    """Match position close PnL from positions_history to trade fills."""
    match_window_ms = 5 * 60 * 1000

    closes_by_symbol: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for c in closes:
        closes_by_symbol[_as_str(c.get("symbol"))].append(c)
    positions_by_symbol: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for p in positions:
        positions_by_symbol[_as_str(p.get("symbol"))].append(p)

    assigned_trade_ids: set[str] = set()
    unmatched_positions: List[Dict[str, object]] = []
    for symbol, pos_list in positions_by_symbol.items():
        if symbol not in closes_by_symbol:
            unmatched_positions.extend(pos_list)
            continue

        symbol_closes = closes_by_symbol[symbol]
        for p in pos_list:
            p_ts = _as_int(p.get("lastUpdateTimestamp"))
            p_pnl = _as_float(p.get("realizedPnl"))
            matching_fills = [
                c
                for c in symbol_closes
                if _as_str(c.get("id")) not in assigned_trade_ids
                and abs(_as_int(c.get("timestamp")) - p_ts) < match_window_ms
            ]

            if not matching_fills:
                unmatched_positions.append(p)
                continue

            total_qty = sum(
                abs(_as_float(f.get("qty")) or _as_float(f.get("amount"))) for f in matching_fills
            )

            if total_qty <= 0:
                closest = min(matching_fills, key=lambda c: abs(_as_int(c.get("timestamp")) - p_ts))
                closest_id = _as_str(closest.get("id"))
                events[closest_id]["pnl"] = p_pnl
                assigned_trade_ids.add(closest_id)
            else:
                for fill in matching_fills:
                    fill_qty = abs(_as_float(fill.get("qty")) or _as_float(fill.get("amount")))
                    proportion = fill_qty / total_qty if total_qty > 0 else 0
                    fill_id = _as_str(fill.get("id"))
                    events[fill_id]["pnl"] = p_pnl * proportion
                    assigned_trade_ids.add(fill_id)

    for c in closes:
        close_id = _as_str(c.get("id"))
        if close_id not in assigned_trade_ids:
            events[close_id]["pnl"] = 0.0

    return unmatched_positions


def determine_position_side(side: str, reduce_only: bool, close_fee_pay: float) -> str:
    side = side.lower()
    if side == "buy":
        return "short" if close_fee_pay != 0.0 or reduce_only else "long"
    if side == "sell":
        return "long" if close_fee_pay != 0.0 or reduce_only else "short"
    return "long"


def normalize_trade(trade: Dict[str, object]) -> Dict[str, object]:
    info = trade.get("info", {}) or {}
    info = info if isinstance(info, dict) else {}
    trade_id = str(trade.get("id") or info.get("tradeId") or info.get("id") or "")
    order_id = str(trade.get("order") or info.get("orderId") or "")
    ts_raw = (
        info.get("tradeTime")
        or trade.get("timestamp")
        or info.get("createdAt")
        or info.get("updatedTime")
        or 0
    )
    try:
        timestamp = int(ensure_millis(_as_float(ts_raw)))
    except Exception:
        try:
            timestamp = int(_as_float(ts_raw))
        except Exception:
            timestamp = 0
    side = str(trade.get("side") or info.get("side") or "").lower()
    reduce_only = bool(trade.get("reduceOnly") or info.get("closeOrder") or False)
    close_fee_pay = _as_float(info.get("closeFeePay"))

    return {
        "id": trade_id,
        "order_id": order_id,
        "timestamp": timestamp,
        "datetime": ts_to_date(timestamp) if timestamp else "",
        "symbol": str(trade.get("symbol") or ""),
        "side": side,
        "qty": abs(_get_required_float(trade, info, "amount", "size")),
        "price": _get_required_float(trade, info, "price"),
        "pnl": 0.0,
        "fees": trade.get("fee"),
        "pb_order_type": "",
        "position_side": determine_position_side(side, reduce_only, close_fee_pay),
        "client_order_id": str(trade.get("clientOrderId") or info.get("clientOid") or ""),
        "raw": [{"source": "fetch_my_trades", "data": dict(trade)}],
    }


def apply_cached_order_details(
    events: List[Dict[str, object]], detail_cache: Dict[str, Tuple[str, str]]
) -> Dict[str, Tuple[str, str]]:
    order_id_cache: Dict[str, Tuple[str, str]] = {}
    for ev in events:
        ev_id = _as_str(ev.get("id"))
        order_id = _as_str(ev.get("order_id"))
        cached = detail_cache.get(ev_id) if ev_id else None
        if cached:
            ev["client_order_id"], ev["pb_order_type"] = cached
            if order_id:
                order_id_cache[order_id] = cached
            continue
        if order_id and order_id in order_id_cache:
            client_oid, pb_type = order_id_cache[order_id]
            ev["client_order_id"] = client_oid
            ev["pb_order_type"] = pb_type
            if ev_id:
                detail_cache[ev_id] = (client_oid, pb_type)
    return order_id_cache


def collect_events_requiring_order_details(
    events: List[Dict[str, object]],
    detail_cache: Dict[str, Tuple[str, str]],
    order_id_cache: Dict[str, Tuple[str, str]],
) -> Dict[str, List[Dict[str, object]]]:
    events_by_order: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for ev in events:
        has_client = bool(ev.get("client_order_id"))
        has_type = bool(ev.get("pb_order_type")) and ev["pb_order_type"] != "unknown"
        if has_client and has_type:
            continue
        order_id = _as_str(ev.get("order_id"))
        if not order_id:
            ev.setdefault("pb_order_type", "unknown")
            continue
        if order_id in order_id_cache:
            client_oid, pb_type = order_id_cache[order_id]
            ev["client_order_id"] = client_oid
            ev["pb_order_type"] = pb_type
            ev_id = _as_str(ev.get("id"))
            if ev_id:
                detail_cache[ev_id] = (client_oid, pb_type)
            continue
        events_by_order[order_id].append(ev)
    return events_by_order


def apply_order_detail_result(
    order_id: str,
    detail: Optional[Tuple[str, str]],
    events_by_order: Dict[str, List[Dict[str, object]]],
    detail_cache: Dict[str, Tuple[str, str]],
    order_id_cache: Dict[str, Tuple[str, str]],
) -> None:
    if detail is None:
        for ev in events_by_order.get(order_id, []):
            if not ev.get("pb_order_type"):
                ev["pb_order_type"] = "unknown"
        return

    client_oid, pb_type = detail
    order_id_cache[order_id] = (client_oid, pb_type)
    for ev in events_by_order.get(order_id, []):
        ev["client_order_id"] = client_oid or _as_str(ev.get("client_order_id"))
        ev["pb_order_type"] = pb_type or "unknown"
        ev_id = _as_str(ev.get("id"))
        if ev_id:
            detail_cache[ev_id] = (
                _as_str(ev.get("client_order_id")),
                _as_str(ev.get("pb_order_type")),
            )


def ensure_order_detail_defaults(events: List[Dict[str, object]]) -> None:
    for ev in events:
        if not ev.get("pb_order_type"):
            ev["pb_order_type"] = "unknown"


def parse_order_detail(detail: object, custom_id_to_snake_fn) -> Optional[Tuple[str, str]]:
    if not isinstance(detail, dict):
        return None
    info = detail.get("info")
    if not isinstance(info, dict):
        info = detail
    client_oid = (
        detail.get("clientOrderId")
        or info.get("clientOrderId")
        or info.get("clientOid")
        or info.get("clientOid")
    )
    if not client_oid:
        return None
    client_oid_str = str(client_oid)
    return client_oid_str, custom_id_to_snake_fn(client_oid_str)


def aggregate_position_pnls_by_symbol(positions: List[Dict[str, object]]) -> Dict[str, float]:
    pos_sum: Dict[str, float] = defaultdict(float)
    for position in positions:
        info = position.get("info")
        info = info if isinstance(info, dict) else {}
        symbol = _as_str(position.get("symbol") or info.get("symbol"))
        if not symbol:
            continue
        pos_sum[symbol] += _as_float(position.get("realizedPnl"))
    return dict(pos_sum)


def should_log_discrepancy(
    local_total: float,
    remote_total: float,
    current_delta: float,
    last_log: float,
    last_delta: Optional[float],
    now: float,
    *,
    min_ratio: float,
    change_threshold: float,
    min_seconds: float,
    throttle_seconds: float,
) -> bool:
    if abs(local_total - remote_total) <= max(1e-8, min_ratio * (abs(remote_total) + 1e-8)):
        return False
    delta_changed = last_delta is None or abs(current_delta - last_delta) > change_threshold * (
        abs(last_delta) + 1.0
    )
    time_since_last = now - last_log
    return (delta_changed and time_since_last >= min_seconds) or (
        time_since_last >= throttle_seconds
    )


def summarize_unmatched_positions(
    unmatched_positions: List[Dict[str, object]],
) -> Tuple[int, float]:
    return len(unmatched_positions), sum(
        _as_float(position.get("realizedPnl")) for position in unmatched_positions
    )


def collect_trade_batch(
    batch: List[Dict[str, object]],
    normalize_trade_fn,
    since_ts: int,
    until_ts: int,
    collected: Dict[Tuple[str, str], Dict[str, object]],
) -> int:
    batch_sorted = sorted(batch, key=lambda item: _as_int(item.get("timestamp")))
    for trade in batch_sorted:
        event = normalize_trade_fn(trade)
        ts = _as_int(event.get("timestamp"))
        if ts < since_ts or ts > until_ts:
            continue
        key = (_as_str(event.get("id")), _as_str(event.get("order_id")))
        collected[key] = event
    return _as_int(batch_sorted[-1].get("timestamp")) if batch_sorted else since_ts


def collect_positions_history_batch(
    batch: List[Dict[str, object]], results: Dict[str, Dict[str, object]], end_at: int
) -> int:
    batch_sorted = sorted(batch, key=lambda item: _as_int(item.get("lastUpdateTimestamp")))
    for position in batch_sorted:
        info = position.get("info")
        info = info if isinstance(info, dict) else {}
        close_id = _as_str(info.get("closeId") or position.get("id"))
        results[close_id] = position
    return _as_int(batch_sorted[-1].get("lastUpdateTimestamp")) if batch_sorted else end_at
