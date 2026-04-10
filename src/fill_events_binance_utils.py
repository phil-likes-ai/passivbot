from __future__ import annotations

import logging
from importlib import import_module
from typing import Callable, Dict, Iterable, List, Optional, Tuple


logger = logging.getLogger("fill_events_manager")


def _ts_to_date(ts: int) -> str:
    return import_module("pure_funcs").ts_to_date(ts)


def _as_str(value: object) -> str:
    return "" if value is None else str(value)


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


def _as_float(value: object) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        return float(value) if value else 0.0
    return 0.0


def resolve_symbol(fetcher, value: Optional[object]) -> str:
    if not value:
        return ""
    try:
        resolved = fetcher._symbol_resolver(_as_str(value))
        if resolved:
            return resolved
    except Exception as exc:
        logger.warning("BinanceFetcher._resolve_symbol: resolver failed for %s (%s)", value, exc)
    return _as_str(value)


def normalize_income(fetcher, entry: Dict[str, object]) -> Dict[str, object]:
    trade_id = entry.get("tradeId") or entry.get("id") or f"income-{entry.get('time')}"
    timestamp = _as_int(entry.get("time") or entry.get("timestamp"))
    raw_symbol = entry.get("symbol")
    ccxt_symbol = resolve_symbol(fetcher, raw_symbol)
    pnl = _as_float(entry.get("income") or entry.get("pnl"))
    position_side = str(entry.get("positionSide") or entry.get("pside") or "unknown").lower()
    return {
        "id": str(trade_id),
        "timestamp": timestamp,
        "datetime": _ts_to_date(timestamp),
        "symbol": ccxt_symbol,
        "side": entry.get("side") or "",
        "qty": 0.0,
        "price": 0.0,
        "pnl": pnl,
        "fees": None,
        "pb_order_type": "",
        "position_side": position_side or "unknown",
        "client_order_id": entry.get("clientOrderId") or "",
    }


def normalize_trade(fetcher, trade: Dict[str, object]) -> Dict[str, object]:
    info = trade.get("info") or {}
    info = info if isinstance(info, dict) else {}
    trade_id = trade.get("id") or info.get("id")
    timestamp = _as_int(trade.get("timestamp") or info.get("time") or info.get("T"))
    pnl = _as_float(info.get("realizedPnl") or trade.get("pnl"))
    position_side = str(info.get("positionSide") or trade.get("position_side") or "unknown").lower()
    fees = trade.get("fees") or trade.get("fee")
    client_order_id = (
        trade.get("clientOrderId")
        or info.get("clientOrderId")
        or info.get("origClientOrderId")
        or info.get("clientOrderID")
        or ""
    )
    symbol = trade.get("symbol")
    if isinstance(symbol, str) and "/" not in symbol:
        symbol = resolve_symbol(fetcher, symbol)
    order_id = trade.get("order") or info.get("orderId") or info.get("origClientOrderId") or info.get("orderID")
    return {
        "id": str(trade_id),
        "timestamp": timestamp,
        "datetime": _ts_to_date(timestamp),
        "symbol": _as_str(symbol),
        "side": trade.get("side") or "",
        "qty": _as_float(trade.get("amount") or trade.get("qty")),
        "price": _as_float(trade.get("price")),
        "pnl": pnl,
        "fees": fees,
        "pb_order_type": "",
        "position_side": position_side or "unknown",
        "client_order_id": client_order_id,
        "order_id": str(order_id) if order_id else "",
        "info": info,
        "raw": [{"source": "fetch_my_trades", "data": dict(trade)}],
    }


def collect_symbols(fetcher, provider: Callable[[], Iterable[str]]) -> List[str]:
    try:
        items = provider() or []
    except Exception as exc:
        logger.warning("BinanceFetcher._collect_symbols: provider failed (%s)", exc)
        return []
    symbols: List[str] = []
    for raw in items:
        normalized = resolve_symbol(fetcher, raw)
        if normalized:
            symbols.append(normalized)
    return symbols


def collect_enrichment_targets(
    merged: Dict[str, Dict[str, object]], trade_events: Dict[str, Dict[str, object]]
) -> List[Tuple[Dict[str, object], str, str, str]]:
    targets: List[Tuple[Dict[str, object], str, str, str]] = []
    for event_id, event in merged.items():
        has_client = bool(event.get("client_order_id"))
        has_type = bool(event.get("pb_order_type")) and event["pb_order_type"] != "unknown"
        if has_client and has_type:
            continue
        trade = trade_events.get(event_id)
        if trade:
            order_id = trade.get("order_id")
            symbol = trade.get("symbol") or event.get("symbol")
        else:
            order_id = event.get("order_id")
            symbol = event.get("symbol")
        if not order_id or not symbol:
            continue
        targets.append((event, event_id, str(order_id), str(symbol)))
    return targets


def apply_enrichment_result(
    event: Dict[str, object],
    event_id: str,
    result: Optional[Tuple[str, str]],
    detail_cache: Dict[str, Tuple[str, str]],
) -> None:
    if not result:
        return
    client_oid, pb_type = result
    event["client_order_id"] = client_oid
    if pb_type:
        event["pb_order_type"] = pb_type
    if event_id:
        detail_cache[event_id] = (client_oid, pb_type or "")


def finalize_merged_events(
    merged: Dict[str, Dict[str, object]],
    detail_cache: Dict[str, Tuple[str, str]],
    custom_id_to_snake_fn,
) -> None:
    for event_id, event in merged.items():
        client_oid = event.get("client_order_id")
        if client_oid and not event.get("pb_order_type"):
            event["pb_order_type"] = custom_id_to_snake_fn(str(client_oid))
        if not event.get("pb_order_type"):
            event["pb_order_type"] = ""
        event["client_order_id"] = _as_str(client_oid)
        if event_id and event.get("client_order_id"):
            detail_cache[event_id] = (
                _as_str(event.get("client_order_id")),
                _as_str(event.get("pb_order_type")),
            )
