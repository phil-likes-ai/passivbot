from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from pure_funcs import ts_to_date


logger = logging.getLogger("fill_events_manager")


def determine_position_side(side: str, closed_size: float) -> str:
    if side == "buy":
        return "short" if closed_size else "long"
    if side == "sell":
        return "long" if closed_size else "short"
    return "long"


def _get_required_float(trade: dict, info: dict, trade_key: str, info_key: str) -> float:
    trade_value = trade.get(trade_key)
    if trade_value not in (None, ""):
        return float(trade_value)

    info_value = info.get(info_key)
    if info_value not in (None, ""):
        return float(info_value)

    raise ValueError(
        f"Bybit trade missing required {trade_key}: trade.{trade_key} and info.{info_key} are both empty "
        f"for trade_id={trade.get('id') or info.get('execId') or info.get('orderId') or trade.get('order')}"
    )


def normalize_trade(trade: dict) -> dict:
    info = trade.get("info", {})
    info = info if isinstance(info, dict) else {}
    order_id = str(info.get("orderId", trade.get("order")))
    trade_id = str(trade.get("id") or info.get("execId") or order_id)
    timestamp = int(trade.get("timestamp") or info.get("execTime", 0))
    qty = _get_required_float(trade, info, "amount", "execQty")
    side = str(trade.get("side") or info.get("side", "")).lower()
    price = _get_required_float(trade, info, "price", "execPrice")
    closed_size = float(info.get("closedSize") or info.get("closeSize") or 0.0)
    position_side = determine_position_side(side, closed_size)
    pnl = float(trade.get("pnl") or 0.0)
    client_order_id = info.get("orderLinkId") or trade.get("clientOrderId")
    fee = trade.get("fee")
    symbol = trade.get("symbol") or info.get("symbol")

    return {
        "id": trade_id,
        "order_id": order_id,
        "timestamp": timestamp,
        "datetime": ts_to_date(timestamp),
        "symbol": symbol,
        "side": side,
        "qty": abs(qty),
        "price": price,
        "pnl": pnl,
        "fees": fee,
        "pb_order_type": "",
        "position_side": position_side,
        "client_order_id": client_order_id or "",
        "closed_size": closed_size,
        "raw": [{"source": "fetch_my_trades", "data": dict(trade)}],
    }


def process_closed_pnl_batch(fetcher, batch: List[Dict[str, object]], start_ms: int, results: Dict[str, Dict[str, object]]) -> None:
    for record in batch:
        updated_ts = int(record.get("updatedTime", 0))
        created_ts = int(record.get("createdTime", 0))
        order_id = record.get("orderId", "")
        if updated_ts < start_ms or order_id in results:
            continue
        raw_symbol = record.get("symbol", "")
        ccxt_symbol = raw_symbol
        if hasattr(fetcher.api, "markets") and fetcher.api.markets:
            for market_symbol, market in fetcher.api.markets.items():
                if market.get("id") == raw_symbol:
                    ccxt_symbol = market_symbol
                    break
        results[order_id] = {
            "info": record,
            "symbol": ccxt_symbol,
            "timestamp": created_ts,
            "datetime": datetime.fromtimestamp(created_ts / 1000, tz=timezone.utc).isoformat(),
            "lastUpdateTimestamp": updated_ts,
            "realizedPnl": float(record.get("closedPnl", 0)),
            "contracts": float(record.get("closedSize", 0)),
            "entryPrice": float(record.get("avgEntryPrice", 0)),
            "lastPrice": float(record.get("avgExitPrice", 0)),
            "leverage": float(record.get("leverage", 1)),
            "side": "long" if record.get("side", "").lower() == "sell" else "short",
        }


def combine(fetcher, trades: List[Dict[str, object]], positions: List[Dict[str, object]], detail_cache: Dict[str, Tuple[str, str]], custom_id_to_snake_fn) -> List[Dict[str, object]]:
    pnl_by_order: Dict[str, Dict] = {}
    raw_pnl_by_order: Dict[str, Dict] = {}
    for entry in positions:
        info = entry.get("info", {})
        order_id = str(info.get("orderId", entry.get("orderId", "")))
        if not order_id:
            continue
        pnl_by_order[order_id] = {
            "closedPnl": float(entry.get("realizedPnl") or info.get("closedPnl") or 0.0),
            "avgEntryPrice": float(info.get("avgEntryPrice") or 0.0),
            "avgExitPrice": float(info.get("avgExitPrice") or 0.0),
            "closedSize": float(info.get("closedSize") or entry.get("contracts") or 0.0),
            "closeFee": float(info.get("closeFee") or 0.0),
            "openFee": float(info.get("openFee") or 0.0),
            "side": str(info.get("side") or "").lower(),
            "symbol": entry.get("symbol") or info.get("symbol"),
        }
        raw_pnl_by_order[order_id] = dict(entry)

    events: List[Dict[str, object]] = []
    matched_count = 0
    computed_count = 0
    for trade in trades:
        event = fetcher._normalize_trade(trade)
        order_id = event.get("order_id")
        cache_entry = detail_cache.get(event["id"])
        if cache_entry:
            event["client_order_id"], event["pb_order_type"] = cache_entry
            if not event["pb_order_type"]:
                event["pb_order_type"] = "unknown"
        elif event["client_order_id"]:
            pb_type = custom_id_to_snake_fn(event["client_order_id"])
            event["pb_order_type"] = pb_type or "unknown"
        else:
            event["pb_order_type"] = "unknown"

        closed_size = float(event.get("closed_size", 0))
        if closed_size > 0 and order_id and order_id in pnl_by_order:
            pnl_record = pnl_by_order[order_id]
            avg_entry = pnl_record["avgEntryPrice"]
            exit_price = event["price"]
            position_side = event["position_side"]
            if avg_entry > 0 and exit_price > 0:
                if position_side == "long":
                    gross_pnl = (exit_price - avg_entry) * closed_size
                else:
                    gross_pnl = (avg_entry - exit_price) * closed_size
                total_closed = pnl_record["closedSize"]
                total_fees = pnl_record["closeFee"] + pnl_record["openFee"]
                fee_portion = (closed_size / total_closed) * total_fees if total_closed > 0 else 0.0
                event["pnl"] = gross_pnl - fee_portion
                computed_count += 1
            else:
                event["pnl"] = pnl_record["closedPnl"]
            matched_count += 1
            if order_id in raw_pnl_by_order:
                event["raw"].append({"source": "positions_history", "data": raw_pnl_by_order[order_id]})

        events.append(event)

    if matched_count > 0:
        logger.debug(
            "[fills] PnL computed for %d/%d close fills using avgEntryPrice",
            computed_count,
            matched_count,
        )
    return events
