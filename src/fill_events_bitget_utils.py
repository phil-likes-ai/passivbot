from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional, Tuple

from pure_funcs import ts_to_date


logger = logging.getLogger("fill_events_manager")


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


def _require_bitget_fill_float(
    raw: Dict[str, object], key: str, field_name: str, trade_id: str
) -> float:
    value = raw.get(key)
    if value in (None, ""):
        raise ValueError(
            f"Bitget fill missing required {field_name} source '{key}' "
            f"for trade_id='{trade_id}'"
        )
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        return float(value)
    raise ValueError(
        f"Bitget fill invalid type for {field_name} source '{key}' "
        f"for trade_id='{trade_id}': {type(value)}"
    )


def resolve_symbol(fetcher, market_symbol: Optional[str]) -> str:
    if not market_symbol:
        return ""
    try:
        resolved = fetcher._symbol_resolver(market_symbol)
    except Exception as exc:
        logger.warning(
            "BitgetFetcher._resolve_symbol: resolver failed for %s (%s); using fallback",
            market_symbol,
            exc,
        )
        resolved = None
    if resolved:
        return resolved
    logger.warning(
        "BitgetFetcher._resolve_symbol: unresolved symbol '%s'; falling back to raw value",
        market_symbol,
    )
    return str(market_symbol)


def normalize_fill(fetcher, raw: Dict[str, object], deduce_side_pside_fn) -> Dict[str, object]:
    timestamp = _as_int(raw.get("cTime"))
    side, position_side = deduce_side_pside_fn(raw)
    trade_id = _as_str(raw.get("tradeId"))
    qty = _require_bitget_fill_float(raw, "baseVolume", "qty", trade_id)
    price = _require_bitget_fill_float(raw, "price", "price", trade_id)
    return {
        "id": trade_id,
        "order_id": raw.get("orderId"),
        "timestamp": timestamp,
        "datetime": ts_to_date(timestamp),
        "symbol": resolve_symbol(fetcher, _as_str(raw.get("symbol"))),
        "symbol_external": raw.get("symbol"),
        "side": side,
        "qty": qty,
        "price": price,
        "pnl": _as_float(raw.get("profit")),
        "fees": raw.get("feeDetail"),
        "pb_order_type": raw.get("pb_order_type", ""),
        "position_side": position_side,
        "client_order_id": raw.get("client_order_id"),
        "raw": [{"source": "fill_history", "data": dict(raw)}],
    }


def apply_detail_result(
    event: Dict[str, object], cache: Dict[str, tuple[str, str]], result: Optional[tuple[str, str]]
) -> int:
    if not result:
        return 0
    client_oid, pb_type = result
    event["client_order_id"] = client_oid
    event["pb_order_type"] = pb_type
    cache[_as_str(event.get("id"))] = (client_oid, pb_type)
    return 1


async def process_fill_batch(
    fetcher,
    fill_list: List[Dict[str, object]],
    detail_cache: Dict[str, Tuple[str, str]],
    events: Dict[str, Dict[str, object]],
) -> Tuple[List[str], int, int]:
    batch_ids: List[str] = []
    detail_hits = 0
    detail_fetches = 0
    pending_tasks: List[asyncio.Task[int]] = []
    for raw in fill_list:
        event = fetcher._normalize_fill(raw)
        event_id = _as_str(event.get("id"))
        if not event_id:
            continue
        batch_ids.append(event_id)
        if event_id in detail_cache:
            client_oid, pb_type = detail_cache[event_id]
            event["client_order_id"] = client_oid
            event["pb_order_type"] = pb_type
            detail_hits += 1
        if not event.get("client_order_id"):
            pending_tasks.append(asyncio.create_task(fetcher._enrich_with_details(event, detail_cache)))
            if len(pending_tasks) >= fetcher.detail_concurrency:
                detail_fetches += await fetcher._flush_detail_tasks(pending_tasks)
        events[event_id] = event
    detail_fetches += await fetcher._flush_detail_tasks(pending_tasks)
    return batch_ids, detail_hits, detail_fetches


def build_batch_events(
    events: Dict[str, Dict[str, object]], batch_ids: List[str]
) -> List[Dict[str, object]]:
    return [dict(events[event_id]) for event_id in batch_ids if events[event_id].get("client_order_id")]


def oldest_fill_timestamp(fill_list: List[Dict[str, object]]) -> int:
    return min(_as_int(raw.get("cTime")) for raw in fill_list)


def next_end_time_for_empty_batch(
    end_param: int, since_ms: Optional[int], buffer_step_ms: int
) -> Optional[int]:
    if since_ms is None or end_param <= since_ms:
        return None
    new_end_time = max(since_ms, end_param - buffer_step_ms)
    if new_end_time == end_param:
        new_end_time = max(since_ms, end_param - 1)
    return new_end_time


def next_end_time_for_short_batch(
    fill_list: List[Dict[str, object]],
    end_param: int,
    since_ms: Optional[int],
    buffer_step_ms: int,
) -> Optional[int]:
    if since_ms is None:
        return None
    if end_param - since_ms < buffer_step_ms:
        return None
    oldest = oldest_fill_timestamp(fill_list)
    new_end_time = max(since_ms, min(end_param, oldest) - 1)
    if new_end_time <= since_ms:
        return None
    return new_end_time
