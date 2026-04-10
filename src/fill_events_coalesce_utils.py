from __future__ import annotations

from typing import Dict, List, Tuple


def coalesce_events(events: List[Dict[str, object]], merge_fee_lists, normalize_raw_field) -> List[Dict[str, object]]:
    """Group events sharing timestamp/symbol/pb_type/side/position."""
    aggregated: Dict[Tuple, Dict[str, object]] = {}
    order: List[Tuple] = []

    def event_source_ids(ev: Dict[str, object]) -> List[str]:
        ids = ev.get("source_ids")
        if ids:
            return [str(x) for x in ids if x]
        return []

    for ev in events:
        key = (
            ev.get("timestamp"),
            ev.get("symbol"),
            ev.get("pb_order_type"),
            ev.get("side"),
            ev.get("position_side"),
        )
        if key not in aggregated:
            aggregated[key] = dict(ev)
            aggregated[key]["id"] = str(ev.get("id", ""))
            src_ids = event_source_ids(ev)
            if src_ids:
                aggregated[key]["source_ids"] = src_ids
            aggregated[key]["qty"] = float(ev.get("qty", 0.0))
            aggregated[key]["pnl"] = float(ev.get("pnl", 0.0))
            aggregated[key]["fees"] = merge_fee_lists(ev.get("fees"), None)
            aggregated[key]["raw"] = normalize_raw_field(ev.get("raw"))
            aggregated[key]["_price_numerator"] = float(ev.get("price", 0.0)) * float(
                ev.get("qty", 0.0)
            )
            order.append(key)
        else:
            agg = aggregated[key]
            agg["id"] = f"{agg['id']}+{ev.get('id', '')}".strip("+")
            src_ids = event_source_ids(ev)
            if src_ids:
                merged_ids = set(agg.get("source_ids") or [])
                merged_ids.update(src_ids)
                agg["source_ids"] = sorted(merged_ids)
            agg["qty"] = float(agg.get("qty", 0.0)) + float(ev.get("qty", 0.0))
            agg["pnl"] = float(agg.get("pnl", 0.0)) + float(ev.get("pnl", 0.0))
            agg["fees"] = merge_fee_lists(agg.get("fees"), ev.get("fees"))
            agg["raw"] = normalize_raw_field(agg.get("raw")) + normalize_raw_field(ev.get("raw"))
            agg["_price_numerator"] = float(agg.get("_price_numerator", 0.0)) + float(
                ev.get("price", 0.0)
            ) * float(ev.get("qty", 0.0))
            if not agg.get("client_order_id") and ev.get("client_order_id"):
                agg["client_order_id"] = ev.get("client_order_id")
            if not agg.get("pb_order_type"):
                agg["pb_order_type"] = ev.get("pb_order_type")
    coalesced: List[Dict[str, object]] = []
    for key in order:
        agg = aggregated[key]
        qty = float(agg.get("qty", 0.0))
        price_numerator = float(agg.get("_price_numerator", 0.0))
        if qty > 0:
            agg["price"] = price_numerator / qty
        agg.pop("_price_numerator", None)
        fees = agg.get("fees")
        if isinstance(fees, list) and len(fees) == 1:
            agg["fees"] = fees[0]
        coalesced.append(agg)
    return coalesced
