from __future__ import annotations

from typing import Dict, List

from pure_funcs import ts_to_date


def fill_event_from_dict_kwargs(data: Dict[str, object], extract_source_ids, normalize_raw_field) -> Dict[str, object]:
    required = [
        "id",
        "timestamp",
        "symbol",
        "side",
        "qty",
        "price",
        "pnl",
        "pb_order_type",
        "position_side",
        "client_order_id",
    ]
    missing = [key for key in required if key not in data]
    if missing:
        raise ValueError(f"Fill event missing required keys: {missing}")
    return {
        "id": str(data["id"]),
        "source_ids": (
            extract_source_ids(data.get("raw"), data.get("id"))
            if not data.get("source_ids")
            else [str(x) for x in data.get("source_ids") if x]
        ),
        "timestamp": int(data["timestamp"]),
        "datetime": str(data.get("datetime") or ts_to_date(int(data["timestamp"]))),
        "symbol": str(data["symbol"]),
        "side": str(data["side"]).lower(),
        "qty": float(data["qty"]),
        "price": float(data["price"]),
        "pnl": float(data["pnl"]),
        "fees": data.get("fees"),
        "pb_order_type": str(data["pb_order_type"]),
        "position_side": str(data["position_side"]).lower(),
        "client_order_id": str(data["client_order_id"]),
        "psize": float(data.get("psize", 0.0)),
        "pprice": float(data.get("pprice", 0.0)),
        "raw": normalize_raw_field(data.get("raw")),
    }


def fill_event_to_dict(event) -> Dict[str, object]:
    return {
        "id": event.id,
        "source_ids": list(event.source_ids) if event.source_ids is not None else [],
        "timestamp": event.timestamp,
        "datetime": event.datetime,
        "symbol": event.symbol,
        "side": event.side,
        "qty": event.qty,
        "price": event.price,
        "pnl": event.pnl,
        "fees": event.fees,
        "pb_order_type": event.pb_order_type,
        "position_side": event.position_side,
        "client_order_id": event.client_order_id,
        "psize": event.psize,
        "pprice": event.pprice,
        "raw": event.raw if event.raw is not None else [],
    }


def fill_event_key(event) -> str:
    return event.id
