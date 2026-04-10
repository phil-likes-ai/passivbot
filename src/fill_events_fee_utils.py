from __future__ import annotations

from typing import Dict, List, Optional, Sequence


def merge_fee_lists(
    fees_a: Optional[Sequence], fees_b: Optional[Sequence]
) -> Optional[List[Dict[str, object]]]:
    def to_list(fees):
        if not fees:
            return []
        if isinstance(fees, dict):
            return [fees]
        return list(fees)

    merged: Dict[str, Dict[str, object]] = {}
    for entry in to_list(fees_a) + to_list(fees_b):
        if not isinstance(entry, dict):
            continue
        currency = str(entry.get("currency") or entry.get("code") or "")
        if currency not in merged:
            merged[currency] = dict(entry)
            try:
                merged[currency]["cost"] = float(entry.get("cost", 0.0))
            except Exception:
                merged[currency]["cost"] = 0.0
        else:
            try:
                merged[currency]["cost"] += float(entry.get("cost", 0.0))
            except Exception:
                pass
    if not merged:
        return None
    return [dict(value) for value in merged.values()]


def fee_cost(fees: Optional[Sequence]) -> float:
    """Sum fee costs defensively, tolerating missing/partial structures."""
    total = 0.0
    if not fees:
        return total
    items: Sequence
    if isinstance(fees, dict):
        items = [fees]
    else:
        try:
            items = list(fees)
        except Exception:
            return total
    for entry in items:
        if not isinstance(entry, dict):
            continue
        try:
            total += float(entry.get("cost", 0.0))
        except Exception:
            continue
    return total


def normalize_fee_dict(fee: Optional[Dict[str, object]]) -> Optional[Dict[str, object]]:
    if not isinstance(fee, dict):
        return None
    out: Dict[str, object] = {}
    currency = fee.get("currency") or fee.get("code")
    if currency:
        out["currency"] = str(currency)
    try:
        out["cost"] = float(fee.get("cost", 0.0))
    except Exception:
        out["cost"] = 0.0
    if fee.get("rate") is not None:
        try:
            out["rate"] = float(fee.get("rate"))
        except Exception:
            pass
    return out


def extract_bybit_fee_from_trade_row(row: Dict[str, object], normalize_fee_dict_fn=normalize_fee_dict) -> Optional[Dict[str, object]]:
    fee = normalize_fee_dict_fn(row.get("fee"))
    if fee is not None:
        return fee
    info = row.get("info")
    info = info if isinstance(info, dict) else {}
    fee_cost_raw = info.get("execFee")
    fee_ccy = info.get("feeCurrency")
    if fee_cost_raw is None:
        return None
    try:
        fee_cost = float(fee_cost_raw)
    except Exception:
        return None
    out: Dict[str, object] = {"cost": fee_cost}
    if fee_ccy:
        out["currency"] = str(fee_ccy)
    fee_rate_raw = info.get("feeRate")
    if fee_rate_raw is not None:
        try:
            out["rate"] = float(fee_rate_raw)
        except Exception:
            pass
    return out
