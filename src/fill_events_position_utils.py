from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import logging


logger = logging.getLogger("fill_events_manager")


def ensure_qty_signage(events: List[Dict[str, object]]) -> None:
    """Normalize qty sign convention: buys positive, sells negative."""
    for ev in events:
        side = str(ev.get("side") or "").lower()
        qty = float(ev.get("qty") or ev.get("amount") or 0.0)
        if qty == 0.0:
            continue
        if side == "buy" and qty < 0:
            ev["qty"] = abs(qty)
        elif side == "sell" and qty > 0:
            ev["qty"] = -abs(qty)


def compute_add_reduce(pos_side: str, qty_signed: float) -> Tuple[float, float]:
    """Compute add/reduce amounts based on position side and signed qty."""
    if pos_side == "short":
        add_amt = max(-qty_signed, 0.0)
        reduce_amt = max(qty_signed, 0.0)
    else:
        add_amt = max(qty_signed, 0.0)
        reduce_amt = max(-qty_signed, 0.0)
    return add_amt, reduce_amt


def compute_psize_pprice(
    events: List[Dict[str, object]],
    initial_state: Optional[Dict[Tuple[str, str], Tuple[float, float]]] = None,
) -> Dict[Tuple[str, str], Tuple[float, float]]:
    """Compute psize/pprice for each fill event using two-phase algorithm."""
    if not events:
        return {}

    grouped: Dict[Tuple[str, str], List[Dict[str, object]]] = defaultdict(list)
    for ev in events:
        key = (
            str(ev.get("symbol") or ""),
            str(ev.get("position_side") or ev.get("pside") or "long").lower(),
        )
        grouped[key].append(ev)

    final_state: Dict[Tuple[str, str], Tuple[float, float]] = {}

    for key, evs in grouped.items():
        evs.sort(key=lambda x: x.get("timestamp", 0))

        psize = initial_state.get(key, (0.0, 0.0))[0] if initial_state else 0.0
        pprice = initial_state.get(key, (0.0, 0.0))[1] if initial_state else 0.0
        states: List[Tuple[float, float, float, float]] = []

        for ev in evs:
            qty_signed = float(ev.get("qty") or ev.get("amount") or 0.0) * float(
                ev.get("c_mult", 1.0) or 1.0
            )
            price = float(ev.get("price") or 0.0)
            add_amt, reduce_amt = compute_add_reduce(key[1], qty_signed)

            before_psize = psize
            before_pprice = pprice

            if add_amt > 0:
                if psize <= 0:
                    pprice = price
                else:
                    pprice = ((psize * pprice) + (add_amt * price)) / (psize + add_amt)
                psize += add_amt
            if reduce_amt > 0:
                psize = max(0.0, psize - reduce_amt)
                if psize <= 1e-12:
                    psize = 0.0
                    pprice = 0.0

            states.append((before_psize, before_pprice, psize, pprice))

        final_state[key] = (psize, pprice)

        for ev, (_, _, after_psize, after_pprice) in zip(evs, states):
            ev["psize"] = round(after_psize, 12)
            ev["pprice"] = after_pprice

    return final_state


def annotate_positions_inplace(
    events: List[Dict[str, object]],
    state: Optional[Dict[Tuple[str, str], Tuple[float, float]]] = None,
    *,
    recompute_pnl: bool = False,
) -> Dict[Tuple[str, str], Tuple[float, float]]:
    """Legacy wrapper around compute_psize_pprice for backward compatibility."""
    if recompute_pnl:
        logger.warning("annotate_positions_inplace: recompute_pnl=True is deprecated and ignored")
    return compute_psize_pprice(events, state)


def compute_realized_pnls_from_trades(
    trades: List[Dict[str, object]],
) -> Tuple[Dict[str, float], Dict[Tuple[str, str], Tuple[float, float]]]:
    """Compute realized PnL per trade by reconstructing positions from fills."""
    per_trade: Dict[str, float] = {}
    positions: Dict[Tuple[str, str], Tuple[float, float]] = {}

    for trade in sorted(trades, key=lambda x: x.get("timestamp", 0)):
        trade_id = str(trade.get("id") or "")
        if not trade_id:
            continue
        symbol = str(trade.get("symbol") or "")
        side = str(trade.get("side") or "").lower()
        pos_side = str(trade.get("position_side") or trade.get("pside") or "long").lower()
        qty = abs(float(trade.get("qty") or trade.get("amount") or 0.0))
        price = float(trade.get("price") or 0.0)
        if qty <= 0 or price <= 0 or not symbol:
            per_trade[trade_id] = 0.0
            continue

        key = (symbol, pos_side)
        pos_size, vwap = positions.get(key, (0.0, 0.0))

        adds = side == "sell" if pos_side == "short" else side == "buy"

        realized = 0.0
        if not adds:
            if pos_size > 0:
                closing_qty = min(pos_size, qty)
                if pos_side == "short":
                    realized += (vwap - price) * closing_qty
                else:
                    realized += (price - vwap) * closing_qty
                pos_size -= closing_qty
                if pos_size < 1e-12:
                    pos_size = 0.0
                    vwap = 0.0
                leftover = qty - closing_qty
                if leftover > 1e-12:
                    pos_size = leftover
                    vwap = price
        else:
            if pos_size <= 0:
                pos_size = qty
                vwap = price
            else:
                vwap = ((pos_size * vwap) + (qty * price)) / (pos_size + qty)
                pos_size += qty

        positions[key] = (pos_size, vwap)
        per_trade[trade_id] = realized

    return per_trade, positions
