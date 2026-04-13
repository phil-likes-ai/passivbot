from __future__ import annotations

import logging
from importlib import import_module

import numpy as np

from utils import symbol_to_coin, ts_to_date, utc_ms


def _round_dynamic(value: float, digits: int) -> float:
    try:
        return float(import_module("passivbot_rust").round_dynamic(value, digits))
    except Exception:
        return round(float(value), digits)


def log_fill_event(self, event) -> str:
    coin = symbol_to_coin(event.symbol, verbose=False) or event.symbol
    pside = event.position_side.lower()
    order_type = event.pb_order_type.lower() if event.pb_order_type else "fill"

    qty_sign = "+" if event.side.lower() == "buy" else "-"
    qty_str = f"{qty_sign}{abs(event.qty):.6g}"

    fill_ts = ""
    if getattr(event, "timestamp", 0):
        fill_ts = ts_to_date(event.timestamp)[:19]
    elif getattr(event, "datetime", ""):
        fill_ts = str(event.datetime)[:19]

    if fill_ts:
        msg = f"[fill] {fill_ts} {coin} {pside} {order_type} {qty_str} @ {event.price:.2f}"
    else:
        msg = f"[fill] {coin} {pside} {order_type} {qty_str} @ {event.price:.2f}"

    is_close = "close" in order_type
    if is_close or event.pnl != 0.0:
        pnl_sign = "+" if event.pnl >= 0 else ""
        rounded_pnl = _round_dynamic(event.pnl, 3)
        msg += f", pnl={pnl_sign}{rounded_pnl} USDT"

    if order_type == "unknown" and event.client_order_id:
        msg += f" (coid={event.client_order_id})"

    fill_id = getattr(event, "id", None)
    if fill_id:
        short_id = str(fill_id)[:12] if len(str(fill_id)) > 12 else str(fill_id)
        msg += f" id={short_id}"

    return msg


def log_new_fill_events(self, new_events: list) -> None:
    if not new_events:
        return

    self._health_fills += len(new_events)
    self._health_pnl += sum(ev.pnl for ev in new_events)

    if len(new_events) > 20:
        total_pnl = sum(ev.pnl for ev in new_events)
        pnl_sign = "+" if total_pnl >= 0 else ""
        logging.info(
            "[fill] %d fills, pnl=%s%s USDT",
            len(new_events),
            pnl_sign,
            _round_dynamic(total_pnl, 3),
        )
    else:
        for event in sorted(new_events, key=lambda e: e.timestamp):
            logging.info(self._log_fill_event(event))

    for event in sorted(new_events, key=lambda e: e.timestamp):
        self._monitor_record_fill_history(event)
        self._monitor_record_event(
            "order.filled",
            ("order", "fill"),
            self._monitor_fill_payload(event),
            symbol=getattr(event, "symbol", None),
            pside=str(getattr(event, "position_side", "") or "").lower() or None,
            ts=int(getattr(event, "timestamp", 0) or 0) or None,
        )


def get_realized_pnl_cumsum_stats(self) -> dict[str, float]:
    if self._pnls_manager is None:
        raise RuntimeError("FillEventsManager unavailable for realized pnl cumsum stats")
    events = self._get_effective_pnl_events()
    if not events:
        return {"max": 0.0, "last": 0.0}
    pnls_cumsum = np.array([float(ev.pnl) for ev in events], dtype=float).cumsum()
    return {"max": float(pnls_cumsum.max()), "last": float(pnls_cumsum[-1])}


def log_realized_loss_gate_blocks(self, out: dict, idx_to_symbol: dict[int, str]) -> None:
    diagnostics = out.get("diagnostics", {}) if isinstance(out, dict) else {}
    blocks = diagnostics.get("loss_gate_blocks", [])
    if not isinstance(blocks, list) or not blocks:
        return
    now_ms = utc_ms()
    for block in blocks:
        if not isinstance(block, dict):
            continue
        symbol = idx_to_symbol.get(int(block.get("symbol_idx", -1)), "unknown")
        pside = str(block.get("pside", "unknown"))
        order_type = str(block.get("order_type", "unknown"))
        throttle_key = f"{symbol}:{pside}:{order_type}"
        last_log_ms = self._loss_gate_last_log_ms.get(throttle_key, 0)
        if (now_ms - last_log_ms) < self._loss_gate_log_interval_ms:
            continue
        self._loss_gate_last_log_ms[throttle_key] = now_ms
        qty = float(block.get("qty", 0.0) or 0.0)
        price = float(block.get("price", 0.0) or 0.0)
        projected_pnl = float(block.get("projected_pnl", 0.0) or 0.0)
        projected_balance = float(block.get("projected_balance_after", 0.0) or 0.0)
        balance_floor = float(block.get("balance_floor", 0.0) or 0.0)
        max_loss_pct = float(block.get("max_realized_loss_pct", 1.0) or 1.0)
        logging.warning(
            "[risk] order blocked by realized-loss gate | %s %s %s qty=%.10g price=%.10g "
            "projected_pnl=%.6f projected_balance=%.6f floor=%.6f max_realized_loss_pct=%.6f | "
            "adjust live.max_realized_loss_pct to change behavior",
            symbol,
            pside,
            order_type,
            qty,
            price,
            projected_pnl,
            projected_balance,
            balance_floor,
            max_loss_pct,
        )
