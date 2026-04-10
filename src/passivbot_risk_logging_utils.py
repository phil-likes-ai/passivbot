from __future__ import annotations

import logging

import numpy as np

from utils import utc_ms


def get_realized_pnl_cumsum_stats(self) -> dict[str, float]:
    """Return gross realized pnl cumsum peak/current from FillEventsManager history."""
    if self._pnls_manager is None:
        return {"max": 0.0, "last": 0.0}
    events = self._get_effective_pnl_events()
    if not events:
        return {"max": 0.0, "last": 0.0}
    pnls_cumsum = np.array([float(ev.pnl) for ev in events], dtype=float).cumsum()
    return {"max": float(pnls_cumsum.max()), "last": float(pnls_cumsum[-1])}


def log_realized_loss_gate_blocks(self, out: dict, idx_to_symbol: dict[int, str]) -> None:
    """Emit visible warnings for close orders blocked by realized-loss gate."""
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
            "[risk] close blocked by realized-loss gate | %s %s %s qty=%.6f price=%.6f "
            "projected_pnl=%.6f projected_balance=%.6f floor=%.6f max_loss_pct=%.2f%%",
            symbol,
            pside,
            order_type,
            qty,
            price,
            projected_pnl,
            projected_balance,
            balance_floor,
            max_loss_pct * 100.0,
        )
