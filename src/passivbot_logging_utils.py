from __future__ import annotations

import logging

from utils import utc_ms


def log_ema_gating(
    self,
    ideal_orders: dict,
    m1_close_emas: dict,
    last_prices: dict,
    symbols: list,
) -> None:
    """Log when entries are blocked due to EMA distance gating."""
    if not hasattr(self, "_ema_gating_last_log_ms"):
        self._ema_gating_last_log_ms = {}
    ema_gating_throttle_ms = 300_000
    now_ms = utc_ms()

    for symbol in symbols:
        for pside in ("long", "short"):
            mode = self.PB_modes.get(symbol, {}).get(pside)
            if mode != "normal":
                continue

            pos = self.positions.get(symbol, {}).get(pside, {})
            pos_size = abs(pos.get("size", 0.0))
            if pos_size > 0:
                continue

            symbol_orders = ideal_orders.get(symbol, [])
            has_initial_entry = any(
                "entry_initial" in (o[2] if len(o) > 2 else "")
                and pside in (o[2] if len(o) > 2 else "")
                for o in symbol_orders
            )
            if has_initial_entry:
                continue

            try:
                span0 = float(self.bp(pside, "ema_span_0", symbol))
                span1 = float(self.bp(pside, "ema_span_1", symbol))
                ema_dist = float(self.bp(pside, "entry_initial_ema_dist", symbol))

                if span0 <= 0 or span1 <= 0:
                    continue

                span2 = (span0 * span1) ** 0.5
                emas = m1_close_emas.get(symbol, {})
                ema0 = emas.get(span0, 0.0)
                ema1 = emas.get(span1, 0.0)
                ema2 = emas.get(span2, 0.0)

                if ema0 <= 0 or ema1 <= 0 or ema2 <= 0:
                    continue

                ema_lower = min(ema0, ema1, ema2)
                ema_upper = max(ema0, ema1, ema2)
                current_price = last_prices.get(symbol, 0.0)

                if current_price <= 0:
                    continue

                if pside == "long":
                    ema_entry_price = ema_lower * (1.0 - ema_dist)
                    is_gated = current_price > ema_entry_price
                    dist_pct = (
                        (current_price / ema_entry_price - 1.0) * 100 if ema_entry_price > 0 else 0
                    )
                else:
                    ema_entry_price = ema_upper * (1.0 + ema_dist)
                    is_gated = current_price < ema_entry_price
                    dist_pct = (
                        (1.0 - current_price / ema_entry_price) * 100 if ema_entry_price > 0 else 0
                    )

                if is_gated and abs(dist_pct) > 0.1:
                    throttle_key = f"{symbol}:{pside}"
                    last_log_ms = self._ema_gating_last_log_ms.get(throttle_key, 0)
                    if (now_ms - last_log_ms) < ema_gating_throttle_ms:
                        continue
                    self._ema_gating_last_log_ms[throttle_key] = now_ms

                    coin = symbol.split("/")[0] if "/" in symbol else symbol
                    logging.info(
                        "[ema] %s %s entry gated | price=%.4g ema_thresh=%.4g (+%.1f%% away)",
                        coin,
                        pside,
                        current_price,
                        ema_entry_price,
                        dist_pct,
                    )
            except Exception as e:
                logging.debug("failed EMA gating log for %s %s: %s", symbol, pside, e)


def maybe_log_ema_debug(
    self,
    ema_bounds_long: dict,
    ema_bounds_short: dict,
    entry_volatility_logrange_ema_1h: dict,
) -> None:
    """Emit a throttled log of EMA inputs so toggling visibility stays simple."""
    ema_debug_logging_enabled = False
    if not ema_debug_logging_enabled:
        return
    self._ema_debug_log_interval_ms = 30_000
    self._last_ema_debug_log_ms = 0
    now = utc_ms()
    if now - getattr(self, "_last_ema_debug_log_ms", 0) < self._ema_debug_log_interval_ms:
        return
    self._last_ema_debug_log_ms = now

    def _safe_span(pside: str, key: str, symbol: str):
        try:
            val = self.bp(pside, key, symbol)
            return int(val) if val is not None else None
        except Exception:
            return None

    logs: list[str] = []
    for pside, bounds in (("long", ema_bounds_long), ("short", ema_bounds_short)):
        if not bounds:
            continue
        side_entries: list[str] = []
        for symbol, (lower, upper) in sorted(bounds.items()):
            span0 = _safe_span(pside, "ema_span_0", symbol)
            span1 = _safe_span(pside, "ema_span_1", symbol)
            grid_lr = (entry_volatility_logrange_ema_1h or {}).get(pside, {}).get(symbol)
            parts = [f"{symbol}"]
            if span0 is not None or span1 is not None:
                parts.append(
                    f"spans=({span0 if span0 is not None else '?'}"
                    f", {span1 if span1 is not None else '?'})"
                )
            parts.append(f"lower={lower:.6g}")
            parts.append(f"upper={upper:.6g}")
            if grid_lr is not None:
                parts.append(f"log_range_ema={grid_lr:.6g}")
            side_entries.append(" ".join(parts))
        if side_entries:
            logs.append(f"{pside} -> " + "; ".join(side_entries))

    if logs:
        logging.info("EMA debug | " + " | ".join(logs))
