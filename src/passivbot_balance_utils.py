from __future__ import annotations

import math
import logging
import time
from importlib import import_module


def _get_pbr():
    return import_module("passivbot_rust")


def _get_valid_balance_attr(self, attr_name: str) -> float:
    if not hasattr(self, attr_name):
        raise AttributeError(f"missing required {attr_name}")
    value = getattr(self, attr_name)
    try:
        value = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid {attr_name}: {value!r}") from exc
    if not math.isfinite(value):
        raise ValueError(f"invalid {attr_name}: {value!r}")
    return value


def get_hysteresis_snapped_balance(self) -> float:
    """Return hysteresis-snapped balance used for sizing."""
    return _get_valid_balance_attr(self, "balance")


def get_raw_balance(self) -> float:
    """Return raw wallet balance (fallback to snapped for legacy test stubs)."""
    if hasattr(self, "balance_raw"):
        return _get_valid_balance_attr(self, "balance_raw")
    return self.get_hysteresis_snapped_balance()


def calc_effective_min_cost_at_price(self, symbol: str, price: float) -> float:
    """Return executable min order cost at the given price for filter/gating logic."""
    pbr = _get_pbr()
    qty_step = float(self.qty_steps[symbol])
    min_qty = float(self.min_qtys[symbol])
    min_cost = float(self.min_costs[symbol])
    c_mult = float(self.c_mults[symbol])
    if min_qty <= 0.0 and qty_step > 0.0:
        min_qty = qty_step
    calc_min_entry_qty = getattr(pbr, "calc_min_entry_qty_py", None)
    if calc_min_entry_qty is not None:
        min_entry_qty = float(calc_min_entry_qty(price, c_mult, qty_step, min_qty, min_cost))
    else:
        if price <= 0.0 or c_mult <= 0.0:
            min_entry_qty = min_qty
        else:
            min_cost_qty = min_cost / price / c_mult
            if qty_step > 0.0:
                min_cost_qty = math.ceil(max(0.0, min_cost_qty) / qty_step) * qty_step
            min_entry_qty = max(min_qty, min_cost_qty)
    return float(pbr.qty_to_cost(min_entry_qty, price, c_mult))


async def handle_balance_update(self, source="REST"):
    if not hasattr(self, "_previous_balance_raw"):
        self._previous_balance_raw = 0.0
    if not hasattr(self, "_previous_balance_snapped"):
        self._previous_balance_snapped = 0.0
    if not hasattr(self, "_last_raw_only_log_time"):
        self._last_raw_only_log_time = 0.0
    balance_raw = self.get_raw_balance()
    balance_snapped = self.get_hysteresis_snapped_balance()
    if balance_raw != self._previous_balance_raw or balance_snapped != self._previous_balance_snapped:
        snap_changed = balance_snapped != self._previous_balance_snapped
        raw_only = not snap_changed
        now = time.time()
        should_log = snap_changed or (now - self._last_raw_only_log_time >= 900.0)
        try:
            equity = balance_raw + (await self.calc_upnl_sum())
            self._monitor_last_equity = float(equity)
            if should_log:
                logging.info(
                    "[balance] raw %.6f -> %.6f | snap %.6f -> %.6f | equity: %.4f source: %s",
                    self._previous_balance_raw,
                    balance_raw,
                    self._previous_balance_snapped,
                    balance_snapped,
                    equity,
                    source,
                )
                if raw_only:
                    self._last_raw_only_log_time = now
            self._monitor_record_event(
                "account.balance",
                ("account", "balance"),
                {
                    "previous_balance_raw": float(self._previous_balance_raw),
                    "balance_raw": float(balance_raw),
                    "previous_balance_snapped": float(self._previous_balance_snapped),
                    "balance_snapped": float(balance_snapped),
                    "equity": float(equity),
                    "source": str(source),
                },
            )
        except Exception:
            logging.exception("[balance] handle_balance_update failed source=%s", source)
        finally:
            self._previous_balance_raw = balance_raw
            self._previous_balance_snapped = balance_snapped
            self.execution_scheduled = True


async def calc_upnl_sum(self):
    """Compute unrealised PnL across fetched positions using latest prices."""
    upnl_sum = 0.0
    last_prices = await self.cm.get_last_prices(
        {x["symbol"] for x in self.fetched_positions}, max_age_ms=60_000
    )
    for elm in self.fetched_positions:
        try:
            upnl = self.calc_pnl(
                elm["position_side"],
                elm["price"],
                last_prices[elm["symbol"]],
                elm["size"],
                self.inverse,
                self.c_mults[elm["symbol"]],
            )
            if upnl:
                upnl_sum += upnl
        except Exception:
            logging.exception(
                "[balance] calc_upnl_sum failed symbol=%s", elm.get("symbol", "<unknown>")
            )
            return 0.0
    return upnl_sum


async def initialize_balance(self):
    balance_raw = await self.fetch_balance()
    if balance_raw is None:
        raise RuntimeError("fetch_balance returned None")
    balance_raw = float(balance_raw)
    self.balance_raw = balance_raw
    self.balance = balance_raw
    self.previous_hysteresis_balance = balance_raw
    return balance_raw


async def update_effective_min_cost(self, symbol=None):
    """Update the effective minimum order cost for one or all symbols."""
    if not hasattr(self, "effective_min_cost"):
        self.effective_min_cost = {}
    if symbol is None:
        symbols = sorted(self.get_symbols_approved_or_has_pos())
    else:
        symbols = [symbol]
    last_prices = await self.cm.get_last_prices(symbols, max_age_ms=600_000)
    for symbol in symbols:
        try:
            self.effective_min_cost[symbol] = self._calc_effective_min_cost_at_price(
                symbol, float(last_prices[symbol])
            )
        except Exception:
            logging.exception("[balance] update_effective_min_cost failed symbol=%s", symbol)
