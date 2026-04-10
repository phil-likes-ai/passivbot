from __future__ import annotations

import logging
from importlib import import_module

import numpy as np

from utils import utc_ms


def calc_unstuck_allowance_for_logging(self, pside: str) -> dict:
    """Calculate raw unstuck allowance values for logging (including negative)."""
    twel = float(self.bot_value(pside, "total_wallet_exposure_limit") or 0.0)
    if twel <= 0.0:
        return {"status": "disabled"}

    pct = float(self.bot_value(pside, "unstuck_loss_allowance_pct") or 0.0)
    if pct <= 0.0:
        return {"status": "unstuck_disabled"}

    if self._pnls_manager is None:
        return {"status": "no_pnl_manager"}

    events = self._get_effective_pnl_events()
    if not events:
        return {"status": "no_history"}

    pnls_cumsum = np.array([ev.pnl for ev in events]).cumsum()
    pnls_cumsum_max, pnls_cumsum_last = float(pnls_cumsum.max()), float(pnls_cumsum[-1])

    balance_raw = self.get_raw_balance()
    balance_peak = balance_raw + (pnls_cumsum_max - pnls_cumsum_last)
    pct_from_peak = (balance_raw / balance_peak - 1.0) * 100.0
    allowance_raw = balance_peak * (pct * twel + pct_from_peak / 100.0)

    return {
        "status": "ok",
        "allowance": allowance_raw,
        "peak": balance_peak,
        "pct_from_peak": pct_from_peak,
    }


def log_unstuck_status(self) -> None:
    """Log unstuck allowance budget for both sides."""
    parts = []
    for pside in ["long", "short"]:
        info = self._calc_unstuck_allowance_for_logging(pside)
        status = info.get("status")
        if status == "disabled":
            parts.append(f"{pside}: disabled")
        elif status == "unstuck_disabled":
            parts.append(f"{pside}: unstuck disabled")
        elif status == "no_pnl_manager" or status == "no_history":
            parts.append(f"{pside}: no pnl history")
        else:
            allowance = info["allowance"]
            if allowance < 0:
                parts.append(
                    "%s: allowance=%.2f (over budget) | peak=%.2f | pct_from_peak=%.1f%%"
                    % (pside, allowance, info["peak"], info["pct_from_peak"])
                )
            else:
                parts.append(
                    "%s: allowance=%.2f | peak=%.2f | pct_from_peak=%.1f%%"
                    % (pside, allowance, info["peak"], info["pct_from_peak"])
                )
    logging.info("[unstuck] %s", " | ".join(parts))


def maybe_log_unstuck_status(self) -> None:
    """Log periodic unstuck status if interval has elapsed."""
    now_ms = utc_ms()
    if (now_ms - self._unstuck_last_log_ms) < self._unstuck_log_interval_ms:
        return
    self._unstuck_last_log_ms = now_ms
    self._log_unstuck_status()


def calc_unstuck_allowances_live(self, allow_new_unstuck: bool) -> dict[str, float]:
    """Calculate unstuck allowances using FillEventsManager."""
    return self._calc_unstuck_allowances(allow_new_unstuck)


def calc_unstuck_allowances(self, allow_new_unstuck: bool) -> dict[str, float]:
    """Calculate unstuck allowances using FillEventsManager data."""
    pbr = import_module("passivbot_rust")
    if not allow_new_unstuck or self._pnls_manager is None:
        return {"long": 0.0, "short": 0.0}

    events = self._get_effective_pnl_events()
    if not events:
        return {"long": 0.0, "short": 0.0}

    pnls_cumsum = np.array([float(ev.pnl) for ev in events], dtype=float).cumsum()
    pnls_cumsum_max, pnls_cumsum_last = pnls_cumsum.max(), pnls_cumsum[-1]
    out = {}
    balance_raw = self.get_raw_balance()
    for pside in ["long", "short"]:
        pct = float(self.bot_value(pside, "unstuck_loss_allowance_pct") or 0.0)
        if pct > 0.0:
            out[pside] = float(
                pbr.calc_auto_unstuck_allowance(
                    balance_raw,
                    pct * float(self.bot_value(pside, "total_wallet_exposure_limit") or 0.0),
                    float(pnls_cumsum_max),
                    float(pnls_cumsum_last),
                )
            )
        else:
            out[pside] = 0.0
    return out
