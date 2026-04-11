from __future__ import annotations

import logging

from config_utils import expand_PB_mode
from utils import utc_ms


def get_max_n_positions(self, pside):
    """Return the configured maximum number of concurrent positions for a side."""
    max_n_positions = min(
        self.bot_value(pside, "n_positions"),
        len(self.approved_coins_minus_ignored_coins[pside]),
    )
    return max(0, int(round(max_n_positions)))


def get_current_n_positions(self, pside):
    """Count open positions for the side."""
    n_positions = 0
    for symbol in self.positions:
        if self.positions[symbol][pside]["size"] != 0.0:
            n_positions += 1
    return n_positions


def get_forced_PB_mode(self, pside, symbol=None):
    """Return an explicitly forced mode for the side or symbol, if configured."""
    if self._equity_hard_stop_enabled(pside):
        state = self._hsl_state(pside)
        if self._equity_hard_stop_runtime_red_latched(pside) and not state["halted"]:
            return "panic"
        if state["halted"]:
            if symbol is None:
                return "graceful_stop"
            return self._equity_hard_stop_halted_mode(pside, symbol)
    if symbol is not None:
        runtime_forced = getattr(self, "_runtime_forced_modes", {}).get(pside, {}).get(symbol)
        if runtime_forced:
            return str(runtime_forced)
    mode = self.config_get(["live", f"forced_mode_{pside}"], symbol)
    if mode:
        return expand_PB_mode(mode)
    if symbol and not self.markets_dict[symbol]["active"]:
        return "tp_only"
    return None


def is_forager_mode(self, pside=None):
    """Return True when the configuration allows forager grid deployment for the side."""
    if pside is None:
        return self.is_forager_mode("long") or self.is_forager_mode("short")
    if self.bot_value(pside, "total_wallet_exposure_limit") <= 0.0:
        return False
    if self.live_value(f"forced_mode_{pside}"):
        return False
    n_positions = self.get_max_n_positions(pside)
    if n_positions == 0:
        return False
    if n_positions >= len(self.approved_coins_minus_ignored_coins[pside]):
        return False
    return True


def mode_override_to_orchestrator_mode(self, mode):
    del self
    if mode is None:
        return None
    m = str(mode).strip().lower()
    if m == "tp_only_with_active_entry_cancellation":
        return "tp_only"
    if m in {"normal", "panic", "graceful_stop", "tp_only", "manual"}:
        return m
    return "manual"


def python_mode_from_orchestrator_state(self, pside: str, symbol: str, side_state: dict, explicit_override):
    del symbol
    if explicit_override:
        return str(explicit_override)
    if bool(side_state.get("active", False)):
        return "normal"
    return self.PB_mode_stop[pside]


def build_orchestrator_mode_overrides_fallback(self, symbols):
    overrides: dict[str, dict[str, str | None]] = {"long": {}, "short": {}}
    pb_modes = getattr(self, "PB_modes", {})
    for pside in ("long", "short"):
        pside_modes = pb_modes.get(pside, {}) if isinstance(pb_modes, dict) else {}
        for symbol in symbols:
            mode = pside_modes.get(symbol)
            overrides[pside][symbol] = self._mode_override_to_orchestrator_mode(mode) if mode else None
    return overrides


def pside_blocks_new_entries(self, pside: str) -> bool:
    forced_mode = self.get_forced_PB_mode(pside)
    return forced_mode in {
        "panic",
        "graceful_stop",
        "tp_only",
        "tp_only_with_active_entry_cancellation",
        "manual",
    }


def build_live_symbol_universe(self) -> list[str]:
    symbols: set[str] = set()
    symbols |= set(getattr(self, "positions", {}))
    symbols |= set(getattr(self, "open_orders", {}))
    symbols |= set(getattr(self, "coin_overrides", {}))
    for pside in ("long", "short"):
        if self._pside_blocks_new_entries(pside):
            continue
        approved = self.approved_coins_minus_ignored_coins.get(pside, set())
        for symbol in approved:
            if self.is_approved(pside, symbol):
                symbols.add(symbol)
    return sorted(symbols)


def _forager_refresh_budget(self, max_calls_per_minute: int) -> int:
    """Token bucket budget for forager candle refreshes."""
    try:
        max_calls = int(max_calls_per_minute)
    except (TypeError, ValueError):
        max_calls = 0
    if max_calls <= 0:
        return 0
    now = utc_ms()
    state = getattr(self, "_forager_refresh_state", None)
    if not isinstance(state, dict):
        state = {"tokens": float(max_calls), "last_ms": now}
    last_ms = int(state.get("last_ms", now) or now)
    tokens = float(state.get("tokens", max_calls))
    elapsed = max(0.0, (now - last_ms) / 60_000.0)
    tokens = min(float(max_calls), tokens + float(max_calls) * elapsed)
    budget = int(tokens)
    state["tokens"] = float(tokens - budget)
    state["last_ms"] = int(now)
    self._forager_refresh_state = state
    return max(0, budget)


def _split_forager_budget_by_side(self, total_budget: int, sides) -> dict[str, int]:
    """Split a cycle budget fairly across sides with round-robin remainder."""
    side_list = [s for s in sides if s in ("long", "short")]
    out = {s: 0 for s in side_list}
    try:
        total = int(total_budget)
    except (TypeError, ValueError):
        total = 0
    if total <= 0 or not side_list:
        return out
    n = len(side_list)
    base = total // n
    rem = total % n
    for s in side_list:
        out[s] = base
    start = int(getattr(self, "_forager_budget_rr", 0) or 0) % n
    for i in range(rem):
        out[side_list[(start + i) % n]] += 1
    self._forager_budget_rr = (start + 1) % n
    return out


def _forager_target_staleness_ms(self, n_symbols: int, max_calls_per_minute: int) -> int:
    """Compute max acceptable staleness for forager candidates based on refresh budget."""
    try:
        n_syms = int(n_symbols)
    except (TypeError, ValueError):
        n_syms = 0
    try:
        max_calls = int(max_calls_per_minute)
    except (TypeError, ValueError):
        max_calls = 0
    if n_syms <= 0 or max_calls <= 0:
        return int(getattr(self, "inactive_coin_candle_ttl_ms", 600_000))
    minutes = max(1.0, float(n_syms) / float(max_calls))
    return int(minutes * 60_000)


def _maybe_log_candle_refresh(
    self,
    context: str,
    symbols,
    *,
    target_age_ms: int | None = None,
    refreshed: int | None = None,
    throttle_ms: int = 60_000,
) -> None:
    """Log a throttled summary of candle staleness for the given symbols."""
    try:
        now = utc_ms()
        boot_delay_ms = int(getattr(self, "candle_refresh_log_boot_delay_ms", 300_000) or 0)
        boot_elapsed = int(now - getattr(self, "start_time_ms", now))
        if boot_elapsed < boot_delay_ms:
            return
        last = int(getattr(self, "_candle_refresh_log_last_ms", 0) or 0)
        if (now - last) < int(throttle_ms):
            return
        sym_list = list(symbols)
        if not sym_list:
            return
        ages = []
        for sym in sym_list:
            try:
                last_final = self.cm.get_last_final_ts(sym)
            except Exception:
                last_final = 0
            if last_final:
                ages.append(max(0, now - int(last_final)))
        if not ages:
            return
        ages.sort()
        median_ms = ages[len(ages) // 2]
        max_ms = ages[-1]
        target_s = f"{int(target_age_ms/1000)}s" if target_age_ms else "n/a"
        refreshed_str = f", refreshed={refreshed}" if refreshed is not None else ""
        logging.debug(
            "[candle] %s symbols=%d%s max_stale=%ds median_stale=%ds target=%s",
            context,
            len(sym_list),
            refreshed_str,
            int(max_ms / 1000),
            int(median_ms / 1000),
            target_s,
        )
        self._candle_refresh_log_last_ms = int(now)
    except Exception:
        logging.debug(
            "[candle] failed to emit candle refresh summary",
            exc_info=True,
        )
        return
