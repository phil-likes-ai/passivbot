from __future__ import annotations

from config_utils import expand_PB_mode


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
            forced_mode = self.get_forced_PB_mode(pside, symbol)
            if forced_mode in ["normal", "graceful_stop"]:
                n_positions += 1
            else:
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
