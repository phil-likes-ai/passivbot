from __future__ import annotations


def get_wallet_exposure_limit(self, pside, symbol=None):
    """Return side WEL from fixed config denominator, honoring per-symbol overrides."""
    if symbol:
        fwel = (
            self.coin_overrides.get(symbol, {}).get("bot", {}).get(pside, {}).get("wallet_exposure_limit")
        )
        if fwel is not None:
            return fwel
    twel = self.bot_value(pside, "total_wallet_exposure_limit")
    if twel <= 0.0:
        return 0.0
    n_positions = int(round(self.bot_value(pside, "n_positions")))
    if n_positions <= 0:
        return 0.0
    return round(twel / n_positions, 8)


def set_wallet_exposure_limits(self):
    """Recalculate wallet exposure limits for both sides and per-symbol overrides."""
    for pside in ["long", "short"]:
        self.config["bot"][pside]["wallet_exposure_limit"] = self.get_wallet_exposure_limit(pside)
        for symbol in self.coin_overrides:
            ov_conf = self.coin_overrides[symbol].get("bot", {}).get(pside, {})
            if "wallet_exposure_limit" in ov_conf:
                self.coin_overrides[symbol]["bot"][pside]["wallet_exposure_limit"] = (
                    self.get_wallet_exposure_limit(pside, symbol)
                )


def is_pside_enabled(self, pside):
    """Return True if trading is enabled for the given side in the current config."""
    return self.bot_value(pside, "total_wallet_exposure_limit") > 0.0 and self.bot_value(
        pside, "n_positions"
    ) > 0.0


def effective_min_cost_is_low_enough(self, pside, symbol):
    """Check whether the symbol meets the effective minimum cost requirement."""
    if not self.live_value("filter_by_min_effective_cost"):
        return True
    base_limit = self.get_wallet_exposure_limit(pside, symbol)
    allowance_pct = float(self.bp(pside, "risk_we_excess_allowance_pct", symbol))
    allowance_multiplier = 1.0 + max(0.0, allowance_pct)
    effective_limit = base_limit * allowance_multiplier
    return (
        self.get_hysteresis_snapped_balance()
        * effective_limit
        * self.bp(pside, "entry_initial_qty_pct", symbol)
        >= self.effective_min_cost.get(symbol, float("inf"))
    )
