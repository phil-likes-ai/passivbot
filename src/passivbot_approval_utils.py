from __future__ import annotations

from utils import utc_ms


def is_approved(self, pside, symbol) -> bool:
    """Return True when a symbol is approved, not ignored, and old enough for trading."""
    if symbol not in self.approved_coins_minus_ignored_coins[pside]:
        return False
    if symbol in self.ignored_coins[pside]:
        return False
    if not self.is_old_enough(pside, symbol):
        return False
    return True


def is_old_enough(self, pside, symbol):
    """Return True if the market age exceeds the configured minimum for forager mode."""
    if self.is_forager_mode(pside) and self.minimum_market_age_millis > 0:
        first_timestamp = self.get_first_timestamp(symbol)
        if first_timestamp:
            return utc_ms() - first_timestamp > self.minimum_market_age_millis
        return False
    return True
