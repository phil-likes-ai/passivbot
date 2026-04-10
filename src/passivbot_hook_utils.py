from __future__ import annotations


def filter_approved_symbols(self, pside: str, symbols: set[str]) -> set[str]:
    """Hook: exchange-specific filtering for approved symbols used for new entries."""
    del self, pside
    return symbols


def assert_supported_live_state(self) -> None:
    """Hook: exchange-specific startup/runtime validation for unsupported live state."""
    del self
    return None


def symbol_is_eligible(self, symbol):
    """Return True when the symbol passes exchange-specific eligibility rules."""
    del self, symbol
    return True
