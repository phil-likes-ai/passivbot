from __future__ import annotations


def has_position(self, pside=None, symbol=None):
    """Return True if the bot currently holds a position for the given side and symbol."""
    if pside is None:
        return self.has_position("long", symbol) or self.has_position("short", symbol)
    if symbol is None:
        return any(self.has_position(pside, s) for s in self.positions)
    return symbol in self.positions and self.positions[symbol][pside]["size"] != 0.0


def get_symbols_with_pos(self, pside=None):
    """Return the set of symbols with open positions for the given side."""
    if pside is None:
        return self.get_symbols_with_pos("long") | self.get_symbols_with_pos("short")
    return {symbol for symbol in self.positions if self.positions[symbol][pside]["size"] != 0.0}


def is_trailing(self, symbol, pside=None):
    """Return True when trailing logic is active for the given symbol and side."""
    if pside is None:
        return self.is_trailing(symbol, "long") or self.is_trailing(symbol, "short")
    return (
        self.bp(pside, "entry_trailing_grid_ratio", symbol) != 0.0
        or self.bp(pside, "close_trailing_grid_ratio", symbol) != 0.0
    )
