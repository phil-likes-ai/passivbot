from __future__ import annotations

import logging

from utils import coin_to_symbol as util_coin_to_symbol, symbol_to_coin


def set_market_specific_settings(self):
    """Initialise per-symbol market metadata (steps, ids, multipliers)."""
    self.symbol_ids = {symbol: self.markets_dict[symbol]["id"] for symbol in self.markets_dict}
    self.symbol_ids_inv = {v: k for k, v in self.symbol_ids.items()}


def get_symbol_id(self, symbol):
    """Return the exchange-native identifier for `symbol`, caching defaults."""
    try:
        return self.symbol_ids[symbol]
    except Exception:
        logging.debug("symbol %s missing from self.symbol_ids. Using raw symbol.", symbol)
        self.symbol_ids[symbol] = symbol
        return symbol


def to_ccxt_symbol(self, symbol: str) -> str:
    """Convert to ccxt standardized symbol."""
    candidates = []
    try:
        candidates.append(self.get_symbol_id_inv(symbol))
    except Exception:
        pass
    try:
        candidates.append(self.coin_to_symbol(symbol))
    except Exception:
        pass
    if candidates:
        return candidates[0]
    logging.info("failed to convert %s to ccxt symbol. Using %s as is.", symbol, symbol)
    return symbol


def get_symbol_id_inv(self, symbol):
    """Return the human-friendly symbol for an exchange-native identifier."""
    try:
        if symbol in self.symbol_ids_inv:
            return self.symbol_ids_inv[symbol]
        return self.coin_to_symbol(symbol)
    except Exception:
        logging.info("failed to convert %s to ccxt symbol. Using %s as is.", symbol, symbol)
        self.symbol_ids_inv[symbol] = symbol
        return symbol


def coin_to_symbol(self, coin, verbose=True):
    """Map a coin identifier to the exchange-specific trading symbol."""
    if coin == "":
        return ""
    if not hasattr(self, "coin_to_symbol_map"):
        self.coin_to_symbol_map = {}
    if coin in self.coin_to_symbol_map:
        return self.coin_to_symbol_map[coin]
    coinf = symbol_to_coin(coin, verbose=verbose)
    if coinf in self.coin_to_symbol_map:
        self.coin_to_symbol_map[coin] = self.coin_to_symbol_map[coinf]
        return self.coin_to_symbol_map[coinf]
    result = util_coin_to_symbol(coin, self.exchange, quote=self.quote, verbose=verbose)
    self.coin_to_symbol_map[coin] = result
    return result
