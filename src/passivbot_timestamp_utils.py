from __future__ import annotations

import logging

from pure_funcs import flatten
from procedures import get_first_timestamps_unified


async def update_first_timestamps(self, symbols=[]):
    """Fetch and cache first trade timestamps for the provided symbols."""
    if not hasattr(self, "first_timestamps"):
        self.first_timestamps = {}
    symbols = sorted(set(symbols + flatten(self.approved_coins_minus_ignored_coins.values())))
    if all(s in self.first_timestamps for s in symbols):
        return
    first_timestamps = await get_first_timestamps_unified(symbols)
    self.first_timestamps.update(first_timestamps)
    for symbol in sorted(self.first_timestamps):
        symbolf = self.coin_to_symbol(symbol, verbose=False)
        if symbolf not in self.markets_dict:
            continue
        if symbolf not in self.first_timestamps:
            self.first_timestamps[symbolf] = self.first_timestamps[symbol]
    for symbol in symbols:
        if symbol not in self.first_timestamps:
            logging.info(f"warning: unable to get first timestamp for {symbol}. Setting to zero.")
            self.first_timestamps[symbol] = 0.0


def get_first_timestamp(self, symbol):
    """Return the cached first tradable timestamp for `symbol`, populating defaults."""
    if symbol not in self.first_timestamps:
        logging.info(f"warning: {symbol} missing from first_timestamps. Setting to zero.")
        self.first_timestamps[symbol] = 0.0
    return self.first_timestamps[symbol]
