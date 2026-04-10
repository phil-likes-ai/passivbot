from __future__ import annotations

import logging

import numpy as np


def normalize_tickers(tickers: dict) -> dict:
    """Fill missing bid/ask/last values using the available fields."""
    for symbol in tickers:
        if tickers[symbol]["last"] is None:
            if tickers[symbol]["bid"] is not None and tickers[symbol]["ask"] is not None:
                tickers[symbol]["last"] = np.mean([tickers[symbol]["bid"], tickers[symbol]["ask"]])
        else:
            for oside in ["bid", "ask"]:
                if tickers[symbol][oside] is None and tickers[symbol]["last"] is not None:
                    tickers[symbol][oside] = tickers[symbol]["last"]
    return tickers


async def update_tickers(self):
    """Fetch latest ticker data and fill in missing bid/ask/last values."""
    if not hasattr(self, "tickers"):
        self.tickers = {}
    try:
        tickers = await self.cca.fetch_tickers()
        self.tickers = normalize_tickers(tickers)
    except Exception as e:
        logging.error("Error with update_tickers %s", e)
