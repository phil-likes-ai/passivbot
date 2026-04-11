from __future__ import annotations

import asyncio
import logging
import random

from ccxt.base.errors import RateLimitExceeded

from passivbot_exceptions import RestartBotException
from utils import utc_ms


def is_rate_limit_like_exception(self, exc: Exception) -> bool:
    if isinstance(exc, RateLimitExceeded):
        return True
    msg = str(exc).lower()
    return any(token in msg for token in ("rate limit", "too many", "429", "10006"))


def exchange_config_backoff_seconds(self, attempt: int) -> float:
    base = 2.0
    if getattr(self, "exchange", "") in {"bybit", "hyperliquid"}:
        base = 5.0
    return min(base * (2 ** max(int(attempt) - 1, 0)), 60.0) + random.uniform(0.0, 0.5)


def exchange_config_success_pause_seconds(self) -> float:
    if getattr(self, "exchange", "") in {"bybit", "hyperliquid", "okx", "kucoin", "bitget"}:
        return 0.2
    return 0.05


async def update_single_symbol_exchange_config(self, symbol: str) -> bool:
    """
    Update exchange config for a single symbol.
    Returns True if the loop should continue, False if it should break (e.g. rate limit).
    """
    retry_after_ms = int(self._exchange_config_retry_after_ms.get(symbol, 0) or 0)
    if retry_after_ms > utc_ms():
        return True
    try:
        await self.update_exchange_config_by_symbols([symbol])
        self.already_updated_exchange_config_symbols.add(symbol)
        self._exchange_config_retry_attempts.pop(symbol, None)
        self._exchange_config_retry_after_ms.pop(symbol, None)
    except RestartBotException:
        raise
    except Exception as e:
        attempts = int(self._exchange_config_retry_attempts.get(symbol, 0) or 0) + 1
        self._exchange_config_retry_attempts[symbol] = attempts
        backoff_s = self._exchange_config_backoff_seconds(attempts)
        self._exchange_config_retry_after_ms[symbol] = utc_ms() + int(backoff_s * 1000.0)
        if self._is_rate_limit_like_exception(e):
            self._health_rate_limits += 1
            logging.warning(
                "[rate] exchange config update hit rate limit for %s; retrying in %.1fs",
                symbol,
                backoff_s,
            )
            logging.debug(
                "[rate] exchange config update rate-limit details for %s",
                symbol,
                exc_info=True,
            )
            return False
        logging.warning(
            "[config] exchange config update failed for %s; retrying in %.1fs: %s",
            symbol,
            backoff_s,
            e,
            exc_info=True,
        )
        return True
    else:
        pause_s = self._exchange_config_success_pause_seconds()
        if pause_s > 0.0:
            await asyncio.sleep(pause_s)
        return True


async def update_exchange_configs(self):
    """Ensure exchange-specific settings are initialised for all active symbols."""
    if not hasattr(self, "already_updated_exchange_config_symbols"):
        self.already_updated_exchange_config_symbols = set()
    if not hasattr(self, "_exchange_config_retry_attempts"):
        self._exchange_config_retry_attempts = {}
    if not hasattr(self, "_exchange_config_retry_after_ms"):
        self._exchange_config_retry_after_ms = {}
    symbols_not_done = [
        x for x in self.active_symbols if x not in self.already_updated_exchange_config_symbols
    ]
    if symbols_not_done:
        for symbol in symbols_not_done:
            if not await self._update_single_symbol_exchange_config(symbol):
                break
