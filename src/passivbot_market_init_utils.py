from __future__ import annotations

import asyncio
import logging

from ccxt.base import errors as ccxt_errors


NetworkError = ccxt_errors.NetworkError
RequestTimeout = getattr(ccxt_errors, "RequestTimeout", NetworkError)


async def ensure_exchange_config_ready_for_market_init(self) -> None:
    """Retry exchange config initialization on transient network errors."""
    for attempt in range(1, 4):
        try:
            await self.update_exchange_config()
            return
        except (RequestTimeout, NetworkError) as e:
            if attempt == 3:
                raise
            logging.warning(
                "[init_markets] update_exchange_config error (attempt %d/3): %s - retrying in %ds",
                attempt,
                e,
                5 * attempt,
            )
            await asyncio.sleep(5 * attempt)


async def apply_post_market_load_setup(self) -> None:
    """Apply follow-up initialization after markets are loaded."""
    self.init_coin_overrides()
    self.refresh_approved_ignored_coins_lists()
    self.set_wallet_exposure_limits()
    await self.update_positions_and_balance()
    await self.update_open_orders()
    self._assert_supported_live_state()
    await self.update_effective_min_cost()
    if self.is_forager_mode():
        await self.update_first_timestamps()


def apply_loaded_markets(self, markets_dict: dict, eligible, reasons) -> None:
    """Apply loaded market metadata and derived symbol lists to bot state."""
    self.markets_dict = markets_dict
    self.eligible_symbols = set(eligible)
    self.ineligible_symbols = reasons
    self.set_market_specific_settings()
    self.max_len_symbol = max(len(s) for s in self.markets_dict)
    self.sym_padding = max(self.sym_padding, self.max_len_symbol + 1)
