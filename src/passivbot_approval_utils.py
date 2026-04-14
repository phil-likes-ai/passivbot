from __future__ import annotations

import logging
import traceback

from utils import _coins_source_side_is_all, normalize_coins_source, symbol_to_coin, utc_ms


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


def get_symbols_approved_or_has_pos(self, pside=None) -> set:
    """Return symbols that are approved for trading or currently have a position."""
    if pside is None:
        return self.get_symbols_approved_or_has_pos("long") | self.get_symbols_approved_or_has_pos(
            "short"
        )
    return (
        self.approved_coins_minus_ignored_coins[pside]
        | self.get_symbols_with_pos(pside)
        | {symbol for symbol in self.coin_overrides if self.get_forced_PB_mode(pside, symbol) == "normal"}
    )


def add_to_coins_lists(self, content, k_coins, log_psides=None):
    """Update approved/ignored coin sets from configuration content."""
    if log_psides is None:
        log_psides = set(content.keys())
    symbols = None
    result = {"added": {}, "removed": {}}
    psides_equal = content["long"] == content["short"]
    for pside in content:
        if not psides_equal or symbols is None:
            coins = content[pside]
            if k_coins == "approved_coins" and _coins_source_side_is_all(coins):
                symbols = set(getattr(self, "eligible_symbols", set()))
            else:
                if isinstance(coins, str):
                    coins = coins.split(",")
                elif isinstance(coins, (list, tuple)):
                    expanded_coins = []
                    for item in coins:
                        if isinstance(item, str) and "," in item:
                            expanded_coins.extend(item.split(","))
                        else:
                            expanded_coins.append(item)
                    coins = expanded_coins

                symbols = [self.coin_to_symbol(coin, verbose=False) for coin in coins if coin]
                symbols = {symbol for symbol in symbols if symbol}
                eligible = getattr(self, "eligible_symbols", None)
                if eligible:
                    skipped = [symbol for symbol in symbols if symbol not in eligible]
                    if skipped:
                        coin_list = ", ".join(
                            sorted(symbol_to_coin(symbol, verbose=False) or symbol for symbol in skipped)
                        )
                        symbol_list = ", ".join(sorted(skipped))
                        warned = getattr(self, "_unsupported_coin_warnings", None)
                        if warned is None:
                            warned = set()
                            setattr(self, "_unsupported_coin_warnings", warned)
                        warn_key = (self.exchange, coin_list, symbol_list, k_coins)
                        if warn_key not in warned:
                            logging.info(
                                "[config] skipping unsupported markets for %s: coins=%s symbols=%s exchange=%s",
                                k_coins,
                                coin_list,
                                symbol_list,
                                getattr(self, "exchange", "?"),
                            )
                            warned.add(warn_key)
                        symbols = symbols - set(skipped)
        symbols_already = getattr(self, k_coins)[pside]
        if symbols_already != symbols:
            added = symbols - symbols_already
            removed = symbols_already - symbols
            if added and pside in log_psides:
                result["added"][pside] = added
            if removed and pside in log_psides:
                result["removed"][pside] = removed
            getattr(self, k_coins)[pside] = symbols
    return result


def refresh_approved_ignored_coins_lists(self):
    """Reload approved and ignored coin lists from config sources."""
    try:
        added_summary = {}
        removed_summary = {}
        for k in ("approved_coins", "ignored_coins"):
            if not hasattr(self, k):
                setattr(self, k, {"long": set(), "short": set()})
            config_sources = self.config.get("_coins_sources", {})
            if k in config_sources:
                raw_source = config_sources[k]
            else:
                raw_source = self.live_value(k)
            parsed = normalize_coins_source(raw_source, allow_all=(k == "approved_coins"))
            if k == "approved_coins":
                log_psides = {ps for ps in parsed if self.is_pside_enabled(ps)}
            else:
                log_psides = set(parsed.keys())
            add_res = self.add_to_coins_lists(parsed, k, log_psides=log_psides)
            if add_res:
                added_summary.setdefault(k, {}).update(add_res.get("added", {}))
                removed_summary.setdefault(k, {}).update(add_res.get("removed", {}))
        self.approved_coins_minus_ignored_coins = {}
        for pside in self.approved_coins:
            if not self.is_pside_enabled(pside):
                if pside not in self._disabled_psides_logged:
                    if self.approved_coins[pside]:
                        logging.info(
                            f"{pside} side disabled (zero exposure or positions); clearing approved list."
                        )
                    else:
                        logging.info(
                            f"{pside} side disabled (zero exposure or positions); approved list already empty."
                        )
                    self._disabled_psides_logged.add(pside)
                self.approved_coins[pside] = set()
                self.approved_coins_minus_ignored_coins[pside] = set()
                continue
            else:
                if pside in self._disabled_psides_logged:
                    logging.info(f"{pside} side re-enabled; restoring approved coin handling.")
                    self._disabled_psides_logged.discard(pside)
            self.approved_coins_minus_ignored_coins[pside] = self._filter_approved_symbols(
                pside, self.approved_coins[pside] - self.ignored_coins[pside]
            )
        for summary in (added_summary.get("approved_coins", {}),):
            if summary:
                parts = []
                for pside, coins in summary.items():
                    if coins:
                        parts.append(f"{pside}: {','.join(sorted(symbol_to_coin(x) for x in coins))}")
                if parts:
                    logging.info("added to approved_coins | %s", " | ".join(parts))
        for summary in (removed_summary.get("approved_coins", {}),):
            if summary:
                parts = []
                for pside, coins in summary.items():
                    if coins:
                        parts.append(f"{pside}: {','.join(sorted(symbol_to_coin(x) for x in coins))}")
                if parts:
                    logging.info("removed from approved_coins | %s", " | ".join(parts))
        for summary in (added_summary.get("ignored_coins", {}),):
            if summary:
                parts = []
                for pside, coins in summary.items():
                    if coins:
                        parts.append(f"{pside}: {','.join(sorted(symbol_to_coin(x) for x in coins))}")
                if parts:
                    logging.info("added to ignored_coins | %s", " | ".join(parts))
        for summary in (removed_summary.get("ignored_coins", {}),):
            if summary:
                parts = []
                for pside, coins in summary.items():
                    if coins:
                        parts.append(f"{pside}: {','.join(sorted(symbol_to_coin(x) for x in coins))}")
                if parts:
                    logging.info("removed from ignored_coins | %s", " | ".join(parts))
        try:
            if not getattr(self, "_stock_perps_warning_logged", False):
                stock_syms = set()
                for syms in self.approved_coins_minus_ignored_coins.values():
                    for sym in syms:
                        base = sym.split("/")[0] if "/" in sym else sym
                        if base.startswith(("xyz:", "XYZ-", "XYZ:")) or sym.startswith(
                            ("xyz:", "XYZ-", "XYZ:")
                        ):
                            stock_syms.add(sym)
                if stock_syms:
                    coins = sorted(
                        {
                            symbol_to_coin(s) or (s.split("/")[0] if "/" in s else s)
                            for s in stock_syms
                        }
                    )
                    logging.warning(
                        "Stock perps detected in approved_coins (%s). HIP-3 isolated margin is currently unsupported; isolated-only symbols will be skipped and existing isolated live state will fail loudly.",
                        ",".join(coins),
                    )
                    self._stock_perps_warning_logged = True
        except Exception:  # error-contract: allow - warning emission must not block refresh
            pass
        self._log_coin_symbol_fallback_summary()
    except Exception as e:
        logging.error(f"error with refresh_approved_ignored_coins_lists {e}")
        traceback.print_exc()
