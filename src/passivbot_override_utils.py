from __future__ import annotations

import logging

from config.access import require_config_value, require_live_value


def init_coin_overrides(self):
    """Populate coin override map keyed by symbols for quick lookup."""
    self.coin_overrides = {
        s: v
        for k, v in self.config.get("coin_overrides", {}).items()
        if (s := self.coin_to_symbol(k))
    }
    if self.coin_overrides:
        logging.debug(
            "Initialized coin overrides for %s",
            ", ".join(sorted(self.coin_overrides.keys())),
        )


def config_get(self, path: list[str], symbol=None):
    """Retrieve a config value, preferring per-symbol overrides when provided."""
    if symbol and symbol in self.coin_overrides:
        d = self.coin_overrides[symbol]
        for p in path:
            if isinstance(d, dict) and p in d:
                d = d[p]
            else:
                d = None
                break
        if d is not None:
            log_key = (symbol, ".".join(path))
            if not hasattr(self, "_override_hits_logged"):
                self._override_hits_logged = set()
            if log_key not in self._override_hits_logged:
                logging.debug("Using override for %s: %s", symbol, ".".join(path))
                self._override_hits_logged.add(log_key)
            return d

    d = self.config
    for p in path:
        if isinstance(d, dict) and p in d:
            d = d[p]
        else:
            raise KeyError(f"Key path {'.'.join(path)} not found in config or coin overrides.")
    return d


def bp(self, pside, key, symbol=None):
    """Condensed helper for config_get(['bot', pside, key], symbol)."""
    return self.config_get(["bot", pside, key], symbol)


def live_value(self, key: str):
    return require_live_value(self.config, key)


def bot_value(self, pside: str, key: str):
    return require_config_value(self.config, f"bot.{pside}.{key}")
