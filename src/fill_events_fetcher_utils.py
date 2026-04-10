from __future__ import annotations

from typing import Callable, List, Optional


def extract_symbol_pool(config: dict, override: Optional[List[str]]) -> List[str]:
    if override:
        return sorted({sym for sym in override if sym})
    live = config.get("live", {})
    approved = live.get("approved_coins")
    symbols: List[str] = []
    if isinstance(approved, dict):
        for vals in approved.values():
            if isinstance(vals, list):
                symbols.extend(vals)
    elif isinstance(approved, list):
        symbols.extend(approved)
    return sorted({sym for sym in symbols if sym})


def symbol_resolver(bot) -> Callable[[Optional[str]], str]:
    def resolver(raw: Optional[str]) -> str:
        if not raw:
            return ""
        if isinstance(raw, str) and "/" in raw:
            return raw
        value = "" if raw is None else str(raw)
        if not value:
            return ""
        try:
            mapped = bot.coin_to_symbol(value, verbose=False)
            if mapped:
                return mapped
        except Exception:
            pass
        if ":" in value and "/" not in value:
            base, _, quote = value.partition(":")
            if base and quote:
                return f"{base}/{quote}:{quote}"
        upper = value.upper()
        for quote in ("USDT", "USDC", "USD"):
            if upper.endswith(quote) and len(upper) > len(quote):
                base = upper[: -len(quote)]
                if base:
                    return f"{base}/{quote}:{quote}"
        return value

    return resolver


def build_fetcher_for_bot(bot, symbols: List[str], classes: dict):
    exchange = getattr(bot, "exchange", "").lower()
    resolver = symbol_resolver(bot)
    static_provider = lambda: symbols  # noqa: E731
    if exchange == "binance":
        return classes["BinanceFetcher"](
            api=bot.cca,
            symbol_resolver=resolver,
            positions_provider=static_provider,
            open_orders_provider=static_provider,
        )
    if exchange == "bitget":
        return classes["BitgetFetcher"](
            api=bot.cca,
            symbol_resolver=lambda value: resolver(value),
        )
    if exchange == "bybit":
        return classes["BybitFetcher"](api=bot.cca)
    if exchange == "fake":
        return classes["FakeFetcher"](api=bot.cca)
    if exchange == "hyperliquid":
        return classes["HyperliquidFetcher"](
            api=bot.cca,
            symbol_resolver=lambda value: resolver(value),
        )
    if exchange == "gateio":
        return classes["GateioFetcher"](api=bot.cca)
    if exchange == "kucoin":
        return classes["KucoinFetcher"](api=bot.cca)
    if exchange == "okx":
        return classes["OkxFetcher"](api=bot.cca)
    raise ValueError(f"Unsupported exchange '{exchange}' for fill events CLI")
