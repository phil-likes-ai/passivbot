from importlib import import_module
import types


fetcher_utils = import_module("fill_events_fetcher_utils")


def test_extract_symbol_pool_prefers_override_then_live_approved():
    assert fetcher_utils.extract_symbol_pool({}, ["BTC", "ETH", "BTC"]) == ["BTC", "ETH"]
    assert fetcher_utils.extract_symbol_pool({"live": {"approved_coins": {"long": ["BTC"], "short": ["ETH"]}}}, None) == ["BTC", "ETH"]


def test_symbol_resolver_prefers_bot_mapping_and_fallback_shapes():
    bot = types.SimpleNamespace(coin_to_symbol=lambda value, verbose=False: "BTC/USDT:USDT" if value == "btc" else None)
    resolver = fetcher_utils.symbol_resolver(bot)
    assert resolver("btc") == "BTC/USDT:USDT"
    assert resolver("ETHUSDT") == "ETH/USDT:USDT"
    assert resolver("SOL:USDT") == "SOL/USDT:USDT"


def test_build_fetcher_for_bot_selects_exchange_specific_class():
    def make_ctor(name):
        def ctor(**kwargs):
            return (name, kwargs)

        return ctor

    classes = {name: make_ctor(name) for name in [
        "BinanceFetcher", "BitgetFetcher", "BybitFetcher", "FakeFetcher", "HyperliquidFetcher", "GateioFetcher", "KucoinFetcher", "OkxFetcher"
    ]}
    bot = types.SimpleNamespace(exchange="bybit", cca=object())
    assert fetcher_utils.build_fetcher_for_bot(bot, ["BTC"], classes)[0] == "BybitFetcher"
