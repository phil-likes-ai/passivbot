from importlib import import_module
import types


binance_utils = import_module("fill_events_binance_utils")


def test_resolve_symbol_uses_resolver_or_falls_back():
    fetcher = types.SimpleNamespace(_symbol_resolver=lambda value: "BTC/USDT:USDT" if value == "BTCUSDT" else None)
    assert binance_utils.resolve_symbol(fetcher, "BTCUSDT") == "BTC/USDT:USDT"
    assert binance_utils.resolve_symbol(fetcher, "ETHUSDT") == "ETHUSDT"


def test_normalize_income_and_trade_build_expected_payloads(monkeypatch):
    monkeypatch.setattr(binance_utils, "_ts_to_date", lambda ts: f"T{ts}")
    fetcher = types.SimpleNamespace(
        _symbol_resolver=lambda value: {"BTC": "BTC/USDT:USDT", "BTCUSDT": "BTC/USDT:USDT"}.get(value, value)
    )

    income = binance_utils.normalize_income(fetcher, {"tradeId": "1", "time": 1000, "symbol": "BTC", "income": 2.0, "positionSide": "LONG"})
    assert income["id"] == "1"
    assert income["symbol"] == "BTC/USDT:USDT"
    assert income["datetime"] == "T1000"

    trade = binance_utils.normalize_trade(
        fetcher,
        {"id": "2", "timestamp": 2000, "symbol": "BTCUSDT", "side": "buy", "amount": 1.0, "price": 100.0, "info": {"realizedPnl": 3.0, "positionSide": "LONG"}},
    )
    assert trade["id"] == "2"
    assert trade["symbol"] == "BTC/USDT:USDT"
    assert trade["pnl"] == 3.0


def test_collect_symbols_handles_provider_failure_and_resolution(monkeypatch):
    fetcher = types.SimpleNamespace(_symbol_resolver=lambda value: f"{value}/USDT:USDT")
    assert binance_utils.collect_symbols(fetcher, lambda: ["BTC", "ETH"]) == ["BTC/USDT:USDT", "ETH/USDT:USDT"]

    warnings = []
    monkeypatch.setattr(binance_utils.logger, "warning", lambda *args: warnings.append(args))
    assert binance_utils.collect_symbols(fetcher, lambda: (_ for _ in ()).throw(RuntimeError("boom"))) == []
    assert warnings


def test_collect_enrichment_targets_and_apply_result():
    merged = {
        "e1": {"id": "e1", "symbol": "BTC/USDT:USDT", "order_id": "o1", "client_order_id": "", "pb_order_type": ""},
        "e2": {"id": "e2", "symbol": "ETH/USDT:USDT", "order_id": "o2", "client_order_id": "cid", "pb_order_type": "entry_long"},
    }
    trade_events = {
        "e1": {"order_id": "o1", "symbol": "BTC/USDT:USDT"},
    }
    detail_cache = {}

    targets = binance_utils.collect_enrichment_targets(merged, trade_events)
    assert [(event_id, order_id, symbol) for _, event_id, order_id, symbol in targets] == [
        ("e1", "o1", "BTC/USDT:USDT")
    ]

    binance_utils.apply_enrichment_result(merged["e1"], "e1", ("cid-1", "close_long"), detail_cache)

    assert merged["e1"]["client_order_id"] == "cid-1"
    assert detail_cache["e1"] == ("cid-1", "close_long")


def test_finalize_merged_events_applies_defaults_and_persists_cache():
    merged = {
        "e1": {"client_order_id": "cid", "pb_order_type": ""},
        "e2": {"client_order_id": None, "pb_order_type": ""},
    }
    detail_cache = {}

    binance_utils.finalize_merged_events(merged, detail_cache, lambda value: f"kind:{value}")

    assert merged["e1"]["pb_order_type"] == "kind:cid"
    assert merged["e2"]["client_order_id"] == ""
    assert detail_cache["e1"] == ("cid", "kind:cid")
