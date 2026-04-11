import pytest
from importlib import import_module


hyper_utils = import_module("fill_events_hyperliquid_utils")


def test_normalize_trade_handles_direction_and_fee_fields(monkeypatch):
    monkeypatch.setattr(hyper_utils, "ts_to_date", lambda ts: f"T{ts}")
    trade = {
        "id": "t1",
        "timestamp": 1000,
        "side": "buy",
        "amount": 2.0,
        "price": 10.0,
        "pnl": 1.5,
        "symbol": "BTC/USDT:USDT",
        "clientOrderId": "cid",
        "info": {"dir": "open long", "feeToken": "USDT", "fee": 0.1, "contractMultiplier": 1.0},
    }
    result = hyper_utils.normalize_trade(trade)

    assert result["id"] == "t1"
    assert result["datetime"] == "T1000"
    assert result["position_side"] == "long"
    assert result["fees"] == {"currency": "USDT", "cost": 0.1}


def test_normalize_trade_missing_qty_raises_valueerror(monkeypatch):
    monkeypatch.setattr(hyper_utils, "ts_to_date", lambda ts: f"T{ts}")
    trade = {
        "id": "t1",
        "timestamp": 1000,
        "side": "buy",
        "price": 10.0,
        "pnl": 1.5,
        "symbol": "BTC/USDT:USDT",
        "clientOrderId": "cid",
        "info": {"dir": "open long", "feeToken": "USDT", "fee": 0.1, "contractMultiplier": 1.0},
    }
    with pytest.raises(ValueError, match="Hyperliquid fill missing required qty"):
        hyper_utils.normalize_trade(trade)


def test_normalize_trade_missing_price_raises_valueerror(monkeypatch):
    monkeypatch.setattr(hyper_utils, "ts_to_date", lambda ts: f"T{ts}")
    trade = {
        "id": "t1",
        "timestamp": 1000,
        "side": "buy",
        "amount": 2.0,
        "pnl": 1.5,
        "symbol": "BTC/USDT:USDT",
        "clientOrderId": "cid",
        "info": {"dir": "open long", "feeToken": "USDT", "fee": 0.1, "contractMultiplier": 1.0},
    }
    with pytest.raises(ValueError, match="Hyperliquid fill missing required price"):
        hyper_utils.normalize_trade(trade)


def test_normalize_trade_valid_with_amount_and_price(monkeypatch):
    monkeypatch.setattr(hyper_utils, "ts_to_date", lambda ts: f"T{ts}")
    trade = {
        "id": "t2",
        "timestamp": 2000,
        "side": "sell",
        "amount": 5.5,
        "price": 25.0,
        "pnl": -2.0,
        "symbol": "ETH/USDT:USDT",
        "clientOrderId": "cid2",
        "info": {"feeToken": "USDT", "fee": -0.25, "contractMultiplier": 1.0},
    }
    result = hyper_utils.normalize_trade(trade)

    assert result["id"] == "t2"
    assert result["datetime"] == "T2000"
    assert result["qty"] == 5.5
    assert result["price"] == 25.0
    assert result["pnl"] == -2.0
    assert result["position_side"] == "short"
    assert result["fees"] == {"currency": "USDT", "cost": -0.25}
