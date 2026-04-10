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
