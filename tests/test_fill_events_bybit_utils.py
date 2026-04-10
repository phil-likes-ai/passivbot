from importlib import import_module
import types


bybit_utils = import_module("fill_events_bybit_utils")


def test_determine_position_side():
    assert bybit_utils.determine_position_side("buy", 0.0) == "long"
    assert bybit_utils.determine_position_side("buy", 1.0) == "short"
    assert bybit_utils.determine_position_side("sell", 0.0) == "short"
    assert bybit_utils.determine_position_side("sell", 1.0) == "long"


def test_normalize_trade_builds_expected_payload():
    trade = {
        "id": "t1",
        "timestamp": 1000,
        "amount": 2.0,
        "side": "buy",
        "price": 10.0,
        "pnl": 1.5,
        "symbol": "BTC/USDT:USDT",
        "clientOrderId": "cid",
        "fee": {"cost": 1.0},
        "info": {"closedSize": 0.0},
    }
    result = bybit_utils.normalize_trade(trade)

    assert result["id"] == "t1"
    assert result["position_side"] == "long"
    assert result["qty"] == 2.0
    assert result["raw"][0]["source"] == "fetch_my_trades"


def test_process_closed_pnl_batch_and_combine():
    fetcher = types.SimpleNamespace(api=types.SimpleNamespace(markets={"BTC/USDT:USDT": {"id": "BTCUSDT"}}))
    results = {}
    bybit_utils.process_closed_pnl_batch(
        fetcher,
        [{"updatedTime": 2, "createdTime": 1, "orderId": "oid", "symbol": "BTCUSDT", "closedPnl": 5, "closedSize": 1, "avgEntryPrice": 100, "avgExitPrice": 110, "leverage": 1, "side": "Sell"}],
        0,
        results,
    )
    assert "oid" in results

    fetcher._normalize_trade = lambda trade: bybit_utils.normalize_trade(trade)
    events = bybit_utils.combine(
        fetcher,
        [{"id": "t1", "timestamp": 1, "amount": 1.0, "side": "sell", "price": 110.0, "symbol": "BTC/USDT:USDT", "clientOrderId": "cid", "info": {"orderId": "oid", "closedSize": 1.0, "positionSide": "LONG"}}],
        list(results.values()),
        {"t1": ("cid", "close_unstuck_long")},
        lambda cid: "close_unstuck_long",
    )
    assert events[0]["pb_order_type"] == "close_unstuck_long"
    assert events[0]["pnl"] == 10.0
