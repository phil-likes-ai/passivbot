from importlib import import_module


position_utils = import_module("fill_events_position_utils")


def test_ensure_qty_signage_normalizes_buy_sell_signs():
    events = [{"side": "buy", "qty": -1.0}, {"side": "sell", "qty": 2.0}]
    position_utils.ensure_qty_signage(events)
    assert events == [{"side": "buy", "qty": 1.0}, {"side": "sell", "qty": -2.0}]


def test_compute_add_reduce_for_long_and_short():
    assert position_utils.compute_add_reduce("long", 2.0) == (2.0, 0.0)
    assert position_utils.compute_add_reduce("long", -2.0) == (0.0, 2.0)
    assert position_utils.compute_add_reduce("short", -2.0) == (2.0, 0.0)
    assert position_utils.compute_add_reduce("short", 2.0) == (0.0, 2.0)


def test_compute_psize_pprice_updates_events_and_final_state():
    events = [
        {"symbol": "BTC/USDT:USDT", "position_side": "long", "qty": 1.0, "price": 100.0},
        {"symbol": "BTC/USDT:USDT", "position_side": "long", "qty": 1.0, "price": 200.0},
    ]
    result = position_utils.compute_psize_pprice(events)
    assert result == {("BTC/USDT:USDT", "long"): (2.0, 150.0)}
    assert events[1]["psize"] == 2.0
    assert events[1]["pprice"] == 150.0


def test_compute_realized_pnls_from_trades_handles_add_and_reduce():
    trades = [
        {"id": "1", "timestamp": 1, "symbol": "BTC/USDT:USDT", "side": "buy", "position_side": "long", "qty": 1.0, "price": 100.0},
        {"id": "2", "timestamp": 2, "symbol": "BTC/USDT:USDT", "side": "sell", "position_side": "long", "qty": 1.0, "price": 110.0},
    ]

    per_trade, positions = position_utils.compute_realized_pnls_from_trades(trades)

    assert per_trade == {"1": 0.0, "2": 10.0}
    assert positions == {("BTC/USDT:USDT", "long"): (0.0, 0.0)}
