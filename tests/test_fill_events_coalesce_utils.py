from importlib import import_module


coalesce_utils = import_module("fill_events_coalesce_utils")


def test_coalesce_events_merges_same_group_and_averages_price():
    events = [
        {"id": "a", "timestamp": 1, "symbol": "BTC", "pb_order_type": "entry", "side": "buy", "position_side": "long", "qty": 1.0, "price": 100.0, "pnl": 1.0, "raw": {"x": 1}},
        {"id": "b", "timestamp": 1, "symbol": "BTC", "pb_order_type": "entry", "side": "buy", "position_side": "long", "qty": 3.0, "price": 200.0, "pnl": 2.0, "raw": {"x": 2}},
    ]

    result = coalesce_utils.coalesce_events(
        events,
        merge_fee_lists=lambda a, b: None,
        normalize_raw_field=lambda raw: [raw],
    )

    assert len(result) == 1
    assert result[0]["id"] == "a+b"
    assert result[0]["qty"] == 4.0
    assert result[0]["pnl"] == 3.0
    assert result[0]["price"] == 175.0


def test_coalesce_events_flattens_single_fee_entry():
    events = [
        {"id": "a", "timestamp": 1, "symbol": "BTC", "pb_order_type": "entry", "side": "buy", "position_side": "long", "qty": 1.0, "price": 100.0, "pnl": 1.0, "fees": {"cost": 1.0}, "raw": None},
    ]

    result = coalesce_utils.coalesce_events(
        events,
        merge_fee_lists=lambda a, b: [{"cost": 1.0}],
        normalize_raw_field=lambda raw: [],
    )

    assert result[0]["fees"] == {"cost": 1.0}
