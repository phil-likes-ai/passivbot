from importlib import import_module


data_model = import_module("fill_events_data_model")


def test_fill_event_from_dict_uses_shared_model_boundary():
    event = data_model.FillEvent.from_dict(
        {
            "id": "t1",
            "timestamp": 1_000,
            "symbol": "BTC/USDT",
            "side": "BUY",
            "qty": 1.0,
            "price": 10.0,
            "pnl": 0.5,
            "pb_order_type": "entry",
            "position_side": "LONG",
            "client_order_id": "cid",
            "raw": {"id": "abc"},
        }
    )
    assert event.side == "buy"
    assert event.position_side == "long"
    assert event.source_ids == ["abc"]


def test_coalesce_events_uses_merge_and_normalization_helpers():
    def merge_fee_lists(left, right):
        items = []
        for value in (left, right):
            if value is None:
                continue
            if isinstance(value, list):
                items.extend(value)
            else:
                items.append(value)
        return items

    result = data_model.coalesce_events(
        [
            {
                "id": "a",
                "timestamp": 1,
                "symbol": "BTC/USDT",
                "pb_order_type": "entry",
                "side": "buy",
                "position_side": "long",
                "qty": 1.0,
                "price": 10.0,
                "pnl": 1.0,
                "fees": {"cost": 0.1},
                "raw": {"id": "a"},
            },
            {
                "id": "b",
                "timestamp": 1,
                "symbol": "BTC/USDT",
                "pb_order_type": "entry",
                "side": "buy",
                "position_side": "long",
                "qty": 3.0,
                "price": 20.0,
                "pnl": 2.0,
                "fees": {"cost": 0.2},
                "raw": {"id": "b"},
            },
        ],
        merge_fee_lists,
    )

    assert len(result) == 1
    assert result[0]["id"] == "a+b"
    assert result[0]["qty"] == 4.0
    assert result[0]["price"] == 17.5
    assert result[0]["pnl"] == 3.0
    assert result[0]["raw"] == [
        {"source": "legacy", "data": {"id": "a"}},
        {"source": "legacy", "data": {"id": "b"}},
    ]
