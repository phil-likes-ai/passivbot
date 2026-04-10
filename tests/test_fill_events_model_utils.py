from importlib import import_module

import pytest


model_utils = import_module("fill_events_model_utils")


def test_fill_event_from_dict_kwargs_builds_expected_payload():
    result = model_utils.fill_event_from_dict_kwargs(
        {
            "id": 1,
            "timestamp": 1000,
            "symbol": "BTC/USDT:USDT",
            "side": "BUY",
            "qty": 1,
            "price": 100,
            "pnl": 2,
            "pb_order_type": "entry_initial_long",
            "position_side": "LONG",
            "client_order_id": "cid",
            "raw": {"foo": 1},
        },
        extract_source_ids=lambda raw, fallback: ["a"],
        normalize_raw_field=lambda raw: [{"source": "legacy", "data": raw}],
    )

    assert result["id"] == "1"
    assert result["source_ids"] == ["a"]
    assert result["side"] == "buy"
    assert result["position_side"] == "long"
    assert result["raw"] == [{"source": "legacy", "data": {"foo": 1}}]


def test_fill_event_from_dict_kwargs_raises_on_missing_required_keys():
    with pytest.raises(ValueError, match="missing required keys"):
        model_utils.fill_event_from_dict_kwargs({}, extract_source_ids=lambda raw, fallback: [], normalize_raw_field=lambda raw: [])


def test_fill_event_to_dict_and_key_use_model_fields():
    event = type(
        "Event",
        (),
        {
            "id": "abc",
            "source_ids": ["x"],
            "timestamp": 1,
            "datetime": "d",
            "symbol": "BTC",
            "side": "buy",
            "qty": 1.0,
            "price": 2.0,
            "pnl": 3.0,
            "fees": None,
            "pb_order_type": "entry",
            "position_side": "long",
            "client_order_id": "cid",
            "psize": 4.0,
            "pprice": 5.0,
            "raw": None,
        },
    )()

    assert model_utils.fill_event_key(event) == "abc"
    assert model_utils.fill_event_to_dict(event)["id"] == "abc"
    assert model_utils.fill_event_to_dict(event)["raw"] == []
