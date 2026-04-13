from importlib import import_module


parse_utils = import_module("fill_events_parse_utils")


def test_normalize_raw_field_handles_none_list_dict_and_unknown():
    assert parse_utils.normalize_raw_field(None) == []
    assert parse_utils.normalize_raw_field([{"a": 1}]) == [{"a": 1}]
    assert parse_utils.normalize_raw_field({"a": 1}) == [{"source": "legacy", "data": {"a": 1}}]
    assert parse_utils.normalize_raw_field("x") == [{"source": "unknown", "data": "x"}]


def test_extract_source_ids_prefers_raw_ids_and_fallback():
    raw = [{"data": {"id": "abc", "info": {"tid": "def"}}}]
    assert parse_utils.extract_source_ids(raw, None) == ["abc", "def"]
    assert parse_utils.extract_source_ids(None, "fallback") == ["fallback"]


def test_extract_source_ids_dedupes_and_sorts_multiple_sources():
    raw = [
        {"data": {"id": "abc", "tradeId": "dup", "info": {"trade_id": "zzz", "id": "dup"}}},
        {"data": {"execId": "mmm", "info": {"tid": "abc"}}},
    ]
    assert parse_utils.extract_source_ids(raw, None) == ["abc", "dup", "mmm", "zzz"]


def test_bybit_trade_helpers_handle_exec_id_and_fallback():
    trade = {"id": "abc", "amount": 2.0}
    assert parse_utils.bybit_trade_dedupe_key(trade) == ("exec_id", "abc")
    assert parse_utils.bybit_trade_qty_abs(trade) == 2.0


def test_bybit_trade_dedupe_key_returns_none_for_invalid_fallback_rows():
    trade = {
        "timestamp": 1,
        "symbol": "BTC/USDT:USDT",
        "side": "buy",
        "order": "oid",
        "amount": 0.0,
        "price": 100.0,
    }
    assert parse_utils.bybit_trade_dedupe_key(trade) is None


def test_bybit_trade_qty_signed_and_event_group_key():
    trade = {"side": "sell", "amount": 2.0}
    assert parse_utils.bybit_trade_qty_signed(trade) == -2.0

    info_trade = {"info": {"side": "Buy", "execQty": "3.5"}}
    assert parse_utils.bybit_trade_qty_signed(info_trade) == 3.5

    event = type(
        "Event",
        (),
        {
            "timestamp": 1,
            "symbol": "BTC/USDT:USDT",
            "pb_order_type": "entry_initial_long",
            "side": "BUY",
            "position_side": "LONG",
        },
    )()
    assert parse_utils.bybit_event_group_key(event) == (
        1,
        "BTC/USDT:USDT",
        "entry_initial_long",
        "buy",
        "long",
    )


def test_custom_id_to_snake_and_deduce_side_pside_fallbacks():
    assert parse_utils.custom_id_to_snake("abc") == "unknown"
    assert parse_utils.custom_id_to_snake("") == "unknown"
    assert parse_utils.deduce_side_pside({"side": "sell"}) == ("sell", "long")
    assert parse_utils.deduce_side_pside({}) == ("buy", "long")
