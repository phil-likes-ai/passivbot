from importlib import import_module


okx_utils = import_module("fill_events_okx_utils")


def test_normalize_inst_id_converts_swap_and_spot_shapes():
    assert okx_utils.normalize_inst_id("BTC-USDT-SWAP") == "BTC/USDT:USDT"
    assert okx_utils.normalize_inst_id("ETH-USDC") == "ETH/USDC:USDC"
    assert okx_utils.normalize_inst_id("BTC/USDT:USDT") == "BTC/USDT:USDT"


def test_determine_position_side_handles_net_and_explicit_modes():
    assert okx_utils.determine_position_side("buy", "net", 1.0) == "short"
    assert okx_utils.determine_position_side("sell", "net", 0.0) == "short"
    assert okx_utils.determine_position_side("buy", "long", 0.0) == "long"
    assert okx_utils.determine_position_side("sell", "", 0.0) == "short"


def test_normalize_fill_builds_expected_payload(monkeypatch):
    monkeypatch.setattr(okx_utils, "ts_to_date", lambda ts: f"T{ts}")
    raw = {
        "tradeId": "t1",
        "ordId": "o1",
        "ts": 1000,
        "instId": "BTC-USDT-SWAP",
        "side": "buy",
        "fillSz": 2.0,
        "fillPx": 100.0,
        "fillPnl": 1.5,
        "posSide": "net",
        "clOrdId": "cid",
        "feeCcy": "USDT",
        "fee": -0.2,
    }

    result = okx_utils.normalize_fill(raw)

    assert result["id"] == "t1"
    assert result["datetime"] == "T1000"
    assert result["symbol"] == "BTC/USDT:USDT"
    assert result["position_side"] == "short"
    assert result["fees"] == {"currency": "USDT", "cost": 0.2}


def test_apply_order_detail_cache_prefers_cache_then_derives_and_persists():
    event = {"id": "t1", "client_order_id": "cid", "pb_order_type": ""}
    detail_cache = {}

    okx_utils.apply_order_detail_cache(event, detail_cache, lambda value: f"kind:{value}")

    assert event["pb_order_type"] == "kind:cid"
    assert detail_cache["t1"] == ("cid", "kind:cid")

    cached_event = {"id": "t1", "client_order_id": "", "pb_order_type": ""}
    okx_utils.apply_order_detail_cache(cached_event, detail_cache, lambda value: value)

    assert cached_event["client_order_id"] == "cid"
    assert cached_event["pb_order_type"] == "kind:cid"


def test_process_fill_batch_filters_and_enriches_events():
    fills = [
        {"tradeId": "t1", "ts": 10, "clOrdId": "cid1"},
        {"tradeId": "t2", "ts": 20, "clOrdId": "cid2"},
        {"tradeId": "", "ts": 30, "clOrdId": "cid3"},
    ]
    detail_cache = {"t2": ("cached", "cached_kind")}
    collected = {}

    def normalize_fill(raw):
        return {
            "id": str(raw.get("tradeId") or ""),
            "timestamp": int(raw.get("ts") or 0),
            "client_order_id": str(raw.get("clOrdId") or ""),
            "pb_order_type": "",
        }

    batch_events, oldest_ts = okx_utils.process_fill_batch(
        fills,
        normalize_fill,
        detail_cache,
        lambda value: f"kind:{value}",
        collected,
        since_ms=15,
        until_ms=25,
    )

    assert oldest_ts == 20
    assert [event["id"] for event in batch_events] == ["t2"]
    assert batch_events[0]["client_order_id"] == "cached"
    assert batch_events[0]["pb_order_type"] == "cached_kind"
    assert collected["t2"]["pb_order_type"] == "cached_kind"


def test_next_after_cursor_uses_last_fill_bill_id():
    assert okx_utils.next_after_cursor([{"billId": "a"}, {"billId": "b"}]) == "b"
    assert okx_utils.next_after_cursor([{"billId": 123}]) == "123"
    assert okx_utils.next_after_cursor([{ }]) is None


def test_finalize_events_sorts_filters_coalesces_and_applies_cache():
    collected = {
        "t2": {"id": "t2", "timestamp": 20, "client_order_id": "cid2", "pb_order_type": ""},
        "t1": {"id": "t1", "timestamp": 10, "client_order_id": "", "pb_order_type": ""},
        "t3": {"id": "t3", "timestamp": 30, "client_order_id": "cid3", "pb_order_type": ""},
    }
    detail_cache = {"t1": ("cached", "cached_kind")}

    def coalesce(events):
        return events[:2]

    result = okx_utils.finalize_events(
        collected,
        detail_cache,
        lambda value: f"kind:{value}",
        coalesce,
        since_ms=10,
        until_ms=25,
    )

    assert [event["id"] for event in result] == ["t1", "t2"]
    assert result[0]["client_order_id"] == "cached"
    assert result[0]["pb_order_type"] == "cached_kind"
    assert result[1]["pb_order_type"] == "kind:cid2"


def test_build_fetch_params_and_boundary_helpers():
    params = okx_utils.build_fetch_params("SWAP", 100, 10, 20, "cursor")

    assert params == {
        "instType": "SWAP",
        "limit": "100",
        "begin": "10",
        "end": "20",
        "after": "cursor",
    }
    assert okx_utils.reached_since_boundary(10, 10)
    assert not okx_utils.reached_since_boundary(11, 10)
    assert okx_utils.short_batch([{}], 2)
    assert not okx_utils.short_batch([{}, {}], 2)
