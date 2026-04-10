from importlib import import_module


kucoin_utils = import_module("fill_events_kucoin_utils")


def test_determine_position_side_matches_close_intent():
    assert kucoin_utils.determine_position_side("buy", False, 0.0) == "long"
    assert kucoin_utils.determine_position_side("buy", True, 0.0) == "short"
    assert kucoin_utils.determine_position_side("sell", False, 0.0) == "short"
    assert kucoin_utils.determine_position_side("sell", False, 1.0) == "long"


def test_normalize_trade_builds_expected_payload(monkeypatch):
    monkeypatch.setattr(kucoin_utils, "ts_to_date", lambda ts: f"T{ts}")
    trade = {
        "id": "t1",
        "order": "o1",
        "timestamp": 1000,
        "symbol": "BTC/USDT:USDT",
        "side": "buy",
        "amount": 2.0,
        "price": 10.0,
        "clientOrderId": "cid",
        "fee": {"cost": 0.1},
        "info": {"closeFeePay": 0.0},
    }

    result = kucoin_utils.normalize_trade(trade)

    assert result["id"] == "t1"
    assert result["datetime"] == "T1000"
    assert result["position_side"] == "long"
    assert result["client_order_id"] == "cid"
    assert result["raw"][0]["source"] == "fetch_my_trades"


def test_apply_cached_order_details_reuses_trade_and_order_cache():
    events = [
        {"id": "t1", "order_id": "o1", "client_order_id": "", "pb_order_type": ""},
        {"id": "t2", "order_id": "o1", "client_order_id": "", "pb_order_type": ""},
    ]
    detail_cache = {"t1": ("cid", "close_long")}

    order_id_cache = kucoin_utils.apply_cached_order_details(events, detail_cache)

    assert order_id_cache == {"o1": ("cid", "close_long")}
    assert events[0]["client_order_id"] == "cid"
    assert events[1]["pb_order_type"] == "close_long"
    assert detail_cache["t2"] == ("cid", "close_long")


def test_collect_and_apply_order_detail_results_updates_pending_events():
    events = [
        {"id": "t1", "order_id": "o1", "client_order_id": "", "pb_order_type": ""},
        {"id": "t2", "order_id": "o1", "client_order_id": "", "pb_order_type": ""},
        {"id": "t3", "order_id": "", "client_order_id": "", "pb_order_type": ""},
    ]
    detail_cache = {}
    order_id_cache = {}

    grouped = kucoin_utils.collect_events_requiring_order_details(
        events, detail_cache, order_id_cache
    )
    kucoin_utils.apply_order_detail_result(
        "o1", ("cid", "entry_long"), grouped, detail_cache, order_id_cache
    )
    kucoin_utils.ensure_order_detail_defaults(events)

    assert sorted(grouped) == ["o1"]
    assert events[0]["client_order_id"] == "cid"
    assert events[1]["pb_order_type"] == "entry_long"
    assert detail_cache["t1"] == ("cid", "entry_long")
    assert events[2]["pb_order_type"] == "unknown"


def test_apply_order_detail_result_none_marks_unknown():
    events_by_order = {"o1": [{"id": "t1", "pb_order_type": ""}]}

    kucoin_utils.apply_order_detail_result("o1", None, events_by_order, {}, {})

    assert events_by_order["o1"][0]["pb_order_type"] == "unknown"


def test_parse_order_detail_reads_client_oid_and_converts_type():
    detail = {"info": {"clientOid": "cid"}}

    result = kucoin_utils.parse_order_detail(detail, lambda value: f"kind:{value}")

    assert result == ("cid", "kind:cid")


def test_parse_order_detail_returns_none_without_client_oid():
    assert kucoin_utils.parse_order_detail({"info": {}}, lambda value: value) is None


def test_aggregate_position_pnls_by_symbol_uses_top_level_and_info_symbols():
    positions = [
        {"symbol": "BTC/USDT:USDT", "realizedPnl": 1.5},
        {"info": {"symbol": "BTC/USDT:USDT"}, "realizedPnl": "2.5"},
        {"info": {"symbol": "ETH/USDT:USDT"}, "realizedPnl": 3.0},
        {"realizedPnl": 9.0},
    ]

    result = kucoin_utils.aggregate_position_pnls_by_symbol(positions)

    assert result == {"BTC/USDT:USDT": 4.0, "ETH/USDT:USDT": 3.0}


def test_should_log_discrepancy_respects_threshold_and_throttle():
    assert not kucoin_utils.should_log_discrepancy(
        10.0,
        10.1,
        -0.1,
        0.0,
        None,
        100.0,
        min_ratio=0.05,
        change_threshold=0.1,
        min_seconds=900.0,
        throttle_seconds=3600.0,
    )

    assert kucoin_utils.should_log_discrepancy(
        10.0,
        20.0,
        -10.0,
        0.0,
        None,
        1000.0,
        min_ratio=0.05,
        change_threshold=0.1,
        min_seconds=900.0,
        throttle_seconds=3600.0,
    )

    assert not kucoin_utils.should_log_discrepancy(
        10.0,
        20.0,
        -10.0,
        950.0,
        -10.0,
        1000.0,
        min_ratio=0.05,
        change_threshold=0.1,
        min_seconds=900.0,
        throttle_seconds=3600.0,
    )

    assert kucoin_utils.should_log_discrepancy(
        10.0,
        20.0,
        -12.0,
        950.0,
        -10.0,
        2000.0,
        min_ratio=0.05,
        change_threshold=0.1,
        min_seconds=900.0,
        throttle_seconds=3600.0,
    )


def test_summarize_unmatched_positions_counts_and_sums_pnl():
    unmatched = [{"realizedPnl": 1.5}, {"realizedPnl": "2.5"}, {}]

    count, total_pnl = kucoin_utils.summarize_unmatched_positions(unmatched)

    assert count == 3
    assert total_pnl == 4.0


def test_collect_trade_batch_normalizes_filters_and_keys_by_trade_and_order():
    collected = {}

    def normalize_trade(trade):
        return {
            "id": trade["id"],
            "order_id": trade.get("order_id", ""),
            "timestamp": trade["timestamp"],
        }

    last_ts = kucoin_utils.collect_trade_batch(
        [
            {"id": "t2", "order_id": "o2", "timestamp": 20},
            {"id": "t1", "order_id": "o1", "timestamp": 10},
            {"id": "t3", "order_id": "o3", "timestamp": 30},
        ],
        normalize_trade,
        15,
        25,
        collected,
    )

    assert last_ts == 30
    assert collected == {("t2", "o2"): {"id": "t2", "order_id": "o2", "timestamp": 20}}


def test_collect_positions_history_batch_sorts_and_upserts_by_close_id():
    results = {}

    last_ts = kucoin_utils.collect_positions_history_batch(
        [
            {"id": "p2", "lastUpdateTimestamp": 20, "info": {"closeId": "c2"}},
            {"id": "p1", "lastUpdateTimestamp": 10, "info": {"closeId": "c1"}},
        ],
        results,
        99,
    )

    assert last_ts == 20
    assert sorted(results) == ["c1", "c2"]
    assert results["c1"]["id"] == "p1"


def test_match_pnls_distributes_pnl_by_fill_qty():
    closes = [
        {"id": "c1", "symbol": "BTC/USDT:USDT", "timestamp": 1_000, "qty": 1.0},
        {"id": "c2", "symbol": "BTC/USDT:USDT", "timestamp": 1_100, "qty": 3.0},
    ]
    positions = [
        {
            "symbol": "BTC/USDT:USDT",
            "lastUpdateTimestamp": 1_050,
            "realizedPnl": 40.0,
        }
    ]
    events = {close["id"]: {"pnl": -1.0} for close in closes}

    unmatched = kucoin_utils.match_pnls(closes, positions, events)

    assert unmatched == []
    assert events["c1"]["pnl"] == 10.0
    assert events["c2"]["pnl"] == 30.0


def test_match_pnls_assigns_closest_fill_when_total_qty_is_zero():
    closes = [
        {"id": "c1", "symbol": "BTC/USDT:USDT", "timestamp": 1_000, "qty": 0.0},
        {"id": "c2", "symbol": "BTC/USDT:USDT", "timestamp": 1_400, "qty": 0.0},
    ]
    positions = [
        {
            "symbol": "BTC/USDT:USDT",
            "lastUpdateTimestamp": 1_350,
            "realizedPnl": 12.5,
        }
    ]
    events = {close["id"]: {"pnl": -1.0} for close in closes}

    unmatched = kucoin_utils.match_pnls(closes, positions, events)

    assert unmatched == []
    assert events["c1"]["pnl"] == 0.0
    assert events["c2"]["pnl"] == 12.5


def test_match_pnls_returns_unmatched_positions_and_zeroes_unassigned_closes():
    closes = [
        {"id": "c1", "symbol": "BTC/USDT:USDT", "timestamp": 1_000, "qty": 1.0},
        {"id": "c2", "symbol": "ETH/USDT:USDT", "timestamp": 2_000, "qty": 2.0},
    ]
    positions = [
        {
            "symbol": "BTC/USDT:USDT",
            "lastUpdateTimestamp": 2_000_000,
            "realizedPnl": 5.0,
        },
        {
            "symbol": "SOL/USDT:USDT",
            "lastUpdateTimestamp": 2_100,
            "realizedPnl": 7.0,
        },
    ]
    events = {close["id"]: {"pnl": -1.0} for close in closes}

    unmatched = kucoin_utils.match_pnls(closes, positions, events)

    assert unmatched == positions
    assert events["c1"]["pnl"] == 0.0
    assert events["c2"]["pnl"] == 0.0
