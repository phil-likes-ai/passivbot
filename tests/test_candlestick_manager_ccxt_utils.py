from importlib import import_module

import asyncio


cm_ccxt_utils = import_module("candlestick_manager_ccxt_utils")


def test_exchange_retry_config_and_build_fetch_params():
    assert cm_ccxt_utils.exchange_retry_config("bybit") == (True, False, 9, 1.0, 20.0)
    assert cm_ccxt_utils.exchange_retry_config("hyperliquid") == (False, True, 5, 0.5, 8.0)
    assert cm_ccxt_utils.build_fetch_params("binance", 101) == {"until": 100}
    assert cm_ccxt_utils.build_fetch_params("bybit", 101) == {"category": "linear"}


def test_summarize_fetch_result_and_classify_fetch_error():
    assert cm_ccxt_utils.summarize_fetch_result([[1], [2]]) == (1, 2, 2)

    err_type, msg_l, is_rate_limit = cm_ccxt_utils.classify_fetch_error(RuntimeError("429 too many"))
    assert err_type == "RuntimeError"
    assert "429" in msg_l
    assert is_rate_limit is True


def test_adjusted_sleep_seconds_handles_rate_limit_and_bybit_network_errors():
    sleep_s, global_backoff = cm_ccxt_utils.adjusted_sleep_seconds(
        current_backoff=0.5,
        is_rate_limit=True,
        is_bybit=False,
        is_hyperliquid=True,
        err_type="RuntimeError",
        msg_l="429 too many",
    )
    assert sleep_s == 10.0
    assert global_backoff == 10.0

    sleep_s, global_backoff = cm_ccxt_utils.adjusted_sleep_seconds(
        current_backoff=0.5,
        is_rate_limit=False,
        is_bybit=True,
        is_hyperliquid=False,
        err_type="NetworkError",
        msg_l="timeout",
    )
    assert sleep_s == 2.0
    assert global_backoff is None


def test_remote_fetch_payload_helpers_build_expected_dicts():
    assert cm_ccxt_utils.error_tf_value(None) is None
    assert cm_ccxt_utils.error_tf_value("1m") == "1m"

    start = cm_ccxt_utils.remote_fetch_start_payload(
        exchange="ex",
        symbol="BTC/USDT:USDT",
        tf="1m",
        since_ts=1,
        limit=2,
        attempt=3,
        params={"until": 4},
    )
    ok = cm_ccxt_utils.remote_fetch_ok_payload(
        exchange="ex",
        symbol="BTC/USDT:USDT",
        tf="1m",
        since_ts=1,
        rows=2,
        first_ts=10,
        last_ts=20,
        elapsed_ms=30,
    )
    err = cm_ccxt_utils.remote_fetch_error_payload(
        exchange="ex",
        symbol="BTC/USDT:USDT",
        tf=None,
        since_ts=1,
        attempt=2,
        elapsed_ms=3,
        params={"a": 1},
        err_type="RuntimeError",
        error="boom",
        error_repr="RuntimeError('boom')",
    )

    assert start["stage"] == "start"
    assert ok["rows"] == 2
    assert err["error_type"] == "RuntimeError"


def test_fetch_error_context_and_optional_semaphore_fetch():
    ctx = cm_ccxt_utils.fetch_error_context(RuntimeError("429 boom"), elapsed_ms=12, params={"x": 1})
    assert ctx["err_type"] == "RuntimeError"
    assert ctx["is_rate_limit"] is True
    assert ctx["params"] == {"x": 1}

    calls = []

    class Exchange:
        async def fetch_ohlcv(self, symbol, timeframe, since, limit, params):
            calls.append((symbol, timeframe, since, limit, params))
            return [[since]]

    async def run_test():
        return await cm_ccxt_utils.fetch_ohlcv_with_optional_semaphore(
            exchange=Exchange(),
            symbol="BTC/USDT:USDT",
            tf="1m",
            since_ms=1,
            limit=2,
            params={"until": 3},
            net_sem=None,
            apply_rate_limit_backoff_fn=lambda: asyncio.sleep(0),
        )

    result = asyncio.run(run_test())
    assert result == [[1]]
    assert calls == [("BTC/USDT:USDT", "1m", 1, 2, {"until": 3})]


def test_normalize_rows_and_pagination_state_helpers():
    dtype = [
        ("ts", "int64"),
        ("o", "float32"),
        ("h", "float32"),
        ("l", "float32"),
        ("c", "float32"),
        ("bv", "float32"),
    ]
    arr = cm_ccxt_utils.normalize_ccxt_ohlcv_rows(
        [[61_000, 1, 2, 0.5, 1.5, 3], [60_000, 9, 9, 9, 9, 9]],
        ex_id="binance",
        one_min_ms=60_000,
        candle_dtype=dtype,
        floor_minute_fn=lambda ts: 60_000,
        normalize_ccxt_volume_to_base_fn=lambda ex_id, close, volume: volume,
    )
    assert arr["ts"].tolist() == [60_000]
    assert arr["c"].tolist() == [9.0]

    since_start, since, end_excl, total_span, prev_last_ts = cm_ccxt_utils.paginated_fetch_state(
        since_ms=120_000,
        end_exclusive_ms=300_000,
        period_ms=60_000,
        since_exclusive=True,
        overlap_candles=1,
    )
    assert (since_start, since, end_excl, total_span, prev_last_ts) == (120_000, 60_000, 300_000, 180_000, None)
    assert cm_ccxt_utils.compute_next_since(180_000, period_ms=60_000, overlap_candles=1, current_since=60_000) == 120_000
    assert cm_ccxt_utils.progress_percent(180_000, 120_000, 180_000) == 33.33333333333333
