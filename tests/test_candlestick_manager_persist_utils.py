from importlib import import_module
import types

import numpy as np


cm_persist_utils = import_module("candlestick_manager_persist_utils")


def test_update_persist_cache_updates_cache_meta_and_replacements():
    arr = np.array([(1,)], dtype=[("ts", np.int64)])
    calls = []
    cm = types.SimpleNamespace(
        _cache={},
        _merge_overwrite=lambda existing, incoming: incoming,
        _ensure_symbol_cache=lambda symbol: np.empty((0,), dtype=arr.dtype),
        _enforce_memory_retention=lambda symbol: calls.append(("retain", symbol)),
        _set_last_refresh_meta=lambda symbol, last_refresh_ms, last_final_ts: calls.append(
            ("meta", symbol, last_refresh_ms, last_final_ts)
        ),
        _check_synthetic_replacement=lambda symbol, incoming: calls.append(("replace", symbol, int(incoming[-1]["ts"]))),
    )

    cm_persist_utils.update_persist_cache(
        cm,
        "BTC/USDT:USDT",
        arr,
        tf_norm="1m",
        merge_cache=False,
        last_refresh_ms=123,
        skip_memory_retention=False,
    )

    assert cm._cache["BTC/USDT:USDT"]["ts"].tolist() == [1]
    assert calls == [
        ("retain", "BTC/USDT:USDT"),
        ("meta", "BTC/USDT:USDT", 123, 1),
        ("replace", "BTC/USDT:USDT", 1),
    ]


def test_notify_persist_observer_never_raises():
    seen = []
    arr = np.array([(1,)], dtype=[("ts", np.int64)])

    cm_persist_utils.notify_persist_observer(lambda symbol, tf, batch: seen.append((symbol, tf, batch.copy())), "BTC", "1m", arr)
    cm_persist_utils.notify_persist_observer(lambda *_args: (_ for _ in ()).throw(RuntimeError("boom")), "BTC", "1m", arr)

    assert seen[0][0:2] == ("BTC", "1m")


def test_check_synthetic_replacement_invalidates_and_batches():
    arr = np.array([(2,), (3,)], dtype=[("ts", np.int64)])
    calls = []
    cm = types.SimpleNamespace(
        _synthetic_timestamps={"BTC": {1, 2, 4}},
        _invalidate_ema_cache=lambda symbol: calls.append(("invalidate", symbol)),
        _candle_replace_batch_mode=True,
        _candle_replace_batch={},
        log=types.SimpleNamespace(debug=lambda *args: calls.append(("debug", args))),
    )

    cm_persist_utils.check_synthetic_replacement(cm, "BTC", arr)

    assert cm._synthetic_timestamps["BTC"] == {1, 4}
    assert cm._candle_replace_batch["BTC"] == 1
    assert calls == [("invalidate", "BTC")]


def test_track_synthetic_timestamps_bounds_history():
    cm = types.SimpleNamespace(_synthetic_timestamps={})

    cm_persist_utils.track_synthetic_timestamps(
        cm,
        "BTC",
        [1, 2, 3, -1],
        utc_now_ms_fn=lambda: 10 * 24 * 60 * 60_000,
        one_min_ms=60_000,
    )

    assert cm._synthetic_timestamps["BTC"] == set()

    cm_persist_utils.track_synthetic_timestamps(
        cm,
        "BTC",
        [9 * 24 * 60 * 60_000],
        utc_now_ms_fn=lambda: 10 * 24 * 60 * 60_000,
        one_min_ms=60_000,
    )

    assert cm._synthetic_timestamps["BTC"] == {9 * 24 * 60 * 60_000}


def test_plan_runtime_synthetic_gap_and_build_runtime_synthetic_gap():
    dtype = np.dtype(
        [("ts", np.int64), ("o", np.float32), ("h", np.float32), ("l", np.float32), ("c", np.float32), ("bv", np.float32)]
    )
    arr = np.array(
        [
            (60_000, 1.0, 1.0, 1.0, 1.5, 0.0),
            (120_000, 1.5, 1.5, 1.5, 2.0, 0.0),
        ],
        dtype=dtype,
    )

    plan = cm_persist_utils.plan_runtime_synthetic_gap(
        arr,
        300_000,
        one_min_ms=60_000,
        max_memory_candles_per_symbol=100,
    )

    assert plan == (180_000, 2.0)

    synth = cm_persist_utils.build_runtime_synthetic_gap(
        180_000,
        300_000,
        2.0,
        one_min_ms=60_000,
        candle_dtype=dtype,
    )

    assert synth["ts"].tolist() == [180_000, 240_000, 300_000]
    assert synth["c"].tolist() == [2.0, 2.0, 2.0]


def test_apply_runtime_synthetic_gap_updates_cache_and_tracks():
    dtype = np.dtype([("ts", np.int64), ("c", np.float32)])
    arr = np.array([(60_000, 1.0)], dtype=dtype)
    synth = np.array([(120_000, 1.0)], dtype=dtype)
    calls = []
    cm = types.SimpleNamespace(
        _cache={},
        _merge_overwrite=lambda existing, incoming: np.concatenate([existing, incoming]),
        _enforce_memory_retention=lambda symbol: calls.append(("retain", symbol)),
        _log=lambda *args, **kwargs: calls.append(("log", kwargs.get("error"))),
    )

    count = cm_persist_utils.apply_runtime_synthetic_gap(
        cm,
        "BTC",
        arr,
        synth,
        track_synthetic_timestamps_fn=lambda symbol, timestamps: calls.append((symbol, timestamps)),
    )

    assert count == 1
    assert cm._cache["BTC"]["ts"].tolist() == [60_000, 120_000]
    assert calls[0] == ("retain", "BTC")
    assert calls[1] == ("BTC", [120000])


def test_ema_helpers_and_clear_tracking_behave_as_expected():
    cm = types.SimpleNamespace(_ema_cache={"BTC": {"x": 1}}, _synthetic_timestamps={"BTC": {1}, "ETH": {2}})

    assert not cm_persist_utils.needs_ema_recompute(cm, "BTC")
    assert cm_persist_utils.needs_ema_recompute(cm, "XRP")
    cm_persist_utils.invalidate_ema_cache(cm, "BTC")
    assert cm._ema_cache == {}
    cm_persist_utils.clear_synthetic_tracking(cm, "BTC")
    assert cm._synthetic_timestamps == {"ETH": {2}}
    cm_persist_utils.clear_synthetic_tracking(cm, None)
    assert cm._synthetic_timestamps == {}
