from importlib import import_module
import types

import numpy as np


cm_index_utils = import_module("candlestick_manager_index_utils")


def test_prune_missing_shards_from_index_removes_missing_and_updates_meta(monkeypatch):
    monkeypatch.setattr(cm_index_utils.os.path, "exists", lambda path: path == "keep.npy")
    idx = {
        "shards": {
            "2024-01-01": {"path": "keep.npy", "min_ts": 100, "max_ts": 200},
            "2024-01-02": {"path": "gone.npy", "min_ts": 300, "max_ts": 400},
        },
        "meta": {},
    }

    removed = cm_index_utils.prune_missing_shards_from_index(types.SimpleNamespace(), idx)

    assert removed == 1
    assert list(idx["shards"].keys()) == ["2024-01-01"]
    assert idx["meta"]["last_final_ts"] == 200
    assert idx["meta"]["inception_ts"] == 100


def test_path_helpers_build_expected_paths(monkeypatch):
    cm = types.SimpleNamespace(
        cache_dir="cache_root",
        exchange_name="bybit",
        _normalize_timeframe_arg=lambda timeframe=None, tf=None: tf or timeframe or "1m",
    )
    cm._symbol_dir = lambda symbol, timeframe=None, tf=None: cm_index_utils.symbol_dir(
        cm, symbol, timeframe=timeframe, tf=tf
    )

    symbol_dir = cm_index_utils.symbol_dir(cm, "BTC/USDT:USDT", tf="1m")
    index_path = cm_index_utils.index_path(cm, "BTC/USDT:USDT", tf="1m")
    shard_path = cm_index_utils.shard_path(cm, "BTC/USDT:USDT", "2024-01-01", tf="1m")

    assert symbol_dir.endswith("ohlcv\\bybit\\1m\\BTC_USDT_USDT") or symbol_dir.endswith("ohlcv/bybit/1m/BTC_USDT_USDT")
    assert index_path.endswith("index.json")
    assert shard_path.endswith("2024-01-01.npy")


def test_ensure_symbol_index_loads_and_populates_meta(monkeypatch, tmp_path):
    idx_path = tmp_path / "index.json"
    idx_path.write_text('{"shards": {}, "meta": {}}', encoding="utf-8")
    monkeypatch.setattr(cm_index_utils.os.path, "getmtime", lambda path: 1.0)
    cm = types.SimpleNamespace(
        _index={},
        _index_mtime={},
        _normalize_timeframe_arg=lambda timeframe=None, tf=None: tf or timeframe or "1m",
        _index_path=lambda symbol, timeframe=None, tf=None: str(idx_path),
        _prune_missing_shards_from_index=lambda idx: 0,
        _log=lambda *args, **kwargs: None,
    )

    idx = cm_index_utils.ensure_symbol_index(cm, "BTC/USDT:USDT", tf="1m")

    assert idx["meta"]["known_gaps"] == []
    assert idx["meta"]["last_refresh_ms"] == 0
    assert idx["meta"]["last_final_ts"] == 0


def test_set_persist_batch_observer_assigns_callback():
    cm = types.SimpleNamespace(_persist_batch_observer=None)
    cb = lambda *args: None

    cm_index_utils.set_persist_batch_observer(cm, cb)

    assert cm._persist_batch_observer is cb


def test_rebuild_index_shards_for_days_updates_valid_and_removes_missing(tmp_path):
    dtype = np.dtype([("ts", np.int64)])
    good_path = tmp_path / "2024-01-01.npy"
    np.save(good_path, np.array([(1,), (2,)], dtype=dtype))

    shards, updated, removed, scanned = cm_index_utils.rebuild_index_shards_for_days(
        {"2024-01-01": (0, 1), "2024-01-02": (2, 3)},
        {"2024-01-01": str(good_path), "2024-01-02": str(tmp_path / "missing.npy")},
        {"2024-01-02": {"path": "gone"}},
        range_start=0,
        range_end=10,
        load_shard_fn=lambda path: np.load(path, allow_pickle=False),
        ensure_dtype_fn=lambda arr: arr,
        candle_dtype=dtype,
    )

    assert updated == 1
    assert removed == 1
    assert scanned == 1
    assert shards["2024-01-01"]["count"] == 2


def test_normalize_future_refresh_resets_future_timestamp():
    meta = {"last_refresh_ms": 200}

    changed, last_refresh = cm_index_utils.normalize_future_refresh(
        meta, now_ms=100, one_min_ms=50
    )

    assert changed is True
    assert last_refresh == 200
    assert meta["last_refresh_ms"] == 0


def test_refresh_and_inception_meta_helpers_roundtrip():
    idx = {"meta": {"last_refresh_ms": 5, "last_final_ts": 7, "inception_ts": 9}}

    assert cm_index_utils.get_last_refresh_ms(idx) == 5
    assert cm_index_utils.get_last_final_ts(idx) == 7
    assert cm_index_utils.get_inception_ts(idx) == 9

    idx = cm_index_utils.set_last_refresh_meta(idx, 11, 13)
    idx, current, changed = cm_index_utils.set_inception_ts(idx, 3)

    assert idx["meta"]["last_refresh_ms"] == 11
    assert idx["meta"]["last_final_ts"] == 13
    assert current == 9
    assert changed is True
    assert idx["meta"]["inception_ts"] == 3


def test_inception_probe_and_min_shard_helpers(tmp_path):
    idx = {"meta": {"inception_ts_probe_ms": 11, "inception_ts_probe_end_ts": 22}, "shards": {"a": {"min_ts": 7}, "b": {"min_ts": 3}}}

    assert cm_index_utils.get_inception_probe_meta(idx) == (11, 22)
    idx = cm_index_utils.set_inception_probe_meta(idx, 33, 44)
    assert idx["meta"]["inception_ts_probe_ms"] == 33
    assert cm_index_utils.get_min_shard_ts_from_index(idx) == 3

    shard_dir = tmp_path / "sym"
    shard_dir.mkdir()
    (shard_dir / "2024-09-06.npy").write_text("x", encoding="utf-8")
    (shard_dir / "2024-09-07.npy").write_text("x", encoding="utf-8")
    assert cm_index_utils.get_min_shard_ts_from_filenames(
        str(shard_dir), lambda key: (123 if key == "2024-09-06" else 456, 0)
    ) == 123


def test_maybe_update_inception_ts_updates_only_when_earlier():
    arr = np.array([(5,)], dtype=[("ts", np.int64)])
    calls = []
    cm = types.SimpleNamespace(
        _get_inception_ts=lambda symbol: 10,
        _set_inception_ts=lambda symbol, ts, save=True: calls.append((symbol, ts, save)),
    )

    changed = cm_index_utils.maybe_update_inception_ts(cm, "BTC", arr, save=False)

    assert changed is True
    assert calls == [("BTC", 5, False)]
