from importlib import import_module

import numpy as np


cm_disk_utils = import_module("candlestick_manager_disk_utils")


def test_plan_disk_load_prefers_complete_legacy_and_merge_when_incomplete():
    days = {
        "2024-09-06": (0, 1),
        "2024-09-07": (2, 3),
        "2024-09-08": (4, 5),
    }
    shard_paths = {
        "2024-09-06": "primary-06.npy",
        "2024-09-07": "primary-07.npy",
    }
    legacy_paths = {
        "2024-09-06": "legacy-06.npy",
        "2024-09-07": "legacy-07.npy",
        "2024-09-08": "legacy-08.npy",
    }

    def legacy_complete(symbol, tf, key):
        return key == "2024-09-06"

    load_keys, day_ctx, legacy_hits, primary_hits, merged_hits = cm_disk_utils.plan_disk_load(
        "BTC/USDT:USDT", "1m", days, shard_paths, legacy_paths, legacy_complete
    )

    assert load_keys == [
        ("2024-09-06", "legacy-06.npy"),
        ("2024-09-07", "legacy-07.npy"),
        ("2024-09-08", "legacy-08.npy"),
    ]
    assert day_ctx["2024-09-06"]["source"] == "legacy"
    assert day_ctx["2024-09-07"]["source"] == "merge"
    assert day_ctx["2024-09-08"]["source"] == "legacy"
    assert legacy_hits == 2
    assert primary_hits == 0
    assert merged_hits == 1


def test_plan_disk_load_prefers_primary_for_non_1m():
    load_keys, day_ctx, legacy_hits, primary_hits, merged_hits = cm_disk_utils.plan_disk_load(
        "BTC/USDT:USDT",
        "1h",
        {"2024-09-06": (0, 1)},
        {"2024-09-06": "primary.npy"},
        {"2024-09-06": "legacy.npy"},
        lambda symbol, tf, key: False,
    )

    assert load_keys == [("2024-09-06", "primary.npy")]
    assert day_ctx["2024-09-06"]["source"] == "primary"
    assert legacy_hits == 0
    assert primary_hits == 1
    assert merged_hits == 0


def test_execute_disk_load_merges_primary_into_legacy_gaps_and_reports_progress():
    dtype = np.dtype([("ts", np.int64)])
    loaded = {
        "legacy.npy": np.array([(2,), (3,)], dtype=dtype),
        "primary.npy": np.array([(1,), (3,)], dtype=dtype),
        "plain.npy": np.array([(5,)], dtype=dtype),
    }
    progress = []

    arrays, _t0 = cm_disk_utils.execute_disk_load(
        [("2024-09-06", "legacy.npy"), ("2024-09-07", "plain.npy")],
        {
            "2024-09-06": {"source": "merge", "primary_path": "primary.npy"},
            "2024-09-07": {"source": "primary"},
        },
        "1m",
        load_shard_fn=lambda path: loaded[path],
        merge_overwrite_fn=lambda primary, legacy: np.unique(
            np.concatenate([primary, legacy])
        ),
        candle_dtype=dtype,
        progress_cb=lambda loaded_count, total, day_key, elapsed: progress.append(
            (loaded_count, total, day_key)
        ),
    )

    assert len(arrays) == 2
    assert progress[-1] == (2, 2, "2024-09-07")


def test_finalize_disk_arrays_sorts_or_returns_none():
    dtype = np.dtype([("ts", np.int64)])
    arr = cm_disk_utils.finalize_disk_arrays(
        [np.array([(2,), (1,)], dtype=dtype), np.array([(3,)], dtype=dtype)]
    )

    assert arr is not None
    assert arr["ts"].tolist() == [1, 2, 3]
    assert cm_disk_utils.finalize_disk_arrays([]) is None


def test_bucket_rows_by_date_key_groups_rows_in_order():
    dtype = np.dtype([("ts", np.int64), ("v", np.int64)])
    arr = np.array([(1, 10), (2, 20), (3, 30)], dtype=dtype)

    buckets = cm_disk_utils.bucket_rows_by_date_key(
        arr, lambda ts: "day-a" if ts < 3 else "day-b"
    )

    assert [key for key, _chunk in buckets] == ["day-a", "day-b"]
    assert buckets[0][1]["ts"].tolist() == [1, 2]
    assert buckets[1][1]["ts"].tolist() == [3]


def test_save_incremental_buckets_merges_existing_and_defers_until_last():
    dtype = np.dtype([("ts", np.int64)])
    saved = []
    shard_paths = {"day-a": "existing-a.npy"}
    existing = {"existing-a.npy": np.array([(1,)], dtype=dtype)}

    result = cm_disk_utils.save_incremental_buckets(
        "BTC/USDT:USDT",
        [
            ("day-a", np.array([(2,)], dtype=dtype)),
            ("day-b", np.array([(3,)], dtype=dtype)),
        ],
        shard_paths,
        tf_norm="1m",
        defer_index=False,
        candle_dtype=dtype,
        load_shard_fn=lambda path: existing[path],
        merge_overwrite_fn=lambda left, right: np.sort(np.concatenate([left, right]), order="ts"),
        save_shard_fn=lambda symbol, key, merged, tf, defer_index: saved.append(
            (symbol, key, merged["ts"].tolist(), tf, defer_index)
        ),
        shard_path_fn=lambda symbol, key, tf: f"saved-{key}-{tf}.npy",
    )

    assert result == ["day-a", "day-b"]
    assert saved[0] == ("BTC/USDT:USDT", "day-a", [1, 2], "1m", True)
    assert saved[1] == ("BTC/USDT:USDT", "day-b", [3], "1m", False)
    assert shard_paths["day-b"] == "saved-day-b-1m.npy"
