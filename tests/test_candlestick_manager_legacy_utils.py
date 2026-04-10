from importlib import import_module
import types

import numpy as np


cm_legacy_utils = import_module("candlestick_manager_legacy_utils")


def test_legacy_day_is_complete_uses_cache():
    cm = types.SimpleNamespace(_legacy_day_quality_cache={("BTC", "1m", "2024-01-01"): True})

    assert cm_legacy_utils.legacy_day_is_complete(cm, "BTC", "1m", "2024-01-01") is True


def test_legacy_day_is_complete_true_for_full_continuous_day(monkeypatch):
    one_min = 60_000
    monkeypatch.setitem(__import__("candlestick_manager").__dict__, "ONE_MIN_MS", one_min)
    monkeypatch.setattr(cm_legacy_utils.os.path, "exists", lambda path: True)
    ts = np.arange(0, 1440 * one_min, one_min, dtype=np.int64)
    arr = np.empty((ts.shape[0],), dtype=[("ts", np.int64)])
    arr["ts"] = ts
    cm = types.SimpleNamespace(
        _legacy_day_quality_cache={},
        _get_legacy_shard_paths=lambda symbol, tf: {"2024-01-01": "dummy.npy"},
        _load_shard=lambda path: arr,
        _date_range_of_key=lambda key: (0, int(ts[-1])),
    )

    assert cm_legacy_utils.legacy_day_is_complete(cm, "BTC", "1m", "2024-01-01") is True
