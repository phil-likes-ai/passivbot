from importlib import import_module
import types

import numpy as np


cm_shard_utils = import_module("candlestick_manager_shard_utils")


def test_load_shard_returns_empty_on_missing_file(monkeypatch):
    monkeypatch.setattr(cm_shard_utils.os.path, "exists", lambda path: False)
    cm = types.SimpleNamespace(log=types.SimpleNamespace(debug=lambda *args, **kwargs: None))

    result = cm_shard_utils.load_shard(cm, "missing.npy")

    assert result.shape == (0,)


def test_load_shard_converts_legacy_2d_array(monkeypatch, tmp_path):
    path = tmp_path / "legacy.npy"
    arr = np.asarray([[1, 2, 3, 4, 5, 6]], dtype=np.float64)
    with open(path, "wb") as f:
        np.save(f, arr, allow_pickle=False)

    cm = types.SimpleNamespace(log=types.SimpleNamespace(debug=lambda *args, **kwargs: None, warning=lambda *args, **kwargs: None))
    result = cm_shard_utils.load_shard(cm, str(path))

    assert result.shape == (1,)
    assert int(result["ts"][0]) == 1
    assert float(result["c"][0]) == 5.0
