from importlib import import_module
import time

from candlestick_manager import ONE_MIN_MS


cm_misc_utils = import_module("candlestick_manager_misc_utils")


def test_looks_like_daily_shard_filename_accepts_only_daily_npy_names():
    assert cm_misc_utils.looks_like_daily_shard_filename("2024-09-06.npy")
    assert not cm_misc_utils.looks_like_daily_shard_filename("2024-9-06.npy")
    assert not cm_misc_utils.looks_like_daily_shard_filename("index.json")


def test_tf_to_ms_parses_supported_units_and_falls_back():
    assert cm_misc_utils.tf_to_ms("1m", ONE_MIN_MS) == ONE_MIN_MS
    assert cm_misc_utils.tf_to_ms("2h", ONE_MIN_MS) == 120 * ONE_MIN_MS
    assert cm_misc_utils.tf_to_ms("30s", ONE_MIN_MS) == ONE_MIN_MS
    assert cm_misc_utils.tf_to_ms("bad", ONE_MIN_MS) == ONE_MIN_MS


def test_quarantine_root_level_timeframe_debris_moves_invalid_root_files(tmp_path):
    tf_dir = tmp_path / "demo" / "1m"
    tf_dir.mkdir(parents=True)
    (tf_dir / "index.json").write_text("{}", encoding="utf-8")
    (tf_dir / "2024-09-06.npy").write_text("x", encoding="utf-8")
    (tf_dir / "BTC_USDT").mkdir()

    moved = cm_misc_utils.quarantine_root_level_timeframe_debris(str(tmp_path))

    assert moved == 2
    assert not (tf_dir / "index.json").exists()
    assert not (tf_dir / "2024-09-06.npy").exists()


def test_utc_now_ms_tracks_time(monkeypatch):
    monkeypatch.setattr(time, "time", lambda: 123.456)
    assert cm_misc_utils.utc_now_ms() == 123456


def test_get_caller_name_returns_current_function_name_suffix():
    def outer():
        return cm_misc_utils.get_caller_name(depth=1)

    result = outer()
    assert result.endswith("outer")


def test_quarantine_gateio_cache_if_stale_moves_old_cache(tmp_path, monkeypatch):
    gateio_symbol_dir = tmp_path / "gateio" / "1m" / "BTC_USDT"
    gateio_symbol_dir.mkdir(parents=True)
    (gateio_symbol_dir / "2024-01-01.npy").write_text("x", encoding="utf-8")
    monkeypatch.setattr(cm_misc_utils, "utc_now_datetime", lambda: cm_misc_utils.datetime(2026, 1, 2, 3, 4, 5))

    cm_misc_utils.quarantine_gateio_cache_if_stale(str(tmp_path), "2024-02-01")

    assert not (tmp_path / "gateio").exists()
    backups = list(tmp_path.glob("gateio_backup_*"))
    assert len(backups) == 1
