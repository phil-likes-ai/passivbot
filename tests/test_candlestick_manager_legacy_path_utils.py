from importlib import import_module


cm_legacy_path_utils = import_module("candlestick_manager_legacy_path_utils")


def test_legacy_coin_from_symbol_handles_ccxt_and_underscored_forms():
    assert cm_legacy_path_utils.legacy_coin_from_symbol("BTC/USDT:USDT") == "BTC"
    assert cm_legacy_path_utils.legacy_coin_from_symbol("HYPE_USDT:USDT") == "HYPE"
    assert cm_legacy_path_utils.legacy_coin_from_symbol("") == ""


def test_legacy_symbol_code_from_symbol_returns_empty_on_failure():
    assert cm_legacy_path_utils.legacy_symbol_code_from_symbol(lambda value: value + "X", "BTC") == "BTCX"
    assert cm_legacy_path_utils.legacy_symbol_code_from_symbol(lambda value: (_ for _ in ()).throw(RuntimeError("boom")), "BTC") == ""


def test_legacy_shard_candidates_and_dirs_match_exchange_layouts():
    archive = lambda symbol: symbol.replace("/", "").replace(":", "")

    bybit_candidates = cm_legacy_path_utils.legacy_shard_candidates(
        "bybit", "BTC/USDT:USDT", "2024-09-06", "1m", archive
    )
    assert any("ohlcvs_bybit" in path for path in bybit_candidates)

    binance_dirs = cm_legacy_path_utils.legacy_shard_dirs(
        "binanceusdm", "BTC/USDT:USDT", "1m", archive
    )
    assert any("ohlcvs_futures" in path for path in binance_dirs)
    assert cm_legacy_path_utils.legacy_shard_candidates("okx", "BTC/USDT:USDT", "2024-09-06", "5m", archive) == []


def test_scan_legacy_shard_paths_collects_daily_files_and_preserves_first_duplicate(tmp_path):
    first = tmp_path / "a"
    second = tmp_path / "b"
    first.mkdir()
    second.mkdir()
    (first / "2024-09-06.npy").write_text("a", encoding="utf-8")
    (second / "2024-09-06.npy").write_text("b", encoding="utf-8")
    (second / "2024-09-07.npy").write_text("c", encoding="utf-8")

    mapping, scanned = cm_legacy_path_utils.scan_legacy_shard_paths([str(first), str(second)])

    assert scanned == [str(first), str(second)]
    assert mapping["2024-09-06"] == str(first / "2024-09-06.npy")
    assert mapping["2024-09-07"] == str(second / "2024-09-07.npy")
