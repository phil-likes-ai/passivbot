from importlib import import_module
import types


cache_utils = import_module("fill_events_cache_utils")


def test_metadata_path_joins_root():
    root = __import__("pathlib").Path("cache")
    cache = types.SimpleNamespace(root=root)
    assert str(cache_utils.metadata_path(cache)).endswith("cache{}metadata.json".format(__import__('os').sep))


def test_load_metadata_returns_default_when_missing(tmp_path):
    cache = types.SimpleNamespace(root=tmp_path, _metadata=None)
    cache.metadata_path = cache_utils.metadata_path(cache)

    data = cache_utils.load_metadata(cache)

    assert data["history_scope"] == "unknown"
    assert data["known_gaps"] == []


def test_save_metadata_writes_file(tmp_path):
    logger = types.SimpleNamespace(debug=lambda *args, **kwargs: None, error=lambda *args, **kwargs: None)
    cache = types.SimpleNamespace(root=tmp_path, _metadata={"history_scope": "all"}, logger=logger)
    cache.metadata_path = cache_utils.metadata_path(cache)

    cache_utils.save_metadata(cache)

    assert cache.metadata_path.exists()


def test_add_known_gap_and_clear_gap_roundtrip(monkeypatch):
    logs = []
    logger = types.SimpleNamespace(info=lambda *args, **kwargs: logs.append(args), debug=lambda *a, **k: None, error=lambda *a, **k: None, warning=lambda *a, **k: None)
    cache = types.SimpleNamespace(
        _metadata={"known_gaps": []},
        logger=logger,
        load_metadata=lambda: cache._metadata,
        save_metadata=lambda metadata=None: None,
        get_history_scope=lambda: "unknown",
    )

    cache_utils.add_known_gap(
        cache,
        1000,
        2000,
        reason="auto",
        confidence=0.0,
        gap_max_retries=3,
        likely_legitimate_confidence=0.7,
    )
    assert len(cache._metadata["known_gaps"]) == 1
    assert cache_utils.clear_gap(cache, 1000, 2000) is True


def test_should_retry_gap_and_coverage_summary():
    cache = types.SimpleNamespace(
        _metadata={
            "oldest_event_ts": 1,
            "newest_event_ts": 2,
            "covered_start_ms": 3,
            "last_refresh_ms": 4,
            "known_gaps": [{"start_ts": 0, "end_ts": 1000, "retry_count": 5, "reason": "x", "confidence": 0.5}],
        },
        load_metadata=lambda: cache._metadata,
        get_history_scope=lambda: "all",
    )

    assert cache_utils.should_retry_gap({"retry_count": 1}, 3) is True
    summary = cache_utils.get_coverage_summary(cache, gap_max_retries=3)
    assert summary["persistent_gaps"] == 1
    assert summary["retryable_gaps"] == 0


def test_history_scope_and_covered_start_helpers():
    cache = types.SimpleNamespace(
        _metadata={"covered_start_ms": 0, "history_scope": "unknown", "last_refresh_ms": 0},
        load_metadata=lambda: cache._metadata,
        save_metadata=lambda metadata=None: None,
    )

    cache_utils.mark_covered_start(cache, 100)
    assert cache_utils.get_covered_start_ms(cache) == 100

    cache_utils.set_history_scope(cache, "all")
    assert cache_utils.get_history_scope(cache) == "all"


def test_load_events_reads_and_sorts_records(tmp_path):
    path = tmp_path / "2024-01-01.json"
    path.write_text('[{"id":"b","timestamp":2,"symbol":"BTC","side":"buy","qty":1,"price":1,"pnl":0,"pb_order_type":"x","position_side":"long","client_order_id":"c"},{"id":"a","timestamp":1,"symbol":"BTC","side":"buy","qty":1,"price":1,"pnl":0,"pb_order_type":"x","position_side":"long","client_order_id":"c"}]', encoding="utf-8")
    cls = type("EventCls", (), {"from_dict": staticmethod(lambda raw: type("E", (), {"timestamp": raw["timestamp"], "id": raw["id"]})())})
    logger = types.SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None, debug=lambda *a, **k: None)
    cache = types.SimpleNamespace(root=tmp_path, logger=logger)

    events = cache_utils.load_events(cache, cls)

    assert [e.id for e in events] == ["a", "b"]


def test_save_days_and_update_metadata_from_events(tmp_path):
    logger = types.SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None, debug=lambda *a, **k: None, error=lambda *a, **k: None)
    cache = types.SimpleNamespace(root=tmp_path, logger=logger, _metadata={"oldest_event_ts": 0, "newest_event_ts": 0, "last_refresh_ms": 0})
    cache.metadata_path = cache_utils.metadata_path(cache)
    cache.save_metadata = lambda metadata=None: cache_utils.save_metadata(cache, metadata)
    cache.load_metadata = lambda: cache._metadata

    event = type("E", (), {"timestamp": 1, "to_dict": lambda self: {"timestamp": 1}})()
    cache_utils.save_days(cache, {"1970-01-01": [event]})
    assert (tmp_path / "1970-01-01.json").exists()

    cache_utils.update_metadata_from_events(cache, [event])
    assert cache._metadata["oldest_event_ts"] == 1
    assert cache._metadata["newest_event_ts"] == 1
