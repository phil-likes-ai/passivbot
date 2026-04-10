from importlib import import_module
import types


cm_logging_utils = import_module("candlestick_manager_logging_utils")


def test_fmt_ts_formats_and_falls_back():
    assert cm_logging_utils.fmt_ts(0).startswith("1970-01-01T00:00:00")
    assert cm_logging_utils.fmt_ts(None) == "-"


def test_progress_log_throttles(monkeypatch):
    calls = []
    monkeypatch.setattr(cm_logging_utils.time, "monotonic", lambda: 10.0)
    cm = types.SimpleNamespace(
        _progress_log_interval_seconds=5.0,
        _progress_last_log={},
        _log=lambda level, event, **fields: calls.append((level, event, fields)),
    )

    cm_logging_utils.progress_log(cm, ("a", "b", "c"), "evt", x=1)
    cm_logging_utils.progress_log(cm, ("a", "b", "c"), "evt", x=1)

    assert calls == [("debug", "evt", {"x": 1})]


def test_throttled_warning_throttles(monkeypatch):
    calls = []
    monkeypatch.setattr(cm_logging_utils.time, "monotonic", lambda: 10.0)
    cm = types.SimpleNamespace(
        _warning_last_log={},
        _warning_throttle_seconds=5.0,
        _log=lambda level, event, **fields: calls.append((level, event, fields)),
    )

    cm_logging_utils.throttled_warning(cm, "k", "evt", y=2)
    cm_logging_utils.throttled_warning(cm, "k", "evt", y=2)

    assert calls == [("warning", "evt", {"y": 2})]


def test_emit_remote_fetch_calls_callback_and_swallows_errors():
    calls = []
    cm = type("CM", (), {"_remote_fetch_callback": lambda self, payload: calls.append(payload)})()
    cm_logging_utils.emit_remote_fetch(cm, {"x": 1})
    assert calls == [{"x": 1}]

    bad = type("CM", (), {"_remote_fetch_callback": lambda self, payload: (_ for _ in ()).throw(RuntimeError("boom"))})()
    assert cm_logging_utils.emit_remote_fetch(bad, {"x": 1}) is None


def test_record_and_log_strict_gaps_summary(monkeypatch):
    monkeypatch.setattr(cm_logging_utils.time, "monotonic", lambda: 20.0)
    calls = []
    cm = types.SimpleNamespace(
        _strict_gaps_summary={},
        _strict_gaps_summary_last_log=0.0,
        _strict_gaps_summary_interval=1.0,
        log=types.SimpleNamespace(debug=lambda *args: calls.append(args)),
    )

    cm_logging_utils.record_strict_gap(cm, "BTC", 3)
    cm_logging_utils.record_strict_gap(cm, "ETH", 1)
    cm_logging_utils.log_strict_gaps_summary(cm)

    assert calls
    assert calls[0][0].startswith("[candle] strict mode gaps:")
    assert cm._strict_gaps_summary == {}


def test_log_persistent_gap_summary_throttles_and_clears(monkeypatch):
    monkeypatch.setattr(cm_logging_utils.time, "monotonic", lambda: 2000.0)
    calls = []
    cm = types.SimpleNamespace(
        _persistent_gap_summary={"BTC": 3, "ETH": 1},
        _persistent_gap_summary_last_log=0.0,
        log=types.SimpleNamespace(info=lambda *args: calls.append(args)),
    )

    cm_logging_utils.log_persistent_gap_summary(cm)

    assert calls
    assert calls[0][0].startswith("[candle] persistent gaps:")
    assert cm._persistent_gap_summary == {}
