from importlib import import_module


pagination_utils = import_module("fill_events_pagination_utils")


def test_check_pagination_progress_returns_key_for_new_params(monkeypatch):
    debug_calls = []
    monkeypatch.setattr(pagination_utils.logger, "debug", lambda *args: debug_calls.append(args))

    result = pagination_utils.check_pagination_progress(None, {"cursor": "a"}, "ctx")

    assert result == (("cursor", "a"),)
    assert debug_calls


def test_check_pagination_progress_returns_none_for_repeat(monkeypatch):
    warning_calls = []
    monkeypatch.setattr(pagination_utils.logger, "warning", lambda *args: warning_calls.append(args))

    prev = (("cursor", "a"),)
    result = pagination_utils.check_pagination_progress(prev, {"cursor": "a"}, "ctx")

    assert result is None
    assert warning_calls
