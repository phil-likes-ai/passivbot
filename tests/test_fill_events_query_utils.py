from importlib import import_module
import types


query_utils = import_module("fill_events_query_utils")


def _ev(ts, pnl, symbol="BTC", pside="long", qty=1.0):
    return types.SimpleNamespace(timestamp=ts, pnl=pnl, symbol=symbol, position_side=pside, qty=qty)


def test_query_helpers_filter_and_accumulate():
    mgr = types.SimpleNamespace(_events=[_ev(1, 1.0, "BTC"), _ev(2, 2.0, "ETH"), _ev(3, 3.0, "BTC")])
    mgr.get_events = lambda start_ms=None, end_ms=None, symbol=None: query_utils.get_events(mgr, start_ms, end_ms, symbol)

    assert [e.timestamp for e in query_utils.get_events(mgr, 2, 3, "BTC")] == [3]
    assert query_utils.get_pnl_sum(mgr) == 6.0
    assert query_utils.get_pnl_cumsum(mgr) == [(1, 1.0), (2, 3.0), (3, 6.0)]


def test_query_helpers_reconstruct_positions_and_summary(monkeypatch):
    monkeypatch.setattr(query_utils, "_format_ms", lambda ts: f"T{ts}")
    mgr = types.SimpleNamespace(
        _events=[_ev(1, 1.0, "BTC", "long", 2.0), _ev(2, -1.0, "ETH", "short", -1.0)],
        cache=types.SimpleNamespace(get_coverage_summary=lambda: {"known_gaps": []}),
        exchange="bybit",
        user="alice",
    )

    assert query_utils.reconstruct_positions(mgr) == {"BTC:long": 2.0, "ETH:short": -1.0}
    assert query_utils.reconstruct_equity_curve(mgr, 10.0) == [(1, 11.0), (2, 10.0)]
    summary = query_utils.get_coverage_summary(mgr)
    assert summary["first_event"] == "T1"
    assert summary["last_event"] == "T2"
    assert summary["symbols_count"] == 2


def test_events_for_days_and_merge_intervals():
    events = [_ev(1_000, 1.0), _ev(86_400_000, 2.0)]
    grouped = query_utils.events_for_days(events, ["1970-01-01", "1970-01-02"])
    assert len(grouped["1970-01-01"]) == 1
    assert len(grouped["1970-01-02"]) == 1

    assert query_utils.merge_intervals([(0, 10), (5, 20), (30, 40)]) == [(0, 20), (30, 40)]
