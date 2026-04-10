import types
from importlib import import_module


pb_fetch_budget_utils = import_module("passivbot_fetch_budget_utils")


def test_compute_fetch_budget_ttls_prefers_most_stale_symbols(monkeypatch):
    monkeypatch.setattr(pb_fetch_budget_utils, "utc_ms", lambda: 10_000)
    bot = types.SimpleNamespace(cm=types.SimpleNamespace(get_last_refresh_ms=lambda s: {"a": 9_000, "b": 5_000, "c": 0}[s]))

    ttl_map, cache_only = pb_fetch_budget_utils.compute_fetch_budget_ttls(
        bot, ["a", "b", "c"], max_age_ms=1_000, max_network_fetches=1
    )

    assert ttl_map["c"] == 1_000
    assert ttl_map["a"] == pb_fetch_budget_utils.CACHE_ONLY_TTL_MS
    assert ttl_map["b"] == pb_fetch_budget_utils.CACHE_ONLY_TTL_MS
    assert cache_only == set()


def test_compute_fetch_budget_ttls_without_budget_uses_real_ttl_for_all():
    bot = types.SimpleNamespace(cm=types.SimpleNamespace(get_last_refresh_ms=lambda s: 0))

    ttl_map, cache_only = pb_fetch_budget_utils.compute_fetch_budget_ttls(
        bot, ["a", "b"], max_age_ms=500, max_network_fetches=None
    )

    assert ttl_map == {"a": 500, "b": 500}
    assert cache_only == set()


def test_compute_fetch_budget_ttls_marks_cache_only_on_refresh_lookup_error(monkeypatch):
    monkeypatch.setattr(pb_fetch_budget_utils, "utc_ms", lambda: 10_000)

    def get_last_refresh_ms(symbol):
        if symbol == "bad":
            raise RuntimeError("boom")
        return 9_000

    bot = types.SimpleNamespace(cm=types.SimpleNamespace(get_last_refresh_ms=get_last_refresh_ms))

    ttl_map, cache_only = pb_fetch_budget_utils.compute_fetch_budget_ttls(
        bot, ["ok", "bad"], max_age_ms=100, max_network_fetches=1
    )

    assert ttl_map["bad"] == 100
    assert ttl_map["ok"] == pb_fetch_budget_utils.CACHE_ONLY_TTL_MS
    assert cache_only == set()
