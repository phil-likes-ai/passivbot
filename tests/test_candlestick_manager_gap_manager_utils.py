from importlib import import_module


cm_gap_mgr_utils = import_module("candlestick_manager_gap_manager_utils")


class _FakeManager:
    def __init__(self):
        self._index = {}
        self.saved = []
        self.logs = []

    def _ensure_symbol_index(self, symbol):
        return self._index.setdefault(symbol, {"meta": {}})

    def _save_index(self, symbol):
        self.saved.append(symbol)

    def _log(self, level, event, **payload):
        self.logs.append((level, event, payload))


def test_gap_manager_roundtrip_add_clear_and_summary():
    mgr = _FakeManager()

    cm_gap_mgr_utils.add_known_gap(
        mgr,
        "BTC/USDT",
        0,
        60_000,
        reason="fetch_failed",
        increment_retry=True,
        retry_count=None,
        now_ms=10,
        one_min_ms=60_000,
        gap_max_retries=3,
        gap_reason_auto="auto_detected",
    )

    gaps = cm_gap_mgr_utils.get_known_gaps_enhanced(
        mgr,
        "BTC/USDT",
        now_ms=10,
        gap_reason_auto="auto_detected",
        gap_max_retries=3,
    )
    assert gaps[0]["retry_count"] == 1

    summary = cm_gap_mgr_utils.get_gap_summary(
        mgr,
        "BTC/USDT",
        now_ms=10,
        one_min_ms=60_000,
        gap_reason_auto="auto_detected",
        gap_max_retries=3,
    )
    assert summary["total_gaps"] == 1
    assert summary["retryable_gaps"] == 1

    cleared = cm_gap_mgr_utils.clear_known_gaps(
        mgr,
        "BTC/USDT",
        date_range=(0, 90_000),
        now_ms=10,
        one_min_ms=60_000,
        gap_reason_auto="auto_detected",
        gap_max_retries=3,
    )
    assert cleared == 1
    assert (
        cm_gap_mgr_utils.get_known_gaps(
            mgr,
            "BTC/USDT",
            now_ms=10,
            gap_reason_auto="auto_detected",
            gap_max_retries=3,
        )
        == []
    )


def test_record_verified_gap_marks_persistent_gap():
    mgr = _FakeManager()

    cm_gap_mgr_utils.record_verified_gap(
        mgr,
        "ETH/USDT",
        120_000,
        180_000,
        reason="no_trades",
        now_ms=10,
        one_min_ms=60_000,
        gap_max_retries=3,
        gap_reason_auto="auto_detected",
    )

    summary = cm_gap_mgr_utils.get_gap_summary(
        mgr,
        "ETH/USDT",
        now_ms=10,
        one_min_ms=60_000,
        gap_reason_auto="auto_detected",
        gap_max_retries=3,
    )
    assert summary["persistent_gaps"] == 1
    assert summary["gaps"][0]["reason"] == "no_trades"
