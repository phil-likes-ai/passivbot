from importlib import import_module


cm_gap_utils = import_module("candlestick_manager_gap_utils")


def test_get_known_gaps_enhanced_supports_new_and_legacy_formats():
    result = cm_gap_utils.get_known_gaps_enhanced(
        [
            {"start_ts": 1, "end_ts": 2, "retry_count": 1, "reason": "x", "added_at": 3},
            [4, 5],
            "bad",
        ],
        now_ms=99,
        gap_reason_auto="auto_detected",
        gap_max_retries=3,
    )

    assert result[0]["start_ts"] == 1
    assert result[1]["retry_count"] == 3
    assert result[1]["added_at"] == 99


def test_merge_and_serialize_known_gaps():
    merged = cm_gap_utils.merge_enhanced_gaps(
        [
            {"start_ts": 0, "end_ts": 60_000, "retry_count": 1, "reason": "a", "added_at": 5},
            {"start_ts": 60_000, "end_ts": 120_000, "retry_count": 3, "reason": "b", "added_at": 4},
        ],
        one_min_ms=60_000,
        gap_reason_auto="auto_detected",
    )

    assert merged == [
        {"start_ts": 0, "end_ts": 120_000, "retry_count": 3, "reason": "a", "added_at": 4}
    ]
    assert cm_gap_utils.serialize_known_gaps(merged, gap_reason_auto="auto_detected")[0]["end_ts"] == 120_000


def test_build_and_simplify_gap_formats():
    enhanced = cm_gap_utils.build_enhanced_gaps_from_tuples(
        [(1, 2)], now_ms=10, gap_reason_auto="auto_detected", gap_max_retries=3
    )

    assert enhanced == [
        {"start_ts": 1, "end_ts": 2, "retry_count": 3, "reason": "auto_detected", "added_at": 10}
    ]
    assert cm_gap_utils.simplify_known_gaps(enhanced) == [(1, 2)]


def test_add_known_gap_and_persistent_transition_helpers():
    gaps, updated, previous_retry, updated_gap = cm_gap_utils.add_known_gap(
        [{"start_ts": 0, "end_ts": 60_000, "retry_count": 2, "reason": "auto_detected", "added_at": 1}],
        start_ts=60_000,
        end_ts=120_000,
        reason="fetch_failed",
        increment_retry=True,
        retry_count=None,
        now_ms=10,
        one_min_ms=60_000,
        gap_max_retries=3,
    )

    assert updated is True
    assert previous_retry == 2
    assert updated_gap == {
        "start_ts": 0,
        "end_ts": 120_000,
        "retry_count": 3,
        "reason": "fetch_failed",
        "added_at": 1,
    }
    assert cm_gap_utils.should_warn_gap_became_persistent(
        updated_gap, previous_retry, gap_max_retries=3
    )


def test_clear_known_gaps_and_summary_helpers():
    gaps = [
        {"start_ts": 0, "end_ts": 60_000, "retry_count": 3, "reason": "a", "added_at": 1},
        {"start_ts": 120_000, "end_ts": 180_000, "retry_count": 1, "reason": "b", "added_at": 2},
    ]

    cleared, remaining = cm_gap_utils.clear_known_gaps(gaps, (0, 90_000))
    summary = cm_gap_utils.gap_summary(gaps, one_min_ms=60_000, gap_max_retries=3)

    assert cleared == 1
    assert remaining == [gaps[1]]
    assert cm_gap_utils.should_retry_gap(gaps[1], gap_max_retries=3)
    assert summary["total_gaps"] == 2
    assert summary["persistent_gaps"] == 1
    assert summary["retryable_gaps"] == 1


def test_record_verified_gap_payload_marks_gap_persistent():
    assert cm_gap_utils.record_verified_gap_payload(
        1, 2, reason="no_trades", gap_max_retries=3
    ) == (1, 2, "no_trades", False, 3)


def test_prune_pre_inception_gaps_trims_and_removes_future_ranges():
    gaps, changed = cm_gap_utils.prune_pre_inception_gaps(
        [
            {"start_ts": 0, "end_ts": 120_000, "retry_count": 3, "reason": "pre_inception", "added_at": 1},
            {"start_ts": 180_000, "end_ts": 240_000, "retry_count": 3, "reason": "pre_inception", "added_at": 2},
            {"start_ts": 0, "end_ts": 60_000, "retry_count": 1, "reason": "other", "added_at": 3},
        ],
        180_000,
        one_min_ms=60_000,
    )

    assert changed is True
    assert gaps == [
        {"start_ts": 0, "end_ts": 120_000, "retry_count": 3, "reason": "pre_inception", "added_at": 1},
        {"start_ts": 0, "end_ts": 60_000, "retry_count": 1, "reason": "other", "added_at": 3},
    ]
