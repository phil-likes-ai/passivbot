from __future__ import annotations

from typing import Any


def get_known_gaps_enhanced(
    raw_gaps: list[Any],
    *,
    now_ms: int,
    gap_reason_auto: str,
    gap_max_retries: int,
) -> list[dict[str, int | str]]:
    out: list[dict[str, int | str]] = []
    for item in raw_gaps:
        try:
            if isinstance(item, dict):
                entry = {
                    "start_ts": int(item.get("start_ts", 0)),
                    "end_ts": int(item.get("end_ts", 0)),
                    "retry_count": int(item.get("retry_count", 0)),
                    "reason": str(item.get("reason", gap_reason_auto)),
                    "added_at": int(item.get("added_at", now_ms)),
                }
                if entry["start_ts"] <= entry["end_ts"]:
                    out.append(entry)
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                start_ts, end_ts = int(item[0]), int(item[1])
                if start_ts <= end_ts:
                    out.append(
                        {
                            "start_ts": start_ts,
                            "end_ts": end_ts,
                            "retry_count": gap_max_retries,
                            "reason": gap_reason_auto,
                            "added_at": now_ms,
                        }
                    )
        except Exception:
            continue
    return out


def simplify_known_gaps(gaps: list[dict[str, int | str]]) -> list[tuple[int, int]]:
    return [(int(gap["start_ts"]), int(gap["end_ts"])) for gap in gaps]


def merge_enhanced_gaps(
    gaps: list[dict[str, int | str]], *, one_min_ms: int, gap_reason_auto: str
) -> list[dict[str, int | str]]:
    ordered = sorted(gaps, key=lambda gap: int(gap["start_ts"]))
    merged: list[dict[str, int | str]] = []
    for gap in ordered:
        if not merged or int(gap["start_ts"]) > int(merged[-1]["end_ts"]) + one_min_ms:
            merged.append(gap)
            continue
        prev = merged[-1]
        merged[-1] = {
            "start_ts": int(prev["start_ts"]),
            "end_ts": max(int(prev["end_ts"]), int(gap["end_ts"])),
            "retry_count": max(int(prev.get("retry_count", 0)), int(gap.get("retry_count", 0))),
            "reason": str(prev.get("reason", gap_reason_auto)),
            "added_at": min(int(prev.get("added_at", 0)), int(gap.get("added_at", 0))),
        }
    return merged


def serialize_known_gaps(
    gaps: list[dict[str, int | str]], *, gap_reason_auto: str
) -> list[dict[str, int | str]]:
    return [
        {
            "start_ts": int(gap["start_ts"]),
            "end_ts": int(gap["end_ts"]),
            "retry_count": int(gap.get("retry_count", 0)),
            "reason": str(gap.get("reason", gap_reason_auto)),
            "added_at": int(gap.get("added_at", 0)),
        }
        for gap in gaps
    ]


def build_enhanced_gaps_from_tuples(
    gaps: list[tuple[int, int]], *, now_ms: int, gap_reason_auto: str, gap_max_retries: int
) -> list[dict[str, int | str]]:
    return [
        {
            "start_ts": int(start_ts),
            "end_ts": int(end_ts),
            "retry_count": gap_max_retries,
            "reason": gap_reason_auto,
            "added_at": now_ms,
        }
        for start_ts, end_ts in gaps
    ]


def add_known_gap(
    gaps: list[dict[str, int | str]],
    *,
    start_ts: int,
    end_ts: int,
    reason: str,
    increment_retry: bool,
    retry_count: int | None,
    now_ms: int,
    one_min_ms: int,
    gap_max_retries: int,
) -> tuple[list[dict[str, int | str]], bool, int, dict[str, int | str] | None]:
    updated = False
    previous_retry_count = 0
    updated_gap = None
    for gap in gaps:
        if int(gap["start_ts"]) <= end_ts + one_min_ms and int(gap["end_ts"]) >= start_ts - one_min_ms:
            gap["start_ts"] = min(int(gap["start_ts"]), int(start_ts))
            gap["end_ts"] = max(int(gap["end_ts"]), int(end_ts))
            previous_retry_count = int(gap.get("retry_count", 0))
            if retry_count is not None:
                gap["retry_count"] = retry_count
            elif increment_retry:
                gap["retry_count"] = min(previous_retry_count + 1, gap_max_retries)
            if reason:
                gap["reason"] = reason
            updated = True
            updated_gap = gap
            break

    if not updated:
        initial_retry = retry_count if retry_count is not None else (1 if increment_retry else 0)
        updated_gap = {
            "start_ts": int(start_ts),
            "end_ts": int(end_ts),
            "retry_count": initial_retry,
            "reason": reason,
            "added_at": now_ms,
        }
        gaps.append(updated_gap)

    return gaps, updated, previous_retry_count, updated_gap


def should_warn_gap_became_persistent(
    gap: dict[str, int | str] | None,
    previous_retry_count: int,
    *,
    gap_max_retries: int,
) -> bool:
    if gap is None:
        return False
    current_retry_count = int(gap.get("retry_count", 0))
    gap_reason = str(gap.get("reason", ""))
    return (
        current_retry_count >= gap_max_retries
        and previous_retry_count < gap_max_retries
        and gap_reason != "pre_inception"
    )


def record_verified_gap_payload(
    start_ts: int,
    end_ts: int,
    *,
    reason: str,
    gap_max_retries: int,
) -> tuple[int, int, str, bool, int]:
    return int(start_ts), int(end_ts), reason, False, gap_max_retries


def should_retry_gap(gap: dict[str, int | str], *, gap_max_retries: int) -> bool:
    return int(gap.get("retry_count", 0)) < gap_max_retries


def clear_known_gaps(
    gaps: list[dict[str, int | str]], date_range: tuple[int, int] | None
) -> tuple[int, list[dict[str, int | str]]]:
    if not gaps:
        return 0, []
    if date_range is None:
        return len(gaps), []
    range_start, range_end = date_range
    remaining = []
    cleared = 0
    for gap in gaps:
        if int(gap["end_ts"]) < range_start or int(gap["start_ts"]) > range_end:
            remaining.append(gap)
        else:
            cleared += 1
    return cleared, remaining


def gap_summary(
    gaps: list[dict[str, int | str]], *, one_min_ms: int, gap_max_retries: int
) -> dict[str, object]:
    if not gaps:
        return {
            "total_gaps": 0,
            "total_minutes": 0,
            "persistent_gaps": 0,
            "retryable_gaps": 0,
            "by_reason": {},
            "gaps": [],
        }

    total_minutes = sum((int(g["end_ts"]) - int(g["start_ts"])) // one_min_ms + 1 for g in gaps)
    persistent = sum(1 for g in gaps if int(g.get("retry_count", 0)) >= gap_max_retries)
    by_reason: dict[str, int] = {}
    for gap in gaps:
        reason = str(gap.get("reason", ""))
        by_reason[reason] = by_reason.get(reason, 0) + 1
    return {
        "total_gaps": len(gaps),
        "total_minutes": total_minutes,
        "persistent_gaps": persistent,
        "retryable_gaps": len(gaps) - persistent,
        "by_reason": by_reason,
        "gaps": gaps,
    }


def prune_pre_inception_gaps(
    gaps: list[dict[str, int | str]], inception_ts: int, *, one_min_ms: int
) -> tuple[list[dict[str, int | str]], bool]:
    if not gaps:
        return gaps, False
    cutoff_end = int(inception_ts) - one_min_ms
    changed = False
    new_gaps: list[dict[str, int | str]] = []
    for gap in gaps:
        try:
            if str(gap.get("reason", "")) != "pre_inception":
                new_gaps.append(gap)
                continue
            start_ts = int(gap.get("start_ts", 0))
            end_ts = int(gap.get("end_ts", 0))
            if end_ts <= cutoff_end:
                new_gaps.append(gap)
                continue
            if start_ts <= cutoff_end:
                trimmed = {
                    "start_ts": start_ts,
                    "end_ts": cutoff_end,
                    "retry_count": int(gap.get("retry_count", 0)),
                    "reason": "pre_inception",
                    "added_at": int(gap.get("added_at", 0)),
                }
                if int(trimmed["start_ts"]) <= int(trimmed["end_ts"]):
                    new_gaps.append(trimmed)
                changed = True
                continue
            changed = True
        except Exception:
            new_gaps.append(gap)
    return new_gaps, changed
