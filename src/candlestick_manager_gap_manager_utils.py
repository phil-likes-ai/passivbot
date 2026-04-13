from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import candlestick_manager_gap_utils as cm_gap_utils


def get_known_gaps_enhanced(
    self,
    symbol: str,
    *,
    now_ms: int,
    gap_reason_auto: str,
    gap_max_retries: int,
) -> List[dict[str, int | str]]:
    idx = self._ensure_symbol_index(symbol)
    gaps = idx.get("meta", {}).get("known_gaps", [])
    return cm_gap_utils.get_known_gaps_enhanced(
        gaps,
        now_ms=now_ms,
        gap_reason_auto=gap_reason_auto,
        gap_max_retries=gap_max_retries,
    )


def get_known_gaps(
    self,
    symbol: str,
    *,
    now_ms: int,
    gap_reason_auto: str,
    gap_max_retries: int,
) -> List[Tuple[int, int]]:
    return cm_gap_utils.simplify_known_gaps(
        get_known_gaps_enhanced(
            self,
            symbol,
            now_ms=now_ms,
            gap_reason_auto=gap_reason_auto,
            gap_max_retries=gap_max_retries,
        )
    )


def save_known_gaps_enhanced(
    self,
    symbol: str,
    gaps: List[dict[str, int | str]],
    *,
    one_min_ms: int,
    gap_reason_auto: str,
) -> None:
    merged = cm_gap_utils.merge_enhanced_gaps(
        gaps, one_min_ms=one_min_ms, gap_reason_auto=gap_reason_auto
    )
    idx = self._ensure_symbol_index(symbol)
    idx["meta"]["known_gaps"] = cm_gap_utils.serialize_known_gaps(
        merged, gap_reason_auto=gap_reason_auto
    )
    self._index[symbol] = idx
    self._save_index(symbol)


def save_known_gaps(
    self,
    symbol: str,
    gaps: List[Tuple[int, int]],
    *,
    now_ms: int,
    gap_reason_auto: str,
    gap_max_retries: int,
    one_min_ms: int,
) -> None:
    enhanced = cm_gap_utils.build_enhanced_gaps_from_tuples(
        gaps,
        now_ms=now_ms,
        gap_reason_auto=gap_reason_auto,
        gap_max_retries=gap_max_retries,
    )
    save_known_gaps_enhanced(
        self,
        symbol,
        enhanced,
        one_min_ms=one_min_ms,
        gap_reason_auto=gap_reason_auto,
    )


def add_known_gap(
    self,
    symbol: str,
    start_ts: int,
    end_ts: int,
    *,
    reason: str,
    increment_retry: bool,
    retry_count: Optional[int],
    now_ms: int,
    one_min_ms: int,
    gap_max_retries: int,
    gap_reason_auto: str,
) -> None:
    gaps = get_known_gaps_enhanced(
        self,
        symbol,
        now_ms=now_ms,
        gap_reason_auto=gap_reason_auto,
        gap_max_retries=gap_max_retries,
    )
    gaps, updated, previous_retry_count, updated_gap = cm_gap_utils.add_known_gap(
        gaps,
        start_ts=int(start_ts),
        end_ts=int(end_ts),
        reason=reason,
        increment_retry=increment_retry,
        retry_count=retry_count,
        now_ms=now_ms,
        one_min_ms=one_min_ms,
        gap_max_retries=gap_max_retries,
    )

    if not updated and updated_gap is not None:
        self._log(
            "debug",
            "gap_added",
            symbol=symbol,
            start_ts=start_ts,
            end_ts=end_ts,
            reason=reason,
            retry_count=updated_gap["retry_count"],
        )
    elif cm_gap_utils.should_warn_gap_became_persistent(
        updated_gap, previous_retry_count, gap_max_retries=gap_max_retries
    ):
        if not hasattr(self, "_persistent_gap_summary"):
            self._persistent_gap_summary = {}
        self._persistent_gap_summary[symbol] = self._persistent_gap_summary.get(symbol, 0) + 1

    save_known_gaps_enhanced(
        self,
        symbol,
        gaps,
        one_min_ms=one_min_ms,
        gap_reason_auto=gap_reason_auto,
    )


def record_verified_gap(
    self,
    symbol: str,
    start_ts: int,
    end_ts: int,
    *,
    reason: str,
    now_ms: int,
    one_min_ms: int,
    gap_max_retries: int,
    gap_reason_auto: str,
) -> None:
    if start_ts > end_ts:
        return
    start_ts, end_ts, reason, increment_retry, retry_count = cm_gap_utils.record_verified_gap_payload(
        start_ts,
        end_ts,
        reason=reason,
        gap_max_retries=gap_max_retries,
    )
    add_known_gap(
        self,
        symbol,
        start_ts,
        end_ts,
        reason=reason,
        increment_retry=increment_retry,
        retry_count=retry_count,
        now_ms=now_ms,
        one_min_ms=one_min_ms,
        gap_max_retries=gap_max_retries,
        gap_reason_auto=gap_reason_auto,
    )


def should_retry_gap(gap: dict[str, int | str], *, gap_max_retries: int) -> bool:
    return cm_gap_utils.should_retry_gap(gap, gap_max_retries=gap_max_retries)


def clear_known_gaps(
    self,
    symbol: str,
    *,
    date_range: Optional[Tuple[int, int]],
    now_ms: int,
    one_min_ms: int,
    gap_reason_auto: str,
    gap_max_retries: int,
) -> int:
    gaps = get_known_gaps_enhanced(
        self,
        symbol,
        now_ms=now_ms,
        gap_reason_auto=gap_reason_auto,
        gap_max_retries=gap_max_retries,
    )
    cleared, remaining = cm_gap_utils.clear_known_gaps(gaps, date_range)
    if cleared == 0:
        return 0
    if date_range is None:
        idx = self._ensure_symbol_index(symbol)
        idx["meta"]["known_gaps"] = []
        self._index[symbol] = idx
        self._save_index(symbol)
        self._log("info", "gaps_cleared", symbol=symbol, cleared_count=cleared)
        return cleared

    range_start, range_end = date_range
    save_known_gaps_enhanced(
        self,
        symbol,
        remaining,
        one_min_ms=one_min_ms,
        gap_reason_auto=gap_reason_auto,
    )
    self._log(
        "info",
        "gaps_cleared",
        symbol=symbol,
        cleared_count=cleared,
        date_range_start=range_start,
        date_range_end=range_end,
    )
    return cleared


def get_gap_summary(
    self,
    symbol: str,
    *,
    now_ms: int,
    one_min_ms: int,
    gap_reason_auto: str,
    gap_max_retries: int,
) -> Dict[str, Any]:
    gaps = get_known_gaps_enhanced(
        self,
        symbol,
        now_ms=now_ms,
        gap_reason_auto=gap_reason_auto,
        gap_max_retries=gap_max_retries,
    )
    summary = cm_gap_utils.gap_summary(gaps, one_min_ms=one_min_ms, gap_max_retries=gap_max_retries)
    if not summary["gaps"]:
        return summary
    return {
        "total_gaps": summary["total_gaps"],
        "total_minutes": summary["total_minutes"],
        "persistent_gaps": summary["persistent_gaps"],
        "retryable_gaps": summary["retryable_gaps"],
        "by_reason": summary["by_reason"],
        "gaps": [
            {
                "start_ts": g["start_ts"],
                "end_ts": g["end_ts"],
                "minutes": (g["end_ts"] - g["start_ts"]) // one_min_ms + 1,
                "retry_count": g.get("retry_count", 0),
                "reason": g.get("reason", gap_reason_auto),
                "persistent": g.get("retry_count", 0) >= gap_max_retries,
            }
            for g in gaps
        ],
    }
