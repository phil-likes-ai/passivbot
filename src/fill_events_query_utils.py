from __future__ import annotations

from importlib import import_module
from typing import Iterable, List, Optional, Sequence, Tuple


def _format_ms(ts):
    return import_module("fill_events_time_utils").format_ms(ts)


def get_events(self, start_ms: Optional[int] = None, end_ms: Optional[int] = None, symbol: Optional[str] = None):
    """Get fill events with optional filtering."""
    events = self._events
    if start_ms is not None:
        events = [ev for ev in events if ev.timestamp >= start_ms]
    if end_ms is not None:
        events = [ev for ev in events if ev.timestamp <= end_ms]
    if symbol:
        events = [ev for ev in events if ev.symbol == symbol]
    return list(events)


def get_pnl_sum(self, start_ms: Optional[int] = None, end_ms: Optional[int] = None, symbol: Optional[str] = None) -> float:
    events = self.get_events(start_ms, end_ms, symbol)
    return float(sum(ev.pnl for ev in events))


def get_pnl_cumsum(self, start_ms: Optional[int] = None, end_ms: Optional[int] = None, symbol: Optional[str] = None) -> List[Tuple[int, float]]:
    events = self.get_events(start_ms, end_ms, symbol)
    total = 0.0
    result = []
    for ev in events:
        total += ev.pnl
        result.append((ev.timestamp, total))
    return result


def get_last_timestamp(self, symbol: Optional[str] = None) -> Optional[int]:
    events = self._events
    if symbol:
        events = [ev for ev in events if ev.symbol == symbol]
    if not events:
        return None
    return max(ev.timestamp for ev in events)


def reconstruct_positions(self, current_positions: Optional[dict[str, float]] = None) -> dict[str, float]:
    positions: dict[str, float] = dict(current_positions or {})
    for ev in self._events:
        key = f"{ev.symbol}:{ev.position_side}"
        positions[key] = positions.get(key, 0.0) + ev.qty
    return positions


def reconstruct_equity_curve(self, starting_equity: float = 0.0) -> list[tuple[int, float]]:
    total = starting_equity
    points: list[tuple[int, float]] = []
    for ev in self._events:
        total += ev.pnl
        points.append((ev.timestamp, total))
    return points


def get_coverage_summary(self) -> dict[str, object]:
    """Return a summary of cache coverage and known gaps."""
    summary = self.cache.get_coverage_summary()
    summary["events_count"] = len(self._events)
    summary["exchange"] = self.exchange
    summary["user"] = self.user
    if self._events:
        summary["first_event"] = _format_ms(self._events[0].timestamp)
        summary["last_event"] = _format_ms(self._events[-1].timestamp)
        symbols = set(ev.symbol for ev in self._events)
        summary["symbols_count"] = len(symbols)
        summary["symbols"] = sorted(symbols)
    return summary


def events_for_days(events: Iterable, days: Iterable[str]):
    from fill_events_time_utils import day_key

    target = {day: [] for day in days}
    for event in events:
        day = day_key(event.timestamp)
        if day in target:
            target[day].append(event)
    for day_events in target.values():
        day_events.sort(key=lambda ev: ev.timestamp)
    return target


def merge_intervals(intervals: Sequence[Tuple[int, int]]) -> List[Tuple[int, int]]:
    cleaned = [(int(start), int(end)) for start, end in intervals if end > start]
    if not cleaned:
        return []
    cleaned.sort(key=lambda x: x[0])
    merged: List[Tuple[int, int]] = []
    cur_start, cur_end = cleaned[0]
    for start, end in cleaned[1:]:
        if start <= cur_end:
            cur_end = max(cur_end, end)
            continue
        merged.append((cur_start, cur_end))
        cur_start, cur_end = start, end
    merged.append((cur_start, cur_end))
    return merged
