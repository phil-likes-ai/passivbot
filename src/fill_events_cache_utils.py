from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path
from collections import defaultdict
from typing import Dict, Mapping, Sequence


def _format_ms(ts):
    return import_module("fill_events_time_utils").format_ms(ts)


def metadata_path(self) -> Path:
    return self.root / "metadata.json"


def load_metadata(self) -> dict:
    """Load cache metadata from disk."""
    if self._metadata is not None:
        return self._metadata

    default = {
        "last_refresh_ms": 0,
        "oldest_event_ts": 0,
        "newest_event_ts": 0,
        "covered_start_ms": 0,
        "known_gaps": [],
        "history_scope": "unknown",
    }

    if not self.metadata_path.exists():
        self._metadata = default
        return self._metadata

    try:
        with self.metadata_path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            data = default
        for key in default:
            data.setdefault(key, default[key])
        self._metadata = data
    except Exception as exc:
        self.logger.warning("[fills] cache metadata: failed to read %s (%s)", self.metadata_path, exc)
        self._metadata = default

    return self._metadata


def save_metadata(self, metadata=None) -> None:
    """Save cache metadata to disk atomically."""
    if metadata is not None:
        self._metadata = metadata
    if self._metadata is None:
        return

    tmp_path = self.metadata_path.with_suffix(".tmp")
    try:
        with tmp_path.open("w", encoding="utf-8") as fh:
            json.dump(self._metadata, fh, indent=2)
        os.replace(tmp_path, self.metadata_path)
        self.logger.debug("FillEventCache.save_metadata: wrote to %s", self.metadata_path)
    except Exception as exc:
        self.logger.error("FillEventCache.save_metadata: failed to write %s (%s)", self.metadata_path, exc)


def load_events(self, fill_event_cls) -> list:
    files = sorted(self.root.glob("*.json"))
    events = []
    for path in files:
        try:
            with path.open("r", encoding="utf-8") as fh:
                payload = json.load(fh) or []
        except Exception as exc:
            self.logger.warning("[fills] cache load: failed to read %s (%s)", path, exc)
            continue
        for raw in payload:
            try:
                events.append(fill_event_cls.from_dict(raw))
            except Exception:
                self.logger.debug("[fills] cache load: skipping malformed record in %s", path)
    events.sort(key=lambda ev: ev.timestamp)
    self.logger.info(
        "[fills] cache loaded: %d events from %d files in %s",
        len(events),
        len(files),
        self.root,
    )
    return events


def save_events(self, events: Sequence, day_key_fn) -> None:
    day_map = defaultdict(list)
    for event in events:
        day_map[day_key_fn(event.timestamp)].append(event)
    for day in day_map:
        day_map[day].sort(key=lambda ev: ev.timestamp)
    save_days(self, day_map)


def save_days(self, day_events: Mapping[str, Sequence]) -> None:
    for day, events in day_events.items():
        path = self.root / f"{day}.json"
        payload = [event.to_dict() for event in sorted(events, key=lambda ev: ev.timestamp)]
        if path.exists():
            try:
                with path.open("r", encoding="utf-8") as fh:
                    current = json.load(fh)
            except Exception:
                current = None
            if current == payload:
                self.logger.debug("FillEventCache.save_days: %s unchanged", path.name)
                continue
        tmp_path = path.with_suffix(".tmp")
        with tmp_path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh)
        os.replace(tmp_path, path)
        self.logger.debug("[fills] cache wrote %d events to %s", len(payload), path.name)


def update_metadata_from_events(self, events: Sequence) -> None:
    """Update metadata timestamps based on events."""
    if not events:
        return
    metadata = self.load_metadata()
    timestamps = [ev.timestamp for ev in events]
    oldest = min(timestamps)
    newest = max(timestamps)

    current_oldest = metadata.get("oldest_event_ts", 0)
    current_newest = metadata.get("newest_event_ts", 0)

    if current_oldest == 0 or oldest < current_oldest:
        metadata["oldest_event_ts"] = oldest
    if newest > current_newest:
        metadata["newest_event_ts"] = newest

    metadata["last_refresh_ms"] = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    self.save_metadata(metadata)


def get_known_gaps(self) -> list:
    return self.load_metadata().get("known_gaps", [])


def get_covered_start_ms(self) -> int:
    metadata = self.load_metadata()
    return int(metadata.get("covered_start_ms", 0) or 0)


def mark_covered_start(self, start_ts: int) -> None:
    metadata = self.load_metadata()
    start_ts = int(start_ts)
    current = int(metadata.get("covered_start_ms", 0) or 0)
    if current == 0 or start_ts < current:
        metadata["covered_start_ms"] = start_ts
    metadata["last_refresh_ms"] = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    self.save_metadata(metadata)


def get_history_scope(self) -> str:
    scope = str(self.load_metadata().get("history_scope", "unknown") or "unknown").lower()
    return scope if scope in {"unknown", "window", "all"} else "unknown"


def set_history_scope(self, scope: str) -> None:
    normalized = str(scope or "unknown").lower()
    if normalized not in {"unknown", "window", "all"}:
        raise ValueError(f"invalid history scope {scope!r}")
    metadata = self.load_metadata()
    if metadata.get("history_scope") == normalized:
        return
    metadata["history_scope"] = normalized
    self.save_metadata(metadata)


def add_known_gap(
    self,
    start_ts: int,
    end_ts: int,
    *,
    reason: str,
    confidence: float,
    gap_max_retries: int,
    likely_legitimate_confidence: float,
) -> None:
    metadata = self.load_metadata()
    gaps = metadata.get("known_gaps", [])
    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

    for gap in gaps:
        if gap["start_ts"] <= end_ts and gap["end_ts"] >= start_ts:
            gap["start_ts"] = min(gap["start_ts"], start_ts)
            gap["end_ts"] = max(gap["end_ts"], end_ts)
            gap["retry_count"] = gap.get("retry_count", 0) + 1
            if gap["retry_count"] >= gap_max_retries:
                gap["confidence"] = max(gap.get("confidence", 0), likely_legitimate_confidence)
            self.logger.info(
                "FillEventCache.add_known_gap: updated gap %s → %s (retry_count=%d)",
                _format_ms(gap["start_ts"]),
                _format_ms(gap["end_ts"]),
                gap["retry_count"],
            )
            self.save_metadata(metadata)
            return

    new_gap = {
        "start_ts": start_ts,
        "end_ts": end_ts,
        "retry_count": 0,
        "reason": reason,
        "added_at": now_ms,
        "confidence": confidence,
    }
    gaps.append(new_gap)
    metadata["known_gaps"] = gaps
    self.logger.info(
        "FillEventCache.add_known_gap: added new gap %s → %s (reason=%s)",
        _format_ms(start_ts),
        _format_ms(end_ts),
        reason,
    )
    self.save_metadata(metadata)


def clear_gap(self, start_ts: int, end_ts: int) -> bool:
    metadata = self.load_metadata()
    gaps = metadata.get("known_gaps", [])
    original_count = len(gaps)
    remaining = []
    for gap in gaps:
        if gap["start_ts"] >= start_ts and gap["end_ts"] <= end_ts:
            self.logger.info(
                "FillEventCache.clear_gap: removed gap %s → %s",
                _format_ms(gap["start_ts"]),
                _format_ms(gap["end_ts"]),
            )
            continue
        if gap["start_ts"] < start_ts < gap["end_ts"]:
            gap["end_ts"] = start_ts
        if gap["start_ts"] < end_ts < gap["end_ts"]:
            gap["start_ts"] = end_ts
        if gap["start_ts"] < gap["end_ts"]:
            remaining.append(gap)

    if len(remaining) != original_count:
        metadata["known_gaps"] = remaining
        self.save_metadata(metadata)
        return True
    return False


def should_retry_gap(gap: Dict[str, object], gap_max_retries: int) -> bool:
    retry_count = gap.get("retry_count", 0)
    try:
        retry_value = retry_count if isinstance(retry_count, (int, float, str)) else 0
        retry_count = int(retry_value or 0)
    except Exception:
        retry_count = 0
    return retry_count < gap_max_retries


def get_coverage_summary(self, *, gap_max_retries: int) -> Dict[str, object]:
    metadata = self.load_metadata()
    gaps = metadata.get("known_gaps", [])
    persistent_gaps = [g for g in gaps if not should_retry_gap(g, gap_max_retries)]
    retryable_gaps = [g for g in gaps if should_retry_gap(g, gap_max_retries)]
    total_gap_ms = sum(g["end_ts"] - g["start_ts"] for g in gaps)

    return {
        "oldest_event_ts": metadata.get("oldest_event_ts", 0),
        "newest_event_ts": metadata.get("newest_event_ts", 0),
        "covered_start_ms": metadata.get("covered_start_ms", 0),
        "last_refresh_ms": metadata.get("last_refresh_ms", 0),
        "history_scope": self.get_history_scope(),
        "total_gaps": len(gaps),
        "persistent_gaps": len(persistent_gaps),
        "retryable_gaps": len(retryable_gaps),
        "total_gap_hours": total_gap_ms / (1000 * 60 * 60) if total_gap_ms > 0 else 0,
        "gaps": [
            {
                "start": _format_ms(g["start_ts"]),
                "end": _format_ms(g["end_ts"]),
                "retry_count": int(g.get("retry_count", 0) or 0),
                "reason": g.get("reason", "unknown"),
                "confidence": g.get("confidence", 0),
            }
            for g in gaps
        ],
    }
