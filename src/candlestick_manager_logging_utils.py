from __future__ import annotations

import time

import numpy as np

import candlestick_manager_misc_utils as cm_misc_utils


def fmt_ts(ms) -> str:
    try:
        return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(int(ms) / 1000.0)) if ms is not None else "-"
    except (TypeError, ValueError, OverflowError, OSError):
        return str(ms)


def log(self, level: str, event: str, **fields) -> None:
    try:
        ex = getattr(self, "_ex_id", self.exchange_name)
    except Exception:
        ex = self.exchange_name
    base = [f"[candle] event={event}"]
    if self.debug_level >= 1:
        try:
            caller = cm_misc_utils.get_caller_name()
            base.append(f"called_by={caller}")
        except Exception:
            pass
    base.append(f"exchange={ex}")
    parts = []
    for k, v in fields.items():
        if k.endswith("_ts") and isinstance(v, (int, np.integer)):
            parts.append(f"{k}={self._fmt_ts(int(v))}")
        else:
            parts.append(f"{k}={v}")
    msg = " ".join(base + parts)
    if level == "debug":
        if self.debug_level <= 0:
            return
        is_network = isinstance(event, str) and (event.startswith("ccxt_") or event.startswith("archive_"))
        if self.debug_level == 1 and not is_network:
            return
        self.log.debug(msg)
    elif level == "info":
        self.log.info(msg)
    elif level == "warning":
        self.log.warning(msg)
    else:
        self.log.error(msg)


def progress_log(self, key, event: str, **fields) -> None:
    if self._progress_log_interval_seconds <= 0.0:
        return
    now = time.monotonic()
    last = self._progress_last_log.get(key, 0.0)
    if (now - last) < self._progress_log_interval_seconds:
        return
    self._progress_last_log[key] = now
    self._log("debug", event, **fields)


def throttled_warning(self, throttle_key: str, event: str, **fields) -> None:
    now = time.monotonic()
    last = self._warning_last_log.get(throttle_key, 0.0)
    if (now - last) < self._warning_throttle_seconds:
        return
    self._warning_last_log[throttle_key] = now
    self._log("warning", event, **fields)


def emit_remote_fetch(self, payload) -> None:
    cb = getattr(self, "_remote_fetch_callback", None)
    if cb is None:
        return
    try:
        cb(payload)
    except Exception:
        return


def record_strict_gap(self, symbol: str, missing_count: int) -> None:
    """Accumulate strict gap counts for summary logging."""
    self._strict_gaps_summary[symbol] = self._strict_gaps_summary.get(symbol, 0) + missing_count


def log_strict_gaps_summary(self) -> None:
    """Log accumulated strict gap summary if any, throttled to once per 15 min."""
    if not self._strict_gaps_summary:
        return
    now = time.monotonic()
    if (now - self._strict_gaps_summary_last_log) < self._strict_gaps_summary_interval:
        return
    self._strict_gaps_summary_last_log = now
    summary = self._strict_gaps_summary
    total = sum(summary.values())
    symbols = ", ".join(f"{s}:{c}" for s, c in sorted(summary.items(), key=lambda x: -x[1])[:5])
    if len(summary) > 5:
        symbols += f", +{len(summary) - 5} more"
    self.log.debug(
        "[candle] strict mode gaps: %d missing candles across %d symbols (%s)",
        total,
        len(summary),
        symbols,
    )
    self._strict_gaps_summary.clear()


def log_persistent_gap_summary(self) -> None:
    """Log accumulated persistent gap summary if any, throttled to once per 30 min."""
    if not hasattr(self, "_persistent_gap_summary") or not self._persistent_gap_summary:
        return
    now = time.monotonic()
    last = getattr(self, "_persistent_gap_summary_last_log", 0.0)
    if (now - last) < 1800.0:
        return
    self._persistent_gap_summary_last_log = now
    summary = self._persistent_gap_summary
    total = sum(summary.values())
    symbols = ", ".join(f"{s}:{c}" for s, c in sorted(summary.items())[:5])
    if len(summary) > 5:
        symbols += f", +{len(summary) - 5} more"
    self.log.info(
        "[candle] persistent gaps: %d across %d symbols (%s). Use --force-refetch-gaps to retry.",
        total,
        len(summary),
        symbols,
    )
    self._persistent_gap_summary.clear()
