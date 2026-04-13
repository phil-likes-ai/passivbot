from __future__ import annotations

import asyncio
import logging
import time
from importlib import import_module

from config.access import get_optional_live_value
from passivbot_format_utils import format_duration as format_duration_static
from utils import utc_ms


_ORIGINAL_UTC_MS = utc_ms


def set_log_silence_watchdog_context(self, *, phase=None, stage=None) -> None:
    if phase is not None:
        self._log_silence_watchdog_phase = str(phase)
    if stage is not None:
        self._log_silence_watchdog_stage = str(stage)


def maybe_log_silence_watchdog(self, *, now_monotonic=None) -> bool:
    threshold = float(getattr(self, "_log_silence_watchdog_seconds", 0.0) or 0.0)
    if threshold <= 0.0:
        return False
    if now_monotonic is None:
        now_monotonic = time.monotonic()

    tracker = getattr(self, "get_last_log_activity_monotonic", None)
    if not callable(tracker):
        tracker = getattr(import_module("passivbot"), "get_last_log_activity_monotonic", None)
    if callable(tracker):
        try:
            tracked_value = tracker()
            if isinstance(tracked_value, (int, float, str)):
                last_log_monotonic = float(tracked_value)
            else:
                raise TypeError(tracked_value)
        except Exception:
            logging.debug(
                "[health] silence watchdog using now_monotonic fallback for last log activity | phase=%s | stage=%s | now_monotonic=%.3f",
                str(getattr(self, "_log_silence_watchdog_phase", "runtime") or "runtime"),
                str(getattr(self, "_log_silence_watchdog_stage", "unknown") or "unknown"),
                now_monotonic,
                exc_info=True,
            )
            last_log_monotonic = now_monotonic
    else:
        last_log_monotonic = now_monotonic

    silent_for_s = max(0.0, now_monotonic - last_log_monotonic)
    if silent_for_s < threshold:
        return False
    phase = str(getattr(self, "_log_silence_watchdog_phase", "runtime") or "runtime")
    stage = str(getattr(self, "_log_silence_watchdog_stage", "unknown") or "unknown")
    uptime_ms = max(0, utc_ms() - int(getattr(self, "_health_start_ms", utc_ms())))
    loop_ms = int(getattr(self, "_last_loop_duration_ms", 0) or 0)
    loop_str = f"{loop_ms / 1000:.1f}s" if loop_ms > 0 else "n/a"
    logging.info(
        "[health] silence watchdog: no logs for %.0fs | phase=%s | stage=%s | uptime=%s | loop=%s",
        silent_for_s,
        phase,
        stage,
        getattr(type(self), "_format_duration", format_duration_static)(uptime_ms),
        loop_str,
    )
    return True


async def run_log_silence_watchdog(self) -> None:
    threshold = float(getattr(self, "_log_silence_watchdog_seconds", 0.0) or 0.0)
    if threshold <= 0.0:
        return
    poll_seconds = min(5.0, max(1.0, threshold / 4.0))
    while not self.stop_signal_received:
        await asyncio.sleep(poll_seconds)
        if self.stop_signal_received:
            break
        self._maybe_log_silence_watchdog()


def start_log_silence_watchdog(self) -> None:
    threshold = float(getattr(self, "_log_silence_watchdog_seconds", 0.0) or 0.0)
    if threshold <= 0.0:
        return
    task = getattr(self, "_log_silence_watchdog_task", None)
    if task is not None and not task.done():
        return
    self._log_silence_watchdog_task = asyncio.create_task(self._run_log_silence_watchdog())


async def stop_log_silence_watchdog(self) -> None:
    task = getattr(self, "_log_silence_watchdog_task", None)
    self._log_silence_watchdog_task = None
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def get_fetch_delay_seconds(self) -> float:
    """Return configured per-fetch delay in seconds."""
    fetch_delay_ms = get_optional_live_value(self.config, "warmup_fetch_delay_ms", None)
    try:
        fetch_value = fetch_delay_ms if isinstance(fetch_delay_ms, (int, float, str)) else None
        if fetch_delay_ms is not None and fetch_value is None:
            raise TypeError(fetch_delay_ms)
        fetch_delay_ms = float(fetch_value) if fetch_value is not None else None
    except Exception:
        logging.debug(
            "[health] invalid warmup_fetch_delay_ms config; using exchange default | exchange=%s | value=%r",
            getattr(self, "exchange", None),
            fetch_delay_ms,
            exc_info=True,
        )
        fetch_delay_ms = None
    if fetch_delay_ms is None:
        exchange_lower = self.exchange.lower() if self.exchange else ""
        fetch_delay_ms = 200.0 if exchange_lower in ("bybit", "hyperliquid") else 0.0
    return max(0.0, float(fetch_delay_ms) / 1000.0)


def stop_data_maintainers(self, verbose=True):
    """Cancel background candle/orderbook tasks and log the outcome."""
    if not hasattr(self, "maintainers"):
        return
    res = {}
    for key in self.maintainers:
        try:
            res[key] = self.maintainers[key].cancel()
        except Exception:
            logging.exception("[ws] error stopping data maintainer task_key=%s", key)
    if hasattr(self, "WS_ohlcvs_1m_tasks"):
        res0s = {}
        for key in self.WS_ohlcvs_1m_tasks:
            try:
                res0 = self.WS_ohlcvs_1m_tasks[key].cancel()
                res0s[key] = res0
            except Exception:
                logging.exception("[ws] error stopping WS_ohlcvs_1m_tasks task_key=%s", key)
        if res0s and verbose:
            logging.info(f"stopped ohlcvs watcher tasks {res0s}")
    if verbose:
        logging.info(f"stopped data maintainers: {res}")
    return res


def maybe_log_health_summary(self) -> None:
    """Log periodic health summary if interval has elapsed."""
    now_ms = utc_ms()
    if (now_ms - self._health_last_summary_ms) < self._health_summary_interval_ms:
        return
    self._health_last_summary_ms = now_ms
    self._log_health_summary()


def get_exchange_time(self):
    """Return current exchange time in milliseconds."""
    del self
    try:
        if utc_ms is not _ORIGINAL_UTC_MS:
            return utc_ms()
        passivbot_utc_ms = getattr(import_module("passivbot"), "utc_ms", None)
        if callable(passivbot_utc_ms) and passivbot_utc_ms is not _ORIGINAL_UTC_MS:
            return passivbot_utc_ms()
    except (ImportError, AttributeError, TypeError):
        pass
    return utc_ms()


def log_health_summary(self) -> None:
    """Log a health summary with uptime and counters."""
    now_ms = utc_ms()
    uptime_ms = now_ms - self._health_start_ms
    uptime_str = getattr(type(self), "_format_duration", format_duration_static)(uptime_ms)

    n_long = 0
    n_short = 0
    for symbol, pos_data in self.positions.items():
        if pos_data.get("long", {}).get("size", 0.0) != 0.0:
            n_long += 1
        if pos_data.get("short", {}).get("size", 0.0) != 0.0:
            n_short += 1

    balance_raw = self.get_raw_balance()
    balance_snapped = self.get_hysteresis_snapped_balance()
    balance_str = f"{balance_raw:.2f} {self.quote}"
    if abs(balance_raw - balance_snapped) > 1e-9:
        balance_str += f" (snap {balance_snapped:.2f})"

    if self._health_fills > 0:
        pnl_sign = "+" if self._health_pnl >= 0 else ""
        fills_str = f"fills={self._health_fills} (pnl={pnl_sign}{self._health_pnl:.2f})"
    else:
        fills_str = "fills=0"

    loop_ms = getattr(self, "_last_loop_duration_ms", 0)
    loop_str = f"{loop_ms / 1000:.1f}s" if loop_ms > 0 else "n/a"

    error_counts = getattr(self, "error_counts", [])
    recent_errors = len([x for x in error_counts if x > now_ms - 1000 * 60 * 60])
    max_errors = 10
    error_budget_str = f"{recent_errors}/{max_errors}"

    try:
        import resource

        getrusage = getattr(resource, "getrusage", None)
        rusage_self = getattr(resource, "RUSAGE_SELF", None)
        if getrusage is None or rusage_self is None:
            raise AttributeError("resource module missing getrusage support")
        rss_mb = getrusage(rusage_self).ru_maxrss / 1024 / 1024
        mem_str = f"rss={rss_mb:.0f}MB"
    except Exception:
        logging.debug("[health] unable to collect process RSS; omitting memory summary suffix", exc_info=True)
        mem_str = ""

    logging.info(
        "[health] uptime=%s | loop=%s | positions=%d long, %d short | balance=%s | "
        "orders=+%d/-%d | %s | errors=%s | ws_reconnects=%d | rate_limits=%d%s",
        uptime_str,
        loop_str,
        n_long,
        n_short,
        balance_str,
        self._health_orders_placed,
        self._health_orders_cancelled,
        fills_str,
        error_budget_str,
        self._health_ws_reconnects,
        self._health_rate_limits,
        f" | {mem_str}" if mem_str else "",
    )


def log_memory_snapshot(self, *, now_ms=None, get_process_rss_bytes=None) -> None:
    """Log process RSS and key cache metrics for observability."""
    if now_ms is None:
        now_ms = utc_ms()
    if get_process_rss_bytes is None:
        from passivbot_utils import get_process_rss_bytes as get_process_rss_bytes_fn

        get_process_rss_bytes = get_process_rss_bytes_fn
    rss = get_process_rss_bytes()
    if rss is None:
        return
    cache_bytes = None
    cache_candles = None
    cache_symbols = None
    cache_top = None
    tf_cache_bytes = None
    tf_cache_ranges = None
    tf_cache_top = None
    try:
        cache = getattr(self.cm, "_cache", {}) if hasattr(self, "cm") else {}
        cache_symbols = len(cache)
        stats = []
        for sym, arr in cache.items():
            if arr is None:
                continue
            arr_bytes = int(getattr(arr, "nbytes", 0))
            arr_rows = int(arr.shape[0]) if hasattr(arr, "shape") else 0
            stats.append((sym, arr_bytes, arr_rows))
        cache_bytes = sum(val for _, val, _ in stats)
        cache_candles = sum(rows for _, _, rows in stats)
        if stats:
            top = sorted(stats, key=lambda item: item[1], reverse=True)[:3]
            cache_top = ", ".join(
                f"{sym}:{bytes_ / (1024 * 1024):.1f}MiB/{rows}" for sym, bytes_, rows in top
            )
        tf_cache = getattr(self.cm, "_tf_range_cache", {}) if hasattr(self, "cm") else {}
        tf_stats = []
        for sym, entries in tf_cache.items():
            if not isinstance(entries, dict):
                continue
            for key, val in entries.items():
                try:
                    tf_label = key[0] if isinstance(key, tuple) and key else str(key)
                except Exception:
                    logging.debug(
                        "[health] tf cache key label extraction failed during memory snapshot; using unknown | symbol=%s | key_type=%s",
                        sym,
                        type(key).__name__,
                        exc_info=True,
                    )
                    tf_label = "unknown"
                arr = val[0] if isinstance(val, tuple) and val else val
                if not hasattr(arr, "nbytes"):
                    continue
                arr_bytes = int(getattr(arr, "nbytes", 0))
                shape = getattr(arr, "shape", None)
                arr_rows = int(shape[0]) if shape else 0
                tf_stats.append(((sym, tf_label), arr_bytes, arr_rows))
        if tf_stats:
            tf_cache_bytes = sum(size for _, size, _ in tf_stats)
            tf_cache_ranges = len(tf_stats)
            top_tf = sorted(tf_stats, key=lambda item: item[1], reverse=True)[:3]
            tf_cache_top = ", ".join(
                f"{sym}:{tf}:{bytes_ / (1024 * 1024):.1f}MiB/{rows}"
                for (sym, tf), bytes_, rows in top_tf
            )
    except Exception:
        logging.debug(
            "[health] cache inspection unavailable during memory snapshot; omitting cache summary",
            exc_info=True,
        )
        cache_bytes = None
    prev = getattr(self, "_mem_log_prev", None)
    pct_change = None
    if prev and prev.get("rss"):
        prev_rss = prev["rss"]
        if prev_rss:
            pct_change = 100.0 * (rss - prev_rss) / prev_rss
    parts = [f"[memory] rss={rss / (1024 * 1024):.2f} MiB"]
    if pct_change is not None:
        parts.append(f"Δ={pct_change:+.2f}% vs previous snapshot")
    if cache_bytes is not None:
        cache_mib = cache_bytes / (1024 * 1024)
        cache_desc = f"cm_cache={cache_mib:.2f} MiB"
        if cache_candles is not None:
            detail = f"{cache_candles} candles"
            if cache_symbols is not None:
                detail += f" across {cache_symbols} symbols"
            cache_desc += f" ({detail})"
        parts.append(cache_desc)
        if cache_top:
            parts.append(f"cm_top={cache_top}")
    if tf_cache_bytes is not None:
        tf_desc = f"cm_tf_cache={tf_cache_bytes / (1024 * 1024):.2f} MiB"
        if tf_cache_ranges is not None:
            tf_desc += f" ({tf_cache_ranges} ranges)"
        parts.append(tf_desc)
        if tf_cache_top:
            parts.append(f"cm_tf_top={tf_cache_top}")
    try:
        loop = asyncio.get_running_loop()
        tasks = asyncio.all_tasks(loop)
        total_tasks = len(tasks)
        pending = sum(1 for t in tasks if not t.done())
        task_counts: dict[str, int] = {}
        for t in tasks:
            coro = getattr(t, "get_coro", None)
            name = None
            if callable(coro):
                try:
                    coro_obj = coro()
                    name = getattr(coro_obj, "__qualname__", None)
                except Exception:
                    logging.debug(
                        "[health] task coro inspection failed during memory snapshot; falling back to task name",
                        exc_info=True,
                    )
                    name = None
            if not name:
                name = getattr(t, "get_name", lambda: None)()
            if not name:
                name = type(t).__name__
            task_counts[name] = task_counts.get(name, 0) + 1
        top_tasks = ", ".join(
            f"{name}:{count}"
            for name, count in sorted(task_counts.items(), key=lambda kv: kv[1], reverse=True)[:4]
        )
        parts.append(f"tasks={total_tasks} pending={pending}")
        if top_tasks:
            parts.append(f"task_top={top_tasks}")
    except Exception:
        logging.debug(
            "[health] task inspection unavailable during memory snapshot; omitting task summary",
            exc_info=True,
        )
    logging.info("; ".join(parts))
    self._mem_log_prev = {"timestamp": now_ms, "rss": rss}
    if cache_bytes is not None:
        self._mem_log_prev["cm_cache_bytes"] = cache_bytes
