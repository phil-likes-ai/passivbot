from __future__ import annotations

import asyncio
import logging
import math
import random
import traceback

from config.access import get_optional_live_value
from passivbot_utils import get_function_name
from utils import utc_ms


ONE_MIN_MS = 60_000


async def refresh_forager_candidate_candles(self) -> None:
    """Best-effort refresh for forager candidate symbols to avoid large bursts."""
    if not self.is_forager_mode():
        return
    max_calls = get_optional_live_value(self.config, "max_ohlcv_fetches_per_minute", 0)
    try:
        max_calls = int(max_calls) if max_calls is not None else 0
    except Exception:
        max_calls = 0

    candidates_by_side: dict[str, set] = {}
    slots_open_any = False
    for pside in ("long", "short"):
        if not self.is_forager_mode(pside):
            continue
        syms = set(self.approved_coins_minus_ignored_coins.get(pside, set()))
        if not syms:
            continue
        candidates_by_side[pside] = syms
        try:
            max_n = int(self.get_max_n_positions(pside))
        except Exception:
            max_n = 0
        try:
            current_n = int(self.get_current_n_positions(pside))
        except Exception:
            current_n = len(self.get_symbols_with_pos(pside))
        if max_n > current_n:
            slots_open_any = True

    if not candidates_by_side:
        return

    all_candidates = set().union(*candidates_by_side.values())
    if not all_candidates:
        return

    if slots_open_any:
        if max_calls > 0:
            budget = self._forager_refresh_budget(max_calls)
            if budget <= 0:
                return
        else:
            budget = len(all_candidates)
    else:
        if max_calls <= 0:
            return
        budget = self._forager_refresh_budget(max_calls)
        if budget <= 0:
            return

    active = set(self.active_symbols) if hasattr(self, "active_symbols") else set()
    candidates = sorted(all_candidates - active)
    if not candidates:
        return

    if slots_open_any:
        rate_limit_age_ms = self._forager_target_staleness_ms(len(all_candidates), max_calls)
        target_age_ms = max(60_000, rate_limit_age_ms) if max_calls > 0 else 60_000
    else:
        target_age_ms = self._forager_target_staleness_ms(len(all_candidates), max_calls)
    now = utc_ms()
    stale: list[tuple[float, str]] = []
    for sym in candidates:
        try:
            last_final = self.cm.get_last_final_ts(sym)
        except Exception:
            last_final = 0
        age_ms = now - int(last_final) if last_final else float("inf")
        if age_ms > target_age_ms:
            stale.append((age_ms, sym))
    if not stale:
        return

    stale.sort(reverse=True)
    to_refresh = [sym for _, sym in stale[:budget]]
    if not to_refresh:
        return

    try:
        now = utc_ms()
        boot_delay_ms = int(getattr(self, "candle_refresh_log_boot_delay_ms", 300_000) or 0)
        boot_elapsed = int(now - getattr(self, "start_time_ms", now))
        if boot_elapsed >= boot_delay_ms:
            last_log = int(getattr(self, "_forager_refresh_log_last_ms", 0) or 0)
            if (now - last_log) >= 90_000:
                oldest_ms = int(stale[0][0]) if stale else 0
                logging.debug(
                    "[candle] forager refresh slots_open=%s candidates=%d stale=%d budget=%d oldest=%ds target=%ds",
                    "yes" if slots_open_any else "no",
                    len(all_candidates),
                    len(stale),
                    len(to_refresh),
                    int(oldest_ms / 1000),
                    int(target_age_ms / 1000),
                )
                self._forager_refresh_log_last_ms = int(now)
    except Exception:
        logging.debug(
            "[candle] failed to emit forager refresh diagnostics",
            exc_info=True,
        )

    end_ts = (now // ONE_MIN_MS) * ONE_MIN_MS - ONE_MIN_MS
    try:
        default_win = int(getattr(self.cm, "default_window_candles", 120) or 120)
    except Exception:
        default_win = 120
    try:
        warmup_ratio = float(get_optional_live_value(self.config, "warmup_ratio", 0.0))
    except Exception:
        warmup_ratio = 0.0
    try:
        max_warmup_minutes = int(get_optional_live_value(self.config, "max_warmup_minutes", 0) or 0)
    except Exception:
        max_warmup_minutes = 0
    span_buffer = 1.0 + max(0.0, warmup_ratio)

    fetch_delay_s = self._get_fetch_delay_seconds()

    for sym in to_refresh:
        try:
            max_span = 0.0
            for pside, syms in candidates_by_side.items():
                if sym not in syms:
                    continue
                try:
                    span_v = self.bp(pside, "forager_volume_ema_span", sym)
                except Exception:
                    span_v = None
                try:
                    span_lr = self.bp(pside, "forager_volatility_ema_span", sym)
                except Exception:
                    span_lr = None
                for span in (span_v, span_lr):
                    if span is not None:
                        try:
                            max_span = max(max_span, float(span))
                        except (TypeError, ValueError):
                            pass
            win = max(default_win, int(math.ceil(max_span * span_buffer))) if max_span > 0.0 else default_win
            if max_warmup_minutes > 0:
                win = min(int(win), int(max_warmup_minutes))
            start_ts = end_ts - ONE_MIN_MS * max(1, win)
            await self.cm.get_candles(
                sym,
                start_ts=start_ts,
                end_ts=end_ts,
                max_age_ms=0,
                strict=False,
                max_lookback_candles=win,
            )
            if fetch_delay_s > 0:
                await asyncio.sleep(fetch_delay_s)
        except TimeoutError as exc:
            logging.warning(
                "Timed out acquiring candle lock for %s; forager refresh will retry (%s)",
                sym,
                exc,
            )
        except Exception as exc:
            logging.error("error refreshing forager candles for %s: %s", sym, exc, exc_info=True)


async def update_ohlcvs_1m_for_actives(self):
    """Ensure active symbols have fresh 1m candles in CandlestickManager (<=60s old)."""
    max_age_ms = 60_000
    try:
        now = utc_ms()
        end_ts = (now // ONE_MIN_MS) * ONE_MIN_MS - ONE_MIN_MS
        try:
            window = int(getattr(self.cm, "default_window_candles", 120))
        except Exception:
            window = 120
        start_ts = end_ts - ONE_MIN_MS * window

        fetch_delay_s = self._get_fetch_delay_seconds()

        symbols = sorted(set(self.active_symbols))
        symbols_with_pos = [s for s in symbols if self.has_position(symbol=s)]
        symbols_without_pos = [s for s in symbols if s not in symbols_with_pos]
        random.shuffle(symbols_without_pos)
        ordered_symbols = symbols_with_pos + symbols_without_pos
        self._maybe_log_candle_refresh(
            "active refresh",
            symbols,
            target_age_ms=max_age_ms,
            refreshed=len(symbols),
            throttle_ms=60_000,
        )
        for sym in ordered_symbols:
            if self.cm.is_rate_limited():
                logging.debug("[candle] active refresh breaking early: rate limit backoff active")
                break
            try:
                await self.cm.get_candles(
                    sym,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    max_age_ms=max_age_ms,
                    strict=False,
                    max_lookback_candles=window,
                )
                if fetch_delay_s > 0:
                    await asyncio.sleep(fetch_delay_s)
            except TimeoutError as exc:
                logging.warning(
                    "Timed out acquiring candle lock for %s; will retry next cycle (%s)",
                    sym,
                    exc,
                )
            except Exception as exc:
                logging.error("error refreshing candles for %s: %s", sym, exc, exc_info=True)
        await self._refresh_forager_candidate_candles()
    except Exception as e:
        logging.error(f"error with {get_function_name()} {e}")
        traceback.print_exc()
