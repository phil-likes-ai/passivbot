from __future__ import annotations

import logging
import math
from typing import Callable, Dict, Optional, Tuple


logger = logging.getLogger(__name__)


def compute_live_warmup_windows(
    symbols_by_side: Dict[str, set],
    bp_lookup: Callable[[str, str, str], float],
    *,
    forager_enabled: Optional[Dict[str, bool]] = None,
    window_candles: Optional[int] = None,
    warmup_ratio: float = 0.0,
    max_warmup_minutes: Optional[int] = None,
    span_buffer: Optional[float] = None,
    large_span_threshold: int = 2 * 24 * 60,
) -> Tuple[Dict[str, int], Dict[str, int], Dict[str, bool]]:
    """Return per-symbol warmup windows for 1m/1h candles."""
    symbols: set = set()
    for symset in symbols_by_side.values():
        symbols.update(symset or set())

    per_symbol_win: Dict[str, int] = {}
    per_symbol_h1_hours: Dict[str, int] = {}
    per_symbol_skip_historical: Dict[str, bool] = {}

    if not symbols:
        return per_symbol_win, per_symbol_h1_hours, per_symbol_skip_historical

    if forager_enabled is None:
        forager_enabled = {}
    is_forager_long = bool(forager_enabled.get("long"))
    is_forager_short = bool(forager_enabled.get("short"))

    if span_buffer is None:
        try:
            ratio = float(warmup_ratio)
        except Exception:
            logger.debug(
                "[warmup] invalid warmup_ratio; defaulting to 0.0",
                extra={"warmup_ratio": warmup_ratio},
                exc_info=True,
            )
            ratio = 0.0
        span_buffer = 1.0 + max(0.0, ratio)

    cap_minutes = None
    try:
        cap_minutes = int(max_warmup_minutes) if max_warmup_minutes is not None else None
    except Exception:
        logger.debug(
            "[warmup] invalid max_warmup_minutes; defaulting to None",
            extra={"max_warmup_minutes": max_warmup_minutes},
            exc_info=True,
        )
        cap_minutes = None
    if cap_minutes is not None and cap_minutes <= 0:
        cap_minutes = None
    cap_hours = None
    if cap_minutes is not None:
        cap_hours = max(1, int(math.ceil(cap_minutes / 60.0)))

    def _to_float(val) -> float:
        try:
            return float(val)
        except Exception:
            logger.debug(
                "[warmup] invalid warmup lookup value; defaulting to 0.0",
                extra={"value": val},
                exc_info=True,
            )
            return 0.0

    def _get_bp(pside: str, key: str, sym: str) -> float:
        try:
            return _to_float(bp_lookup(pside, key, sym))
        except Exception:
            logger.debug(
                "[warmup] bp lookup failed; defaulting to 0.0",
                extra={"position_side": pside, "key": key, "symbol": sym},
                exc_info=True,
            )
            return 0.0

    if window_candles is not None:
        win = max(1, int(window_candles))
        if cap_minutes is not None:
            win = min(win, cap_minutes)
        h1_hours = max(1, int(math.ceil(win / 60.0)))
        if cap_hours is not None:
            h1_hours = min(h1_hours, cap_hours)
        skip_historical = win <= large_span_threshold
        for sym in sorted(symbols):
            per_symbol_win[sym] = win
            per_symbol_h1_hours[sym] = h1_hours
            per_symbol_skip_historical[sym] = skip_historical
        return per_symbol_win, per_symbol_h1_hours, per_symbol_skip_historical

    for sym in sorted(symbols):
        max_1m_span = 0.0
        max_h1_span = 0.0
        for pside in ("long", "short"):
            if sym not in symbols_by_side.get(pside, set()):
                continue
            max_1m_span = max(
                max_1m_span,
                _get_bp(pside, "ema_span_0", sym),
                _get_bp(pside, "ema_span_1", sym),
            )
            if (pside == "long" and is_forager_long) or (pside == "short" and is_forager_short):
                max_1m_span = max(
                    max_1m_span,
                    _get_bp(pside, "forager_volume_ema_span", sym),
                    _get_bp(pside, "forager_volatility_ema_span", sym),
                )
            max_h1_span = max(max_h1_span, _get_bp(pside, "entry_volatility_ema_span_hours", sym))

        if max_1m_span > 0.0:
            win = int(math.ceil(max_1m_span * span_buffer))
        else:
            win = 1
        win = max(1, win)
        if cap_minutes is not None:
            win = min(win, cap_minutes)
        per_symbol_win[sym] = win
        per_symbol_skip_historical[sym] = win <= large_span_threshold

        if max_h1_span > 0.0:
            h1_hours = max(1, int(math.ceil(max_h1_span * span_buffer)))
            if cap_hours is not None:
                h1_hours = min(h1_hours, cap_hours)
            per_symbol_h1_hours[sym] = h1_hours
        else:
            per_symbol_h1_hours[sym] = 0

    return per_symbol_win, per_symbol_h1_hours, per_symbol_skip_historical
