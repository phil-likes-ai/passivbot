from __future__ import annotations

import logging
import math
from datetime import datetime, timezone

import asyncio
import random

from config.access import get_optional_live_value
from utils import format_approved_ignored_coins, utc_ms


def _require_total_wallet_exposure_limit(self, position_side: str) -> float:
    """Return a required finite TWEL value or raise with actionable context."""
    try:
        raw_value = self.bot_value(position_side, "total_wallet_exposure_limit")
    except KeyError as exc:
        raise ValueError(
            f"Missing required {position_side} total_wallet_exposure_limit in startup banner config"
        ) from exc
    if raw_value is None or raw_value == "":
        raise ValueError(
            f"Missing required {position_side} total_wallet_exposure_limit in startup banner config"
        )
    try:
        twel = float(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Invalid {position_side} total_wallet_exposure_limit in startup banner config: {raw_value!r}"
        ) from exc
    if not math.isfinite(twel):
        raise ValueError(
            f"Invalid {position_side} total_wallet_exposure_limit in startup banner config: {raw_value!r}"
        )
    return twel


def log_startup_banner(self) -> None:
    """Log a startup banner with key configuration info."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    user = self.user
    exchange = self.exchange

    twel_long = _require_total_wallet_exposure_limit(self, "long")
    twel_short = _require_total_wallet_exposure_limit(self, "short")
    long_enabled = twel_long > 0.0
    short_enabled = twel_short > 0.0
    if long_enabled and short_enabled:
        mode = "LONG + SHORT"
    elif long_enabled:
        mode = "LONG only"
    elif short_enabled:
        mode = "SHORT only"
    else:
        mode = "DISABLED"

    n_pos_long = int(self.bot_value("long", "n_positions") or 0)
    n_pos_short = int(self.bot_value("short", "n_positions") or 0)
    n_pos = f"{n_pos_long}L" if long_enabled else ""
    if short_enabled:
        n_pos = f"{n_pos}/{n_pos_short}S" if n_pos else f"{n_pos_short}S"

    if long_enabled and short_enabled:
        twel_str = f"L:{twel_long:.0%} S:{twel_short:.0%}"
    elif long_enabled:
        twel_str = f"{twel_long:.0%}"
    elif short_enabled:
        twel_str = f"{twel_short:.0%}"
    else:
        twel_str = "0%"

    line1 = f"  PASSIVBOT  │  {exchange}:{user}  │  {now}  "
    line2 = f"  Mode: {mode}  │  Positions: {n_pos}  │  TWEL: {twel_str}  "
    width = max(len(line1), len(line2), 50)
    border = "═" * width

    line1 = line1.ljust(width)
    line2 = line2.ljust(width)

    logging.info("╔%s╗", border)
    logging.info("║%s║", line1)
    logging.info("╠%s╣", border)
    logging.info("║%s║", line2)
    logging.info("╚%s╝", border)


async def maybe_apply_boot_stagger(self) -> None:
    """Apply optional randomized boot stagger before startup initialization."""
    boot_stagger = get_optional_live_value(self.config, "boot_stagger_seconds", None)
    if boot_stagger is None:
        exchange_lower = (self.exchange or "").lower()
        boot_stagger = 30.0 if exchange_lower == "hyperliquid" else 0.0
    try:
        stagger_value = boot_stagger if isinstance(boot_stagger, (int, float, str)) else None
        if boot_stagger is not None and stagger_value is None:
            raise TypeError(boot_stagger)
        boot_stagger = float(stagger_value) if stagger_value is not None else 0.0
    except Exception:
        logging.debug(
            "[boot] invalid boot_stagger_seconds config; defaulting to 0.0",
            extra={"boot_stagger_seconds": boot_stagger},
            exc_info=True,
        )
        boot_stagger = 0.0
    if boot_stagger > 0:
        delay = random.uniform(0, boot_stagger)
        logging.info(
            "[boot] stagger delay: waiting %.1fs before init (max=%.0fs)...",
            delay,
            boot_stagger,
        )
        await asyncio.sleep(delay)


async def finalize_startup_ready(self) -> None:
    """Mark the bot ready, emit startup events, and optionally enter the loop."""
    logging.info("[boot] starting execution loop...")
    logging.info("[boot] ══════════════════════════════════════════════════════════════════════")
    logging.info("[boot] READY - Bot initialization complete, entering main trading loop")
    logging.info("[boot] ══════════════════════════════════════════════════════════════════════")
    self._bot_ready = True
    ready_ts = utc_ms()
    self._monitor_record_event(
        "bot.ready",
        ("bot", "lifecycle", "ready"),
        {"debug_mode": bool(self.debug_mode)},
        ts=ready_ts,
    )
    await self._monitor_flush_snapshot(force=True, ts=ready_ts)
    if not self.debug_mode:
        await self.run_execution_loop()


async def handle_startup_error(self, exc: Exception, boot_stage: str) -> None:
    """Record and publish startup failure details before re-raising."""
    error_ts = utc_ms()
    self._monitor_record_error(
        "error.bot",
        exc,
        tags=("error", "bot", "startup"),
        payload={"source": "start_bot", "stage": boot_stage},
        ts=error_ts,
    )
    await self._monitor_flush_snapshot(force=True, ts=error_ts)
    self._monitor_emit_stop(
        "startup_error",
        ts=error_ts,
        payload={"stage": boot_stage, "error_type": type(exc).__name__},
    )


async def run_startup_preloop(self, set_stage) -> bool:
    """Run the startup stages before the execution loop begins.

    Returns False when startup should abort gracefully after emitting stop state.
    """
    set_stage("format_approved_ignored_coins")
    await format_approved_ignored_coins(self.config, self.user_info["exchange"], quote=self.quote)

    set_stage("init_markets")
    await self.init_markets()
    await self._monitor_flush_snapshot(force=True, ts=utc_ms())

    set_stage("warmup_candles_staggered")
    try:
        await self.warmup_candles_staggered()
    except Exception as e:
        logging.info("[boot] warmup skipped due to exception", exc_info=e)

    if self._equity_hard_stop_enabled():
        set_stage("equity_hard_stop_initialize_from_history")
        await self._equity_hard_stop_initialize_from_history()
        if self.stop_signal_received:
            self._monitor_emit_stop(
                "startup_aborted",
                ts=utc_ms(),
                payload={"stage": "equity_hard_stop_initialize_from_history", "stop_signal_received": True},
            )
            return False

    set_stage("post_init_sleep")
    await asyncio.sleep(1)
    self._log_memory_snapshot()

    logging.info("[boot] starting data maintainers...")
    set_stage("start_data_maintainers")
    await self.start_data_maintainers()
    return True
