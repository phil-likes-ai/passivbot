from __future__ import annotations
import os
import time

# fix Crashes on Windows
from tools.event_loop_policy import set_windows_event_loop_policy

set_windows_event_loop_policy()

from ccxt.base import errors as ccxt_errors
import random
import traceback
import argparse
import asyncio
import json
import sys
import signal
import hjson
import bisect
import pprint
import numpy as np
import inspect
import passivbot_rust as pbr
import logging
import math
from pathlib import Path
from cli_utils import (
    add_help_all_argument,
    build_command_parser,
    expand_help_all_argv,
    get_cli_prog,
    help_all_requested,
)
from candlestick_manager import CandlestickManager, CANDLE_DTYPE, synthesize_1m_from_higher_tf
from fill_events_manager import (
    FillEventsManager,
    _build_fetcher_for_bot,
    _extract_symbol_pool,
    compute_psize_pprice,
)
from monitor_publisher import MonitorPublisher
from passivbot_exceptions import RestartBotException
import passivbot_hsl as pb_hsl
import passivbot_monitor as pb_monitor
import passivbot_approval_utils as pb_approval_utils
import passivbot_balance_utils as pb_balance_utils
import passivbot_client_utils as pb_client_utils
import passivbot_debug_utils as pb_debug_utils
import passivbot_exchange_config as pb_exchange_config
import passivbot_execution as pb_execution
import passivbot_exposure_utils as pb_exposure_utils
import passivbot_fill_event_utils as pb_fill_event_utils
import passivbot_fetch_budget_utils as pb_fetch_budget_utils
import passivbot_format_utils as pb_format_utils
import passivbot_hook_utils as pb_hook_utils
import passivbot_logging_utils as pb_logging_utils
import passivbot_market_init_utils as pb_market_init_utils
import passivbot_mode_utils as pb_mode_utils
import passivbot_orchestrator_utils as pb_orchestrator_utils
import passivbot_override_utils as pb_override_utils
import passivbot_pnls_utils as pb_pnls_utils
import passivbot_order_update_utils as pb_order_update_utils
import passivbot_position_logging_utils as pb_position_logging_utils
import passivbot_position_utils as pb_position_utils
import passivbot_startup_utils as pb_startup_utils
import passivbot_timestamp_utils as pb_timestamp_utils
import passivbot_trailing_utils as pb_trailing_utils
import passivbot_runtime_ops as pb_runtime_ops
import passivbot_runtime as pb_runtime
import passivbot_symbol_utils as pb_symbol_utils
import passivbot_ticker_utils as pb_ticker_utils
import passivbot_unstuck_utils as pb_unstuck_utils
from passivbot_order_utils import (
    custom_id_to_snake,
    has_open_unstuck_order,
    order_to_order_tuple,
    order_type_id_to_hex4,
    snake_of,
    trailing_bundle_default_dict as _trailing_bundle_default_dict,
    trailing_bundle_from_arrays as _trailing_bundle_from_arrays,
    trailing_bundle_tuple_to_dict as _trailing_bundle_tuple_to_dict,
    try_decode_type_id_from_custom_id,
    type_token,
)
from passivbot_utils import (
    calc_pnl,
    clip_by_timestamp,
    get_caller_name,
    get_function_name,
    get_process_rss_bytes as _get_process_rss_bytes,
    order_has_match,
    orders_matching,
    or_default,
    order_market_diff,
)
from passivbot_warmup_utils import compute_live_warmup_windows
from typing import Dict, Iterable, Tuple, List, Optional, Any, Callable
from config import get_template_config, load_input_config, prepare_config
from config.access import (
    get_optional_config_value,
    get_optional_live_value,
    require_config_value,
    require_live_value,
)
from config.coerce import (
    normalize_hsl_cooldown_position_policy,
    normalize_hsl_signal_mode,
)
from config.pnl_lookback import parse_pnls_max_lookback_days
from config.overrides import parse_overrides
from logging_setup import (
    configure_logging,
    get_last_log_activity_monotonic,
    resolve_live_log_file_settings,
    resolve_log_level,
)
from utils import (
    load_markets,
    coin_to_symbol,
    symbol_to_coin,
    utc_ms,
    ts_to_date,
    make_get_filepath,
    format_approved_ignored_coins,
    filter_markets,
    to_ccxt_exchange_id,
    coin_symbol_warning_counts,
    _coins_source_side_is_all,
    normalize_coins_source,
)
from prettytable import PrettyTable
from uuid import uuid4
from copy import deepcopy
from dataclasses import dataclass
from collections import defaultdict, Counter
from sortedcontainers import SortedDict

try:
    import psutil  # type: ignore
except Exception:
    psutil = None

try:
    import resource  # type: ignore
except Exception:
    resource = None
from config_utils import (
    add_config_arguments,
    update_config_with_args,
    expand_PB_mode,
    merge_negative_cli_values,
)
from procedures import (
    load_broker_code,
    load_user_info,
    get_first_timestamps_unified,
    print_async_exception,
)
from utils import get_file_mod_ms
from downloader import compute_per_coin_warmup_minutes
import re

NetworkError = ccxt_errors.NetworkError
RateLimitExceeded = ccxt_errors.RateLimitExceeded
# Some isolated tests stub ccxt.base.errors without RequestTimeout; treat it as a
# NetworkError-class transient startup error when the dedicated symbol is absent.
RequestTimeout = getattr(ccxt_errors, "RequestTimeout", NetworkError)

# Orchestrator-only: ideal orders are computed via Rust orchestrator (JSON API).
# Legacy Python order calculation paths are removed in this branch.

from custom_endpoint_overrides import (
    apply_rest_overrides_to_ccxt,
    configure_custom_endpoint_loader,
    get_custom_endpoint_source,
    load_custom_endpoint_config,
    resolve_custom_endpoint_override,
)

calc_min_entry_qty = pbr.calc_min_entry_qty_py
round_ = pbr.round_
round_up = pbr.round_up
round_dn = pbr.round_dn
round_dynamic = pbr.round_dynamic
calc_order_price_diff = pbr.calc_order_price_diff

DEFAULT_MAX_MEMORY_CANDLES_PER_SYMBOL = 20_000
PARTIAL_FILL_MERGE_MAX_DELAY_MS = 60_000
FILL_EVENT_FETCH_OVERLAP_COUNT = 20
FILL_EVENT_FETCH_OVERLAP_MAX_MS = 86_400_000  # 24 hours
FILL_EVENT_FETCH_LIMIT_DEFAULT = 20


# Legacy EMA helper removed; CandlestickManager provides EMA utilities


from pure_funcs import (
    numpyize,
    denumpyize,
    filter_orders,
    multi_replace,
    shorten_custom_id,
    determine_side_from_order_tuple,
    str2bool,
    flatten,
    log_dict_changes,
    ensure_millis,
)

ONE_MIN_MS = 60_000


class Passivbot:
    def __init__(self, config: dict):
        """Initialise the bot with configuration, user context, and runtime caches."""
        self.config = config
        try:
            lvl_raw = get_optional_config_value(config, "logging.level", 1)
            lvl = int(float(lvl_raw)) if lvl_raw is not None else 1
        except Exception:
            lvl = 1
        self.logging_level = max(0, min(int(lvl), 3))
        self.user = require_live_value(config, "user")
        self.user_info = load_user_info(self.user)
        self.exchange = self.user_info["exchange"]
        self.broker_code = load_broker_code(self.user_info["exchange"])
        self.exchange_ccxt_id = to_ccxt_exchange_id(self.exchange)
        self.endpoint_override = resolve_custom_endpoint_override(self.exchange_ccxt_id)
        self.ws_enabled = True
        if self.endpoint_override:
            self.ws_enabled = not self.endpoint_override.disable_ws
            source_path = get_custom_endpoint_source()
            logging.info(
                "Custom endpoint override active for %s (disable_ws=%s, source=%s)",
                self.exchange_ccxt_id,
                self.endpoint_override.disable_ws,
                source_path if source_path else "auto-discovered",
            )
        self.custom_id_max_length = 36
        self.sym_padding = 17
        self.action_str_max_len = max(
            len(a)
            for a in [
                "posting order",
                "cancelling order",
                "removed order",
                "added order",
            ]
        )
        self.order_details_str_len = 34
        self.order_type_str_len = 32
        self.stop_websocket = False
        raw_balance_override = get_optional_live_value(self.config, "balance_override", None)
        self.balance_override = (
            None if raw_balance_override in (None, "") else float(raw_balance_override)
        )
        self._balance_override_logged = False
        self.balance = 1e-12
        self.balance_raw = 1e-12
        self.previous_hysteresis_balance = None
        self.balance_hysteresis_snap_pct = float(
            get_optional_live_value(self.config, "balance_hysteresis_snap_pct", 0.02)
        )
        # hedge_mode controls whether simultaneous long/short on same coin is allowed.
        # This is the config-level setting; exchange-specific bots may override
        # self.hedge_mode to False if the exchange doesn't support two-way mode.
        # Effective hedge_mode = config setting AND exchange capability.
        self._config_hedge_mode = bool(get_optional_live_value(self.config, "hedge_mode", True))
        self.hedge_mode = True  # Exchange capability, may be overridden by subclass
        self.inverse = False
        self.active_symbols = []
        self.fetched_positions = []
        self.fetched_open_orders = []
        self.open_orders = {}
        self.positions = {}
        self.symbol_ids = {}
        self.min_costs = {}
        self.min_qtys = {}
        self.qty_steps = {}
        self.price_steps = {}
        self.c_mults = {}
        self.max_leverage = {}
        self.pside_int_map = {"long": 0, "short": 1}
        self.PB_modes = {"long": {}, "short": {}}
        # Legacy pnls_cache_filepath removed; FillEventsManager handles caching
        self.quote = "USDT"

        self.minimum_market_age_millis = (
            float(require_live_value(config, "minimum_coin_age_days")) * 24 * 60 * 60 * 1000
        )
        # Legacy EMA caches removed; use CandlestickManager EMA helpers
        # Legacy ohlcvs_1m fields removed in favor of CandlestickManager
        self.stop_signal_received = False
        self.cca = None
        self.ccp = None
        self.create_ccxt_sessions()
        self.debug_mode = False
        self.balance_threshold = 1.0  # don't create orders if balance is less than threshold
        self.hyst_pct = 0.02
        self.state_change_detected_by_symbol = set()
        self.recent_order_executions = []
        self.recent_order_cancellations = []
        self._disabled_psides_logged = set()
        self._last_coin_symbol_warning_counts = {
            "symbol_to_coin_fallbacks": 0,
            "coin_to_symbol_fallbacks": 0,
        }
        self._last_plan_detail: dict[str, tuple[int, int, int]] = {}
        self._last_action_summary: dict[tuple[str, str], str] = {}
        self.start_time_ms = utc_ms()
        self._bot_ready = False
        self._monitor_last_equity = float(self.balance_raw)
        self._monitor_stop_emitted = False
        self.monitor_publisher: Optional[MonitorPublisher] = None
        self.monitor_enabled = bool(get_optional_config_value(config, "monitor.enabled", False))
        if self.monitor_enabled:
            try:
                self.monitor_publisher = MonitorPublisher.from_config(
                    exchange=self.exchange,
                    user=self.user,
                    config=require_config_value(config, "monitor"),
                )
            except Exception as exc:
                logging.error("[monitor] failed to initialize monitor publisher: %s", exc)
                self.monitor_publisher = None
        # CandlestickManager settings from config.live
        # Use denormalized exchange name for cache paths (e.g., "binance" not "binanceusdm")
        cm_kwargs = {
            "exchange": self.cca,
            "exchange_name": self.exchange,
            "debug": self.logging_level,
        }
        mem_cap_raw = require_live_value(config, "max_memory_candles_per_symbol")
        mem_cap_effective = DEFAULT_MAX_MEMORY_CANDLES_PER_SYMBOL
        try:
            if mem_cap_raw is not None:
                mem_cap_effective = int(float(mem_cap_raw))
        except Exception:
            logging.warning(
                "Unable to parse live.max_memory_candles_per_symbol=%r, using default %d",
                mem_cap_raw,
                DEFAULT_MAX_MEMORY_CANDLES_PER_SYMBOL,
            )
            mem_cap_effective = DEFAULT_MAX_MEMORY_CANDLES_PER_SYMBOL
        if mem_cap_effective <= 0:
            logging.warning(
                "live.max_memory_candles_per_symbol=%r is non-positive; using default %d",
                mem_cap_raw,
                DEFAULT_MAX_MEMORY_CANDLES_PER_SYMBOL,
            )
            mem_cap_effective = DEFAULT_MAX_MEMORY_CANDLES_PER_SYMBOL
        cm_kwargs["max_memory_candles_per_symbol"] = mem_cap_effective
        disk_cap = require_live_value(config, "max_disk_candles_per_symbol_per_tf")
        if disk_cap is not None:
            cm_kwargs["max_disk_candles_per_symbol_per_tf"] = int(disk_cap)
        lock_timeout = get_optional_live_value(config, "candle_lock_timeout_seconds", None)
        if lock_timeout not in (None, ""):
            try:
                cm_kwargs["lock_timeout_seconds"] = float(lock_timeout)
            except Exception:
                logging.warning(
                    "Unable to parse live.candle_lock_timeout_seconds=%r; using default",
                    lock_timeout,
                )
        max_concurrent = get_optional_live_value(config, "max_concurrent_api_requests", None)
        if max_concurrent not in (None, "", 0):
            try:
                cm_kwargs["max_concurrent_requests"] = int(max_concurrent)
            except Exception:
                logging.warning(
                    "Unable to parse live.max_concurrent_api_requests=%r; ignoring",
                    max_concurrent,
                )
        raw_page_debug = get_optional_config_value(config, "logging.candle_page_debug_symbols", None)
        page_debug_symbols = []
        if raw_page_debug not in (None, "", []):
            if isinstance(raw_page_debug, str):
                raw = raw_page_debug.strip()
                if raw:
                    if raw == "*":
                        page_debug_symbols = ["*"]
                    else:
                        raw = raw.replace(",", " ").replace(";", " ")
                        page_debug_symbols = [s for s in raw.split() if s]
            elif isinstance(raw_page_debug, (list, tuple, set)):
                page_debug_symbols = [str(s) for s in raw_page_debug if s]
            if page_debug_symbols:
                cm_kwargs["page_debug_symbols"] = page_debug_symbols
        # Archive fetching: disabled by default for live bots (avoids timeout issues)
        # Set live.enable_archive_candle_fetch=true to enable if needed
        archive_enabled = get_optional_live_value(config, "enable_archive_candle_fetch", False)
        cm_kwargs["archive_enabled"] = bool(archive_enabled)
        self.cm = CandlestickManager(**cm_kwargs)
        if self.monitor_publisher is not None:
            self.cm.set_persist_batch_observer(self._monitor_handle_candlestick_persist)
        # TTL (minutes) for EMA candles on non-traded symbols
        ttl_min = require_live_value(config, "inactive_coin_candle_ttl_minutes")
        self.inactive_coin_candle_ttl_ms = int(float(ttl_min) * 60_000)
        raw_mem_interval = get_optional_config_value(
            config, "logging.memory_snapshot_interval_minutes", 30.0
        )
        try:
            interval_minutes = float(raw_mem_interval)
        except Exception:
            logging.warning(
                "Unable to parse logging.memory_snapshot_interval_minutes=%r; using fallback 30",
                raw_mem_interval,
            )
            interval_minutes = 30.0
        if interval_minutes <= 0.0:
            logging.warning(
                "logging.memory_snapshot_interval_minutes=%r is non-positive; using fallback 30",
                raw_mem_interval,
            )
            interval_minutes = 30.0
        self.memory_snapshot_interval_ms = max(60_000, int(interval_minutes * 60_000))
        raw_volume_threshold = get_optional_config_value(
            config, "logging.volume_refresh_info_threshold_seconds", 30.0
        )
        try:
            volume_threshold = float(raw_volume_threshold)
        except Exception:
            logging.warning(
                "Unable to parse logging.volume_refresh_info_threshold_seconds=%r; using fallback 30",
                raw_volume_threshold,
            )
            volume_threshold = 30.0
        if volume_threshold < 0:
            logging.warning(
                "logging.volume_refresh_info_threshold_seconds=%r is negative; using 0",
                raw_volume_threshold,
            )
            volume_threshold = 0.0
        self.volume_refresh_info_threshold_seconds = float(volume_threshold)
        raw_candle_check_interval = get_optional_config_value(
            config, "logging.candle_disk_check_interval_minutes", 60.0
        )
        try:
            candle_check_minutes = float(raw_candle_check_interval)
        except Exception:
            logging.warning(
                "Unable to parse logging.candle_disk_check_interval_minutes=%r; using fallback 60",
                raw_candle_check_interval,
            )
            candle_check_minutes = 60.0
        if candle_check_minutes < 0:
            logging.warning(
                "logging.candle_disk_check_interval_minutes=%r is negative; disabling",
                raw_candle_check_interval,
            )
            candle_check_minutes = 0.0
        self.candle_disk_check_interval_ms = int(candle_check_minutes * 60_000)
        raw_tail_slack_min = get_optional_config_value(
            config, "logging.candle_disk_check_tail_slack_minutes", 1.0
        )
        try:
            tail_slack_min = float(raw_tail_slack_min)
        except Exception:
            logging.warning(
                "Unable to parse logging.candle_disk_check_tail_slack_minutes=%r; using 1",
                raw_tail_slack_min,
            )
            tail_slack_min = 1.0
        if tail_slack_min < 0:
            tail_slack_min = 0.0
        self.candle_disk_check_tail_slack_ms = int(tail_slack_min * 60_000)
        raw_tail_slack_hours = get_optional_config_value(
            config, "logging.candle_disk_check_tail_slack_hours", 1.0
        )
        try:
            tail_slack_hours = float(raw_tail_slack_hours)
        except Exception:
            logging.warning(
                "Unable to parse logging.candle_disk_check_tail_slack_hours=%r; using 1",
                raw_tail_slack_hours,
            )
            tail_slack_hours = 1.0
        if tail_slack_hours < 0:
            tail_slack_hours = 0.0
        self.candle_disk_check_tail_slack_hour_ms = int(tail_slack_hours * 60 * 60_000)
        self._candle_disk_check_last_ms = 0
        auto_gs = bool(self.live_value("auto_gs"))
        self.PB_mode_stop = {
            "long": "graceful_stop" if auto_gs else "manual",
            "short": "graceful_stop" if auto_gs else "manual",
        }

        # FillEventsManager for PnL tracking (replaces legacy self.pnls list)
        self._pnls_manager: Optional[FillEventsManager] = None
        self._pnls_initialized = False

        # Health tracking for periodic summary
        self._health_start_ms = utc_ms()
        self._health_orders_placed = 0
        self._health_orders_cancelled = 0
        self._health_fills = 0
        self._health_pnl = 0.0  # sum of realized PnL from fills
        self._health_errors = 0
        self._health_ws_reconnects = 0
        self._health_rate_limits = 0
        self._health_last_summary_ms = 0
        self._health_summary_interval_ms = 15 * 60 * 1000  # 15 minutes
        self._last_loop_duration_ms = 0

        raw_silence_watchdog = get_optional_config_value(
            config, "logging.silence_watchdog_seconds", 60.0
        )
        try:
            silence_watchdog_seconds = float(raw_silence_watchdog)
        except Exception:
            logging.warning(
                "Unable to parse logging.silence_watchdog_seconds=%r; using fallback 60",
                raw_silence_watchdog,
            )
            silence_watchdog_seconds = 60.0
        if silence_watchdog_seconds < 0:
            logging.warning(
                "logging.silence_watchdog_seconds=%r is negative; disabling",
                raw_silence_watchdog,
            )
            silence_watchdog_seconds = 0.0
        self._log_silence_watchdog_seconds = float(silence_watchdog_seconds)
        self._log_silence_watchdog_phase = "boot"
        self._log_silence_watchdog_stage = "idle"
        self._log_silence_watchdog_task: Optional[asyncio.Task] = None
        self._bot_ready = False

        # Unstuck logging throttle
        self._unstuck_last_log_ms = 0
        self._unstuck_log_interval_ms = 5 * 60 * 1000  # 5 minutes

        # Realized-loss gate logging throttle
        self._loss_gate_last_log_ms = {}
        self._loss_gate_log_interval_ms = 5 * 60 * 1000  # 5 minutes
        self._orchestrator_prev_close_ema = {}
        self._orchestrator_close_ema_fallback_counts = {}
        self.hsl = self._parse_hsl_config()
        self._runtime_forced_modes = {"long": {}, "short": {}}
        self._equity_hard_stop_supervisor_running = False
        self._equity_hard_stop_status_log_interval_ms = 15 * 60 * 1000
        self._equity_hard_stop_cooldown_log_interval_ms = 60 * 1000
        self._equity_hard_stop = {
            pside: {
                "runtime": pbr.EquityHardStopRuntime(),
                "strategy_pnl_peak": pbr.EquityHardStopRollingPeak(),
                "halted": False,
                "no_restart_latched": False,
                "last_metrics": None,
                "last_red_progress": None,
                "red_flat_confirmations": 0,
                "pending_red_since_ms": None,
                "cooldown_until_ms": None,
                "pending_stop_event": None,
                "last_stop_event": None,
                "last_status_log_ms": 0,
                "last_cooldown_log_ms": 0,
                "cooldown_intervention_active": False,
                "cooldown_repanic_reset_pending": False,
                "last_cooldown_intervention_log_ms": 0,
                "cooldown_unresolved_residue": False,
            }
            for pside in ("long", "short")
        }

    _monitor_record_event = pb_monitor._monitor_record_event
    _monitor_record_error = pb_monitor._monitor_record_error
    _monitor_emit_stop = pb_monitor._monitor_emit_stop
    _monitor_hsl_payload = pb_monitor._monitor_hsl_payload
    _monitor_order_payload = pb_monitor._monitor_order_payload
    _monitor_fill_payload = pb_monitor._monitor_fill_payload
    _monitor_record_fill_history = pb_monitor._monitor_record_fill_history
    _monitor_record_price_ticks = pb_monitor._monitor_record_price_ticks
    _monitor_handle_candlestick_persist = pb_monitor._monitor_handle_candlestick_persist
    _build_health_summary_payload = pb_monitor._build_health_summary_payload
    _monitor_recent_orders_payload = pb_monitor._monitor_recent_orders_payload
    _build_monitor_market_section = pb_monitor._build_monitor_market_section
    _build_monitor_trailing_section = pb_monitor._build_monitor_trailing_section
    _build_monitor_forager_section = pb_monitor._build_monitor_forager_section
    _build_monitor_unstuck_section = pb_monitor._build_monitor_unstuck_section
    _build_monitor_runtime_market_hints = pb_monitor._build_monitor_runtime_market_hints
    _build_monitor_runtime_unstuck_hints = pb_monitor._build_monitor_runtime_unstuck_hints
    _update_monitor_runtime_hints = pb_monitor._update_monitor_runtime_hints
    _build_monitor_recent_section = pb_monitor._build_monitor_recent_section
    _build_monitor_position_side_payload = pb_monitor._build_monitor_position_side_payload
    _build_monitor_positions_section = pb_monitor._build_monitor_positions_section
    _build_monitor_snapshot = pb_monitor._build_monitor_snapshot
    _monitor_flush_snapshot = pb_monitor._monitor_flush_snapshot

    def _equity_hard_stop_enabled(self) -> bool:
        return bool(self.equity_hard_stop_loss["enabled"])

    def _equity_hard_stop_latch_path(self) -> str:
        return make_get_filepath(f"caches/equity_hard_stop/{self.exchange}/{self.user}.json")

    def _equity_hard_stop_write_latch(self, metrics: dict) -> str:
        path = self._equity_hard_stop_latch_path()
        payload = dict(metrics)
        tmp_path = path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
        os.replace(tmp_path, path)
        return path

    def _equity_hard_stop_remove_latch_file(self) -> None:
        path = self._equity_hard_stop_latch_path()
        if os.path.isfile(path):
            os.remove(path)

    def _equity_hard_stop_reset_state(self) -> None:
        self._equity_hard_stop_runtime.reset()
        self._equity_hard_stop_strategy_pnl_peak.reset()
        self._equity_hard_stop_last_metrics = None
        self._equity_hard_stop_last_red_progress = None
        self._equity_hard_stop_red_flat_confirmations = 0
        self._equity_hard_stop_pending_red_since_ms = None
        self._equity_hard_stop_halted = False
        self._equity_hard_stop_no_restart_latched = False
        self._equity_hard_stop_halted_until_ms = None
        self._equity_hard_stop_cooldown_intervention_active = False
        self._equity_hard_stop_cooldown_repanic_reset_pending = False
        self._equity_hard_stop_last_cooldown_intervention_log_ms = 0
        self._equity_hard_stop_pending_stop_event = None
        self._equity_hard_stop_last_stop_event = None
        self._runtime_forced_modes = {"long": {}, "short": {}}

    def _equity_hard_stop_runtime_initialized(self) -> bool:
        return bool(self._equity_hard_stop_runtime.initialized())

    def _equity_hard_stop_runtime_red_latched(self) -> bool:
        return bool(self._equity_hard_stop_runtime.red_latched())

    def _equity_hard_stop_runtime_tier(self) -> str:
        return str(self._equity_hard_stop_runtime.tier())

    def _equity_hard_stop_cooldown_position_policy(self) -> str:
        return normalize_hsl_cooldown_position_policy(
            get_optional_live_value(self.config, "hsl_position_during_cooldown_policy", "panic")
        )

    async def _calc_upnl_sum_strict(self) -> float:
        if not self.fetched_positions:
            return 0.0
        symbols = {x["symbol"] for x in self.fetched_positions}
        last_prices = await self.cm.get_last_prices(symbols, max_age_ms=60_000)
        upnl_sum = 0.0
        for elm in self.fetched_positions:
            symbol = elm["symbol"]
            if symbol not in last_prices:
                raise RuntimeError(f"missing last price for {symbol} while evaluating hard stop")
            upnl = calc_pnl(
                elm["position_side"],
                elm["price"],
                last_prices[symbol],
                elm["size"],
                self.inverse,
                self.c_mults[symbol],
            )
            if not math.isfinite(upnl):
                raise RuntimeError(
                    f"non-finite upnl for {symbol} {elm['position_side']} while evaluating hard stop"
                )
            upnl_sum += upnl
        return upnl_sum

    @staticmethod
    def _equity_hard_stop_fee_cost(fill: Any) -> float:
        if fill is None:
            return 0.0
        if isinstance(fill, dict):
            fee_obj = fill.get("fee")
            if isinstance(fee_obj, dict):
                return float(fee_obj.get("cost", 0.0) or 0.0)
            if isinstance(fee_obj, (int, float, str)):
                return float(fee_obj or 0.0)
            fees_obj = fill.get("fees")
        else:
            fees_obj = getattr(fill, "fees", None)
        if isinstance(fees_obj, dict):
            return float(fees_obj.get("cost", 0.0) or 0.0)
        if isinstance(fees_obj, (list, tuple)):
            total = 0.0
            for item in fees_obj:
                if isinstance(item, dict):
                    total += float(item.get("cost", 0.0) or 0.0)
            return total
        return 0.0

    def _equity_hard_stop_realized_pnl_now(self, pside: Optional[str] = None) -> float:
        if self._pnls_manager is None:
            return 0.0
        realized = 0.0
        for event in self._pnls_manager.get_events():
            if pside is not None and self._equity_hard_stop_fill_pside(event) != pside:
                continue
            realized += float(getattr(event, "pnl", 0.0) or 0.0)
            realized += self._equity_hard_stop_fee_cost(event)
        return realized

    def _pnls_lookback_start_ms(self) -> Optional[int]:
        config = getattr(self, "config", None)
        if config is None:
            return None
        lookback = parse_pnls_max_lookback_days(
            require_live_value(config, "pnls_max_lookback_days"),
            field_name="live.pnls_max_lookback_days",
        )
        return lookback.event_history_start_ms(self.get_exchange_time())

    def _get_effective_pnl_events(self) -> list:
        if self._pnls_manager is None:
            return []
        start_ms = self._pnls_lookback_start_ms()
        if start_ms is None:
            return self._pnls_manager.get_events()
        return self._pnls_manager.get_events(start_ms=start_ms)

    def _equity_hard_stop_lookback_ms(self) -> Optional[int]:
        lookback = parse_pnls_max_lookback_days(
            require_live_value(self.config, "pnls_max_lookback_days"),
            field_name="live.pnls_max_lookback_days",
        )
        return lookback.hsl_window_ms()

    def _equity_hard_stop_apply_sample(
        self,
        timestamp_ms: int,
        balance: float,
        realized_pnl: float,
        unrealized_pnl: float,
    ) -> dict:
        if not math.isfinite(balance) or balance <= 0.0:
            raise ValueError(f"balance must be finite and > 0, got {balance}")
        if not math.isfinite(realized_pnl):
            raise ValueError(f"realized_pnl must be finite, got {realized_pnl}")
        if not math.isfinite(unrealized_pnl):
            raise ValueError(f"unrealized_pnl must be finite, got {unrealized_pnl}")
        last_metrics = self._equity_hard_stop_last_metrics
        current_minute = int(timestamp_ms) // 60_000
        if last_metrics is not None and int(last_metrics["timestamp_ms"]) // 60_000 == current_minute:
            cached = dict(last_metrics)
            cached["changed"] = False
            cached["elapsed_minutes"] = 0
            self._equity_hard_stop_last_metrics = cached
            return cached
        lookback_ms = self._equity_hard_stop_lookback_ms()
        prev_tier = self._equity_hard_stop_runtime_tier()
        red_threshold = float(self.equity_hard_stop_loss["red_threshold"])
        ratio_yellow = float(self.equity_hard_stop_loss["tier_ratios"]["yellow"])
        ratio_orange = float(self.equity_hard_stop_loss["tier_ratios"]["orange"])
        ema_span_minutes = float(self.equity_hard_stop_loss["ema_span_minutes"])
        strategy_pnl = realized_pnl + unrealized_pnl
        peak_strategy_pnl = float(
            self._equity_hard_stop_strategy_pnl_peak.update(
                int(timestamp_ms),
                float(strategy_pnl),
                int(lookback_ms) if lookback_ms is not None else (2**64 - 1),
            )
        )
        baseline_balance = balance - realized_pnl
        equity = balance + unrealized_pnl
        peak_strategy_equity = max(float(equity), float(baseline_balance + peak_strategy_pnl))
        step = self._equity_hard_stop_runtime.apply_sample(
            timestamp_ms=int(timestamp_ms),
            equity=float(equity),
            peak_strategy_equity=float(peak_strategy_equity),
            red_threshold=red_threshold,
            ema_span_minutes=ema_span_minutes,
            tier_ratio_yellow=ratio_yellow,
            tier_ratio_orange=ratio_orange,
        )
        if not isinstance(step, dict):
            raise TypeError(
                "passivbot_rust.EquityHardStopRuntime.apply_sample() must return a dict, "
                f"got {type(step).__name__}"
            )

        metrics = {
            "timestamp_ms": int(timestamp_ms),
            "balance": float(balance),
            "realized_pnl": float(realized_pnl),
            "unrealized_pnl": float(unrealized_pnl),
            "strategy_pnl": float(strategy_pnl),
            "peak_strategy_pnl": float(peak_strategy_pnl),
            "baseline_balance": float(baseline_balance),
            "equity": float(equity),
            "peak_strategy_equity": float(step["peak_strategy_equity"]),
            "rolling_peak_strategy_equity": float(step["rolling_peak_strategy_equity"]),
            "drawdown_raw": float(step["drawdown_raw"]),
            "drawdown_ema": float(step["drawdown_ema"]),
            "drawdown_score": float(step["drawdown_score"]),
            "red_threshold": red_threshold,
            "tier": str(step["tier"]),
            "changed": bool(step["changed"]) or str(step["tier"]) != prev_tier,
            "alpha": float(step["alpha"]),
            "elapsed_minutes": int(step["elapsed_minutes"]),
        }
        self._equity_hard_stop_last_metrics = metrics
        return metrics

    def _equity_hard_stop_log_transition(self, metrics: dict, prev_tier: str) -> None:
        logging.info(
            "[risk] equity hard stop tier transition %s -> %s | balance=%.6f equity=%.6f "
            "peak_strategy_equity=%.6f drawdown_raw=%.6f drawdown_ema=%.6f drawdown_score=%.6f "
            "strategy_pnl=%.6f peak_strategy_pnl=%.6f "
            "red_threshold=%.6f yellow=%.3f orange=%.3f",
            prev_tier,
            metrics["tier"],
            metrics["balance"],
            metrics["equity"],
            metrics["peak_strategy_equity"],
            metrics["drawdown_raw"],
            metrics["drawdown_ema"],
            metrics["drawdown_score"],
            metrics["strategy_pnl"],
            metrics["peak_strategy_pnl"],
            metrics["red_threshold"],
            float(self.equity_hard_stop_loss["tier_ratios"]["yellow"]),
            float(self.equity_hard_stop_loss["tier_ratios"]["orange"]),
        )

    def _equity_hard_stop_build_latch_payload(
        self,
        *,
        stop_event_timestamp_ms: int,
        balance: Optional[float] = None,
        realized_pnl: Optional[float] = None,
        unrealized_pnl: Optional[float] = None,
        strategy_pnl: Optional[float] = None,
        peak_strategy_pnl: Optional[float] = None,
        equity: float,
        peak_strategy_equity: float,
        trigger_peak_strategy_equity: float,
        drawdown_raw: float,
        drawdown_ema: float,
        drawdown_score: float,
        no_restart_latched: bool,
        cooldown_until_ms: Optional[int],
    ) -> dict:
        return {
            "triggered_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "exchange": str(self.exchange),
            "user": str(self.user),
            "tier": "red",
            "red_threshold": float(self.equity_hard_stop_loss["red_threshold"]),
            "ema_span_minutes": float(self.equity_hard_stop_loss["ema_span_minutes"]),
            "cooldown_minutes_after_red": float(
                self.equity_hard_stop_loss["cooldown_minutes_after_red"]
            ),
            "no_restart_drawdown_threshold": float(self.equity_hard_stop_loss["no_restart_drawdown_threshold"]),
            "tier_ratios": {
                "yellow": float(self.equity_hard_stop_loss["tier_ratios"]["yellow"]),
                "orange": float(self.equity_hard_stop_loss["tier_ratios"]["orange"]),
            },
            "orange_tier_mode": str(self.equity_hard_stop_loss["orange_tier_mode"]),
            "panic_close_order_type": str(self.equity_hard_stop_loss["panic_close_order_type"]),
            "stop_event_timestamp_ms": int(stop_event_timestamp_ms),
            "balance": None if balance is None else float(balance),
            "realized_pnl": None if realized_pnl is None else float(realized_pnl),
            "unrealized_pnl": None if unrealized_pnl is None else float(unrealized_pnl),
            "strategy_pnl": None if strategy_pnl is None else float(strategy_pnl),
            "peak_strategy_pnl": None if peak_strategy_pnl is None else float(peak_strategy_pnl),
            "equity": float(equity),
            "peak_strategy_equity": float(peak_strategy_equity),
            "trigger_peak_strategy_equity": float(trigger_peak_strategy_equity),
            "drawdown_raw": float(drawdown_raw),
            "drawdown_ema": float(drawdown_ema),
            "drawdown_score": float(drawdown_score),
            "no_restart_latched": bool(no_restart_latched),
            "auto_restart_eligible": bool(
                (not no_restart_latched)
                and float(self.equity_hard_stop_loss["cooldown_minutes_after_red"]) > 0.0
            ),
            "cooldown_until_ms": None if cooldown_until_ms is None else int(cooldown_until_ms),
        }

    async def _equity_hard_stop_compute_stop_event(self, stop_event_ts_ms: int) -> dict:
        balance = float(self.get_raw_balance())
        unrealized_pnl = float(await self._calc_upnl_sum_strict())
        realized_pnl = float(self._equity_hard_stop_realized_pnl_now())
        strategy_pnl = realized_pnl + unrealized_pnl
        peak_strategy_pnl = float(
            max(
                strategy_pnl,
                (self._equity_hard_stop_last_metrics or {}).get("peak_strategy_pnl", strategy_pnl),
            )
        )
        equity = float(balance + unrealized_pnl)
        trigger_peak_strategy_equity = float(self._equity_hard_stop_runtime.peak_strategy_equity())
        peak_strategy_equity = float(max(equity, (balance - realized_pnl) + peak_strategy_pnl))
        if not math.isfinite(trigger_peak_strategy_equity) or trigger_peak_strategy_equity <= 0.0:
            raise RuntimeError(
                f"invalid hard-stop trigger_peak_strategy_equity at stop finalization: {trigger_peak_strategy_equity}"
            )
        if not math.isfinite(peak_strategy_equity) or peak_strategy_equity <= 0.0:
            raise RuntimeError(f"invalid hard-stop rolling peak_strategy_equity at stop finalization: {peak_strategy_equity}")
        drawdown_ema = float(self._equity_hard_stop_runtime.drawdown_ema())
        drawdown_raw = max(0.0, 1.0 - equity / max(peak_strategy_equity, 1e-12))
        return {
            "stop_event_timestamp_ms": int(stop_event_ts_ms),
            "balance": balance,
            "realized_pnl": realized_pnl,
            "unrealized_pnl": unrealized_pnl,
            "strategy_pnl": strategy_pnl,
            "peak_strategy_pnl": peak_strategy_pnl,
            "equity": equity,
            "peak_strategy_equity": peak_strategy_equity,
            "trigger_peak_strategy_equity": trigger_peak_strategy_equity,
            "drawdown_raw": drawdown_raw,
            "drawdown_ema": drawdown_ema,
            "drawdown_score": min(drawdown_raw, drawdown_ema),
        }

    async def _equity_hard_stop_wait_for_cooldown(self, cooldown_until_ms: int) -> None:
        self._equity_hard_stop_halted_until_ms = int(cooldown_until_ms)
        while not self.stop_signal_received:
            now_ms = int(self.get_exchange_time())
            if now_ms >= cooldown_until_ms:
                return
            remaining_seconds = max(0.0, (cooldown_until_ms - now_ms) / 1000.0)
            logging.info("[risk] RED cooldown active | remaining_seconds=%.1f", remaining_seconds)
            await asyncio.sleep(min(float(self.live_value("execution_delay_seconds")), 5.0))

    def _equity_hard_stop_reset_after_restart(self) -> None:
        self._equity_hard_stop_runtime.reset()
        self._equity_hard_stop_strategy_pnl_peak.reset()
        self._equity_hard_stop_clear_runtime_forced_modes()
        self._equity_hard_stop_red_flat_confirmations = 0
        self._equity_hard_stop_last_red_progress = None
        self._equity_hard_stop_pending_red_since_ms = None
        self._equity_hard_stop_halted = False
        self._equity_hard_stop_no_restart_latched = False
        self._equity_hard_stop_halted_until_ms = None
        self._equity_hard_stop_cooldown_intervention_active = False
        self._equity_hard_stop_cooldown_repanic_reset_pending = False
        self._equity_hard_stop_last_cooldown_intervention_log_ms = 0
        self._equity_hard_stop_pending_stop_event = None

    def _equity_hard_stop_position_symbols(self) -> list[str]:
        symbols = []
        for symbol, position in self.positions.items():
            if any(
                float(position.get(pside, {}).get("size", 0.0) or 0.0) != 0.0
                for pside in ("long", "short")
            ):
                symbols.append(symbol)
        return sorted(symbols)

    def _equity_hard_stop_halted_mode(self, pside: str, symbol: str | None) -> str:
        policy = self._equity_hard_stop_cooldown_position_policy()
        size = 0.0
        if symbol is not None:
            size = float(self.positions.get(symbol, {}).get(pside, {}).get("size", 0.0) or 0.0)
        if size == 0.0:
            return "graceful_stop"
        if policy == "panic":
            return "panic"
        if policy == "manual":
            return "manual"
        if policy == "tp_only":
            return "tp_only"
        return "graceful_stop"

    def _equity_hard_stop_refresh_halted_runtime_forced_modes(self) -> None:
        if not self._equity_hard_stop_halted:
            self._equity_hard_stop_clear_runtime_forced_modes()
            return
        forced = {"long": {}, "short": {}}
        symbols = set(self.positions.keys()) | set(self.open_orders.keys()) | set(self.active_symbols)
        for symbol in symbols:
            for pside in ("long", "short"):
                forced[pside][symbol] = self._equity_hard_stop_halted_mode(pside, symbol)
        self._runtime_forced_modes = forced

    async def _equity_hard_stop_refresh_cooldown_after_repanic(self, now_ms: int) -> None:
        cooldown_minutes = float(self.equity_hard_stop_loss["cooldown_minutes_after_red"])
        cooldown_ms = int(round(cooldown_minutes * 60_000.0)) if cooldown_minutes > 0.0 else 0
        cooldown_until_ms = now_ms + cooldown_ms if cooldown_ms > 0 else None
        stop_event = await self._equity_hard_stop_compute_stop_event(now_ms)
        payload = self._equity_hard_stop_build_latch_payload(
            stop_event_timestamp_ms=now_ms,
            balance=stop_event.get("balance"),
            realized_pnl=stop_event.get("realized_pnl"),
            unrealized_pnl=stop_event.get("unrealized_pnl"),
            strategy_pnl=stop_event.get("strategy_pnl"),
            peak_strategy_pnl=stop_event.get("peak_strategy_pnl"),
            equity=float(stop_event["equity"]),
            peak_strategy_equity=float(stop_event["peak_strategy_equity"]),
            trigger_peak_strategy_equity=float(stop_event["trigger_peak_strategy_equity"]),
            drawdown_raw=float(stop_event["drawdown_raw"]),
            drawdown_ema=float(stop_event["drawdown_ema"]),
            drawdown_score=float(stop_event["drawdown_score"]),
            no_restart_latched=False,
            cooldown_until_ms=cooldown_until_ms,
        )
        self._equity_hard_stop_last_stop_event = payload
        self._equity_hard_stop_halted_until_ms = cooldown_until_ms
        self._equity_hard_stop_cooldown_intervention_active = False
        self._equity_hard_stop_cooldown_repanic_reset_pending = False
        self._equity_hard_stop_last_cooldown_intervention_log_ms = 0
        latch_path = self._equity_hard_stop_write_latch(payload)
        self._equity_hard_stop_refresh_halted_runtime_forced_modes()
        logging.critical(
            "[risk] cooldown violation repanic flattened; cooldown reset from flat_ts=%s to cooldown_until_ms=%s latch=%s",
            now_ms,
            cooldown_until_ms if cooldown_until_ms is not None else "none",
            latch_path,
        )

    async def _equity_hard_stop_handle_position_during_cooldown(self, now_ms: int) -> bool:
        if not self._equity_hard_stop_halted or self._equity_hard_stop_no_restart_latched:
            return False
        cooldown_until_ms = self._equity_hard_stop_halted_until_ms
        if cooldown_until_ms is None or now_ms >= cooldown_until_ms:
            return False

        symbols = self._equity_hard_stop_position_symbols()
        policy = self._equity_hard_stop_cooldown_position_policy()
        if not symbols:
            if self._equity_hard_stop_cooldown_repanic_reset_pending:
                await self._equity_hard_stop_refresh_cooldown_after_repanic(now_ms)
                return True
            if self._equity_hard_stop_cooldown_intervention_active:
                logging.info(
                    "[risk] cooldown intervention ended flat; policy=%s original_cooldown_until_ms=%s",
                    policy,
                    cooldown_until_ms,
                )
            self._equity_hard_stop_cooldown_intervention_active = False
            self._equity_hard_stop_cooldown_repanic_reset_pending = False
            self._equity_hard_stop_last_cooldown_intervention_log_ms = 0
            self._equity_hard_stop_refresh_halted_runtime_forced_modes()
            return False

        should_log = (
            not self._equity_hard_stop_cooldown_intervention_active
            or self._equity_hard_stop_last_cooldown_intervention_log_ms == 0
            or now_ms - self._equity_hard_stop_last_cooldown_intervention_log_ms
            >= self._equity_hard_stop_cooldown_log_interval_ms
        )
        if should_log:
            logging.critical(
                "[risk] detected non-flat position during RED cooldown | policy=%s symbols=%s cooldown_until_ms=%s",
                policy,
                ",".join(symbols),
                cooldown_until_ms,
            )
            self._equity_hard_stop_last_cooldown_intervention_log_ms = now_ms
        self._equity_hard_stop_cooldown_intervention_active = True

        if policy == "normal":
            self._equity_hard_stop_reset_after_restart()
            self._equity_hard_stop_remove_latch_file()
            logging.critical(
                "[risk] operator override during RED cooldown: resumed normal operation and reset drawdown tracker"
            )
            return True

        self._equity_hard_stop_cooldown_repanic_reset_pending = policy == "panic"
        self._equity_hard_stop_refresh_halted_runtime_forced_modes()
        return False

    async def _equity_hard_stop_initialize_from_history(self) -> None:
        if not self._equity_hard_stop_enabled():
            return
        self._equity_hard_stop_reset_state()
        history = await self.get_balance_equity_history(current_balance=self.get_raw_balance())
        if "timeline" not in history:
            raise ValueError("get_balance_equity_history() missing required key: timeline")
        timeline = history["timeline"]
        if not isinstance(timeline, list):
            raise TypeError(
                f"get_balance_equity_history()['timeline'] must be a list, got {type(timeline).__name__}"
            )

        cooldown_minutes = float(self.equity_hard_stop_loss["cooldown_minutes_after_red"])
        no_restart_drawdown_threshold = float(
            self.equity_hard_stop_loss["no_restart_drawdown_threshold"]
        )
        cooldown_ms = int(round(cooldown_minutes * 60_000.0)) if cooldown_minutes > 0.0 else 0
        cooldown_until_ms = None
        pending_red = False
        n_rows = 0
        latest_terminal_stop = None
        for row in timeline:
            if not isinstance(row, dict):
                continue
            required = ("timestamp", "balance", "realized_pnl", "unrealized_pnl")
            if any(key not in row for key in required):
                continue
            ts = int(row["timestamp"])
            balance = float(row["balance"])
            realized_pnl = float(row["realized_pnl"])
            unrealized_pnl = float(row["unrealized_pnl"])

            if cooldown_until_ms is not None:
                if ts < cooldown_until_ms:
                    continue
                self._equity_hard_stop_reset_after_restart()
                cooldown_until_ms = None
                pending_red = False

            current_metrics = self._equity_hard_stop_apply_sample(
                int(ts), balance, realized_pnl, unrealized_pnl
            )
            n_rows += 1

            if self._equity_hard_stop_runtime_tier() == "red":
                pending_red = True
                self._equity_hard_stop_pending_red_since_ms = int(ts)

            is_flat = bool(row["is_flat"]) if "is_flat" in row else False
            if pending_red and is_flat:
                peak_strategy_equity = float(current_metrics["peak_strategy_equity"])
                trigger_peak_strategy_equity = float(
                    self._equity_hard_stop_runtime.peak_strategy_equity()
                )
                if not math.isfinite(peak_strategy_equity) or peak_strategy_equity <= 0.0:
                    raise RuntimeError(
                        "invalid peak_strategy_equity during hard-stop replay at "
                        f"ts={ts}: {peak_strategy_equity}"
                    )
                if (
                    not math.isfinite(trigger_peak_strategy_equity)
                    or trigger_peak_strategy_equity <= 0.0
                ):
                    raise RuntimeError(
                        "invalid trigger_peak_strategy_equity during hard-stop replay at "
                        f"ts={ts}: {trigger_peak_strategy_equity}"
                    )
                stop_drawdown_raw = float(current_metrics["drawdown_raw"])
                if stop_drawdown_raw >= no_restart_drawdown_threshold or cooldown_ms <= 0:
                    payload = self._equity_hard_stop_build_latch_payload(
                        stop_event_timestamp_ms=ts,
                        balance=balance,
                        realized_pnl=realized_pnl,
                        unrealized_pnl=unrealized_pnl,
                        strategy_pnl=float(current_metrics["strategy_pnl"]),
                        peak_strategy_pnl=float(current_metrics["peak_strategy_pnl"]),
                        equity=float(current_metrics["equity"]),
                        peak_strategy_equity=peak_strategy_equity,
                        trigger_peak_strategy_equity=trigger_peak_strategy_equity,
                        drawdown_raw=float(current_metrics["drawdown_raw"]),
                        drawdown_ema=float(current_metrics["drawdown_ema"]),
                        drawdown_score=float(current_metrics["drawdown_score"]),
                        no_restart_latched=bool(
                            stop_drawdown_raw >= no_restart_drawdown_threshold
                        ),
                        cooldown_until_ms=None,
                    )
                    self._equity_hard_stop_last_stop_event = payload
                    latest_terminal_stop = payload
                    latch_path = self._equity_hard_stop_write_latch(payload)
                    logging.critical(
                        "[risk] hard-stop replay found terminal RED stop event in exchange-derived "
                        "history | stop_ts=%s drawdown_raw=%.6f "
                        "no_restart_drawdown_threshold=%.6f diagnostic=%s",
                        ts,
                        stop_drawdown_raw,
                        no_restart_drawdown_threshold,
                        latch_path,
                    )
                    break
                cooldown_until_ms = ts + cooldown_ms
                pending_red = False
                self._equity_hard_stop_pending_red_since_ms = None

        if latest_terminal_stop is not None:
            self.stop_signal_received = True
            return

        now_ms = int(self.get_exchange_time())
        if cooldown_until_ms is not None:
            if now_ms >= cooldown_until_ms:
                self._equity_hard_stop_reset_after_restart()
                cooldown_until_ms = None
                pending_red = False
            else:
                self._equity_hard_stop_halted = True
                self._equity_hard_stop_no_restart_latched = False
                self._equity_hard_stop_halted_until_ms = cooldown_until_ms
                self._equity_hard_stop_cooldown_intervention_active = False
                self._equity_hard_stop_cooldown_repanic_reset_pending = False
                self._equity_hard_stop_last_cooldown_intervention_log_ms = 0
                self._equity_hard_stop_refresh_halted_runtime_forced_modes()
                logging.critical(
                    "[risk] reconstructed active RED cooldown from exchange-derived history | remaining_seconds=%.1f policy=%s",
                    (cooldown_until_ms - now_ms) / 1000.0,
                    self._equity_hard_stop_cooldown_position_policy(),
                )
                return

        current_balance = self.get_raw_balance()
        current_realized = self._equity_hard_stop_realized_pnl_now()
        current_upnl = await self._calc_upnl_sum_strict()
        current_metrics = self._equity_hard_stop_apply_sample(
            now_ms,
            float(current_balance),
            float(current_realized),
            float(current_upnl),
        )
        logging.info(
            "[risk] hard-stop initialized from equity history | rows=%d tier=%s equity=%.6f "
            "peak_strategy_equity=%.6f rolling_peak_strategy_equity=%.6f "
            "drawdown_raw=%.6f drawdown_ema=%.6f drawdown_score=%.6f",
            n_rows,
            current_metrics["tier"],
            current_metrics["equity"],
            current_metrics["peak_strategy_equity"],
            current_metrics["rolling_peak_strategy_equity"],
            current_metrics["drawdown_raw"],
            current_metrics["drawdown_ema"],
            current_metrics["drawdown_score"],
        )
        if current_metrics["tier"] == "red":
            self._equity_hard_stop_pending_red_since_ms = int(current_metrics["timestamp_ms"])

    async def _equity_hard_stop_check(self) -> Optional[dict]:
        if not self._equity_hard_stop_enabled():
            return None
        if not self._equity_hard_stop_runtime_initialized():
            await self._equity_hard_stop_initialize_from_history()
        now_ms = int(self.get_exchange_time())
        if self._equity_hard_stop_halted:
            if await self._equity_hard_stop_handle_position_during_cooldown(now_ms):
                if not self._equity_hard_stop_halted:
                    return None
            if self._equity_hard_stop_halted:
                cooldown_until_ms = self._equity_hard_stop_halted_until_ms
                if (
                    not self._equity_hard_stop_no_restart_latched
                    and cooldown_until_ms is not None
                    and now_ms >= cooldown_until_ms
                ):
                    self._equity_hard_stop_reset_after_restart()
                    self._equity_hard_stop_remove_latch_file()
                    logging.info("[risk] RED cooldown elapsed; trading resumed")
                else:
                    self._equity_hard_stop_refresh_halted_runtime_forced_modes()
                    return {
                        "halted": True,
                        "cooldown_until_ms": cooldown_until_ms,
                    }

        prev_latched = self._equity_hard_stop_runtime_red_latched()
        prev_tier = self._equity_hard_stop_runtime_tier()
        balance = self.get_raw_balance()
        realized_pnl = self._equity_hard_stop_realized_pnl_now()
        unrealized_pnl = await self._calc_upnl_sum_strict()
        metrics = self._equity_hard_stop_apply_sample(
            now_ms,
            float(balance),
            float(realized_pnl),
            float(unrealized_pnl),
        )
        if metrics["changed"]:
            self._equity_hard_stop_log_transition(metrics, prev_tier)
        if metrics["tier"] == "red" and not prev_latched:
            self._equity_hard_stop_pending_red_since_ms = int(metrics["timestamp_ms"])
            logging.critical(
                "[risk] equity hard stop RED triggered | equity=%.6f "
                "peak_strategy_equity=%.6f rolling_peak_strategy_equity=%.6f "
                "drawdown_score=%.6f "
                "red_threshold=%.6f",
                metrics["equity"],
                metrics["peak_strategy_equity"],
                metrics["rolling_peak_strategy_equity"],
                metrics["drawdown_score"],
                metrics["red_threshold"],
            )
        elif metrics["tier"] != "red":
            self._equity_hard_stop_pending_red_since_ms = None
        return metrics

    def _equity_hard_stop_set_red_runtime_forced_modes(self) -> None:
        forced = {"long": {}, "short": {}}
        symbols = set(self.positions.keys()) | set(self.open_orders.keys()) | set(self.active_symbols)
        for symbol in symbols:
            for pside in ("long", "short"):
                forced[pside][symbol] = "panic"
        self._runtime_forced_modes = forced

    def _equity_hard_stop_clear_runtime_forced_modes(self) -> None:
        self._runtime_forced_modes = {"long": {}, "short": {}}

    def _equity_hard_stop_count_open_positions(self) -> int:
        n_positions = 0
        for pos in self.positions.values():
            for pside in ("long", "short"):
                if float(pos.get(pside, {}).get("size", 0.0) or 0.0) != 0.0:
                    n_positions += 1
        return n_positions

    def _equity_hard_stop_count_blocking_open_orders(self) -> tuple[int, int]:
        entry_orders = 0
        nonpanic_close_orders = 0
        for orders in self.open_orders.values():
            for order in orders:
                reduce_only = bool(order.get("reduce_only") or order.get("reduceOnly"))
                if not reduce_only:
                    entry_orders += 1
                    continue
                pb_type = self._resolve_pb_order_type(order).lower()
                if "panic" not in pb_type:
                    nonpanic_close_orders += 1
        return entry_orders, nonpanic_close_orders

    def _equity_hard_stop_log_red_progress(
        self,
        n_positions: int,
        entry_orders: int,
        nonpanic_close_orders: int,
        flat_confirmations: int,
    ) -> None:
        progress = (n_positions, entry_orders, nonpanic_close_orders, flat_confirmations)
        if progress == self._equity_hard_stop_last_red_progress:
            return
        self._equity_hard_stop_last_red_progress = progress
        logging.info(
            "[risk] RED supervisor progress | positions=%d entry_orders=%d "
            "nonpanic_close_orders=%d flat_confirmations=%d/2",
            n_positions,
            entry_orders,
            nonpanic_close_orders,
            flat_confirmations,
        )

    async def _equity_hard_stop_finalize_red_stop(self, stop_event: Optional[dict] = None) -> None:
        stop_ts_ms = int(self.get_exchange_time())
        if stop_event is None:
            stop_event = await self._equity_hard_stop_compute_stop_event(stop_ts_ms)
        else:
            stop_ts_ms = int(stop_event["stop_event_timestamp_ms"])
        cooldown_minutes = float(self.equity_hard_stop_loss["cooldown_minutes_after_red"])
        no_restart_drawdown_threshold = float(self.equity_hard_stop_loss["no_restart_drawdown_threshold"])
        no_restart_latched = bool(stop_event["drawdown_raw"] >= no_restart_drawdown_threshold)
        cooldown_ms = int(round(cooldown_minutes * 60_000.0)) if cooldown_minutes > 0.0 else 0
        cooldown_until_ms = (
            None if no_restart_latched or cooldown_ms <= 0 else int(stop_ts_ms + cooldown_ms)
        )
        payload = self._equity_hard_stop_build_latch_payload(
            stop_event_timestamp_ms=stop_ts_ms,
            balance=stop_event.get("balance"),
            realized_pnl=stop_event.get("realized_pnl"),
            unrealized_pnl=stop_event.get("unrealized_pnl"),
            strategy_pnl=stop_event.get("strategy_pnl"),
            peak_strategy_pnl=stop_event.get("peak_strategy_pnl"),
            equity=float(stop_event["equity"]),
            peak_strategy_equity=float(stop_event["peak_strategy_equity"]),
            trigger_peak_strategy_equity=float(stop_event["trigger_peak_strategy_equity"]),
            drawdown_raw=float(stop_event["drawdown_raw"]),
            drawdown_ema=float(stop_event["drawdown_ema"]),
            drawdown_score=float(stop_event["drawdown_score"]),
            no_restart_latched=no_restart_latched,
            cooldown_until_ms=cooldown_until_ms,
        )
        self._equity_hard_stop_last_stop_event = payload
        latch_path = self._equity_hard_stop_write_latch(payload)

        if no_restart_latched or cooldown_until_ms is None:
            logging.critical(
                "[risk] RED stop finalized (terminal) | stop_ts=%s equity=%.6f "
                "peak_strategy_equity=%.6f drawdown_raw=%.6f "
                "no_restart_drawdown_threshold=%.6f latch=%s",
                stop_ts_ms,
                stop_event["equity"],
                stop_event["peak_strategy_equity"],
                stop_event["drawdown_raw"],
                no_restart_drawdown_threshold,
                latch_path,
            )
            self._equity_hard_stop_clear_runtime_forced_modes()
            self._equity_hard_stop_pending_stop_event = None
            self.stop_signal_received = True
            return

        self._equity_hard_stop_halted = True
        self._equity_hard_stop_no_restart_latched = False
        self._equity_hard_stop_halted_until_ms = cooldown_until_ms
        self._equity_hard_stop_cooldown_intervention_active = False
        self._equity_hard_stop_cooldown_repanic_reset_pending = False
        self._equity_hard_stop_last_cooldown_intervention_log_ms = 0
        self._equity_hard_stop_pending_stop_event = None
        self._equity_hard_stop_refresh_halted_runtime_forced_modes()
        logging.critical(
            "[risk] RED stop finalized (cooldown active) | stop_ts=%s "
            "drawdown_raw=%.6f cooldown_until_ms=%s policy=%s latch=%s",
            stop_ts_ms,
            stop_event["drawdown_raw"],
            cooldown_until_ms,
            self._equity_hard_stop_cooldown_position_policy(),
            latch_path,
        )
        return

    async def _equity_hard_stop_run_red_supervisor(self) -> None:
        if self._equity_hard_stop_supervisor_running:
            return
        self._equity_hard_stop_supervisor_running = True
        self._equity_hard_stop_red_flat_confirmations = 0
        self._equity_hard_stop_last_red_progress = None
        self._equity_hard_stop_pending_stop_event = None
        try:
            logging.critical("[risk] entering RED supervisor loop (panic-close until confirmed flat)")
            while not self.stop_signal_received:
                if not await self.update_pos_oos_pnls_ohlcvs():
                    await asyncio.sleep(0.5)
                    continue

                n_positions = self._equity_hard_stop_count_open_positions()
                entry_orders, nonpanic_close_orders = self._equity_hard_stop_count_blocking_open_orders()
                if n_positions == 0 and entry_orders == 0 and nonpanic_close_orders == 0:
                    if self._equity_hard_stop_red_flat_confirmations == 0:
                        self._equity_hard_stop_pending_stop_event = (
                            await self._equity_hard_stop_compute_stop_event(
                                int(self.get_exchange_time())
                            )
                        )
                    self._equity_hard_stop_red_flat_confirmations += 1
                else:
                    self._equity_hard_stop_red_flat_confirmations = 0
                    self._equity_hard_stop_pending_stop_event = None
                self._equity_hard_stop_log_red_progress(
                    n_positions,
                    entry_orders,
                    nonpanic_close_orders,
                    self._equity_hard_stop_red_flat_confirmations,
                )
                if self._equity_hard_stop_red_flat_confirmations >= 2:
                    await self._equity_hard_stop_finalize_red_stop(
                        self._equity_hard_stop_pending_stop_event
                    )
                    return

                self._equity_hard_stop_set_red_runtime_forced_modes()
                try:
                    await self.execute_to_exchange()
                except RestartBotException as e:
                    logging.error("[risk] RED supervisor ignored restart request: %s", e)
                except Exception as e:
                    logging.error("[risk] RED supervisor execute_to_exchange failed: %s", e)
                    traceback.print_exc()
                await asyncio.sleep(float(self.live_value("execution_delay_seconds")))
        finally:
            self._equity_hard_stop_supervisor_running = False

    def _apply_equity_hard_stop_orange_overlay(self) -> None:
        if not self._equity_hard_stop_enabled():
            return
        if self._equity_hard_stop_runtime_red_latched() or self._equity_hard_stop_runtime_tier() != "orange":
            return
        orange_mode = str(self.equity_hard_stop_loss["orange_tier_mode"])
        symbols = (
            set(self.PB_modes["long"].keys())
            | set(self.PB_modes["short"].keys())
            | set(self.positions.keys())
            | set(self.open_orders.keys())
        )
        for symbol in symbols:
            for pside in ("long", "short"):
                if symbol not in self.PB_modes[pside]:
                    continue
                current_mode = self.PB_modes[pside][symbol]
                if orange_mode == "graceful_stop":
                    if current_mode == "normal":
                        self.PB_modes[pside][symbol] = "graceful_stop"
                else:
                    size = float(self.positions.get(symbol, {}).get(pside, {}).get("size", 0.0) or 0.0)
                    if size == 0.0:
                        continue
                    if current_mode in ("normal", "graceful_stop"):
                        self.PB_modes[pside][symbol] = "tp_only_with_active_entry_cancellation"

    _hsl_psides = pb_hsl._hsl_psides
    _hsl_state = pb_hsl._hsl_state
    _parse_hsl_config = pb_hsl._parse_hsl_config
    _equity_hard_stop_enabled = pb_hsl._equity_hard_stop_enabled
    _equity_hard_stop_signal_mode = pb_hsl._equity_hard_stop_signal_mode
    _equity_hard_stop_cooldown_position_policy = pb_hsl._equity_hard_stop_cooldown_position_policy
    _equity_hard_stop_halted_mode = pb_hsl._equity_hard_stop_halted_mode
    _equity_hard_stop_panic_close_order_type = pb_hsl._equity_hard_stop_panic_close_order_type
    _equity_hard_stop_signal_values = pb_hsl._equity_hard_stop_signal_values
    _equity_hard_stop_latch_path = pb_hsl._equity_hard_stop_latch_path
    _equity_hard_stop_write_latch = pb_hsl._equity_hard_stop_write_latch
    _equity_hard_stop_remove_latch_file = pb_hsl._equity_hard_stop_remove_latch_file
    _equity_hard_stop_reset_state = pb_hsl._equity_hard_stop_reset_state
    _equity_hard_stop_runtime_initialized = pb_hsl._equity_hard_stop_runtime_initialized
    _equity_hard_stop_runtime_red_latched = pb_hsl._equity_hard_stop_runtime_red_latched
    _equity_hard_stop_runtime_tier = pb_hsl._equity_hard_stop_runtime_tier
    _equity_hard_stop_fill_pside = staticmethod(pb_hsl._equity_hard_stop_fill_pside)
    _calc_upnl_sum_strict = pb_hsl._calc_upnl_sum_strict
    _equity_hard_stop_fee_cost = staticmethod(pb_hsl._equity_hard_stop_fee_cost)
    _get_exchange_fee_rates = pb_hsl._get_exchange_fee_rates
    _orchestrator_exchange_params = pb_hsl._orchestrator_exchange_params
    _equity_hard_stop_realized_pnl_now = pb_hsl._equity_hard_stop_realized_pnl_now
    _equity_hard_stop_lookback_ms = pb_hsl._equity_hard_stop_lookback_ms
    _equity_hard_stop_apply_sample = pb_hsl._equity_hard_stop_apply_sample
    _equity_hard_stop_log_transition = pb_hsl._equity_hard_stop_log_transition
    _equity_hard_stop_format_remaining_time = staticmethod(
        pb_hsl._equity_hard_stop_format_remaining_time
    )
    _equity_hard_stop_build_latch_payload = pb_hsl._equity_hard_stop_build_latch_payload
    _equity_hard_stop_compute_stop_event = pb_hsl._equity_hard_stop_compute_stop_event
    _equity_hard_stop_infer_replay_contract = pb_hsl._equity_hard_stop_infer_replay_contract
    _equity_hard_stop_log_cooldown_status = pb_hsl._equity_hard_stop_log_cooldown_status
    _equity_hard_stop_position_symbols = pb_hsl._equity_hard_stop_position_symbols
    _equity_hard_stop_refresh_cooldown_after_repanic = (
        pb_hsl._equity_hard_stop_refresh_cooldown_after_repanic
    )
    _equity_hard_stop_handle_position_during_cooldown = (
        pb_hsl._equity_hard_stop_handle_position_during_cooldown
    )
    _equity_hard_stop_reset_after_restart = pb_hsl._equity_hard_stop_reset_after_restart
    _equity_hard_stop_replay_from_boundary = pb_hsl._equity_hard_stop_replay_from_boundary
    _equity_hard_stop_refresh_halted_runtime_forced_modes = (
        pb_hsl._equity_hard_stop_refresh_halted_runtime_forced_modes
    )
    _equity_hard_stop_initialize_from_history = pb_hsl._equity_hard_stop_initialize_from_history
    _equity_hard_stop_log_status = pb_hsl._equity_hard_stop_log_status
    _equity_hard_stop_check = pb_hsl._equity_hard_stop_check
    _equity_hard_stop_set_red_runtime_forced_modes = pb_hsl._equity_hard_stop_set_red_runtime_forced_modes
    _equity_hard_stop_clear_runtime_forced_modes = pb_hsl._equity_hard_stop_clear_runtime_forced_modes
    _equity_hard_stop_count_open_positions = pb_hsl._equity_hard_stop_count_open_positions
    _equity_hard_stop_count_blocking_open_orders = pb_hsl._equity_hard_stop_count_blocking_open_orders
    _equity_hard_stop_log_red_progress = pb_hsl._equity_hard_stop_log_red_progress
    _equity_hard_stop_finalize_red_stop = pb_hsl._equity_hard_stop_finalize_red_stop
    _equity_hard_stop_run_red_supervisor = pb_hsl._equity_hard_stop_run_red_supervisor
    _apply_equity_hard_stop_orange_overlay = pb_hsl._apply_equity_hard_stop_orange_overlay

    async def start_bot(self):
        """Initialise state, warm cached data, and launch background loops."""
        self._log_startup_banner()
        self._bot_ready = False
        logging.info("[boot] starting bot %s...", self.exchange)
        boot_stage = "start"

        def set_boot_stage(stage: str) -> None:
            nonlocal boot_stage
            boot_stage = stage

        try:
            maybe_apply_boot_stagger = getattr(self, "_maybe_apply_boot_stagger", None)
            if maybe_apply_boot_stagger is None:
                async def maybe_apply_boot_stagger():
                    await pb_startup_utils.maybe_apply_boot_stagger(self)

            run_startup_preloop = getattr(self, "_run_startup_preloop", None)
            if run_startup_preloop is None:
                async def run_startup_preloop(set_stage):
                    return await pb_startup_utils.run_startup_preloop(self, set_stage)

            finalize_startup_ready = getattr(self, "_finalize_startup_ready", None)
            if finalize_startup_ready is None:
                async def finalize_startup_ready():
                    await pb_startup_utils.finalize_startup_ready(self)

            handle_startup_error = getattr(self, "_handle_startup_error", None)
            if handle_startup_error is None:
                async def handle_startup_error(exc, stage):
                    await pb_startup_utils.handle_startup_error(self, exc, stage)
            self._monitor_record_event(
                "bot.start",
                ("bot", "lifecycle", "start"),
                {
                    "exchange": self.exchange,
                    "user": self.user,
                    "pid": os.getpid(),
                    "quote": self.quote,
                    "start_time_ms": int(self.start_time_ms),
                },
                ts=int(self.start_time_ms),
            )

            boot_stage = "boot_stagger"
            await maybe_apply_boot_stagger()

            if not await run_startup_preloop(set_boot_stage):
                return

            await finalize_startup_ready()
        except Exception as exc:
            await handle_startup_error(exc, boot_stage)
            raise

    async def init_markets(self, verbose=True):
        """Load exchange market metadata and refresh approval lists."""
        # called at bot startup and once an hour thereafter
        self.init_markets_last_update_ms = utc_ms()
        ensure_exchange_config_ready = getattr(self, "_ensure_exchange_config_ready_for_market_init", None)
        if ensure_exchange_config_ready is None:
            async def ensure_exchange_config_ready():
                await pb_market_init_utils.ensure_exchange_config_ready_for_market_init(self)
        await ensure_exchange_config_ready()
        # Reuse existing ccxt session when available (ensures shared options such as fetchMarkets types).
        cc_instance = getattr(self, "cca", None)
        self.markets_dict = await load_markets(
            self.exchange, 0, verbose=False, cc=cc_instance, quote=self.quote
        )
        # ineligible symbols cannot open new positions
        eligible, _, reasons = filter_markets(
            self.markets_dict, self.exchange, quote=self.quote, verbose=verbose
        )
        apply_loaded_markets = getattr(self, "_apply_loaded_markets", None)
        if apply_loaded_markets is None:
            pb_market_init_utils.apply_loaded_markets(self, self.markets_dict, eligible, reasons)
        else:
            apply_loaded_markets(self.markets_dict, eligible, reasons)
        # await self.init_flags()
        # await self.update_tickers()
        # self.set_live_configs()
        post_market_load_setup = getattr(self, "_apply_post_market_load_setup", None)
        if post_market_load_setup is None:
            await pb_market_init_utils.apply_post_market_load_setup(self)
        else:
            await post_market_load_setup()

    def _resolve_live_warmup_float(
        self,
        key: str,
        default: float,
        *,
        min_value: float | None = None,
        max_value: float | None = None,
    ) -> float:
        raw_value = get_optional_live_value(self.config, key, default)
        if raw_value in (None, ""):
            return float(default)
        try:
            value = float(raw_value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"invalid live.{key} during warmup: {raw_value!r}") from exc
        if not math.isfinite(value):
            raise RuntimeError(f"invalid live.{key} during warmup: {raw_value!r}")
        if min_value is not None and value < min_value:
            raise RuntimeError(
                f"live.{key} must be >= {min_value} during warmup, got {value!r}"
            )
        if max_value is not None and value > max_value:
            raise RuntimeError(
                f"live.{key} must be <= {max_value} during warmup, got {value!r}"
            )
        return value

    def _resolve_live_warmup_int(
        self,
        key: str,
        default: int,
        *,
        min_value: int | None = None,
        max_value: int | None = None,
    ) -> int:
        value = int(self._resolve_live_warmup_float(key, float(default)))
        if min_value is not None and value < min_value:
            raise RuntimeError(f"live.{key} must be >= {min_value} during warmup, got {value!r}")
        if max_value is not None and value > max_value:
            raise RuntimeError(f"live.{key} must be <= {max_value} during warmup, got {value!r}")
        return value

    async def warmup_candles_staggered(
        self,
        *,
        concurrency: int | None = None,
        window_candles: int | None = None,
        ttl_ms: int = 300_000,
    ) -> None:
        """Warm up recent candles for all approved symbols in a staggered way.

        - concurrency: max in-flight symbols; if None, uses config or exchange-specific default
        - window_candles: number of 1m candles to warm; defaults to CandlestickManager.default_window_candles
        - ttl_ms: skip refresh if data newer than this TTL exists

        Logs a minimal countdown when warming >20 symbols.
        """
        # Build symbol set: lazy warmup. If slots are open, warm eligible symbols for that side.
        # If slots are full, warm only symbols with positions.
        if not hasattr(self, "approved_coins_minus_ignored_coins"):
            return
        symbols_by_side: Dict[str, set] = {}
        forager_needed = {"long": False, "short": False}
        slots_open_by_side: Dict[str, bool] = {}
        pos_counts: Dict[str, int] = {}
        max_counts: Dict[str, int] = {}
        for pside in ("long", "short"):
            try:
                max_n = int(self.get_max_n_positions(pside))
            except Exception as exc:
                raise RuntimeError(
                    f"failed to resolve max position count during warmup for {pside}"
                ) from exc
            try:
                current_n = int(self.get_current_n_positions(pside))
            except Exception as exc:
                raise RuntimeError(
                    f"failed to resolve current position count during warmup for {pside}"
                ) from exc
            max_counts[pside] = max_n
            pos_counts[pside] = current_n
            slots_open = max_n > current_n
            slots_open_by_side[pside] = bool(slots_open)
            forager_needed[pside] = bool(self.is_forager_mode(pside) and slots_open)
            if slots_open:
                symbols_by_side[pside] = set(self.get_symbols_approved_or_has_pos(pside))
            else:
                symbols_by_side[pside] = set(self.get_symbols_with_pos(pside))
        symbols = sorted(set().union(*symbols_by_side.values()))
        if not symbols:
            return

        # Determine concurrency: explicit arg > config > exchange-specific default
        if concurrency is None:
            cfg_concurrency = self._resolve_live_warmup_int(
                "warmup_concurrency",
                0,
                min_value=0,
            )
            if cfg_concurrency > 0:
                concurrency = cfg_concurrency
            else:
                # Exchange-specific defaults: Hyperliquid has stricter rate limits
                exchange_lower = self.exchange.lower() if self.exchange else ""
                if exchange_lower == "hyperliquid":
                    concurrency = 1
                else:
                    concurrency = 8
        concurrency = max(1, int(concurrency))

        # Random jitter delay to prevent API rate limit storms when multiple bots start simultaneously
        max_jitter = self._resolve_live_warmup_float(
            "warmup_jitter_seconds",
            30.0,
            min_value=0.0,
        )
        if max_jitter > 0:
            jitter = random.uniform(0, max_jitter)
            if jitter > 5:
                logging.info(
                    "[boot] warmup jitter: waiting %.1fs before starting (max=%.0fs)...",
                    jitter,
                    max_jitter,
                )
                # For longer waits, log progress every 10 seconds
                waited = 0.0
                while waited < jitter:
                    sleep_chunk = min(10.0, jitter - waited)
                    await asyncio.sleep(sleep_chunk)
                    waited += sleep_chunk
                    if waited < jitter:
                        logging.info("[boot] warmup jitter: %.0fs remaining...", jitter - waited)
            else:
                logging.info("[boot] warmup jitter: sleeping %.1fs (max=%.0fs)", jitter, max_jitter)
                await asyncio.sleep(jitter)

        n = len(symbols)
        now = utc_ms()
        end_final = (now // ONE_MIN_MS) * ONE_MIN_MS - ONE_MIN_MS
        # Determine window per symbol based on actual EMA needs (lazy & frugal).
        # Fetch max-span * (1 + warmup_ratio) to give EMAs enough runway without overfetching.
        default_win = int(getattr(self.cm, "default_window_candles", 120))
        warmup_ratio = self._resolve_live_warmup_float(
            "warmup_ratio",
            0.0,
            min_value=0.0,
            max_value=1.0,
        )
        max_warmup_minutes = self._resolve_live_warmup_int(
            "max_warmup_minutes",
            0,
            min_value=0,
        )
        large_span_threshold = 2 * 24 * 60  # minutes; match CandlestickManager large-span logic

        per_symbol_win, per_symbol_h1_hours, per_symbol_skip_historical = compute_live_warmup_windows(
            symbols_by_side,
            lambda pside, key, sym: self.bp(pside, key, sym),
            forager_enabled=forager_needed,
            window_candles=window_candles,
            warmup_ratio=warmup_ratio,
            max_warmup_minutes=max_warmup_minutes,
            large_span_threshold=large_span_threshold,
        )
        end_final_hour = (now // (60 * ONE_MIN_MS)) * (60 * ONE_MIN_MS) - 60 * ONE_MIN_MS
        try:
            await self.rebuild_required_candle_indices(
                symbols,
                per_symbol_win,
                per_symbol_h1_hours,
                end_final,
                end_final_hour,
            )
        except Exception as exc:
            raise RuntimeError("failed to rebuild required candle indices during warmup") from exc

        sem = asyncio.Semaphore(max(1, int(concurrency)))
        completed = 0
        started_ms = utc_ms()
        last_log_ms = started_ms

        # Informative kickoff log
        if n > 0:
            wmins = [per_symbol_win[s] for s in symbols]
            wmin, wmax = (min(wmins), max(wmins)) if wmins else (default_win, default_win)
            logging.info(
                f"[warmup] starting: {n} symbols, concurrency={concurrency}, ttl={int(ttl_ms/1000)}s, window=[{wmin},{wmax}]m"
            )
            longest_span = int(math.ceil(wmax / max(1.0, (1.0 + warmup_ratio))))
            logging.info(
                "[warmup] target | longest_span=%dm warmup_ratio=%.3g max_warmup_minutes=%s",
                int(longest_span),
                float(warmup_ratio),
                "none" if not max_warmup_minutes else str(int(max_warmup_minutes)),
            )
            logging.info(
                "[warmup] slot view | long: %d/%d open=%s forager=%s symbols=%d | short: %d/%d open=%s forager=%s symbols=%d",
                pos_counts.get("long", 0),
                max_counts.get("long", 0),
                "yes" if slots_open_by_side.get("long") else "no",
                "yes" if forager_needed.get("long") else "no",
                len(symbols_by_side.get("long", set())),
                pos_counts.get("short", 0),
                max_counts.get("short", 0),
                "yes" if slots_open_by_side.get("short") else "no",
                "yes" if forager_needed.get("short") else "no",
                len(symbols_by_side.get("short", set())),
            )
            long_syms = symbols_by_side.get("long", set())
            short_syms = symbols_by_side.get("short", set())
            long_wins = [per_symbol_win[s] for s in long_syms if s in per_symbol_win]
            short_wins = [per_symbol_win[s] for s in short_syms if s in per_symbol_win]
            long_min = min(long_wins) if long_wins else 0
            long_max = max(long_wins) if long_wins else 0
            short_min = min(short_wins) if short_wins else 0
            short_max = max(short_wins) if short_wins else 0
            logging.info(
                "[warmup] windows | long:[%d,%d]m short:[%d,%d]m",
                long_min,
                long_max,
                short_min,
                short_max,
            )
            # Enable batch mode for zero-candle synthesis warnings during warmup
            self.cm.start_synth_candle_batch()
            # Enable batch mode for candle replacement logs during warmup
            self.cm.start_candle_replace_batch()

        fetch_delay_s = self._get_fetch_delay_seconds()
        warmup_failures: list[str] = []

        async def one(sym: str):
            nonlocal completed, last_log_ms
            async with sem:
                try:
                    win = int(per_symbol_win.get(sym, default_win))
                    skip_hist = bool(per_symbol_skip_historical.get(sym, True))
                    start_ts = int(end_final - ONE_MIN_MS * max(1, win))
                    await self.cm.get_candles(
                        sym,
                        start_ts=start_ts,
                        end_ts=None,
                        max_age_ms=ttl_ms,
                        strict=False,
                        skip_historical_gap_fill=skip_hist,  # allow gap fill on large warmup spans
                        max_lookback_candles=win,
                    )
                except Exception as exc:
                    warmup_failures.append(f"1m:{sym}:{type(exc).__name__}:{exc}")
                finally:
                    if fetch_delay_s > 0:
                        await asyncio.sleep(fetch_delay_s)
                    completed += 1
                    # Time-based throttle: log every ~2s or on completion
                    if n > 20:
                        now_ms = utc_ms()
                        if (completed == n) or (now_ms - last_log_ms >= 2000) or completed == 1:
                            elapsed_s = max(0.001, (now_ms - started_ms) / 1000.0)
                            rate = completed / elapsed_s
                            remaining = max(0, n - completed)
                            eta_s = int(remaining / max(1e-6, rate))
                            pct = int(100 * completed / n)
                            logging.info(
                                f"[warmup] candles: {completed}/{n} {pct}% elapsed={int(elapsed_s)}s eta~{eta_s}s"
                            )
                            last_log_ms = now_ms

        try:
            await asyncio.gather(*(one(s) for s in symbols))

            # Warm 1h candles for grid log-range EMAs
            hour_sem = asyncio.Semaphore(max(1, int(concurrency)))

            async def warm_hour(sym: str):
                async with hour_sem:
                    warm_hours = int(per_symbol_h1_hours.get(sym, 0) or 0)
                    if warm_hours <= 0:
                        return
                    start_ts = int(end_final_hour - warm_hours * 60 * ONE_MIN_MS)
                    try:
                        await self.cm.get_candles(
                            sym,
                            start_ts=start_ts,
                            end_ts=None,
                            max_age_ms=ttl_ms,
                            timeframe="1h",
                            strict=False,
                            skip_historical_gap_fill=True,  # Live warmup: don't waste time on old gaps
                            max_lookback_candles=warm_hours,
                        )
                    except Exception as exc:
                        warmup_failures.append(f"1h:{sym}:{type(exc).__name__}:{exc}")
                    finally:
                        if fetch_delay_s > 0:
                            await asyncio.sleep(fetch_delay_s)

            await asyncio.gather(*(warm_hour(s) for s in symbols))
        finally:
            # Flush batched zero-candle synthesis warnings
            self.cm.flush_synth_candle_batch()
            # Flush batched candle replacement logs
            self.cm.flush_candle_replace_batch()

        if warmup_failures:
            failures_preview = "; ".join(warmup_failures[:3])
            if len(warmup_failures) > 3:
                failures_preview += f"; ... (+{len(warmup_failures) - 3} more)"
            raise RuntimeError(
                "warmup_candles_staggered failed for required symbol/timeframe fetches: "
                + failures_preview
            )

    async def rebuild_required_candle_indices(
        self,
        symbols: Iterable[str],
        per_symbol_win: Dict[str, int],
        per_symbol_h1_hours: Dict[str, int],
        end_final: int,
        end_final_hour: int,
    ) -> None:
        """Rebuild candle index metadata for the required warmup ranges."""
        if not getattr(self, "cm", None):
            return

        symbols = list(symbols or [])
        if not symbols:
            return

        started = utc_ms()
        logging.info(
            "[boot] rebuilding candle index for %d symbols (recent ranges only)...", len(symbols)
        )

        def _rebuild_sync() -> Tuple[int, int]:
            updated_total = 0
            removed_total = 0
            for sym in symbols:
                win = int(per_symbol_win.get(sym, 0) or 0)
                if win > 0 and end_final > 0:
                    start_ts = max(0, int(end_final - win * ONE_MIN_MS))
                    res = self.cm.rebuild_index_for_range(
                        sym,
                        start_ts,
                        int(end_final),
                        timeframe="1m",
                        log_level="debug",
                    )
                    updated_total += int(res.get("updated", 0) or 0)
                    removed_total += int(res.get("removed", 0) or 0)
                warm_hours = int(per_symbol_h1_hours.get(sym, 0) or 0)
                if warm_hours > 0 and end_final_hour > 0:
                    start_ts = max(0, int(end_final_hour - warm_hours * 60 * ONE_MIN_MS))
                    res = self.cm.rebuild_index_for_range(
                        sym,
                        start_ts,
                        int(end_final_hour),
                        timeframe="1h",
                        log_level="debug",
                    )
                    updated_total += int(res.get("updated", 0) or 0)
                    removed_total += int(res.get("removed", 0) or 0)
            return updated_total, removed_total

        updated_total, removed_total = await asyncio.to_thread(_rebuild_sync)
        elapsed_s = max(0.0, (utc_ms() - started) / 1000.0)
        logging.info(
            "[boot] candle index rebuild complete: updated=%d removed=%d elapsed=%.2fs",
            updated_total,
            removed_total,
            elapsed_s,
        )

    async def audit_required_candle_disk_coverage(
        self, symbols: Optional[Iterable[str]] = None
    ) -> None:
        """Check disk coverage for required candle ranges and log missing spans."""
        try:
            if self.cm is None:
                return
        except Exception:
            return

        # Only log for symbols that are actively relevant to the live bot.
        def _should_log_symbol(sym: str) -> bool:
            try:
                if sym in getattr(self, "active_symbols", []):
                    return True
            except Exception:  # error-contract: allow - best-effort logging helper
                pass
            try:
                if sym in getattr(self, "open_orders", {}) and self.open_orders.get(sym):
                    return True
            except Exception:  # error-contract: allow - best-effort logging helper
                pass
            try:
                return bool(self.has_position(sym))
            except Exception:
                return False

        symbol_filter = set(symbols) if symbols is not None else None
        symbols_by_side: Dict[str, set] = {}
        forager_needed = {"long": False, "short": False}
        for pside in ("long", "short"):
            try:
                max_n = int(self.get_max_n_positions(pside))
            except Exception:
                max_n = 0
            try:
                current_n = int(self.get_current_n_positions(pside))
            except Exception:
                current_n = len(self.get_symbols_with_pos(pside))
            slots_open = max_n > current_n
            forager_needed[pside] = bool(self.is_forager_mode(pside) and slots_open)
            try:
                if slots_open:
                    syms = set(self.get_symbols_approved_or_has_pos(pside))
                else:
                    syms = set(self.get_symbols_with_pos(pside))
            except Exception:
                syms = set()
            if symbol_filter is not None:
                syms = syms & symbol_filter
            symbols_by_side[pside] = syms
        symbol_list = sorted(set().union(*symbols_by_side.values()))
        if not symbol_list:
            return

        forager_enabled = {
            "long": bool(forager_needed.get("long")),
            "short": bool(forager_needed.get("short")),
        }

        try:
            warmup_ratio = float(get_optional_live_value(self.config, "warmup_ratio", 0.0))
        except Exception:
            warmup_ratio = 0.0
        try:
            max_warmup_minutes = int(
                get_optional_live_value(self.config, "max_warmup_minutes", 0) or 0
            )
        except Exception:
            max_warmup_minutes = 0

        per_symbol_win, per_symbol_h1_hours, _ = compute_live_warmup_windows(
            symbols_by_side,
            lambda pside, key, sym: self.bp(pside, key, sym),
            forager_enabled=forager_enabled,
            warmup_ratio=warmup_ratio,
            max_warmup_minutes=max_warmup_minutes,
        )

        now = utc_ms()
        end_final = (now // ONE_MIN_MS) * ONE_MIN_MS - ONE_MIN_MS
        end_final_hour = (now // (60 * ONE_MIN_MS)) * (60 * ONE_MIN_MS) - 60 * ONE_MIN_MS
        tail_slack_ms = int(getattr(self, "candle_disk_check_tail_slack_ms", 0) or 0)
        tail_slack_hour_ms = int(getattr(self, "candle_disk_check_tail_slack_hour_ms", 0) or 0)
        end_final = max(0, int(end_final) - tail_slack_ms)
        end_final_hour = max(0, int(end_final_hour) - tail_slack_hour_ms)

        for sym in symbol_list:
            win = int(per_symbol_win.get(sym, 0) or 0)
            if win > 0 and end_final > 0:
                start_ts = max(0, int(end_final - win * ONE_MIN_MS))
                log_level = "debug"
                self.cm.check_disk_coverage(
                    sym,
                    start_ts,
                    int(end_final),
                    timeframe="1m",
                    log_level=log_level,
                )
            warm_hours = int(per_symbol_h1_hours.get(sym, 0) or 0)
            if warm_hours > 0 and end_final_hour > 0:
                start_ts = max(0, int(end_final_hour - warm_hours * 60 * ONE_MIN_MS))
                log_level = "debug"
                self.cm.check_disk_coverage(
                    sym,
                    start_ts,
                    int(end_final_hour),
                    timeframe="1h",
                    log_level=log_level,
                )

    async def run_execution_loop(self):
        """Main execution loop coordinating order generation and exchange interaction."""
        failed_update_pos_oos_pnls_ohlcvs_count = 0
        max_n_fails = 10
        if self._equity_hard_stop_enabled() and not all(
            self._equity_hard_stop_runtime_initialized(pside)
            or not self._equity_hard_stop_enabled(pside)
            for pside in self._hsl_psides()
        ):
            await self._equity_hard_stop_initialize_from_history()
        while not self.stop_signal_received:
            try:
                loop_start_ms = utc_ms()
                self.execution_scheduled = False
                self.state_change_detected_by_symbol = set()
                self._set_log_silence_watchdog_context(
                    phase="runtime", stage="update_pos_oos_pnls_ohlcvs"
                )
                if not await self.update_pos_oos_pnls_ohlcvs():
                    await asyncio.sleep(0.5)
                    failed_update_pos_oos_pnls_ohlcvs_count += 1
                    if failed_update_pos_oos_pnls_ohlcvs_count > max_n_fails:
                        await self.restart_bot_on_too_many_errors()
                    continue
                failed_update_pos_oos_pnls_ohlcvs_count = 0
                if self._equity_hard_stop_enabled():
                    await self._equity_hard_stop_check()
                    if any(
                        self._equity_hard_stop_runtime_red_latched(pside)
                        and not self._hsl_state(pside)["halted"]
                        for pside in self._hsl_psides()
                        if self._equity_hard_stop_enabled(pside)
                    ):
                        await self._equity_hard_stop_run_red_supervisor()
                        continue
                self._set_log_silence_watchdog_context(phase="runtime", stage="execute_to_exchange")
                res = await self.execute_to_exchange()
                if self.debug_mode:
                    return res
                # Track loop duration for health reporting
                self._last_loop_duration_ms = utc_ms() - loop_start_ms
                # Periodic health summary
                self._maybe_log_health_summary()
                self._maybe_log_unstuck_status()
                self._set_log_silence_watchdog_context(phase="runtime", stage="flush_snapshot")
                await self._monitor_flush_snapshot()
                self._set_log_silence_watchdog_context(phase="runtime", stage="execution_delay")
                await asyncio.sleep(float(self.live_value("execution_delay_seconds")))
                sleep_duration = 30
                self._set_log_silence_watchdog_context(phase="runtime", stage="scheduled_wait")
                for i in range(sleep_duration * 10):
                    if self.execution_scheduled:
                        break
                    await asyncio.sleep(0.1)
            except RestartBotException:
                raise  # Propagate restart without incrementing error count
            except RateLimitExceeded as e:
                self._health_errors += 1
                self._health_rate_limits += 1
                self._monitor_record_error(
                    "error.exchange",
                    e,
                    tags=("error", "exchange", "rate_limit"),
                    payload={"source": "run_execution_loop"},
                )
                logging.warning("[rate] execution loop hit rate limit; backing off 5s...")
                await self.restart_bot_on_too_many_errors()
                await asyncio.sleep(5.0)
            except Exception as e:
                self._health_errors += 1
                self._monitor_record_error(
                    "error.bot",
                    e,
                    tags=("error", "bot"),
                    payload={"source": "run_execution_loop"},
                )
                logging.error(f"error with {get_function_name()} {e}")
                traceback.print_exc()
                await self.restart_bot_on_too_many_errors()
                await asyncio.sleep(1.0)

    async def shutdown_gracefully(self):
        if getattr(self, "_shutdown_in_progress", False):
            return
        self._shutdown_in_progress = True
        self.stop_signal_received = True
        stop_ts = utc_ms()
        self._monitor_emit_stop("shutdown_gracefully", ts=stop_ts)
        logging.info("[shutdown] shutdown requested; closing background tasks and sessions")
        try:
            self.stop_data_maintainers(verbose=False)
        except Exception as e:
            logging.error("[shutdown] error stopping maintainers: %s", e)
        await asyncio.sleep(0)
        try:
            if getattr(self, "ccp", None) is not None:
                await self.ccp.close()
        except Exception as e:
            logging.error("[shutdown] error closing private ccxt session: %s", e)
        try:
            if getattr(self, "cca", None) is not None:
                await self.cca.close()
        except Exception as e:
            logging.error("[shutdown] error closing public ccxt session: %s", e)
        await self._monitor_flush_snapshot(force=True, ts=utc_ms())
        publisher = getattr(self, "monitor_publisher", None)
        if publisher is not None:
            publisher.close()
        logging.info("[shutdown] cleanup complete")

    async def update_pos_oos_pnls_ohlcvs(self) -> bool:
        """Refresh positions, open orders, realised PnL, and 1m candles."""
        if self.stop_signal_received:
            return False
        balance_ok, positions_ok = await self.update_positions_and_balance()
        if not positions_ok:
            return False
        if not balance_ok:
            return False

        # Build task list: open_orders and fill events (pnls)
        async def _run_named_update(name, coro):
            try:
                return await coro
            except Exception as exc:
                raise RuntimeError(f"{name} failed during update_pos_oos_pnls_ohlcvs") from exc

        open_orders_ok, pnls_ok = await asyncio.gather(
            _run_named_update("update_open_orders", self.update_open_orders()),
            _run_named_update("update_pnls", self.update_pnls()),
        )

        if open_orders_ok is not True or pnls_ok is not True:
            return False
        if self.stop_signal_received:
            return False
        await self.update_ohlcvs_1m_for_actives()
        return True

    async def execute_to_exchange(self):
        """Run one execution cycle including config sync and order placement/cancellation."""
        await self.execution_cycle()
        # await self.update_EMAs()
        await self.update_exchange_configs()
        to_cancel, to_create = await self.calc_orders_to_cancel_and_create()

        # debug duplicates
        seen = set()
        for elm in to_cancel:
            key = str(elm["price"]) + str(elm["qty"])
            if key in seen:
                logging.debug("duplicate cancel candidate: %s", elm)
            seen.add(key)

        seen = set()
        for elm in to_create:
            key = str(elm["price"]) + str(elm["qty"])
            if key in seen:
                logging.debug("duplicate create candidate: %s", elm)
            seen.add(key)
        # format custom_id
        if self.debug_mode:
            if to_cancel:
                print(f"would cancel {len(to_cancel)} order{'s' if len(to_cancel) > 1 else ''}")
        else:
            res = await self.execute_cancellations_parent(to_cancel)
        if self.debug_mode:
            if to_create:
                print(f"would create {len(to_create)} order{'s' if len(to_create) > 1 else ''}")
        elif self.get_raw_balance() < self.balance_threshold:
            logging.info(
                "[balance] too low: %.2f %s; not creating orders", self.get_raw_balance(), self.quote
            )
        else:
            # to_create_mod = [x for x in to_create if not order_has_match(x, to_cancel)]
            to_create_mod = []
            for x in to_create:
                xf = f"{x['symbol']} {x['side']} {x['position_side']} {x['qty']} @ {x['price']}"
                if order_has_match(x, to_cancel):
                    logging.debug(
                        "matching order cancellation found; will be delayed until next cycle: %s",
                        xf,
                    )
                elif delay_time_ms := self.order_was_recently_updated(x):
                    logging.info(
                        "[order] recent execution found; delaying for up to %.1f secs: %s",
                        delay_time_ms / 1000,
                        xf,
                    )
                else:
                    to_create_mod.append(x)
            if self.state_change_detected_by_symbol:
                logging.info(
                    "[order] state change detected; skipping order creation for %s until next cycle",
                    self.state_change_detected_by_symbol,
                )
                to_create_mod = [
                    x
                    for x in to_create_mod
                    if x["symbol"] not in self.state_change_detected_by_symbol
                ]
            res = None
            try:
                res = await self.execute_orders_parent(to_create_mod)
            except RestartBotException:
                raise  # Propagate restart without incrementing error count
            except Exception as e:
                logging.error(f"error executing orders {to_create_mod} {e}")
                print_async_exception(res)
                traceback.print_exc()
                await self.restart_bot_on_too_many_errors()
        if to_cancel or to_create:
            self.execution_scheduled = True
        if self.debug_mode:
            return to_cancel, to_create

    async def execute_orders_parent(self, orders: [dict]) -> [dict]:
        """Submit a batch of orders after throttling and bookkeeping."""
        orders = orders[: int(self.live_value("max_n_creations_per_batch"))]
        if not orders:
            return []
        grouped_orders: dict[str, list[dict]] = defaultdict(list)
        for order in orders:
            self.add_to_recent_order_executions(order)
            self.log_order_action(
                order,
                "posting order",
                context=order.get("_context", "plan_sync"),
                level=logging.DEBUG,
                delta=order.get("_delta"),
            )
            grouped_orders[order["symbol"]].append(order)
        self._log_order_action_summary(grouped_orders, "post")
        res = await self.execute_orders(orders)
        if res in [None, False]:
            raise RuntimeError(f"execute_orders returned invalid result {res!r}")
        if not isinstance(res, list):
            raise TypeError(f"execute_orders returned non-list result {type(res).__name__}")
        if len(orders) != len(res):
            raise RuntimeError(
                f"execute_orders returned {len(res)} executions for {len(orders)} orders"
            )
        to_return = []
        for ex, order in zip(res, orders):
            if not self.did_create_order(ex):
                raise RuntimeError(
                    f"execute_orders returned unacknowledged result for {order['symbol']}: {ex!r}"
                )
            debug_prints = {}
            for key in order:
                if key not in ex:
                    debug_prints.setdefault("missing", []).append((key, order[key]))
                    ex[key] = order[key]
                elif ex[key] is None:
                    debug_prints.setdefault("is_none", []).append((key, order[key]))
                    ex[key] = order[key]
            if debug_prints and self.debug_mode:
                print("debug create_orders", debug_prints)
            to_return.append(ex)
        if to_return:
            for elm in to_return:
                self.add_new_order(elm, source="POST")
                self._monitor_record_event(
                    "order.opened",
                    ("order", "open"),
                    self._monitor_order_payload(elm, source="POST"),
                    symbol=elm.get("symbol"),
                    pside=elm.get("position_side"),
                )
            self._health_orders_placed += len(to_return)
        return to_return

    async def execute_cancellations_parent(self, orders: [dict]) -> [dict]:
        """Submit a batch of cancellations, prioritising reduce-only orders."""
        max_cancellations = int(self.live_value("max_n_cancellations_per_batch"))
        if not orders:
            return []
        if len(orders) > max_cancellations:
            # prioritize cancelling reduce-only orders
            try:
                reduce_only_orders = [
                    x for x in orders if x.get("reduce_only") or x.get("reduceOnly")
                ]
                rest = [x for x in orders if not x["reduce_only"]]
                orders = (reduce_only_orders + rest)[:max_cancellations]
            except Exception as e:
                logging.error(f"debug filter cancellations {e}")
                orders = orders[:max_cancellations]
        grouped_orders: dict[str, list[dict]] = defaultdict(list)
        for order in orders:
            self.add_to_recent_order_cancellations(order)
            self.log_order_action(
                order,
                "cancelling order",
                context=order.get("_context", "plan_sync"),
                level=logging.DEBUG,
                delta=order.get("_delta"),
            )
            grouped_orders[order["symbol"]].append(order)
        self._log_order_action_summary(grouped_orders, "cancel")
        res = await self.execute_cancellations(orders)
        if res in [None, False]:
            raise RuntimeError(f"execute_cancellations returned invalid result {res!r}")
        if not isinstance(res, list):
            raise TypeError(
                f"execute_cancellations returned non-list result {type(res).__name__}"
            )
        to_return = []
        if len(orders) != len(res):
            for od in orders:
                self.state_change_detected_by_symbol.add(od["symbol"])
            raise RuntimeError(
                f"execute_cancellations returned {len(res)} executions for {len(orders)} orders"
            )
        for ex, od in zip(res, orders):
            if not self.did_cancel_order(ex, od):
                self.state_change_detected_by_symbol.add(od["symbol"])
                raise RuntimeError(
                    f"execute_cancellations returned unacknowledged result for {od['symbol']}: {ex!r}"
                )
            debug_prints = {}
            for key in od:
                if key not in ex:
                    debug_prints.setdefault("missing", []).append((key, od[key]))
                    ex[key] = od[key]
                elif ex[key] is None:
                    debug_prints.setdefault("is_none", []).append((key, od[key]))
                    ex[key] = od[key]
            if debug_prints and self.debug_mode:
                print("debug cancel_orders", debug_prints)
            to_return.append(ex)
        if to_return:
            for elm in to_return:
                self.remove_order(elm, source="POST")
                self._monitor_record_event(
                    "order.canceled",
                    ("order", "cancel"),
                    self._monitor_order_payload(elm, source="POST"),
                    symbol=elm.get("symbol"),
                    pside=elm.get("position_side"),
                )
            self._health_orders_cancelled += len(to_return)
        return to_return

    def log_order_action(
        self,
        order,
        action,
        source="passivbot",
        *,
        level=logging.DEBUG,
        context: str | None = None,
        delta: dict | None = None,
    ):
        """Log a structured message describing an order action."""
        pb_order_type = self._resolve_pb_order_type(order)

        def _fmt(val):
            try:
                return f"{float(val):g}"
            except (TypeError, ValueError):
                return str(val)

        side = order.get("side", "?")
        qty = _fmt(order.get("qty", "?"))
        position_side = order.get("position_side", "?")
        price = _fmt(order.get("price", "?"))
        symbol = order.get("symbol", "?")
        coin = symbol_to_coin(symbol, verbose=False) or symbol
        details = f"{side} {qty} {position_side}@{price}"
        extra_parts = []
        if context:
            extra_parts.append(f"context={context}")
        elif order.get("_context"):
            extra_parts.append(f"context={order.get('_context')}")
        if delta:
            parts = []
            po, pn = delta.get("price_old"), delta.get("price_new")
            qo, qn = delta.get("qty_old"), delta.get("qty_new")
            if po is not None and pn is not None:
                parts.append(f"price {po} -> {pn} ({delta.get('price_pct_diff','?')}%)")
            if qo is not None and qn is not None:
                parts.append(f"qty {qo} -> {qn} ({delta.get('qty_pct_diff','?')}%)")
            if parts:
                extra_parts.append("delta=" + "; ".join(parts))
        msg = f"[order] {action: >{self.action_str_max_len}} {coin} | {details} | type={pb_order_type} | src={source}"
        if extra_parts:
            msg += " | " + " ".join(extra_parts)
        logging.log(level, msg)

    def _log_order_action_summary(self, grouped_orders: dict[str, list[dict]], action: str) -> None:
        """Emit condensed INFO summaries for batched order actions, skipping repeats."""
        max_entries = 4
        for symbol, orders in grouped_orders.items():
            if not orders:
                continue
            descriptors = []
            for order in orders:
                pb_order_type = self._resolve_pb_order_type(order)
                qty = order.get("qty")
                price = order.get("price")
                qty_str = f"{float(qty):g}" if isinstance(qty, (int, float)) else str(qty)
                price_str = f"{float(price):g}" if isinstance(price, (int, float)) else str(price)
                desc = (
                    f"{order.get('side','?')} {order.get('position_side','?')} "
                    f"{qty_str}@{price_str} {pb_order_type}"
                )
                extras = []
                context = order.get("_context")
                reason = order.get("_reason")
                if context:
                    extras.append(context)
                if reason and reason != context:
                    extras.append(f"reason={reason}")
                delta = order.get("_delta") or {}
                price_diff = delta.get("price_pct_diff")
                qty_diff = delta.get("qty_pct_diff")
                delta_parts = []
                if isinstance(price_diff, (int, float)) and price_diff:
                    delta_parts.append(f"Δp={price_diff:.3g}%")
                if isinstance(qty_diff, (int, float)) and qty_diff:
                    delta_parts.append(f"Δq={qty_diff:.3g}%")
                extras.extend(delta_parts)
                if extras:
                    desc += f" [{' '.join(extras)}]"
                descriptors.append(desc)
            if not descriptors:
                continue
            display = "; ".join(descriptors[:max_entries])
            if len(descriptors) > max_entries:
                display += f"; ... +{len(descriptors) - max_entries} more"
            key = (symbol, action)
            if self._last_action_summary.get(key) == display:
                continue
            self._last_action_summary[key] = display
            reason_counts = Counter(order.get("_reason") for order in orders if order.get("_reason"))
            reason_str = ""
            if reason_counts:
                reason_str = " | reasons=" + ", ".join(
                    f"{reason}:{count}" for reason, count in sorted(reason_counts.items())
                )
            coin = symbol_to_coin(symbol, verbose=False) or symbol
            logging.info("[order] %6s %s | %s%s", action, coin, display, reason_str)

    def _resolve_pb_order_type(self, order) -> str:
        """Best-effort decoding of Passivbot order type for logging."""
        if not isinstance(order, dict):
            return "unknown"
        pb_type = order.get("pb_order_type")
        if pb_type:
            return str(pb_type)
        symbol = order.get("symbol")
        if symbol and symbol in self.open_orders:
            for existing in self.open_orders[symbol]:
                if order_has_match(order, [existing], tolerance_price=0.0, tolerance_qty=0.0):
                    existing_type = existing.get("pb_order_type")
                    if existing_type:
                        return str(existing_type)
                    candidate = self._decode_pb_type_from_ids(existing)
                    if candidate:
                        return candidate
        candidate_ids = [
            order.get("custom_id"),
            order.get("customId"),
            order.get("client_order_id"),
            order.get("clientOrderId"),
            order.get("client_oid"),
            order.get("clientOid"),
            order.get("order_link_id"),
            order.get("orderLinkId"),
        ]
        candidate = self._decode_pb_type_from_ids(order, candidate_ids)
        if candidate:
            return candidate
        return "unknown"

    def _decode_pb_type_from_ids(
        self, order: dict, candidate_ids: Optional[list] = None
    ) -> Optional[str]:
        ids = candidate_ids
        if ids is None:
            ids = [
                order.get("custom_id"),
                order.get("customId"),
                order.get("client_order_id"),
                order.get("clientOrderId"),
                order.get("client_oid"),
                order.get("clientOid"),
                order.get("order_link_id"),
                order.get("orderLinkId"),
            ]
        for cid in ids:
            if not cid:
                continue
            snake = custom_id_to_snake(str(cid))
            if snake and snake != "unknown":
                return snake
        return None

    def did_create_order(self, executed) -> bool:
        """Return True if the exchange acknowledged order creation."""
        try:
            return "id" in executed and executed["id"] is not None
        except Exception:
            return False

    def did_cancel_order(self, executed, order=None) -> bool:
        """Return True when the exchange response confirms cancellation."""
        if isinstance(executed, list) and len(executed) == 1:
            return self.did_cancel_order(executed[0], order)
        try:
            return "id" in executed and executed["id"] is not None
        except Exception:
            return False

    # Legacy: wait_for_ohlcvs_1m_to_update removed (CandlestickManager handles freshness)

    # Legacy: get_ohlcvs_1m_filepath removed

    # Legacy: trim_ohlcvs_1m removed

    # Legacy: dump_ohlcvs_1m_to_cache removed

    async def update_trailing_data(self) -> None:
        """Update trailing price metrics using CandlestickManager candles.

        For each symbol and side with a trailing position, iterate candles since the
        last position change and compute:
        - max_since_open: highest high since open
        - min_since_max: lowest low after the most recent new high
        - min_since_open: lowest low since open
        - max_since_min: highest high (or close per legacy) after the most recent new low
        Fetches per-symbol candles concurrently to reduce latency.
        """
        if not hasattr(self, "trailing_prices"):
            self.trailing_prices = {}
        last_position_changes = self.get_last_position_changes()
        symbols = set(self.trailing_prices) | set(last_position_changes) | set(self.active_symbols)

        # Initialize containers for all symbols first
        for symbol in symbols:
            self.trailing_prices[symbol] = {
                "long": _trailing_bundle_default_dict(),
                "short": _trailing_bundle_default_dict(),
            }

        # Build concurrent fetches per symbol that has position changes
        fetch_plan = {}
        for symbol in symbols:
            if symbol not in last_position_changes:
                continue
            # Determine earliest start among sides to avoid duplicate fetches
            starts = [last_position_changes[symbol][ps] for ps in last_position_changes[symbol]]
            if not starts:
                continue
            start_ts = int(min(starts))
            fetch_plan[symbol] = start_ts

        tasks = {
            sym: asyncio.create_task(self.cm.get_candles(sym, start_ts=st, end_ts=None, strict=False))
            for sym, st in fetch_plan.items()
        }

        results = {}
        for sym, task in tasks.items():
            try:
                results[sym] = await task
            except Exception as e:
                logging.debug("failed to fetch candles for trailing %s: %s", sym, e)
                results[sym] = None

        # Compute trailing metrics per symbol/side
        for symbol, arr in results.items():
            if arr is None or arr.size == 0:
                continue
            if symbol not in last_position_changes:
                continue
            arr = np.sort(arr, order="ts")
            for pside, changed_ts in last_position_changes[symbol].items():
                mask = arr["ts"] > int(changed_ts)
                if not np.any(mask):
                    continue
                subset = arr[mask]
                try:
                    bundle = _trailing_bundle_from_arrays(subset["h"], subset["l"], subset["c"])
                    self.trailing_prices[symbol][pside] = bundle
                except Exception as e:
                    logging.debug("failed to compute trailing bundle for %s %s: %s", symbol, pside, e)

    async def update_exchange_config_by_symbols(self, symbols):
        """Exchange-specific hook to refresh config for the given symbols."""
        # defined by each exchange child class
        pass

    async def update_exchange_config(self):
        """Exchange-specific hook to refresh global config state."""
        # defined by each exchange child class
        pass

    async def execution_cycle(self):
        """Prepare bot state before talking to the exchange in an execution loop."""
        await self.update_effective_min_cost()
        self.refresh_approved_ignored_coins_lists()
        self.set_wallet_exposure_limits()
        if any(self.is_forager_mode(pside) for pside in ("long", "short")):
            await self.update_first_timestamps()
        self._assert_supported_live_state()
        self.active_symbols = self._build_live_symbol_universe()
        for symbol in self.active_symbols:
            if symbol not in self.positions:
                self.positions[symbol] = {
                    "long": {"size": 0.0, "price": 0.0},
                    "short": {"size": 0.0, "price": 0.0},
                }
            if symbol not in self.open_orders:
                self.open_orders[symbol] = []
        self.set_wallet_exposure_limits()
        await self.update_trailing_data()

    def _log_mode_changes(self, res: dict, previous_PB_modes: dict) -> None:
        """Log mode changes with DEBUG for all details and INFO for user-relevant events.

        DEBUG: All mode changes (full detail, no throttling)
        INFO: Selective logging:
          - "added" with "normal" -> forager selection (with slot context)
          - "added" with "graceful_stop" -> only on startup
          - "removed" -> coin exiting (useful)
          - "changed" normal<->graceful_stop -> suppress (oscillation noise)
          - "changed" to/from tp_only/manual/panic -> significant, always log
        """
        is_first_run = previous_PB_modes is None

        # Collect slot info for context
        slot_info = {}
        for pside in ["long", "short"]:
            try:
                max_n = self.get_max_n_positions(pside)
                current_n = self.get_current_n_positions(pside)
                slots_open = max_n > current_n
                slot_info[pside] = {"max": max_n, "current": current_n, "open": slots_open}
            except Exception:
                slot_info[pside] = {"max": 0, "current": 0, "open": False}

        # Initialize throttle cache if needed (for INFO level only)
        if not hasattr(self, "_mode_change_last_log_ms"):
            self._mode_change_last_log_ms = {}
        mode_change_throttle_ms = 300_000  # 5 minutes for INFO-level throttle
        now_ms = utc_ms()

        for change_type, changes in res.items():
            for elm in changes:
                # Always log at DEBUG (full detail)
                logging.debug("[mode] %s %s", change_type, elm)

                # Determine if this should be logged at INFO
                should_log_info = False
                info_suffix = ""

                try:
                    # Parse element: "long.XRP/USDT:USDT: normal" or "long.XRP/USDT:USDT: old -> new"
                    parts = elm.split(".")
                    pside = parts[0] if parts else "long"
                    pside_info = slot_info.get(pside, {"max": 0, "current": 0, "open": False})

                    if change_type == "added":
                        # New coin entering mode system
                        if ": normal" in elm:
                            # Forager selection - always useful
                            should_log_info = True
                            if pside_info["open"]:
                                info_suffix = (
                                    f" (forager slot {pside_info['current']+1}/{pside_info['max']})"
                                )
                            else:
                                info_suffix = f" (slot {pside_info['current']}/{pside_info['max']})"
                        elif is_first_run:
                            # First run - show all modes for visibility
                            should_log_info = True
                        # else: "added" with graceful_stop when not first run -> skip INFO

                    elif change_type == "removed":
                        # Coin exiting - always useful
                        should_log_info = True

                    elif change_type == "changed":
                        # Mode changed - check if it's oscillation or significant
                        is_oscillation = (
                            "normal -> graceful_stop" in elm or "graceful_stop -> normal" in elm
                        )
                        if is_oscillation:
                            # Oscillation - suppress at INFO (already logged at DEBUG)
                            should_log_info = False
                        else:
                            # Significant mode change (tp_only, manual, panic, etc.)
                            should_log_info = True

                except Exception:
                    # On parse error, log at INFO to be safe
                    should_log_info = True

                if should_log_info:
                    # Apply throttle for INFO level
                    try:
                        symbol_part = elm.split(":")[0]
                        throttle_key = f"info:{change_type}:{symbol_part}"
                        last_log_ms = self._mode_change_last_log_ms.get(throttle_key, 0)
                        if (now_ms - last_log_ms) < mode_change_throttle_ms:
                            continue
                        self._mode_change_last_log_ms[throttle_key] = now_ms
                    except Exception:  # error-contract: allow - logging throttle fallback
                        pass
                    logging.info("[mode] %s %s%s", change_type, elm, info_suffix)

    async def get_filtered_coins(
        self, pside: str, *, max_network_fetches: Optional[int] = None
    ) -> List[str]:
        """Select ideal coins for a side using EMA-based volume and log-range filters.

        Steps (for forager mode):
        - Filter by age and effective min cost
        - Rank by 1m EMA quote volume
        - Drop the lowest filter_volume_drop_pct fraction
        - Rank remaining by 1m EMA log range
        - Return up to n_positions most volatile symbols
        For non-forager mode, returns all approved candidates.
        """
        # filter coins by age
        # filter coins by min effective cost
        # filter coins by relative volume
        # filter coins by log range
        if self.get_forced_PB_mode(pside):
            return []
        candidates = self.approved_coins_minus_ignored_coins[pside]
        candidates = [s for s in candidates if self.is_old_enough(pside, s)]
        min_cost_flags = {s: self.effective_min_cost_is_low_enough(pside, s) for s in candidates}
        if not any(min_cost_flags.values()):
            if self.live_value("filter_by_min_effective_cost"):
                self.warn_on_high_effective_min_cost(pside)
            return []
        try:
            slots_open = self.get_max_n_positions(pside) > self.get_current_n_positions(pside)
        except Exception:
            slots_open = False
        if self.is_forager_mode(pside):
            # filter coins by relative volume and log range
            clip_pct = self.bot_value(pside, "forager_volume_drop_pct")
            if not clip_pct:
                clip_pct = self.bot_value(pside, "filter_volume_drop_pct")
            volatility_drop = self.bot_value(pside, "filter_volatility_drop_pct")
            weights = self.bot_value(pside, "forager_score_weights")
            if not isinstance(weights, dict):
                weights = {
                    "volume": 0.0,
                    "ema_readiness": 0.0,
                    "volatility": 1.0,
                }
            max_n_positions = self.get_max_n_positions(pside)
            # Apply max_ohlcv_fetches_per_minute in all cases (slots open or full).
            max_calls = get_optional_live_value(self.config, "max_ohlcv_fetches_per_minute", 0)
            try:
                max_calls = int(max_calls) if max_calls is not None else 0
            except Exception:
                max_calls = 0
            if slots_open:
                rate_limit_age_ms = self._forager_target_staleness_ms(len(candidates), max_calls)
                # Respect rate limit even with open slots; floor at 60s for responsiveness.
                max_age_ms = max(60_000, rate_limit_age_ms) if max_calls > 0 else 60_000
            else:
                max_age_ms = self._forager_target_staleness_ms(len(candidates), max_calls)
            # Use pre-computed per-side budget from caller if available;
            # otherwise fall back to computing it here (for backward compat).
            if max_network_fetches is None:
                fetch_budget = self._forager_refresh_budget(max_calls) if max_calls > 0 else None
            else:
                try:
                    fetch_budget = max(0, int(max_network_fetches))
                except Exception:
                    fetch_budget = 0
            if clip_pct > 0.0:
                volumes, log_ranges = await self.calc_volumes_and_log_ranges(
                    pside,
                    symbols=candidates,
                    max_age_ms=max_age_ms,
                    max_network_fetches=fetch_budget,
                )
            else:
                volumes = {
                    symbol: float(len(candidates) - idx) for idx, symbol in enumerate(candidates)
                }
                log_ranges = await self.calc_log_range(
                    pside,
                    eligible_symbols=candidates,
                    max_age_ms=max_age_ms,
                    max_network_fetches=fetch_budget,
                )
            if volatility_drop > 0.0:
                ranked = sorted(
                    candidates,
                    key=lambda symbol: float(log_ranges.get(symbol, 0.0)),
                    reverse=True,
                )
                keep_from = min(
                    len(ranked),
                    max(0, int(round(len(ranked) * float(volatility_drop)))),
                )
                candidates = ranked[keep_from:]
                if not candidates:
                    return []
            features = [
                {
                    "index": idx,
                    "enabled": min_cost_flags.get(symbol, True),
                    "volume_score": volumes.get(symbol, 0.0),
                    "volatility_score": log_ranges.get(symbol, 0.0),
                    "ema_readiness_score": 0.0,
                }
                for idx, symbol in enumerate(candidates)
            ]
            selected = pbr.select_coin_indices_py(
                features,
                max_n_positions,
                clip_pct,
                weights,
                True,
            )
            ideal_coins = [candidates[i] for i in selected]
            if not ideal_coins and self.live_value("filter_by_min_effective_cost"):
                if any(not flag for flag in min_cost_flags.values()):
                    self.warn_on_high_effective_min_cost(pside)
        else:
            eligible = [s for s in candidates if min_cost_flags.get(s, True)]
            if not eligible:
                if self.live_value("filter_by_min_effective_cost"):
                    self.warn_on_high_effective_min_cost(pside)
                return []
            # all approved coins are selected, no filtering by volume and log range
            ideal_coins = sorted(eligible)
        return ideal_coins

    async def calc_volumes_and_log_ranges(
        self,
        pside: str,
        symbols: Optional[Iterable[str]] = None,
        *,
        max_age_ms: Optional[int] = 60_000,
        max_network_fetches: Optional[int] = None,
    ) -> Tuple[Dict[str, float], Dict[str, float]]:
        """Compute 1m EMA quote volume and 1m EMA log range per symbol with one candles fetch.

        This uses CandlestickManager.get_latest_ema_metrics() to avoid calling get_candles() twice
        per symbol (once for volume and once for log range).

        If *max_network_fetches* is set, at most that many symbols will be allowed to
        trigger a network fetch.  The remaining symbols receive a very large TTL so they
        return cached data (or 0.0 if nothing is cached) without hitting the API.
        """
        span_volume = int(round(self.bot_value(pside, "forager_volume_ema_span")))
        span_volatility = int(round(self.bot_value(pside, "forager_volatility_ema_span")))
        try:
            warmup_ratio = float(get_optional_live_value(self.config, "warmup_ratio", 0.0))
        except Exception:
            warmup_ratio = 0.0
        try:
            max_warmup_minutes = int(
                get_optional_live_value(self.config, "max_warmup_minutes", 0) or 0
            )
        except Exception:
            max_warmup_minutes = 0
        span_buffer = 1.0 + max(0.0, warmup_ratio)
        max_span = max(span_volume, span_volatility)
        window_candles = max(1, int(math.ceil(max_span * span_buffer))) if max_span > 0 else 1
        if max_warmup_minutes > 0:
            window_candles = min(int(window_candles), int(max_warmup_minutes))
        if symbols is None:
            symbols = self.get_symbols_approved_or_has_pos(pside)

        syms = list(symbols)

        per_sym_ttl, cache_only_never_fetched = self._compute_fetch_budget_ttls(
            syms, max_age_ms, max_network_fetches
        )

        async def one(symbol: str):
            try:
                if symbol in cache_only_never_fetched:
                    return (0.0, 0.0)
                ttl = per_sym_ttl.get(symbol)
                if ttl is None or ttl == 0:
                    if max_age_ms is not None:
                        ttl = int(max_age_ms)
                    else:
                        has_pos = self.has_position(symbol)
                        has_oo = (
                            bool(self.open_orders.get(symbol)) if hasattr(self, "open_orders") else False
                        )
                        ttl = (
                            60_000
                            if (has_pos or has_oo)
                            else int(getattr(self, "inactive_coin_candle_ttl_ms", 600_000))
                        )
                res = await self.cm.get_latest_ema_metrics(
                    symbol,
                    {"qv": span_volume, "log_range": span_volatility},
                    max_age_ms=ttl,
                    window_candles=window_candles,
                    timeframe=None,
                )
                vol = float(res.get("qv", float("nan")))
                lr = float(res.get("log_range", float("nan")))
                return (0.0 if not np.isfinite(vol) else vol, 0.0 if not np.isfinite(lr) else lr)
            except Exception:
                return (0.0, 0.0)

        tasks = {s: asyncio.create_task(one(s)) for s in syms}
        volumes: Dict[str, float] = {}
        log_ranges: Dict[str, float] = {}
        started_ms = utc_ms()
        for sym, task in tasks.items():
            try:
                vol, lr = await task
            except Exception:
                vol, lr = 0.0, 0.0
            volumes[sym] = float(vol)
            log_ranges[sym] = float(lr)

        # Throttle EMA ranking logs to at most once per 5 minutes per metric.
        # Log only when rankings have changed since last logged snapshot.
        elapsed_s = max(0.001, (utc_ms() - started_ms) / 1000.0)
        now_ms = utc_ms()
        ema_log_throttle_ms = (
            300_000  # 5 minutes between logs per metric (reduced from 60s to reduce forager noise)
        )

        if volumes:
            top_n = min(8, len(volumes))
            top = sorted(volumes.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
            top_syms = tuple(sym for sym, _ in top)
            if not hasattr(self, "_volume_top_cache"):
                self._volume_top_cache = {}
            if not hasattr(self, "_volume_top_last_log_ms"):
                self._volume_top_last_log_ms = {}
            cache_key = (pside, span_volume)
            last_top = self._volume_top_cache.get(cache_key)
            last_log_ms = self._volume_top_last_log_ms.get(cache_key, 0)
            # Require both: rankings changed AND enough time has passed
            if last_top != top_syms and (now_ms - last_log_ms) >= ema_log_throttle_ms:
                self._volume_top_cache[cache_key] = top_syms
                self._volume_top_last_log_ms[cache_key] = now_ms
                summary = ", ".join(f"{symbol_to_coin(sym)}={val:.2f}" for sym, val in top)
                logging.info(
                    f"[ranking] volume EMA span {span_volume}: {len(syms)} coins elapsed={int(elapsed_s)}s, top{top_n}: {summary}"
                )
        if log_ranges:
            top_n = min(8, len(log_ranges))
            top = sorted(log_ranges.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
            top_syms = tuple(sym for sym, _ in top)
            if not hasattr(self, "_log_range_top_cache"):
                self._log_range_top_cache = {}
            if not hasattr(self, "_log_range_top_last_log_ms"):
                self._log_range_top_last_log_ms = {}
            cache_key = (pside, span_volatility)
            last_top = self._log_range_top_cache.get(cache_key)
            last_log_ms = self._log_range_top_last_log_ms.get(cache_key, 0)
            # Require both: rankings changed AND enough time has passed
            if last_top != top_syms and (now_ms - last_log_ms) >= ema_log_throttle_ms:
                self._log_range_top_cache[cache_key] = top_syms
                self._log_range_top_last_log_ms[cache_key] = now_ms
                summary = ", ".join(f"{symbol_to_coin(sym)}={val:.6f}" for sym, val in top)
                logging.info(
                    f"[ranking] log_range EMA span {span_volatility}: {len(syms)} coins elapsed={int(elapsed_s)}s, top{top_n}: {summary}"
                )

        return volumes, log_ranges

    def warn_on_high_effective_min_cost(self, pside):
        """Log a warning if min effective cost filtering removes every candidate."""
        if not self.live_value("filter_by_min_effective_cost"):
            return
        if not self.is_pside_enabled(pside):
            return
        approved_coins_filtered = [
            x
            for x in self.approved_coins_minus_ignored_coins[pside]
            if self.effective_min_cost_is_low_enough(pside, x)
        ]
        if len(approved_coins_filtered) == 0:
            logging.info(
                f"Warning: No {pside} symbols are approved due to min effective cost too high. "
                + f"Suggestions: 1) increase account balance, 2) "
                + f"set 'filter_by_min_effective_cost' to false, 3) reduce n_{pside}s"
            )

    async def update_pnls(self):
        """Fetch latest fills using FillEventsManager and update the cache."""
        if self.stop_signal_received:
            return False

        try:
            await self.init_pnls()  # will do nothing if already initiated

            if self._pnls_manager is None:
                raise RuntimeError("FillEventsManager unavailable after init_pnls")

            # Use the same lookback window
            lookback = parse_pnls_max_lookback_days(
                self.live_value("pnls_max_lookback_days"),
                field_name="live.pnls_max_lookback_days",
            )
            age_limit = lookback.fill_cache_age_limit_ms(self.get_exchange_time())

            # Get existing event IDs and source IDs before refresh
            existing_ids: set[str] = set()
            existing_source_ids: set[str] = set()
            for ev in self._pnls_manager.get_events():
                if getattr(ev, "id", None):
                    existing_ids.add(ev.id)
                src_ids = getattr(ev, "source_ids", None)
                if src_ids:
                    existing_source_ids.update(str(x) for x in src_ids if x)
                elif getattr(ev, "id", None):
                    existing_source_ids.add(ev.id)

            # Check if we need a full refresh (cache empty or too old)
            events = self._pnls_manager.get_events()
            needs_full_refresh = not events
            history_scope = self._pnls_manager.get_history_scope()
            if lookback.is_all and events and history_scope != "all":
                needs_full_refresh = True
                cache_key = "_fills_full_refresh_logged"
                if not getattr(self, cache_key, False):
                    setattr(self, cache_key, True)
                    logging.debug(
                        "[fills] Cache history scope %s is narrower than requested full history; doing full refresh",
                        history_scope,
                    )
            elif events and age_limit is not None:
                oldest_event_ts = events[0].timestamp
                if oldest_event_ts > age_limit + 1000 * 60 * 60 * 24:  # > 1 day newer than limit
                    needs_full_refresh = True
                    # Log once per session to avoid spam
                    cache_key = "_fills_full_refresh_logged"
                    if not getattr(self, cache_key, False):
                        setattr(self, cache_key, True)
                        logging.debug(
                            "[fills] Cache oldest event (%s) is newer than lookback (%s), doing full refresh",
                            ts_to_date(oldest_event_ts)[:19],
                            ts_to_date(age_limit)[:19],
                        )

            if needs_full_refresh:
                # Full refresh with proper lookback window
                if not getattr(self, "_fills_full_refresh_logged", False):
                    if age_limit is None:
                        logging.debug("[fills] Performing full refresh from full available history")
                    else:
                        logging.debug(
                            "[fills] Performing full refresh from %s", ts_to_date(age_limit)[:19]
                        )
                await self._pnls_manager.refresh(
                    start_ms=None if age_limit is None else int(age_limit),
                    end_ms=None,
                )
                self._pnls_manager.set_history_scope("all" if lookback.is_all else "window")
            else:
                # Incremental refresh
                await self._pnls_manager.refresh_latest(overlap=20)

            # Find and log new events (those not in cache before refresh)
            all_events = self._pnls_manager.get_events()
            new_events = []
            seen_new_source_ids: set[str] = set()
            for ev in all_events:
                src_ids = getattr(ev, "source_ids", None)
                if src_ids:
                    src_ids = [str(x) for x in src_ids if x]
                else:
                    src_ids = [ev.id] if getattr(ev, "id", None) else []
                if not src_ids:
                    continue
                if any(src_id in existing_source_ids for src_id in src_ids):
                    continue
                if any(src_id in seen_new_source_ids for src_id in src_ids):
                    continue
                new_events.append(ev)
                seen_new_source_ids.update(src_ids)
            if new_events:
                self._log_new_fill_events(new_events)

            return True

        except RateLimitExceeded:
            self._health_rate_limits += 1
            self._monitor_record_event(
                "error.exchange",
                ("error", "exchange", "rate_limit"),
                {"source": "update_pnls", "message": "rate limit exceeded"},
            )
            logging.warning("[rate] hit rate limit while fetching fill events; retrying next cycle")
            return False
        except Exception as e:
            self._monitor_record_error(
                "error.exchange",
                e,
                tags=("error", "exchange"),
                payload={"source": "update_pnls"},
            )
            logging.error("[fills] Failed to update FillEventsManager: %s", e)
            if self.logging_level >= 2:
                traceback.print_exc()
            raise

    # -------------------------------------------------------------------------
    # FillEventsManager Helpers
    # -------------------------------------------------------------------------

    def _log_fill_event(self, event) -> str:
        return pb_fill_event_utils.log_fill_event(self, event)

    def _log_new_fill_events(self, new_events: list) -> None:
        return pb_fill_event_utils.log_new_fill_events(self, new_events)

    def _get_realized_pnl_cumsum_stats(self) -> dict[str, float]:
        return pb_fill_event_utils.get_realized_pnl_cumsum_stats(self)

    def _log_realized_loss_gate_blocks(self, out: dict, idx_to_symbol: dict[int, str]) -> None:
        return pb_fill_event_utils.log_realized_loss_gate_blocks(self, out, idx_to_symbol)

    # Legacy init_fill_events, update_fill_events, etc. removed - using FillEventsManager

    async def get_balance_equity_history(
        self, fill_events: Optional[List[dict]] = None, current_balance: Optional[float] = None
    ) -> Dict[str, Any]:
        """Replay canonical fill events to produce historical balance/equity curves."""
        await self.init_pnls()

        def _safe_float(val: Any, default: float = 0.0) -> float:
            try:
                if val is None:
                    return default
                return float(val)
            except Exception:
                return default

        def _normalize_symbol(symbol: Any) -> str:
            sym = str(symbol) if symbol else ""
            if not sym:
                return ""
            if sym in self.c_mults:
                return sym
            try:
                converted = self.get_symbol_id_inv(sym)
                if converted:
                    return converted
            except (KeyError, TypeError, ValueError):
                pass
            return sym

        def _ensure_slot(container: Dict[str, Dict[str, Dict[str, float]]], symbol: str):
            if symbol not in container:
                container[symbol] = {
                    "long": {"size": 0.0, "price": 0.0},
                    "short": {"size": 0.0, "price": 0.0},
                }
            return container[symbol]

        def _determine_action(
            pside: str, side: str, qty_signed: Optional[float], explicit: Optional[str]
        ):
            if explicit in ("increase", "decrease"):
                return explicit
            if qty_signed is not None and qty_signed != 0.0:
                return "increase" if qty_signed > 0 else "decrease"
            side = side.lower()
            if pside == "long":
                return "increase" if side == "buy" else "decrease"
            return "increase" if side == "sell" else "decrease"

        def _extract_events(source: List[dict]) -> List[dict]:
            out = []
            for fill in source:
                ts_raw = fill.get("timestamp")
                if ts_raw is None:
                    continue
                try:
                    ts = int(ensure_millis(ts_raw))
                except (TypeError, ValueError, OverflowError):
                    continue
                symbol = _normalize_symbol(fill.get("symbol"))
                if not symbol:
                    continue
                pside = str(fill.get("position_side", fill.get("pside", "long"))).lower()
                if pside not in ("long", "short"):
                    pside = "long"
                qty_signed = fill.get("qty_signed")
                qty_fallback_keys = ("qty", "amount", "size", "contracts")
                qty_val = _safe_float(
                    (
                        qty_signed
                        if qty_signed is not None
                        else next(
                            (fill.get(k) for k in qty_fallback_keys if fill.get(k) is not None), 0.0
                        )
                    ),
                    0.0,
                )
                qty = abs(qty_val)
                if qty <= 0.0:
                    continue
                price_keys = ("price", "avgPrice", "average", "avg_price", "execPrice")
                price = next((fill.get(k) for k in price_keys if fill.get(k) is not None), None)
                if price is None:
                    info = fill.get("info", {})
                    price = (
                        info.get("avgPrice") or info.get("execPrice") or info.get("avg_exec_price")
                    )
                price = _safe_float(price, 0.0)
                if price <= 0.0:
                    continue
                pnl_val = _safe_float(fill.get("pnl", 0.0), 0.0)
                fee_cost = 0.0
                fee_obj = fill.get("fee")
                if isinstance(fee_obj, dict):
                    fee_cost = _safe_float(fee_obj.get("cost", 0.0), 0.0)
                elif isinstance(fee_obj, (int, float, str)):
                    fee_cost = _safe_float(fee_obj, 0.0)
                elif isinstance(fill.get("fees"), (list, tuple)):
                    fee_cost = sum(
                        _safe_float(x.get("cost", 0.0), 0.0)
                        for x in fill["fees"]
                        if isinstance(x, dict)
                    )
                side = str(fill.get("side", "")).lower()
                action = _determine_action(pside, side, qty_signed, fill.get("action"))
                out.append(
                    {
                        "timestamp": ts,
                        "symbol": symbol,
                        "pside": pside,
                        "qty": qty,
                        "price": price,
                        "action": action,
                        "pnl": pnl_val,
                        "fee": fee_cost,
                        "pb_order_type": str(fill.get("pb_order_type") or "").lower(),
                        "c_mult": float(self.c_mults.get(symbol, 1.0)),
                    }
                )
            return sorted(out, key=lambda x: x["timestamp"])

        def _current_position_state() -> Dict[Tuple[str, str], Tuple[float, float]]:
            out: Dict[Tuple[str, str], Tuple[float, float]] = {}
            for symbol, slots in (self.positions or {}).items():
                norm_symbol = _normalize_symbol(symbol)
                if not norm_symbol or not isinstance(slots, dict):
                    continue
                for pside in ("long", "short"):
                    pos = slots.get(pside, {})
                    if not isinstance(pos, dict):
                        continue
                    size = abs(_safe_float(pos.get("size"), 0.0))
                    price = _safe_float(pos.get("price"), 0.0) if size > 1e-12 else 0.0
                    out[(norm_symbol, pside)] = (size, price)
            return out

        if fill_events is None:
            if self._pnls_manager:
                fill_events = [ev.to_dict() for ev in self._pnls_manager.get_events()]
            else:
                fill_events = []

        events = _extract_events(fill_events)
        current_position_state = _current_position_state()
        if events:
            try:
                compute_psize_pprice(
                    events,
                    final_state=current_position_state,
                    log_discrepancies=True,
                    log_prefix=f"{self.exchange}:{self.user} balance-equity replay",
                )
            except TypeError:
                compute_psize_pprice(events)
        if not events:
            ts_now = self.get_exchange_time()
            balance_now = (
                float(current_balance) if current_balance is not None else self.get_raw_balance()
            )
            point = {
                "timestamp": ts_now,
                "balance": balance_now,
                "equity": balance_now,
                "unrealized_pnl": 0.0,
                "realized_pnl": 0.0,
                "unrealized_pnl_long": 0.0,
                "unrealized_pnl_short": 0.0,
                "realized_pnl_long": 0.0,
                "realized_pnl_short": 0.0,
                "is_flat": True,
                "is_flat_long": True,
                "is_flat_short": True,
                "panic_fill_count": 0,
            }
            return {
                "timeline": [point],
                "panic_flatten_events": [],
                "fill_events": [],
                "balances": [{"timestamp": point["timestamp"], "balance": balance_now}],
                "equities": [
                    {
                        "timestamp": point["timestamp"],
                        "equity": balance_now,
                        "unrealized_pnl": 0.0,
                    }
                ],
                "metadata": {
                    "lookback_days": parse_pnls_max_lookback_days(
                        self.live_value("pnls_max_lookback_days"),
                        field_name="live.pnls_max_lookback_days",
                    ).display_value,
                    "resolution_ms": ONE_MIN_MS,
                    "events_used": 0,
                    "symbols_covered": [],
                    "missing_price_symbols": [],
                },
            }

        lookback = parse_pnls_max_lookback_days(
            self.live_value("pnls_max_lookback_days"),
            field_name="live.pnls_max_lookback_days",
        )
        ts_now = self.get_exchange_time()
        lookback_start = lookback.balance_history_start_ms(ts_now)

        balance_now = (
            float(current_balance) if current_balance is not None else self.get_raw_balance()
        )
        balance_now = max(balance_now, 0.0)
        total_realised = sum(
            evt["pnl"] + evt.get("fee", 0.0) for evt in events if evt["timestamp"] <= ts_now
        )
        baseline_balance = balance_now - total_realised

        if lookback_start is None:
            start_ts = ensure_millis(events[0]["timestamp"])
            record_start_minute = int(math.floor(start_ts / ONE_MIN_MS) * ONE_MIN_MS)
        else:
            start_ts = min(ensure_millis(events[0]["timestamp"]), lookback_start)
            record_start_minute = int(math.floor(lookback_start / ONE_MIN_MS) * ONE_MIN_MS)
        start_minute = int(math.floor(start_ts / ONE_MIN_MS) * ONE_MIN_MS)
        end_minute = int(math.floor(ts_now / ONE_MIN_MS) * ONE_MIN_MS)
        if end_minute < record_start_minute:
            end_minute = record_start_minute

        symbols = {evt["symbol"] for evt in events if evt["symbol"]}
        price_lookup: Dict[str, Dict[int, float]] = {}
        approximate_price_sources: Dict[str, Dict[str, int]] = {}
        if symbols and getattr(self, "cm", None) is not None:
            tasks = {
                sym: asyncio.create_task(
                    self.cm.get_candles(sym, start_ts=start_minute, end_ts=end_minute, strict=False)
                )
                for sym in symbols
            }
            for sym, task in tasks.items():
                try:
                    arr = await task
                except Exception as exc:
                    logging.error(f"error fetching candles for {sym} {exc}")
                    arr = np.empty((0,), dtype=CANDLE_DTYPE)
                price_lookup[sym] = {
                    int(row["ts"]): float(row["c"]) for row in arr if float(row["c"]) > 0.0
                }
            is_hyperliquid = str(getattr(self, "exchange", "")).lower() == "hyperliquid"
            if is_hyperliquid:
                lookback_minutes = int(max(0, (end_minute - start_minute) // ONE_MIN_MS)) + 1
                tf_plan: list[tuple[str, int]] = []
                if lookback_minutes > 5000:
                    tf_plan.append(("5m", 5))
                if lookback_minutes > 5000 * 5:
                    tf_plan.append(("15m", 15))
                for timeframe, tf_minutes in tf_plan:
                    tf_tasks = {
                        sym: asyncio.create_task(
                            self.cm.get_candles(
                                sym,
                                start_ts=start_minute,
                                end_ts=end_minute,
                                strict=False,
                                timeframe=timeframe,
                            )
                        )
                        for sym in symbols
                    }
                    for sym, task in tf_tasks.items():
                        try:
                            arr = await task
                        except Exception as exc:
                            logging.error(
                                "error fetching %s candles for %s during equity history replay: %s",
                                timeframe,
                                sym,
                                exc,
                            )
                            continue
                        if arr is None or arr.size == 0:
                            continue
                        synth = synthesize_1m_from_higher_tf(arr, tf_minutes)
                        if synth.size == 0:
                            continue
                        added = 0
                        lookup = price_lookup.setdefault(sym, {})
                        for row in synth:
                            ts = int(row["ts"])
                            close = float(row["c"])
                            if ts < start_minute or ts > end_minute:
                                continue
                            if ts in lookup:
                                continue
                            lookup[ts] = float(close)
                            added += 1
                        if added > 0:
                            approximate_price_sources.setdefault(sym, {})[timeframe] = added
        else:
            price_lookup = {sym: {} for sym in symbols}

        positions: Dict[str, Dict[str, Dict[str, float]]] = {}
        active_symbols: set[str] = set()
        timeline: List[Dict[str, float]] = []
        panic_flatten_events: List[Dict[str, Any]] = []
        missing_price_symbols: set[str] = set()
        realized_pnl_pside_running = {"long": 0.0, "short": 0.0}
        actual_pside_flat = {
            pside: not any(
                size > 1e-12
                for (sym, ps), (size, _price) in current_position_state.items()
                if ps == pside
            )
            for pside in ("long", "short")
        }
        last_event_ts_by_pside = {
            pside: max((evt["timestamp"] for evt in events if evt["pside"] == pside), default=None)
            for pside in ("long", "short")
        }

        def _pside_is_flat(pside: str) -> bool:
            return not any(
                positions.get(sym, {}).get(pside, {}).get("size", 0.0) > 1e-12 for sym in positions
            )

        def _apply_event(evt: dict):
            slot = _ensure_slot(positions, evt["symbol"])[evt["pside"]]
            qty = evt["qty"]
            price = evt["price"]
            if evt["action"] == "increase":
                old_size = slot["size"]
                new_size = old_size + qty
                if new_size <= 0.0:
                    slot["size"], slot["price"] = 0.0, 0.0
                elif old_size <= 0.0:
                    slot["size"], slot["price"] = new_size, price
                else:
                    slot["price"] = max(
                        (old_size * slot["price"] + qty * price) / new_size,
                        0.0,
                    )
                    slot["size"] = new_size
            else:
                slot["size"] = max(slot["size"] - qty, 0.0)
                if slot["size"] <= 0.0:
                    slot["price"] = 0.0
            has_pos = slot["size"] > 1e-12
            if has_pos:
                active_symbols.add(evt["symbol"])
            elif not any(
                positions[evt["symbol"]][ps]["size"] > 1e-12 for ps in ("long", "short")
            ):
                active_symbols.discard(evt["symbol"])

        balance = baseline_balance
        event_idx = 0
        last_price: Dict[str, float] = {}

        minute = start_minute
        while minute <= end_minute:
            boundary = minute + ONE_MIN_MS
            panic_fill_count = 0
            while event_idx < len(events) and events[event_idx]["timestamp"] < boundary:
                evt = events[event_idx]
                _apply_event(evt)
                realized_delta = evt["pnl"] + evt.get("fee", 0.0)
                balance += realized_delta
                realized_pnl_pside_running[evt["pside"]] += realized_delta
                if "panic" in str(evt.get("pb_order_type") or ""):
                    panic_fill_count += 1
                    after_psize = _safe_float(evt.get("psize"), math.nan)
                    authoritative_flat_override = (
                        actual_pside_flat.get(evt["pside"], False)
                        and last_event_ts_by_pside.get(evt["pside"]) == evt["timestamp"]
                    )
                    if authoritative_flat_override and (
                        not math.isfinite(after_psize) or after_psize > 1e-12
                    ):
                        logging.warning(
                            "[risk] balance-equity replay trusting current flat %s state over residual panic replay size | timestamp=%s replay_after_psize=%s symbol=%s",
                            evt["pside"],
                            evt["timestamp"],
                            f"{after_psize:.12f}" if math.isfinite(after_psize) else "nan",
                            evt["symbol"],
                        )
                    if (
                        (math.isfinite(after_psize) and after_psize <= 1e-12)
                        or authoritative_flat_override
                        or _pside_is_flat(evt["pside"])
                    ):
                        panic_flatten_events.append(
                            {
                                "timestamp": int(evt["timestamp"]),
                                "minute_timestamp": int(minute),
                                "pside": str(evt["pside"]),
                                "symbol": str(evt["symbol"]),
                            }
                        )
                event_idx += 1
            upnl = 0.0
            upnl_by_pside = {"long": 0.0, "short": 0.0}
            for symbol in list(active_symbols):
                price = price_lookup.get(symbol, {}).get(minute)
                if price is None:
                    price = last_price.get(symbol)
                else:
                    last_price[symbol] = price
                if price is None or price <= 0.0:
                    missing_price_symbols.add(symbol)
                    continue
                slot = positions.get(symbol)
                if not slot:
                    continue
                for pside in ("long", "short"):
                    size = slot[pside]["size"]
                    if size <= 0.0:
                        continue
                    avg_price = slot[pside]["price"]
                    if avg_price <= 0.0:
                        continue
                    c_mult = self.c_mults.get(symbol, 1.0)
                    pside_upnl = calc_pnl(pside, avg_price, price, size, self.inverse, c_mult)
                    upnl += pside_upnl
                    upnl_by_pside[pside] += pside_upnl
            if minute >= record_start_minute:
                timeline.append(
                    {
                        "timestamp": minute,
                        "balance": balance,
                        "equity": balance + upnl,
                        "unrealized_pnl": upnl,
                        "realized_pnl": balance - baseline_balance,
                        "unrealized_pnl_long": upnl_by_pside["long"],
                        "unrealized_pnl_short": upnl_by_pside["short"],
                        "realized_pnl_long": realized_pnl_pside_running["long"],
                        "realized_pnl_short": realized_pnl_pside_running["short"],
                        "is_flat": len(active_symbols) == 0,
                        "is_flat_long": not any(
                            positions.get(sym, {}).get("long", {}).get("size", 0.0) > 1e-12
                            for sym in positions
                        ),
                        "is_flat_short": not any(
                            positions.get(sym, {}).get("short", {}).get("size", 0.0) > 1e-12
                            for sym in positions
                        ),
                        "panic_fill_count": int(panic_fill_count),
                    }
                )
            minute += ONE_MIN_MS

        if not timeline:
            point = {
                "timestamp": ts_now,
                "balance": balance_now,
                "equity": balance_now,
                "unrealized_pnl": 0.0,
                "realized_pnl": 0.0,
                "unrealized_pnl_long": 0.0,
                "unrealized_pnl_short": 0.0,
                "realized_pnl_long": 0.0,
                "realized_pnl_short": 0.0,
                "is_flat": True,
                "is_flat_long": True,
                "is_flat_short": True,
            }
            timeline = [point]

        balances = [{"timestamp": row["timestamp"], "balance": row["balance"]} for row in timeline]
        equities = [
            {
                "timestamp": row["timestamp"],
                "equity": row["equity"],
                "unrealized_pnl": row["unrealized_pnl"],
            }
            for row in timeline
        ]
        metadata = {
            "lookback_days": lookback.display_value,
            "resolution_ms": ONE_MIN_MS,
            "events_used": len(events),
            "symbols_covered": sorted(symbols),
            "missing_price_symbols": sorted(missing_price_symbols),
            "approximate_price_sources": approximate_price_sources,
        }
        return {
            "timeline": timeline,
            "panic_flatten_events": panic_flatten_events,
            "fill_events": events,
            "balances": balances,
            "equities": equities,
            "metadata": metadata,
        }

    async def update_open_orders(self):
        """Refresh open orders from the exchange and reconcile the local cache."""
        if not hasattr(self, "open_orders"):
            self.open_orders = {}
        if self.stop_signal_received:
            return False
        res = None
        try:
            res = await self.fetch_open_orders()
            if res in [None, False]:
                raise RuntimeError(f"fetch_open_orders returned invalid result {res!r}")
            self.fetched_open_orders = res
            open_orders = res
            oo_ids_old = {elm["id"] for sublist in self.open_orders.values() for elm in sublist}
            oo_ids_new = {elm["id"] for elm in open_orders}
            added_orders = [oo for oo in open_orders if oo["id"] not in oo_ids_old]
            removed_orders = [
                oo
                for oo in [elm for sublist in self.open_orders.values() for elm in sublist]
                if oo["id"] not in oo_ids_new
            ]
            schedule_update_positions = False
            if len(removed_orders) > 20:
                logging.info(f"removed {len(removed_orders)} orders")
            else:
                for order in removed_orders:
                    if not self.order_was_recently_cancelled(order):
                        # means order is no longer in open orders, but wasn't cancelled by bot
                        # possible fill
                        # force another update_positions
                        schedule_update_positions = True
                        self.log_order_action(
                            order, "missing order", "fetch_open_orders", level=logging.INFO
                        )
                    else:
                        self.log_order_action(
                            order, "removed order", "fetch_open_orders", level=logging.DEBUG
                        )
            if len(added_orders) > 20:
                logging.info(f"[order] added {len(added_orders)} new orders")
            else:
                for order in added_orders:
                    self.log_order_action(
                        order, "added order", "fetch_open_orders", level=logging.DEBUG
                    )
            self.open_orders = {}
            for elm in open_orders:
                if elm["symbol"] not in self.open_orders:
                    self.open_orders[elm["symbol"]] = []
                self.open_orders[elm["symbol"]].append(elm)
            if schedule_update_positions:
                await asyncio.sleep(1.5)
                await self.update_positions_and_balance()
            return True
        except RateLimitExceeded:
            self._health_rate_limits += 1
            logging.warning("[rate] hit rate limit while fetching open orders; retrying next cycle")
            return False
        except Exception as e:
            logging.error(f"error with {get_function_name()} {e}")
            if res is not None:
                print_async_exception(res)
            traceback.print_exc()
            raise

    async def _fetch_and_apply_positions(self):
        """Fetch raw positions, apply them to local state and return snapshots.

        Returns:
            Tuple of (success: bool, old_positions, new_positions).

        Raises:
            Exception: On API errors (caller handles via restart_bot_on_too_many_errors).
        """
        if not hasattr(self, "positions"):
            self.positions = {}
        res = await self.fetch_positions()
        if res is None:
            raise RuntimeError("fetch_positions returned None")
        positions_list_new = res
        fetched_positions_old = deepcopy(self.fetched_positions)
        self.fetched_positions = positions_list_new
        positions_new = {
            sym: {
                "long": {"size": 0.0, "price": 0.0},
                "short": {"size": 0.0, "price": 0.0},
            }
            for sym in set(list(self.positions) + list(self.active_symbols))
        }
        for elm in positions_list_new:
            symbol, pside, pprice = elm["symbol"], elm["position_side"], elm["price"]
            psize = abs(elm["size"]) * (-1.0 if elm["position_side"] == "short" else 1.0)
            if symbol not in positions_new:
                positions_new[symbol] = {
                    "long": {"size": 0.0, "price": 0.0},
                    "short": {"size": 0.0, "price": 0.0},
                }
            positions_new[symbol][pside] = {"size": psize, "price": pprice}
        self.positions = positions_new
        return True, fetched_positions_old, self.fetched_positions

    async def update_positions(self, *, log_changes: bool = True):
        """Fetch positions, update local caches, and optionally log any changes."""
        ok, fetched_positions_old, fetched_positions_new = await self._fetch_and_apply_positions()
        if not ok:
            return False
        if log_changes and fetched_positions_old is not None:
            try:
                await self.log_position_changes(fetched_positions_old, fetched_positions_new)
            except Exception as e:
                logging.error(f"error logging position changes {e}")
        return True

    async def update_balance(self):
        """Fetch and apply the latest wallet balance.

        Returns:
            bool: True on success, False if balance_override is used but invalid.

        Raises:
            Exception: On API errors (caller handles via restart_bot_on_too_many_errors).
        """
        if not hasattr(self, "balance_override"):
            self.balance_override = None
        if not hasattr(self, "_balance_override_logged"):
            self._balance_override_logged = False
        if not hasattr(self, "previous_hysteresis_balance"):
            self.previous_hysteresis_balance = None
        if not hasattr(self, "balance_hysteresis_snap_pct"):
            self.balance_hysteresis_snap_pct = 0.02
        balance_raw = None
        if not hasattr(self, "balance_raw"):
            if hasattr(self, "balance"):
                self.balance_raw = self.get_raw_balance()
            else:
                balance_raw = await pb_balance_utils.initialize_balance(self)

        if self.balance_override is not None:
            balance_raw = float(self.balance_override)
            if not self._balance_override_logged:
                logging.info("Using balance override: %.6f", balance_raw)
                self._balance_override_logged = True
        else:
            if not hasattr(self, "fetch_balance"):
                raise NotImplementedError("update_balance requires fetch_balance implementation")
            if balance_raw is None:
                balance_raw = await self.fetch_balance()

        if balance_raw is None:
            raise RuntimeError("fetch_balance returned None")
        if isinstance(balance_raw, bool):
            raise TypeError(f"fetch_balance returned invalid boolean balance {balance_raw!r}")
        try:
            balance_raw = float(balance_raw)
        except (TypeError, ValueError):
            raise TypeError(f"fetch_balance returned non-numeric balance {balance_raw!r}")
        if not math.isfinite(balance_raw):
            raise ValueError(f"fetch_balance returned non-finite balance {balance_raw!r}")

        balance_snapped = balance_raw
        if self.balance_override is None:
            if self.previous_hysteresis_balance is None:
                self.previous_hysteresis_balance = balance_raw
            balance_snapped = pbr.hysteresis(
                balance_raw, self.previous_hysteresis_balance, self.balance_hysteresis_snap_pct
            )
            self.previous_hysteresis_balance = balance_snapped
        self.balance_raw = balance_raw
        self.balance = balance_snapped
        return True

    async def update_positions_and_balance(self):
        """Convenience helper to refresh both positions and balance concurrently."""
        balance_task = asyncio.create_task(self.update_balance())
        positions_task = asyncio.create_task(self._fetch_and_apply_positions())
        try:
            balance_ok, positions_res = await asyncio.gather(balance_task, positions_task)
        except Exception as exc:
            collaborator_errors = []
            for name, task in (
                ("update_balance", balance_task),
                ("_fetch_and_apply_positions", positions_task),
            ):
                if not task.done():
                    task.cancel()
            for name, task in (
                ("update_balance", balance_task),
                ("_fetch_and_apply_positions", positions_task),
            ):
                try:
                    await task
                except asyncio.CancelledError:
                    continue
                except Exception as task_exc:
                    collaborator_errors.append((name, task_exc))
            if collaborator_errors:
                primary_name, primary_exc = collaborator_errors[0]
                raise RuntimeError(
                    f"{primary_name} failed during update_positions_and_balance"
                ) from primary_exc
            if balance_task.done() and not balance_task.cancelled():
                task_exc = balance_task.exception()
                if task_exc is not None:
                    raise RuntimeError(
                        "update_balance failed during update_positions_and_balance"
                    ) from task_exc
            if positions_task.done() and not positions_task.cancelled():
                task_exc = positions_task.exception()
                if task_exc is not None:
                    raise RuntimeError(
                        "_fetch_and_apply_positions failed during update_positions_and_balance"
                    ) from task_exc
            raise RuntimeError(
                "update_positions_and_balance failed before collaborator state could be resolved"
            ) from exc
        positions_ok, fetched_positions_old, fetched_positions_new = positions_res
        if positions_ok and fetched_positions_old is not None:
            try:
                await self.log_position_changes(fetched_positions_old, fetched_positions_new)
            except Exception as e:
                logging.error(f"error logging position changes {e}")
        if balance_ok and positions_ok:
            await self.handle_balance_update(source="REST")
        return balance_ok, positions_ok

    async def calc_ideal_orders(self):
        """Compute desired entry and exit orders for every active symbol."""
        return await self.calc_ideal_orders_orchestrator()

    def _bot_params_to_rust_dict(self, pside: str, symbol: str | None) -> dict:
        """Build a dict matching Rust `BotParams` for JSON orchestrator input."""
        # Values which are configured globally (not per symbol) live under bot_value.
        global_keys = {
            "n_positions",
            "total_wallet_exposure_limit",
            "risk_twel_enforcer_threshold",
            "unstuck_loss_allowance_pct",
        }
        # Maintain 1:1 field coverage with `passivbot-rust/src/types.rs BotParams`.
        fields = [
            "close_grid_markup_end",
            "close_grid_markup_start",
            "close_grid_qty_pct",
            "close_trailing_retracement_pct",
            "close_trailing_grid_ratio",
            "close_trailing_qty_pct",
            "close_trailing_threshold_pct",
            "entry_grid_double_down_factor",
            "entry_grid_spacing_volatility_weight",
            "entry_grid_spacing_we_weight",
            "entry_grid_spacing_pct",
            "entry_volatility_ema_span_hours",
            "entry_initial_ema_dist",
            "entry_initial_qty_pct",
            "entry_trailing_double_down_factor",
            "entry_trailing_retracement_pct",
            "entry_trailing_retracement_we_weight",
            "entry_trailing_retracement_volatility_weight",
            "entry_trailing_grid_ratio",
            "entry_trailing_threshold_pct",
            "entry_trailing_threshold_we_weight",
            "entry_trailing_threshold_volatility_weight",
            "forager_volatility_ema_span",
            "forager_volume_ema_span",
            "forager_volume_drop_pct",
            "forager_score_weights",
            "ema_span_0",
            "ema_span_1",
            "n_positions",
            "total_wallet_exposure_limit",
            "wallet_exposure_limit",
            "risk_wel_enforcer_threshold",
            "risk_twel_enforcer_threshold",
            "risk_we_excess_allowance_pct",
            "unstuck_close_pct",
            "unstuck_ema_dist",
            "unstuck_loss_allowance_pct",
            "unstuck_threshold",
        ]
        out: dict[str, float | int] = {}
        for key in fields:
            if key in global_keys:
                val = self.bot_value(pside, key)
            else:
                val = self.bp(pside, key, symbol) if symbol is not None else self.bp(pside, key)
            out_key = key
            if key == "forager_volatility_ema_span":
                out_key = "filter_volatility_ema_span"
            elif key == "forager_volume_ema_span":
                out_key = "filter_volume_ema_span"
            if key == "forager_score_weights":
                if not isinstance(val, dict):
                    raise TypeError(
                        f"bot.{pside}.forager_score_weights must be a dict, got {type(val).__name__}"
                    )
                out[out_key] = {
                    "volume": float(val["volume"]),
                    "ema_readiness": float(val["ema_readiness"]),
                    "volatility": float(val["volatility"]),
                }
            elif key == "n_positions":
                out[out_key] = int(round(val or 0.0))
            else:
                out[out_key] = float(val or 0.0)
        out.update(
            {
                "hsl_enabled": bool(self.bot_value(pside, "hsl_enabled")),
                "hsl_red_threshold": float(self.bot_value(pside, "hsl_red_threshold")),
                "hsl_ema_span_minutes": float(self.bot_value(pside, "hsl_ema_span_minutes")),
                "hsl_cooldown_minutes_after_red": float(
                    self.bot_value(pside, "hsl_cooldown_minutes_after_red")
                ),
                "hsl_no_restart_drawdown_threshold": float(
                    self.bot_value(pside, "hsl_no_restart_drawdown_threshold")
                ),
                "hsl_tier_ratio_yellow": float(self.bot_value(pside, "hsl_tier_ratios.yellow")),
                "hsl_tier_ratio_orange": float(self.bot_value(pside, "hsl_tier_ratios.orange")),
                "hsl_orange_tier_mode": str(self.bot_value(pside, "hsl_orange_tier_mode")),
                "hsl_panic_close_order_type": str(
                    self.bot_value(pside, "hsl_panic_close_order_type")
                ),
            }
        )
        return out

    def _pb_mode_to_orchestrator_mode(self, mode: str) -> str:
        m = (mode or "").strip().lower()
        if m == "tp_only_with_active_entry_cancellation":
            return "tp_only"
        if m in {"normal", "panic", "graceful_stop", "tp_only", "manual"}:
            return m
        return "manual"

    def _apply_orchestrator_symbol_states(
        self,
        diagnostics: dict,
        idx_to_symbol: dict[int, str],
        explicit_overrides: dict[str, dict[str, Optional[str]]],
    ) -> None:
        previous_PB_modes = deepcopy(self.PB_modes) if hasattr(self, "PB_modes") else None
        symbol_states = diagnostics.get("symbol_states", []) if isinstance(diagnostics, dict) else []
        if not symbol_states:
            return
        pb_modes = {"long": {}, "short": {}}
        for row in symbol_states:
            if not isinstance(row, dict):
                continue
            symbol = idx_to_symbol.get(int(row.get("symbol_idx", -1)))
            if symbol is None:
                continue
            for pside in ("long", "short"):
                side_state = row.get(pside, {})
                explicit_override = explicit_overrides.get(pside, {}).get(symbol)
                pb_modes[pside][symbol] = self._python_mode_from_orchestrator_state(
                    pside,
                    symbol,
                    side_state if isinstance(side_state, dict) else {},
                    explicit_override,
                )

        for symbol in set(self.positions) | set(self.open_orders) | set(pb_modes["long"]) | set(pb_modes["short"]):
            for pside in ("long", "short"):
                if symbol not in pb_modes[pside]:
                    explicit_override = explicit_overrides.get(pside, {}).get(symbol)
                    if explicit_override:
                        pb_modes[pside][symbol] = str(explicit_override)
                    else:
                        pb_modes[pside][symbol] = self.PB_mode_stop[pside]

        self.PB_modes = pb_modes
        self.active_symbols = sorted(set(pb_modes["long"]) | set(pb_modes["short"]) | set(self.open_orders))
        res = log_dict_changes(previous_PB_modes, self.PB_modes)
        self._log_mode_changes(res, previous_PB_modes)

    def _orchestrator_mode_override(self, pside: str, symbol: str) -> Optional[str]:
        if self._equity_hard_stop_enabled(pside):
            state = self._hsl_state(pside)
            if self._equity_hard_stop_runtime_red_latched(pside) and not state["halted"]:
                return "panic"
            if state["halted"]:
                return self._equity_hard_stop_halted_mode(pside, symbol)
            if self._equity_hard_stop_runtime_tier(pside) == "orange":
                orange_mode = str(self.hsl[pside]["orange_tier_mode"])
                if orange_mode == "graceful_stop":
                    return "graceful_stop"
                if orange_mode == "tp_only_with_active_entry_cancellation":
                    size = float(self.positions.get(symbol, {}).get(pside, {}).get("size", 0.0) or 0.0)
                    if size != 0.0:
                        return "tp_only_with_active_entry_cancellation"

        runtime_forced = getattr(self, "_runtime_forced_modes", {}).get(pside, {}).get(symbol)
        if runtime_forced:
            return str(runtime_forced)

        forced_mode = self.config_get(["live", f"forced_mode_{pside}"], symbol)
        if forced_mode:
            return expand_PB_mode(forced_mode)
        if not self.markets_dict.get(symbol, {}).get("active", True):
            return "tp_only"
        ineligible_reason = getattr(self, "ineligible_symbols", {}).get(symbol)
        if ineligible_reason is not None:
            return "tp_only" if ineligible_reason == "not active" else "manual"
        return None

    def _build_orchestrator_mode_overrides(
        self, symbols: Iterable[str]
    ) -> dict[str, dict[str, Optional[str]]]:
        overrides: dict[str, dict[str, Optional[str]]] = {"long": {}, "short": {}}
        for pside in ("long", "short"):
            for symbol in symbols:
                overrides[pside][symbol] = self._orchestrator_mode_override(pside, symbol)
        return overrides

    async def calc_ideal_orders_orchestrator_from_snapshot(
        self, snapshot: dict, *, return_snapshot: bool
    ):
        symbols = snapshot["symbols"]
        last_prices = snapshot["last_prices"]
        Passivbot._monitor_record_price_ticks(self, last_prices, ts=utc_ms(), source="orchestrator_snapshot")
        m1_close_emas = snapshot["m1_close_emas"]
        m1_volume_emas = snapshot["m1_volume_emas"]
        m1_log_range_emas = snapshot["m1_log_range_emas"]
        h1_log_range_emas = snapshot["h1_log_range_emas"]

        unstuck_allowances = snapshot.get("unstuck_allowances", {"long": 0.0, "short": 0.0})
        realized_pnl_cumsum = snapshot.get("realized_pnl_cumsum", {"max": 0.0, "last": 0.0})
        max_realized_loss_pct = float(self.live_value("max_realized_loss_pct") or 1.0)
        if hasattr(self, "_build_orchestrator_mode_overrides"):
            mode_overrides = self._build_orchestrator_mode_overrides(symbols)
        else:
            mode_overrides = Passivbot._build_orchestrator_mode_overrides_fallback(self, symbols)

        global_bp = {
            "long": self._bot_params_to_rust_dict("long", None),
            "short": self._bot_params_to_rust_dict("short", None),
        }
        # Effective hedge_mode = config setting AND exchange capability.
        # If either is False, we block same-coin hedging in the orchestrator.
        effective_hedge_mode = self._config_hedge_mode and self.hedge_mode
        input_dict = pb_orchestrator_utils.build_orchestrator_input_base(
            balance=self.get_hysteresis_snapped_balance(),
            balance_raw=self.get_raw_balance(),
            filter_by_min_effective_cost=bool(self.live_value("filter_by_min_effective_cost")),
            market_orders_allowed=bool(self.live_value("market_orders_allowed")),
            market_order_near_touch_threshold=float(self.live_value("market_order_near_touch_threshold")),
            panic_close_market=bool(
                any(
                    Passivbot._equity_hard_stop_panic_close_order_type(self, pside) == "market"
                    for pside in ("long", "short")
                    if Passivbot._equity_hard_stop_enabled(self, pside)
                )
            ),
            unstuck_allowance_long=float(unstuck_allowances.get("long", 0.0)),
            unstuck_allowance_short=float(unstuck_allowances.get("short", 0.0)),
            max_realized_loss_pct=max_realized_loss_pct,
            realized_pnl_cumsum_max=float(realized_pnl_cumsum.get("max", 0.0) or 0.0),
            realized_pnl_cumsum_last=float(realized_pnl_cumsum.get("last", 0.0) or 0.0),
            global_bp=global_bp,
            effective_hedge_mode=effective_hedge_mode,
        )

        symbol_to_idx: dict[str, int] = {s: i for i, s in enumerate(symbols)}
        idx_to_symbol: dict[int, str] = {i: s for s, i in symbol_to_idx.items()}

        for symbol in symbols:
            idx = symbol_to_idx[symbol]
            mprice = float(last_prices.get(symbol, 0.0))
            if not math.isfinite(mprice) or mprice <= 0.0:
                raise Exception(f"invalid market price for {symbol}: {mprice}")

            active = bool(self.markets_dict.get(symbol, {}).get("active", True))
            effective_min_cost = float(
                getattr(self, "effective_min_cost", {}).get(symbol, 0.0) or 0.0
            )
            if effective_min_cost <= 0.0:
                effective_min_cost = self._calc_effective_min_cost_at_price(symbol, mprice)

            def side_input(pside: str) -> dict:
                return pb_orchestrator_utils.build_side_input(
                    pside=pside,
                    symbol=symbol,
                    mode_overrides=mode_overrides,
                    positions=self.positions,
                    trailing_prices=self.trailing_prices,
                    bot_params_to_rust_dict_fn=self._bot_params_to_rust_dict,
                    mode_override_to_orchestrator_mode_fn=lambda mode: Passivbot._mode_override_to_orchestrator_mode(
                        self, mode
                    ),
                    trailing_bundle_default_fn=_trailing_bundle_default_dict,
                )

            input_dict["symbols"].append(
                pb_orchestrator_utils.build_symbol_input(
                    symbol=symbol,
                    idx=idx,
                    mprice=mprice,
                    active=active,
                    qty_step=self.qty_steps[symbol],
                    price_step=self.price_steps[symbol],
                    min_qty=self.min_qtys[symbol],
                    min_cost=self.min_costs[symbol],
                    c_mult=self.c_mults[symbol],
                    maker_fee=pb_orchestrator_utils.get_required_market_fee(
                        markets_dict=self.markets_dict,
                        symbol=symbol,
                        fee_side="maker",
                    ),
                    taker_fee=pb_orchestrator_utils.get_required_market_fee(
                        markets_dict=self.markets_dict,
                        symbol=symbol,
                        fee_side="taker",
                    ),
                    effective_min_cost=effective_min_cost,
                    m1_close_emas=m1_close_emas[symbol],
                    m1_volume_emas=m1_volume_emas[symbol],
                    m1_log_range_emas=m1_log_range_emas[symbol],
                    h1_log_range_emas=h1_log_range_emas[symbol],
                    side_input_fn=side_input,
                )
            )

        try:
            out_json = pbr.compute_ideal_orders_json(json.dumps(input_dict))
        except Exception as e:
            pb_orchestrator_utils.log_missing_ema_error(
                error=e,
                idx_to_symbol=idx_to_symbol,
                logger=logging,
            )
            raise
        out = json.loads(out_json)
        self._log_realized_loss_gate_blocks(out, idx_to_symbol)
        if hasattr(self, "_apply_orchestrator_symbol_states"):
            self._apply_orchestrator_symbol_states(
                out.get("diagnostics", {}),
                idx_to_symbol,
                mode_overrides,
            )
        orders = out.get("orders", [])
        ideal_orders = pb_orchestrator_utils.build_ideal_orders_by_symbol(
            orders=orders,
            idx_to_symbol=idx_to_symbol,
            order_type_snake_to_id_fn=pbr.order_type_snake_to_id,
        )

        # Log unstuck coin selection
        unstuck_payload = pb_orchestrator_utils.extract_unstuck_log_payload(
            orders=orders,
            idx_to_symbol=idx_to_symbol,
            positions=self.positions,
            last_prices=last_prices,
            unstuck_allowances=unstuck_allowances,
        )
        if unstuck_payload is not None:
            logging.info(
                "[unstuck] selecting %s %s | entry=%.2f now=%.2f (%s%.1f%%) | allowance=%.2f",
                unstuck_payload["coin"],
                unstuck_payload["pside"],
                unstuck_payload["entry_price"],
                unstuck_payload["current_price"],
                unstuck_payload["sign"],
                unstuck_payload["price_diff_pct"],
                unstuck_payload["allowance"],
            )

        # Log EMA gating for symbols in normal mode with no position and no initial entry
        self._log_ema_gating(ideal_orders, m1_close_emas, last_prices, symbols)

        ideal_orders_f, _wel_blocked = self._to_executable_orders(ideal_orders, last_prices)
        ideal_orders_f = self._finalize_reduce_only_orders(ideal_orders_f, last_prices)

        if return_snapshot:
            snapshot_out = {
                "ts_ms": int(utc_ms()),
                "exchange": str(getattr(self, "exchange", "")),
                "user": str(self.config_get(["live", "user"]) or ""),
                "active_symbols": list(symbols),
                "orchestrator_input": input_dict,
                "orchestrator_output": out,
            }
            return ideal_orders_f, snapshot_out
        return ideal_orders_f, None

    async def _load_orchestrator_ema_bundle(
        self, symbols: list[str], modes: dict[str, dict[str, str]]
    ) -> tuple[
        dict[str, dict[float, float]],
        dict[str, dict[float, float]],
        dict[str, dict[float, float]],
        dict[str, dict[float, float]],
        dict[str, float],
        dict[str, float],
    ]:
        """Fetch the EMA values required by the Rust orchestrator for the given symbols.

        Returns:
        - m1_close_emas[symbol][span] = ema_close
        - m1_volume_emas[symbol][span] = ema_quote_volume
        - m1_log_range_emas[symbol][span] = ema_log_range (1m)
        - h1_log_range_emas[symbol][span] = ema_log_range (1h)
        - volumes_long[symbol], log_ranges_long[symbol] (for convenience)
        """
        # Gather full EMA context for the live symbol universe.
        # Python should provide the market-state bundle; Rust decides which branches use it.
        need_close_spans: dict[str, set[float]] = {s: set() for s in symbols}
        need_h1_lr_spans: dict[str, set[float]] = {s: set() for s in symbols}

        for pside in ["long", "short"]:
            for symbol in symbols:
                span0 = float(self.bp(pside, "ema_span_0", symbol))
                span1 = float(self.bp(pside, "ema_span_1", symbol))
                span2 = float((span0 * span1) ** 0.5) if span0 > 0.0 and span1 > 0.0 else 0.0
                for sp in (span0, span1, span2):
                    if sp > 0.0 and math.isfinite(sp):
                        need_close_spans[symbol].add(sp)
                h1_span = float(self.bp(pside, "entry_volatility_ema_span_hours", symbol) or 0.0)
                if h1_span > 0.0 and math.isfinite(h1_span):
                    need_h1_lr_spans[symbol].add(h1_span)

        # Forager metrics use global spans (per side); include them for all symbols.
        vol_span_long = float(self.bot_value("long", "forager_volume_ema_span") or 0.0)
        lr_span_long = float(self.bot_value("long", "forager_volatility_ema_span") or 0.0)
        vol_span_short = float(self.bot_value("short", "forager_volume_ema_span") or 0.0)
        lr_span_short = float(self.bot_value("short", "forager_volatility_ema_span") or 0.0)
        m1_volume_spans = sorted(
            {s for s in (vol_span_long, vol_span_short) if s > 0.0 and math.isfinite(s)}
        )
        m1_lr_spans = sorted(
            {s for s in (lr_span_long, lr_span_short) if s > 0.0 and math.isfinite(s)}
        )
        if not hasattr(self, "_orchestrator_prev_close_ema"):
            self._orchestrator_prev_close_ema = {}
        if not hasattr(self, "_orchestrator_close_ema_fallback_counts"):
            self._orchestrator_close_ema_fallback_counts = {}

        async def fetch_map(symbol: str, spans: list[float], fn, ema_type: str):
            out: dict[float, float] = {}
            if not spans:
                return out
            for sp in spans:
                span = float(sp)
                try:
                    val = float(await fn(symbol, span))
                except Exception as e:
                    logging.warning(
                        "[ema] dropping %s span for %s span=%.8g reason=%s: %s",
                        ema_type,
                        symbol,
                        span,
                        type(e).__name__,
                        e,
                    )
                    continue
                if math.isfinite(val):
                    out[span] = val
                else:
                    logging.warning(
                        "[ema] dropping %s span for %s span=%.8g reason=non-finite value %s",
                        ema_type,
                        symbol,
                        span,
                        val,
                    )
            return out

        async def fetch_required_map(symbol: str, spans: list[float], fn, ema_type: str):
            out: dict[float, float] = {}
            if not spans:
                return out
            missing: list[tuple[float, str]] = []
            for sp in spans:
                span = float(sp)
                try:
                    val = float(await fn(symbol, span))
                except Exception as e:
                    reason = f"{type(e).__name__}: {e}"
                else:
                    if math.isfinite(val):
                        out[span] = val
                        continue
                    reason = f"non-finite {ema_type} value {val}"
                logging.warning(
                    "[ema] missing required %s span for %s span=%.8g reason=%s",
                    ema_type,
                    symbol,
                    span,
                    reason,
                )
                missing.append((span, reason))
            if missing:
                detail = "; ".join([f"span={sp:.8g} reason={why}" for sp, why in missing])
                raise RuntimeError(f"[ema] missing required {ema_type} EMA for {symbol}: {detail}")
            return out

        async def fetch_close_map(symbol: str, spans: list[float]) -> dict[float, float]:
            out: dict[float, float] = {}
            if not spans:
                return out
            now_ms = int(utc_ms())
            prev_by_span = self._orchestrator_prev_close_ema.setdefault(symbol, {})
            missing: list[tuple[float, str]] = []
            for sp in spans:
                span = float(sp)
                key = (symbol, span)
                reason = None
                try:
                    val = float(await ema_close(symbol, span))
                except Exception as e:
                    reason = f"{type(e).__name__}: {e}"
                else:
                    if math.isfinite(val):
                        out[span] = val
                        prev_by_span[span] = (val, now_ms)
                        prev_fallback_count = int(
                            self._orchestrator_close_ema_fallback_counts.get(key, 0)
                        )
                        if prev_fallback_count > 0:
                            logging.info(
                                "[ema] close EMA recovered %s span=%.8g after %d fallback(s)",
                                symbol,
                                span,
                                prev_fallback_count,
                            )
                        self._orchestrator_close_ema_fallback_counts[key] = 0
                    else:
                        reason = f"non-finite close EMA value {val}"
                if reason is None:
                    continue
                prev = prev_by_span.get(span)
                if prev is not None:
                    prev_val = float(prev[0])
                    prev_ts = int(prev[1])
                    if math.isfinite(prev_val):
                        out[span] = prev_val
                        n_fallbacks = int(
                            self._orchestrator_close_ema_fallback_counts.get(key, 0)
                        ) + 1
                        self._orchestrator_close_ema_fallback_counts[key] = n_fallbacks
                        age_ms = max(0, now_ms - prev_ts)
                        logging.warning(
                            "[ema] close EMA fallback %s span=%.8g ema=%.12g age_ms=%d"
                            " n_fallbacks=%d reason=%s",
                            symbol,
                            span,
                            prev_val,
                            age_ms,
                            n_fallbacks,
                            reason,
                        )
                        continue
                missing.append((span, reason))
            if missing:
                detail = "; ".join([f"span={sp:.8g} reason={why}" for sp, why in missing])
                raise RuntimeError(
                    f"[ema] missing required close EMA for {symbol}; no previous EMA fallback available: {detail}"
                )
            return out

        async def ema_close(symbol: str, span: float) -> float:
            # 1m candles finalize once/min; 60s TTL avoids redundant network fetches.
            return float(await self.cm.get_latest_ema_close(symbol, span=span, max_age_ms=60_000))

        async def ema_qv(symbol: str, span: float) -> float:
            return float(
                await self.cm.get_latest_ema_quote_volume(symbol, span=span, max_age_ms=60_000)
            )

        async def ema_lr_1m(symbol: str, span: float) -> float:
            return float(await self.cm.get_latest_ema_log_range(symbol, span=span, max_age_ms=60_000))

        async def ema_lr_1h(symbol: str, span: float) -> float:
            return float(
                await self.cm.get_latest_ema_log_range(symbol, span=span, tf="1h", max_age_ms=600_000)
            )

        async def load_symbol_bundle(sym: str):
            close = await fetch_close_map(sym, sorted(need_close_spans[sym]))
            h1 = await fetch_required_map(
                sym, sorted(need_h1_lr_spans[sym]), ema_lr_1h, "h1_log_range"
            )
            vol = await fetch_map(sym, m1_volume_spans, ema_qv, "m1_volume")
            lr1m = await fetch_map(sym, m1_lr_spans, ema_lr_1m, "m1_log_range")
            return close, vol, lr1m, h1

        # Ordering: symbols with open positions first (they need EMA data
        # for correct order calculation), remaining symbols shuffled to
        # avoid alphabetic starvation.
        symbols_with_pos = [s for s in symbols if self.has_position(symbol=s)]
        symbols_without_pos = [s for s in symbols if s not in symbols_with_pos]
        random.shuffle(symbols_without_pos)
        ordered_symbols = symbols_with_pos + symbols_without_pos

        get_fetch_delay_seconds = getattr(self, "_get_fetch_delay_seconds", None)
        if callable(get_fetch_delay_seconds):
            fetch_delay_s = float(get_fetch_delay_seconds())
        elif hasattr(self, "config") or hasattr(self, "exchange"):
            fetch_delay_s = float(Passivbot._get_fetch_delay_seconds(self))
        else:
            fetch_delay_s = 0.0
        if fetch_delay_s > 0:
            # Strict exchanges benefit from pacing expensive 1h refreshes when
            # all symbol TTLs expire at the same hour boundary.
            symbol_results = []
            for sym in ordered_symbols:
                try:
                    res = await load_symbol_bundle(sym)
                except Exception as e:
                    res = e
                symbol_results.append(res)
                await asyncio.sleep(fetch_delay_s)
        else:
            symbol_tasks = [asyncio.create_task(load_symbol_bundle(sym)) for sym in ordered_symbols]
            symbol_results = []
            for task in symbol_tasks:
                try:
                    symbol_results.append(await task)
                except Exception as e:
                    symbol_results.append(e)

        m1_close_emas: dict[str, dict[float, float]] = {}
        m1_volume_emas: dict[str, dict[float, float]] = {}
        m1_log_range_emas: dict[str, dict[float, float]] = {}
        h1_log_range_emas: dict[str, dict[float, float]] = {}
        errors: list[tuple[str, Exception]] = []
        for sym, res in zip(ordered_symbols, symbol_results):
            if isinstance(res, Exception):
                errors.append((sym, res))
                continue
            close, vol, lr1m, h1 = res
            m1_close_emas[sym] = close
            m1_volume_emas[sym] = vol
            m1_log_range_emas[sym] = lr1m
            h1_log_range_emas[sym] = h1
        if errors:
            for sym, err in errors[1:]:
                logging.debug(
                    "[ema] additional symbol EMA bundle failure %s: %s: %s",
                    sym,
                    type(err).__name__,
                    err,
                )
            raise errors[0][1]

        # Convenience: compute the single-span values used by legacy forager logging.
        volumes_long = {s: m1_volume_emas[s].get(vol_span_long, 0.0) for s in symbols}
        log_ranges_long = {s: m1_log_range_emas[s].get(lr_span_long, 0.0) for s in symbols}

        return (
            m1_close_emas,
            m1_volume_emas,
            m1_log_range_emas,
            h1_log_range_emas,
            volumes_long,
            log_ranges_long,
        )

    async def calc_ideal_orders_orchestrator(self, *, return_snapshot: bool = False):
        """Compute desired orders using Rust orchestrator (JSON API)."""
        symbols = sorted(set(getattr(self, "active_symbols", []) or self._build_live_symbol_universe()))
        if not symbols:
            return ({}, None) if return_snapshot else {}
        mode_overrides = self._build_orchestrator_mode_overrides(symbols)

        # Get latest prices: prefer bulk allMids (1 API call for all symbols)
        # over per-symbol get_current_close (N API calls). Falls back to CM if unavailable.
        last_prices = {}
        try:
            if (
                hasattr(self, "cca")
                and self.cca is not None
                and self.exchange
                and self.exchange.lower() == "hyperliquid"
            ):
                # Call allMids directly – much cheaper than fetch_tickers which tries
                # to map ALL coins (including unmapped HIP-3 @NNN IDs → warning spam).
                fetched = await self.cca.fetch(
                    self._hl_info_url(),
                    method="POST",
                    headers={"Content-Type": "application/json"},
                    body=json.dumps({"type": "allMids"}),
                )
                # Build reverse map: coin_name → symbol (e.g. "BTC" → "BTC/USDC:USDC")
                coin_to_sym = {v: k for k, v in self.symbol_ids.items()} if self.symbol_ids else {}
                for coin, mid_str in fetched.items():
                    sym = coin_to_sym.get(coin)
                    if sym and sym in symbols:
                        try:
                            last_prices[sym] = float(mid_str)
                        except (ValueError, TypeError):
                            pass
            elif hasattr(self, "fetch_tickers"):
                tickers = await self.fetch_tickers()
                for sym in symbols:
                    tick = tickers.get(sym)
                    if tick and tick.get("last") is not None:
                        last_prices[sym] = float(tick["last"])
            # Feed prices into CM cache so downstream EMA/close lookups hit cache
            if last_prices:
                now_ms = int(utc_ms())
                for sym, price in last_prices.items():
                    self.cm.set_current_close(sym, price, now_ms)
        except Exception as e:
            logging.debug("bulk price fetch failed, falling back to CM: %s", e)
            last_prices = {}
        # Fill any symbols still missing via CandlestickManager (individual fetches)
        missing = [s for s in symbols if s not in last_prices or last_prices[s] <= 0.0]
        if missing:
            cm_prices = await self.cm.get_last_prices(missing, max_age_ms=10_000)
            last_prices.update(cm_prices)
        Passivbot._monitor_record_price_ticks(self, last_prices, ts=utc_ms(), source="orchestrator_live")

        # Ensure effective min cost is up to date.
        if not hasattr(self, "effective_min_cost") or not self.effective_min_cost:
            await self.update_effective_min_cost()

        (
            m1_close_emas,
            m1_volume_emas,
            m1_log_range_emas,
            h1_log_range_emas,
            _volumes_long,
            _log_ranges_long,
        ) = await self._load_orchestrator_ema_bundle(symbols, mode_overrides)

        unstuck_allowances = self._calc_unstuck_allowances_live(
            allow_new_unstuck=not self.has_open_unstuck_order()
        )
        try:
            realized_pnl_cumsum = self._get_realized_pnl_cumsum_stats()
        except RuntimeError:
            realized_pnl_cumsum = {"max": 0.0, "last": 0.0}
        max_realized_loss_pct = float(self.live_value("max_realized_loss_pct") or 1.0)

        global_bp = {
            "long": self._bot_params_to_rust_dict("long", None),
            "short": self._bot_params_to_rust_dict("short", None),
        }
        # Effective hedge_mode = config setting AND exchange capability.
        # If either is False, we block same-coin hedging in the orchestrator.
        effective_hedge_mode = self._config_hedge_mode and self.hedge_mode
        input_dict = {
            "balance": self.get_hysteresis_snapped_balance(),
            "balance_raw": self.get_raw_balance(),
            "global": {
                "filter_by_min_effective_cost": bool(self.live_value("filter_by_min_effective_cost")),
                "market_orders_allowed": bool(self.live_value("market_orders_allowed")),
                "market_order_near_touch_threshold": float(
                    self.live_value("market_order_near_touch_threshold")
                ),
                "panic_close_market": bool(
                    any(
                        Passivbot._equity_hard_stop_panic_close_order_type(self, pside) == "market"
                        for pside in ("long", "short")
                        if Passivbot._equity_hard_stop_enabled(self, pside)
                    )
                ),
                "unstuck_allowance_long": float(unstuck_allowances.get("long", 0.0)),
                "unstuck_allowance_short": float(unstuck_allowances.get("short", 0.0)),
                "max_realized_loss_pct": max_realized_loss_pct,
                "realized_pnl_cumsum_max": float(realized_pnl_cumsum.get("max", 0.0) or 0.0),
                "realized_pnl_cumsum_last": float(realized_pnl_cumsum.get("last", 0.0) or 0.0),
                "sort_global": True,
                "global_bot_params": global_bp,
                "hedge_mode": effective_hedge_mode,
            },
            "symbols": [],
            "peek_hints": None,
        }

        symbol_to_idx: dict[str, int] = {s: i for i, s in enumerate(symbols)}
        idx_to_symbol: dict[int, str] = {i: s for s, i in symbol_to_idx.items()}

        for symbol in symbols:
            idx = symbol_to_idx[symbol]
            mprice = float(last_prices.get(symbol, 0.0))
            if not math.isfinite(mprice) or mprice <= 0.0:
                raise Exception(f"invalid market price for {symbol}: {mprice}")

            active = bool(self.markets_dict.get(symbol, {}).get("active", True))
            effective_min_cost = float(self.effective_min_cost.get(symbol, 0.0) or 0.0)
            if effective_min_cost <= 0.0:
                effective_min_cost = self._calc_effective_min_cost_at_price(symbol, mprice)

            def side_input(pside: str) -> dict:
                mode = self._mode_override_to_orchestrator_mode(mode_overrides[pside].get(symbol))
                pos = self.positions.get(symbol, {}).get(pside, {"size": 0.0, "price": 0.0})
                trailing = self.trailing_prices.get(symbol, {}).get(pside)
                if not trailing:
                    trailing = _trailing_bundle_default_dict()
                else:
                    trailing = dict(trailing)
                return {
                    "mode": mode,
                    "position": {"size": float(pos["size"]), "price": float(pos["price"])},
                    "trailing": {
                        "min_since_open": float(trailing.get("min_since_open", 0.0)),
                        "max_since_min": float(trailing.get("max_since_min", 0.0)),
                        "max_since_open": float(trailing.get("max_since_open", 0.0)),
                        "min_since_max": float(trailing.get("min_since_max", 0.0)),
                    },
                    "bot_params": self._bot_params_to_rust_dict(pside, symbol),
                }

            # Build EMA bundle for this symbol.
            m1_close_pairs = [[float(k), float(v)] for k, v in sorted(m1_close_emas[symbol].items())]
            m1_volume_pairs = [
                [float(k), float(v)] for k, v in sorted(m1_volume_emas[symbol].items())
            ]
            m1_lr_pairs = [[float(k), float(v)] for k, v in sorted(m1_log_range_emas[symbol].items())]
            h1_lr_pairs = [[float(k), float(v)] for k, v in sorted(h1_log_range_emas[symbol].items())]

            input_dict["symbols"].append(
                {
                    "symbol_idx": int(idx),
                    "order_book": {"bid": mprice, "ask": mprice},
                    "exchange": {
                        "qty_step": float(self.qty_steps[symbol]),
                        "price_step": float(self.price_steps[symbol]),
                        "min_qty": float(self.min_qtys[symbol]),
                        "min_cost": float(self.min_costs[symbol]),
                        "c_mult": float(self.c_mults[symbol]),
                        "maker_fee": float(
                            self.markets_dict.get(symbol, {}).get("maker", 0.0) or 0.0
                        ),
                        "taker_fee": float(
                            self.markets_dict.get(symbol, {}).get("taker", 0.0) or 0.0
                        ),
                    },
                    "tradable": bool(active),
                    "next_candle": None,
                    "effective_min_cost": float(effective_min_cost),
                    "emas": {
                        "m1": {
                            "close": m1_close_pairs,
                            "log_range": m1_lr_pairs,
                            "volume": m1_volume_pairs,
                        },
                        "h1": {"close": [], "log_range": h1_lr_pairs, "volume": []},
                    },
                    "long": side_input("long"),
                    "short": side_input("short"),
                }
            )

        try:
            out_json = pbr.compute_ideal_orders_json(json.dumps(input_dict))
        except Exception as e:
            msg = str(e)
            if "MissingEma" in msg:
                match = re.search(r"symbol_idx\s*:\s*(\d+)", msg)
                if match:
                    idx = int(match.group(1))
                    symbol = idx_to_symbol.get(idx)
                    if symbol:
                        logging.error("[ema] Missing EMA for %s (symbol_idx=%d)", symbol, idx)
            raise
        out = json.loads(out_json)
        self._log_realized_loss_gate_blocks(out, idx_to_symbol)
        if hasattr(self, "_apply_orchestrator_symbol_states"):
            self._apply_orchestrator_symbol_states(
                out.get("diagnostics", {}),
                idx_to_symbol,
                mode_overrides,
            )
        orders = out.get("orders", [])
        if hasattr(self, "_update_monitor_runtime_hints"):
            self._update_monitor_runtime_hints(
                symbols=symbols,
                last_prices=last_prices,
                m1_close_emas=m1_close_emas,
                h1_log_range_emas=h1_log_range_emas,
                idx_to_symbol=idx_to_symbol,
                orders=orders,
            )

        ideal_orders: dict[str, list] = {}
        for o in orders:
            symbol = idx_to_symbol.get(int(o["symbol_idx"]))
            if symbol is None:
                continue
            order_type = str(o["order_type"])
            order_type_id = int(pbr.order_type_snake_to_id(order_type))
            execution_type = str(o.get("execution_type", "limit"))
            tup = (float(o["qty"]), float(o["price"]), order_type, order_type_id, execution_type)
            ideal_orders.setdefault(symbol, []).append(tup)

        # Log unstuck coin selection
        for o in orders:
            order_type_str = o.get("order_type", "")
            if "close_unstuck" in order_type_str:
                symbol = idx_to_symbol.get(int(o.get("symbol_idx", -1)))
                if symbol:
                    pside = "long" if "long" in order_type_str else "short"
                    pos = self.positions.get(symbol, {}).get(pside, {})
                    entry_price = pos.get("price", 0.0)
                    current_price = last_prices.get(symbol, 0.0)
                    if entry_price > 0 and current_price > 0:
                        price_diff_pct = (current_price / entry_price - 1.0) * 100
                        sign = "+" if price_diff_pct >= 0 else ""
                    else:
                        price_diff_pct = 0.0
                        sign = ""
                    coin = symbol.split("/")[0] if "/" in symbol else symbol
                    allowance = unstuck_allowances.get(pside, 0.0)
                    logging.info(
                        "[unstuck] selecting %s %s | entry=%.2f now=%.2f (%s%.1f%%) | allowance=%.2f",
                        coin,
                        pside,
                        entry_price,
                        current_price,
                        sign,
                        price_diff_pct,
                        allowance,
                    )
                break  # Only one unstuck order per cycle

        # Log EMA gating for symbols in normal mode with no position and no initial entry
        self._log_ema_gating(ideal_orders, m1_close_emas, last_prices, symbols)

        ideal_orders_f, _wel_blocked = self._to_executable_orders(ideal_orders, last_prices)
        ideal_orders_f = self._finalize_reduce_only_orders(ideal_orders_f, last_prices)

        if return_snapshot:
            snapshot = {
                "ts_ms": int(utc_ms()),
                "exchange": str(getattr(self, "exchange", "")),
                "user": str(self.config_get(["live", "user"]) or ""),
                "active_symbols": list(symbols),
                "realized_pnl_cumsum": realized_pnl_cumsum,
                "orchestrator_input": input_dict,
                "orchestrator_output": out,
            }
            return ideal_orders_f, snapshot
        return ideal_orders_f

    def _to_executable_orders(
        self, ideal_orders: dict, last_prices: Dict[str, float]
    ) -> tuple[Dict[str, list], set[str]]:
        """Convert raw order tuples into api-ready dicts and find WEL-restricted symbols."""
        ideal_orders_f: Dict[str, list] = {}
        wel_blocked_symbols: set[str] = set()

        for symbol, orders in ideal_orders.items():
            ideal_orders_f[symbol] = []
            last_mprice = last_prices[symbol]
            seen = set()
            with_mprice_diff = []
            for order in orders:
                side = determine_side_from_order_tuple(order)
                diff = order_market_diff(side, order[1], last_mprice)
                with_mprice_diff.append((diff, order, side))
                if (
                    isinstance(order, tuple)
                    and isinstance(order[2], str)
                    and "close_auto_reduce_wel" in order[2]
                ):
                    wel_blocked_symbols.add(symbol)
            any_partial = any("partial" in order[2] for _, order, _ in with_mprice_diff)
            for mprice_diff, order, order_side in sorted(with_mprice_diff, key=lambda item: item[0]):
                position_side = "long" if "long" in order[2] else "short"
                if order[0] == 0.0:
                    continue
                if mprice_diff > float(self.live_value("price_distance_threshold")):
                    if any_partial and "entry" in order[2]:
                        logging.debug(
                            "gated by price_distance_threshold (partial) | %s %s %s diff=%.5f",
                            symbol,
                            position_side,
                            order[2],
                            mprice_diff,
                        )
                        continue
                    if any(token in order[2] for token in ("initial", "unstuck")):
                        logging.debug(
                            "gated by price_distance_threshold (initial/unstuck) | %s %s %s diff=%.5f",
                            symbol,
                            position_side,
                            order[2],
                            mprice_diff,
                        )
                        continue
                    if not self.has_position(position_side, symbol):
                        logging.debug(
                            "gated by price_distance_threshold (no position) | %s %s %s diff=%.5f",
                            symbol,
                            position_side,
                            order[2],
                            mprice_diff,
                        )
                        continue
                seen_key = str(abs(order[0])) + str(order[1]) + order[2]
                if seen_key in seen:
                    logging.debug("duplicate ideal order for %s skipped: %s", symbol, order)
                    continue
                pb_order_type = snake_of(order[3])
                if len(order) >= 5:
                    execution_type = str(order[4]).lower()
                else:
                    execution_type = "limit"
                    panic_close_pref = self._equity_hard_stop_panic_close_order_type(
                        position_side
                    )
                    if "panic" in pb_order_type or "panic" in str(order[2]).lower():
                        execution_type = "market" if panic_close_pref == "market" else "limit"
                if execution_type not in {"limit", "market"}:
                    execution_type = "limit"
                ideal_orders_f[symbol].append(
                    {
                        "symbol": symbol,
                        "side": order_side,
                        "position_side": position_side,
                        "qty": abs(order[0]),
                        "price": order[1],
                        "reduce_only": "close" in order[2],
                        "custom_id": self.format_custom_id_single(order[3]),
                        "type": execution_type,
                        "pb_order_type": pb_order_type,
                    }
                )
                seen.add(seen_key)
        return self._finalize_reduce_only_orders(ideal_orders_f, last_prices), wel_blocked_symbols

    def _finalize_reduce_only_orders(
        self, orders_by_symbol: Dict[str, list], last_prices: Dict[str, float]
    ) -> Dict[str, list]:
        """Bound reduce-only quantities so they never exceed the current position size (per order and in sum)."""
        for symbol, orders in orders_by_symbol.items():
            market_price = float(last_prices.get(symbol, 0.0))

            # 1) clamp each reduce-only order to position size
            for order in orders:
                if not order.get("reduce_only"):
                    continue
                pos = self.positions.get(order["symbol"], {}).get(order["position_side"], {})
                pos_size_abs = abs(float(pos.get("size", 0.0)))
                if abs(order["qty"]) > pos_size_abs:
                    logging.warning(
                        "trimmed reduce-only qty to position size | order=%s | position=%s",
                        order,
                        pos,
                    )
                    order["qty"] = pos_size_abs

            # 2) cap sum(reduce_only qty) <= pos size by reducing furthest-from-market closes first
            for pside in ("long", "short"):
                pos_size_abs = abs(
                    float(self.positions.get(symbol, {}).get(pside, {}).get("size", 0.0))
                )
                if pos_size_abs <= 0.0:
                    continue
                ro = [o for o in orders if o.get("reduce_only") and o.get("position_side") == pside]
                if not ro:
                    continue
                total = sum(float(o.get("qty", 0.0)) for o in ro)
                if total <= pos_size_abs + 1e-12:
                    continue
                excess = total - pos_size_abs
                # furthest first: larger order_market_diff
                ro_sorted = sorted(
                    ro,
                    key=lambda o: order_market_diff(
                        o.get("side", ""), float(o.get("price", 0.0)), market_price
                    ),
                    reverse=True,
                )
                for o in ro_sorted:
                    if excess <= 0.0:
                        break
                    q = float(o.get("qty", 0.0))
                    if q <= 0.0:
                        continue
                    reduce_by = min(q, excess)
                    new_q = q - reduce_by
                    o["qty"] = float(round(new_q, 12))
                    excess -= reduce_by
                # drop any zeroed reduce-only orders
                orders_by_symbol[symbol] = [
                    o
                    for o in orders_by_symbol[symbol]
                    if not (o.get("reduce_only") and float(o.get("qty", 0.0)) <= 0.0)
                ]

        return orders_by_symbol

    async def calc_orders_to_cancel_and_create(self):
        """Determine which existing orders to cancel and which new ones to place."""
        if not hasattr(self, "_last_plan_detail"):
            self._last_plan_detail = {}
        ideal_orders = await self.calc_ideal_orders()

        actual_orders = self._snapshot_actual_orders()
        keys = ("symbol", "side", "position_side", "qty", "price")
        to_cancel, to_create = [], []
        plan_summaries = []
        for symbol, symbol_orders in actual_orders.items():
            ideal_list = ideal_orders.get(symbol, []) if isinstance(ideal_orders, dict) else []
            cancel_, create_ = self._reconcile_symbol_orders(symbol, symbol_orders, ideal_list, keys)
            cancel_, create_ = self._annotate_order_deltas(cancel_, create_)
            pre_cancel = len(cancel_)
            pre_create = len(create_)
            cancel_, create_, skipped = self._apply_order_match_tolerance(cancel_, create_)
            plan_summaries.append(
                (symbol, pre_cancel, len(cancel_), pre_create, len(create_), skipped)
            )
            to_cancel += cancel_
            to_create += create_

        to_cancel = await self._sort_orders_by_market_diff(to_cancel, "to_cancel")
        to_create = await self._sort_orders_by_market_diff(to_create, "to_create")
        if plan_summaries:
            total_pre_cancel = sum(p[1] for p in plan_summaries)
            total_cancel = sum(p[2] for p in plan_summaries)
            total_pre_create = sum(p[3] for p in plan_summaries)
            total_create = sum(p[4] for p in plan_summaries)
            total_skipped = sum(p[5] for p in plan_summaries)
            detail_parts = []
            untouched_cancel = total_pre_cancel - total_cancel
            untouched_create = total_pre_create - total_create
            for symbol, pre_c, c, pre_cr, cr, skipped in plan_summaries:
                prev = self._last_plan_detail.get(symbol)
                current = (c, cr, skipped)
                self._last_plan_detail[symbol] = current
                if c or cr or skipped:
                    if prev != current:
                        detail_parts.append(f"{symbol}:c{pre_c}->{c} cr{pre_cr}->{cr} skip{skipped}")
            detail = " | ".join(detail_parts[:6])
            summary_key = (
                total_pre_cancel,
                total_cancel,
                total_pre_create,
                total_create,
                total_skipped,
                untouched_cancel,
                untouched_create,
                detail,
            )
            if summary_key != getattr(self, "_last_order_plan_summary", None):
                self._last_order_plan_summary = summary_key
                if total_cancel or total_create or total_skipped:
                    extra = []
                    if untouched_cancel:
                        extra.append(f"unchanged_cancel={untouched_cancel}")
                    if untouched_create:
                        extra.append(f"unchanged_create={untouched_create}")
                    # Use DEBUG when no actual work was done (all orders skipped/unchanged)
                    log_level = logging.INFO if (total_cancel or total_create) else logging.DEBUG
                    logging.log(
                        log_level,
                        "[order] order plan summary | cancel %d->%d | create %d->%d | skipped=%d%s%s",
                        total_pre_cancel,
                        total_cancel,
                        total_pre_create,
                        total_create,
                        total_skipped,
                        f" | {' '.join(extra)}" if extra else "",
                        f" | details: {detail}" if detail else "",
                    )
        return to_cancel, to_create

    def _snapshot_actual_orders(self) -> dict[str, list[dict]]:
        """Return a normalized snapshot of currently open orders keyed by symbol."""
        actual_orders: dict[str, list[dict]] = {}
        for symbol in self.active_symbols:
            symbol_orders = []
            for idx, order in enumerate(self.open_orders.get(symbol, [])):
                try:
                    order_symbol = order["symbol"]
                    side = order["side"]
                    if side not in {"buy", "sell"}:
                        raise ValueError(f"invalid side {side!r}")
                    position_side = order["position_side"]
                    if position_side not in {"long", "short"}:
                        raise ValueError(f"invalid position_side {position_side!r}")
                    qty_raw = order["qty"]
                    if isinstance(qty_raw, bool):
                        raise TypeError(f"invalid boolean qty {qty_raw!r}")
                    qty = float(qty_raw)
                    if not math.isfinite(qty) or qty <= 0.0:
                        raise ValueError(f"invalid qty {qty_raw!r}")
                    price_raw = order["price"]
                    if isinstance(price_raw, bool):
                        raise TypeError(f"invalid boolean price {price_raw!r}")
                    price = float(price_raw)
                    if not math.isfinite(price) or price <= 0.0:
                        raise ValueError(f"invalid price {price_raw!r}")
                    symbol_orders.append(
                        {
                            "symbol": order_symbol,
                            "side": side,
                            "position_side": position_side,
                            "qty": qty,
                            "price": price,
                            "reduce_only": (position_side == "long" and side == "sell")
                            or (position_side == "short" and side == "buy"),
                            "id": order.get("id"),
                            "custom_id": order.get("custom_id"),
                        }
                    )
                except Exception as exc:
                    raise RuntimeError(
                        f"Malformed open order for {symbol} at index {idx}: {order!r}"
                    ) from exc
            actual_orders[symbol] = symbol_orders
        return actual_orders

    def _reconcile_symbol_orders(
        self,
        symbol: str,
        actual_orders: list[dict],
        ideal_orders: list,
        keys: tuple[str, ...],
    ) -> tuple[list[dict], list[dict]]:
        """Return cancel/create lists for a single symbol after mode filtering."""
        to_cancel, to_create = filter_orders(actual_orders, ideal_orders, keys)
        to_cancel, to_create = self._apply_mode_filters(symbol, to_cancel, to_create)
        return to_cancel, to_create

    def _annotate_order_deltas(
        self, to_cancel: list[dict], to_create: list[dict]
    ) -> tuple[list[dict], list[dict]]:
        """
        Attach best-effort delta info between existing and desired orders to aid logging.

        Matches orders by symbol/side/position_side and closest price distance.
        """
        remaining_create = list(to_create)
        for order in to_create:
            order.setdefault("_context", "new")
            order.setdefault("_reason", "new")
        for cancel_order in to_cancel:
            cancel_order.setdefault("_context", "retire")
            cancel_order.setdefault("_reason", "retire")

        def pct(a: float, b: float) -> float:
            if a == 0 and b == 0:
                return 0.0
            if a == 0:
                return float("inf")
            return abs(b - a) / abs(a) * 100.0

        def coerce_delta_numeric(order: dict, field: str):
            value = order.get(field)
            if isinstance(value, bool):
                raise RuntimeError(
                    f"Invalid {field} in order delta annotation: {order!r}"
                )
            try:
                numeric = float(value)
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"Invalid {field} in order delta annotation: {order!r}"
                ) from exc
            if not math.isfinite(numeric) or numeric <= 0.0:
                raise RuntimeError(
                    f"Invalid {field} in order delta annotation: {order!r}"
                )
            return numeric

        # annotate cancellations
        for cancel_order in to_cancel:
            candidates = [
                (idx, co)
                for idx, co in enumerate(remaining_create)
                if co.get("symbol") == cancel_order.get("symbol")
                and co.get("side") == cancel_order.get("side")
                and co.get("position_side") == cancel_order.get("position_side")
            ]
            if not candidates:
                continue
            # choose closest by price difference
            best_idx, best_order = min(
                candidates,
                key=lambda c: abs(
                    coerce_delta_numeric(c[1], "price") - coerce_delta_numeric(cancel_order, "price")
                ),
            )
            raw_price_diff = pct(
                coerce_delta_numeric(cancel_order, "price"),
                coerce_delta_numeric(best_order, "price"),
            )
            raw_qty_diff = pct(
                coerce_delta_numeric(cancel_order, "qty"),
                coerce_delta_numeric(best_order, "qty"),
            )
            price_diff = round(raw_price_diff, 4) if math.isfinite(raw_price_diff) else raw_price_diff
            qty_diff = round(raw_qty_diff, 4) if math.isfinite(raw_qty_diff) else raw_qty_diff
            reason_parts = []
            if price_diff > 0:
                reason_parts.append("price")
            if qty_diff > 0:
                reason_parts.append("qty")
            reason = "+".join(reason_parts) if reason_parts else "adjustment"
            cancel_order["_delta"] = {
                "price_old": cancel_order.get("price"),
                "price_new": best_order.get("price"),
                "price_pct_diff": price_diff,
                "qty_old": cancel_order.get("qty"),
                "qty_new": best_order.get("qty"),
                "qty_pct_diff": qty_diff,
            }
            cancel_order["_context"] = "replace"
            cancel_order["_reason"] = reason
            # also annotate the matched create order
            best_order["_delta"] = {
                "price_old": cancel_order.get("price"),
                "price_new": best_order.get("price"),
                "price_pct_diff": price_diff,
                "qty_old": cancel_order.get("qty"),
                "qty_new": best_order.get("qty"),
                "qty_pct_diff": qty_diff,
            }
            best_order["_context"] = "replace"
            best_order["_reason"] = reason
            remaining_create.pop(best_idx)

        for ord in remaining_create:
            ord.setdefault("_context", "new")
            ord.setdefault("_reason", "fresh")
        return to_cancel, to_create

    def _apply_order_match_tolerance(
        self, to_cancel: list[dict], to_create: list[dict]
    ) -> tuple[list[dict], list[dict], int]:
        """Drop cancel/create pairs that are within tolerance to avoid churn.

        Returns (remaining_cancel, remaining_create, skipped_pairs)
        """
        tolerance = float(self.live_value("order_match_tolerance_pct"))
        if tolerance <= 0.0:
            return to_cancel, to_create, 0

        used_cancel: set[int] = set()
        kept_create: list[dict] = []
        skipped = 0

        def pct_diff(a: float, b: float) -> float:
            if b == 0:
                return 0.0 if a == 0 else float("inf")
            return abs(a - b) / abs(b) * 100.0

        for order in to_create:
            match_idx = None
            for idx, existing in enumerate(to_cancel):
                if idx in used_cancel:
                    continue
                try:
                    if orders_matching(
                        order,
                        existing,
                        tolerance_qty=tolerance,
                        tolerance_price=tolerance,
                    ):
                        match_idx = idx
                        break
                except Exception as exc:
                    raise RuntimeError(
                        f"Failed to compare candidate orders for tolerance matching on {symbol}: create={order!r} cancel={existing!r}"
                    ) from exc
            if match_idx is None:
                kept_create.append(order)
            else:
                used_cancel.add(match_idx)
                skipped += 1
                try:
                    price_diff = pct_diff(float(order["price"]), float(to_cancel[match_idx]["price"]))
                    qty_diff = pct_diff(float(order["qty"]), float(to_cancel[match_idx]["qty"]))
                    logging.debug(
                        "skipped_recreate | %s | tolerance=%.4f%% price_diff=%.4f%% qty_diff=%.4f%%",
                        order.get("symbol", "?"),
                        tolerance * 100.0,
                        price_diff,
                        qty_diff,
                    )
                except Exception:
                    logging.debug(
                        "skipped_recreate | %s | tolerance=%.4f%%",
                        order.get("symbol", "?"),
                        tolerance * 100.0,
                    )

        remaining_cancel = [o for i, o in enumerate(to_cancel) if i not in used_cancel]
        return remaining_cancel, kept_create, skipped

    def _apply_mode_filters(
        self,
        symbol: str,
        to_cancel: list[dict],
        to_create: list[dict],
    ) -> tuple[list[dict], list[dict]]:
        """Apply mode-specific cancel/create filtering rules."""
        for pside in ["long", "short"]:
            mode = self.PB_modes[pside].get(symbol)
            if mode == "manual":
                to_cancel = [x for x in to_cancel if x["position_side"] != pside]
                to_create = [x for x in to_create if x["position_side"] != pside]
            elif mode == "tp_only":
                to_cancel = [
                    x
                    for x in to_cancel
                    if (
                        x["position_side"] != pside
                        or (x["position_side"] == pside and x["reduce_only"])
                    )
                ]
                to_create = [
                    x
                    for x in to_create
                    if (
                        x["position_side"] != pside
                        or (x["position_side"] == pside and x["reduce_only"])
                    )
                ]
            elif mode == "tp_only_with_active_entry_cancellation":
                # Keep active close-order management and entry-order cancellation.
                # Entries are never created, but existing entry orders are allowed in to_cancel.
                to_create = [
                    x
                    for x in to_create
                    if (
                        x["position_side"] != pside
                        or (x["position_side"] == pside and x["reduce_only"])
                    )
                ]
        return to_cancel, to_create

    async def _sort_orders_by_market_diff(self, orders: list[dict], log_label: str) -> list[dict]:
        """Return orders sorted by market diff, fetching prices concurrently."""
        if not orders:
            return []
        market_prices = await self._fetch_market_prices({order["symbol"] for order in orders})
        entries = []
        for order in orders:
            market_price = market_prices[order["symbol"]]
            diff = order_market_diff(order["side"], order["price"], market_price)
            entries.append((diff, order))
        entries.sort(key=lambda item: item[0])
        return [order for _, order in entries]

    def _coerce_required_market_price(self, symbol: str, price) -> float:
        if isinstance(price, bool):
            raise TypeError(f"invalid boolean market price for {symbol}: {price!r}")
        try:
            numeric = float(price)
        except (TypeError, ValueError) as exc:
            raise TypeError(f"invalid market price for {symbol}: {price!r}") from exc
        if not math.isfinite(numeric) or numeric <= 0.0:
            raise ValueError(f"non-positive market price for {symbol}: {numeric}")
        return numeric

    async def _fetch_market_prices(self, symbols: set[str]) -> dict[str, float]:
        """Fetch current close prices for the supplied symbols."""
        results: dict[str, float] = {}
        tasks: dict[str, asyncio.Task] = {}
        errors: dict[str, str] = {}
        for symbol in symbols:
            try:
                fetch_result = self.cm.get_current_close(symbol, max_age_ms=10_000)
                if inspect.isawaitable(fetch_result):
                    tasks[symbol] = asyncio.create_task(fetch_result)
                else:
                    results[symbol] = self._coerce_required_market_price(symbol, fetch_result)
            except Exception as exc:
                errors[symbol] = str(exc)
        for symbol, task in tasks.items():
            try:
                results[symbol] = self._coerce_required_market_price(symbol, await task)
            except Exception as exc:
                errors[symbol] = str(exc)
        if errors:
            details = "; ".join(f"{symbol}: {msg}" for symbol, msg in sorted(errors.items()))
            raise RuntimeError(f"failed fetching market prices for order sorting: {details}")
        return results

    async def restart_bot_on_too_many_errors(self):
        """Restart the bot if the hourly execution error budget is exhausted."""
        if not hasattr(self, "error_counts"):
            self.error_counts = []
        now = utc_ms()
        self.error_counts = [x for x in self.error_counts if x > now - 1000 * 60 * 60] + [now]
        max_n_errors_per_hour = 10
        logging.info(
            f"error count: {len(self.error_counts)} of {max_n_errors_per_hour} errors per hour"
        )
        if len(self.error_counts) >= max_n_errors_per_hour:
            await self.restart_bot()
            raise Exception("too many errors... restarting bot.")

    def format_custom_id_single(self, order_type_id: int) -> str:
        """Build a custom id embedding the order type marker and a UUID suffix."""
        token = type_token(order_type_id, with_marker=True)  # "0xABCD"
        return (token + uuid4().hex)[: self.custom_id_max_length]

    def debug_dump_bot_state_to_disk(self):
        """Persist internal state snapshots to disk for debugging purposes."""
        if not hasattr(self, "tmp_debug_ts"):
            self.tmp_debug_ts = 0
            self.tmp_debug_cache = make_get_filepath(f"caches/{self.exchange}/{self.user}_debug/")
        if utc_ms() - self.tmp_debug_ts > 1000 * 60 * 3:
            logging.info(f"debug dumping bot state to disk")
            for k, v in vars(self).items():
                try:
                    json.dump(
                        denumpyize(v), open(os.path.join(self.tmp_debug_cache, k + ".json"), "w")
                    )
                except Exception as e:
                    logging.error(f"debug failed to dump to disk {k} {e}")
            self.tmp_debug_ts = utc_ms()

    # Legacy EMA maintenance (init_EMAs_single/update_EMAs) removed in favor of CandlestickManager

    def get_symbols_with_pos(self, pside=None):
        """Return the set of symbols with open positions for the given side."""
        if pside is None:
            return self.get_symbols_with_pos("long") | self.get_symbols_with_pos("short")
        return set([s for s in self.positions if self.positions[s][pside]["size"] != 0.0])

    def get_symbols_approved_or_has_pos(self, pside=None) -> set:
        """Return symbols that are approved for trading or currently have a position."""
        if pside is None:
            return self.get_symbols_approved_or_has_pos(
                "long"
            ) | self.get_symbols_approved_or_has_pos("short")
        return (
            self.approved_coins_minus_ignored_coins[pside]
            | self.get_symbols_with_pos(pside)
            | {s for s in self.coin_overrides if self.get_forced_PB_mode(pside, s) == "normal"}
        )

    # Legacy get_ohlcvs_1m_file_mods removed

    async def restart_bot(self):
        """Stop all tasks and raise to trigger an external bot restart."""
        logging.info("Initiating bot restart...")
        # Note: Do NOT set stop_signal_received=True here - that would cause
        # the main loop to exit instead of restart. The flag is only for
        # user-initiated stops (SIGINT/SIGTERM).
        self.stop_data_maintainers()
        await self.cca.close()
        if self.ccp is not None:
            await self.ccp.close()
        raise RestartBotException("Bot will restart.")

    async def _refresh_forager_candidate_candles(self) -> None:
        """Best-effort refresh for forager candidate symbols to avoid large bursts."""
        if not self.is_forager_mode():
            return
        max_calls = get_optional_live_value(self.config, "max_ohlcv_fetches_per_minute", 0)
        try:
            max_calls = int(max_calls) if max_calls is not None else 0
        except Exception:
            max_calls = 0

        candidates_by_side: Dict[str, set] = {}
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
                # Respect rate limit even with open slots; use token bucket budget.
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

        # Skip actives; they are refreshed in update_ohlcvs_1m_for_actives
        active = set(self.active_symbols) if hasattr(self, "active_symbols") else set()
        candidates = sorted(all_candidates - active)
        if not candidates:
            return

        if slots_open_any:
            rate_limit_age_ms = self._forager_target_staleness_ms(len(all_candidates), max_calls)
            # Respect rate limit even with open slots; floor at 60s for responsiveness.
            target_age_ms = max(60_000, rate_limit_age_ms) if max_calls > 0 else 60_000
        else:
            target_age_ms = self._forager_target_staleness_ms(len(all_candidates), max_calls)
        now = utc_ms()
        stale: List[Tuple[float, str]] = []
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

        # Throttled visibility into forager refresh behavior (debug only).
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
            max_warmup_minutes = int(
                get_optional_live_value(self.config, "max_warmup_minutes", 0) or 0
            )
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
                win = (
                    max(default_win, int(math.ceil(max_span * span_buffer)))
                    if max_span > 0.0
                    else default_win
                )
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
        """Ensure active symbols have fresh 1m candles in CandlestickManager (<=60s old).

        Uses CandlestickManager.get_candles with max_age_ms=60_000 so it refreshes
        only when its internal last refresh is older than the TTL. Fetches a small
        recent window ending at the latest finalized minute.
        """
        # 1m candles only finalize once per minute; refreshing more often wastes API budget.
        # Use 60s TTL so each symbol is fetched at most once per minute.
        max_age_ms = 60_000
        try:
            now = utc_ms()
            end_ts = (now // ONE_MIN_MS) * ONE_MIN_MS - ONE_MIN_MS
            # Use manager default window if available, otherwise a reasonable fallback
            try:
                window = int(getattr(self.cm, "default_window_candles", 120))
            except Exception:
                window = 120
            start_ts = end_ts - ONE_MIN_MS * window

            fetch_delay_s = self._get_fetch_delay_seconds()

            symbols = sorted(set(self.active_symbols))
            # Prioritize symbols with open positions (need fresh candles for
            # correct order calculation), shuffle the rest to avoid alphabetic
            # starvation when a 429 forces cache-only for late symbols.
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
                # If a 429 triggered a global backoff in the CandlestickManager,
                # stop the loop early; remaining symbols would all hit the same
                # backoff.  They will be picked up on the next cycle; the
                # position-first + shuffle ordering prevents systematic starvation.
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
            # Best-effort refresh for forager candidates (lazy & budgeted)
            await self._refresh_forager_candidate_candles()
        except Exception as e:
            logging.error(f"error with {get_function_name()} {e}")
            traceback.print_exc()

    async def maintain_hourly_cycle(self):
        """Periodically refresh market metadata while the bot is running."""
        # Random jitter (0–120s) so multiple bots on the same VPS don't fire
        # init_markets simultaneously and blow through IP-based rate limits.
        jitter_s = random.uniform(0, 120)
        logging.info("[hourly] starting maintenance cycle (jitter=%.1fs)", jitter_s)
        while not self.stop_signal_received:
            try:
                now = utc_ms()
                mem_prev = getattr(self, "_mem_log_prev", None)
                last_mem_log_ts = None
                if isinstance(mem_prev, dict):
                    last_mem_log_ts = mem_prev.get("timestamp")
                interval = getattr(self, "memory_snapshot_interval_ms", 3_600_000)
                if last_mem_log_ts is None or now - last_mem_log_ts >= interval:
                    self._log_memory_snapshot(now_ms=now)
                candle_check_interval = int(getattr(self, "candle_disk_check_interval_ms", 0) or 0)
                last_candle_check = int(getattr(self, "_candle_disk_check_last_ms", 0) or 0)
                boot_delay_ms = int(getattr(self, "candle_disk_check_boot_delay_ms", 300_000) or 0)
                boot_elapsed = int(now - getattr(self, "start_time_ms", now))
                if (
                    candle_check_interval > 0
                    and boot_elapsed >= boot_delay_ms
                    and (last_candle_check == 0 or now - last_candle_check >= candle_check_interval)
                ):
                    self._candle_disk_check_last_ms = now
                    try:
                        await self.audit_required_candle_disk_coverage()
                    except Exception as exc:
                        logging.error(
                            "error running candle disk coverage audit: %s", exc, exc_info=True
                        )
                # update markets dict once every hour, with per-instance jitter
                hourly_interval_ms = 1000 * 60 * 60 + int(jitter_s * 1000)
                if now - self.init_markets_last_update_ms > hourly_interval_ms:
                    try:
                        await self.init_markets(verbose=False)
                    except RateLimitExceeded:
                        self._health_rate_limits += 1
                        logging.warning(
                            "[rate] hourly init_markets hit rate limit; will retry next cycle"
                        )
                        await asyncio.sleep(10)
                await asyncio.sleep(1)
            except Exception as e:
                logging.error(f"error with {get_function_name()} {e}")
                traceback.print_exc()
                await self.restart_bot_on_too_many_errors()
                await asyncio.sleep(5)

    async def start_data_maintainers(self):
        """Spawn background tasks responsible for market metadata and order watching."""
        if hasattr(self, "maintainers"):
            self.stop_data_maintainers()
        maintainer_names = ["maintain_hourly_cycle"]
        if self.ws_enabled:
            maintainer_names.append("watch_orders")
        else:
            logging.info("Websocket maintainers skipped (ws disabled via custom endpoints).")
        self.maintainers = {
            name: asyncio.create_task(getattr(self, name)()) for name in maintainer_names
        }

    # Legacy websocket 1m ohlcv watchers removed; CandlestickManager is authoritative

    async def calc_log_range(
        self,
        pside: str,
        eligible_symbols: Optional[Iterable[str]] = None,
        *,
        max_age_ms: Optional[int] = 60_000,
        max_network_fetches: Optional[int] = None,
    ) -> Dict[str, float]:
        """Compute 1m EMA of log range per symbol: EMA(ln(high/low)).

        Returns mapping symbol -> ema_log_range; non-finite/failed computations yield 0.0.

        If *max_network_fetches* is set, at most that many symbols will be allowed to
        trigger a network fetch; the rest use cached data only.
        """
        if eligible_symbols is None:
            eligible_symbols = self.eligible_symbols
        span = int(round(self.bot_value(pside, "forager_volatility_ema_span")))
        try:
            warmup_ratio = float(get_optional_live_value(self.config, "warmup_ratio", 0.0))
        except Exception:
            warmup_ratio = 0.0
        try:
            max_warmup_minutes = int(
                get_optional_live_value(self.config, "max_warmup_minutes", 0) or 0
            )
        except Exception:
            max_warmup_minutes = 0
        span_buffer = 1.0 + max(0.0, warmup_ratio)
        window_candles = max(1, int(math.ceil(span * span_buffer))) if span > 0 else 1
        if max_warmup_minutes > 0:
            window_candles = min(int(window_candles), int(max_warmup_minutes))

        syms = list(eligible_symbols)

        per_sym_ttl, cache_only_never_fetched = self._compute_fetch_budget_ttls(
            syms, max_age_ms, max_network_fetches
        )

        # Compute EMA of log range on 1m candles: ln(high/low)
        async def one(symbol: str):
            try:
                if symbol in cache_only_never_fetched:
                    return 0.0
                ttl = per_sym_ttl.get(symbol)
                if ttl is None or ttl == 0:
                    # If caller passes a TTL, use it; otherwise select per-symbol TTL
                    if max_age_ms is not None:
                        ttl = int(max_age_ms)
                    else:
                        # More generous TTL for non-traded symbols
                        has_pos = self.has_position(symbol)
                        has_oo = (
                            bool(self.open_orders.get(symbol)) if hasattr(self, "open_orders") else False
                        )
                        ttl = (
                            60_000
                            if (has_pos or has_oo)
                            else int(getattr(self, "inactive_coin_candle_ttl_ms", 600_000))
                        )
                res = await self.cm.get_latest_ema_metrics(
                    symbol,
                    {"log_range": span},
                    max_age_ms=ttl,
                    window_candles=window_candles,
                    timeframe=None,
                )
                val = float(res.get("log_range", float("nan")))
                return float(val) if np.isfinite(val) else 0.0
            except Exception:
                return 0.0

        tasks = {s: asyncio.create_task(one(s)) for s in syms}
        out = {}
        n = len(syms)
        started_ms = utc_ms()
        for sym, task in tasks.items():
            try:
                out[sym] = await task
            except Exception:
                out[sym] = 0.0
        elapsed_s = max(0.001, (utc_ms() - started_ms) / 1000.0)
        now_ms = utc_ms()
        ema_log_throttle_ms = 300_000  # 5 minutes between logs per metric
        if out:
            top_n = min(8, len(out))
            top = sorted(out.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
            top_syms = tuple(sym for sym, _ in top)
            # Only log when the ranking changes (membership/order) to reduce noise.
            # Also throttle to at most once per 5 minutes per metric.
            if not hasattr(self, "_log_range_top_cache"):
                self._log_range_top_cache = {}
            if not hasattr(self, "_log_range_top_last_log_ms"):
                self._log_range_top_last_log_ms = {}
            cache_key = (pside, span)
            last_top = self._log_range_top_cache.get(cache_key)
            last_log_ms = self._log_range_top_last_log_ms.get(cache_key, 0)
            if last_top != top_syms and (now_ms - last_log_ms) >= ema_log_throttle_ms:
                self._log_range_top_cache[cache_key] = top_syms
                self._log_range_top_last_log_ms[cache_key] = now_ms
                summary = ", ".join(f"{symbol_to_coin(sym)}={val:.6f}" for sym, val in top)
                logging.info(
                    f"[ranking] log_range EMA span {span}: {n} coins elapsed={int(elapsed_s)}s, top{top_n}: {summary}"
                )
        return out

    async def calc_volumes(
        self,
        pside: str,
        symbols: Optional[Iterable[str]] = None,
        *,
        max_age_ms: Optional[int] = 60_000,
    ) -> Dict[str, float]:
        """Compute 1m EMA of quote volume per symbol.

        Returns mapping symbol -> ema_quote_volume; non-finite/failed computations yield 0.0.
        """
        span = int(round(self.bot_value(pside, "forager_volume_ema_span")))
        try:
            warmup_ratio = float(get_optional_live_value(self.config, "warmup_ratio", 0.0))
        except Exception:
            warmup_ratio = 0.0
        try:
            max_warmup_minutes = int(
                get_optional_live_value(self.config, "max_warmup_minutes", 0) or 0
            )
        except Exception:
            max_warmup_minutes = 0
        span_buffer = 1.0 + max(0.0, warmup_ratio)
        window_candles = max(1, int(math.ceil(span * span_buffer))) if span > 0 else 1
        if max_warmup_minutes > 0:
            window_candles = min(int(window_candles), int(max_warmup_minutes))
        if symbols is None:
            symbols = self.get_symbols_approved_or_has_pos(pside)

        # Compute EMA of quote volume on 1m candles
        async def one(symbol: str):
            try:
                if max_age_ms is not None:
                    ttl = int(max_age_ms)
                else:
                    has_pos = self.has_position(symbol)
                    has_oo = (
                        bool(self.open_orders.get(symbol)) if hasattr(self, "open_orders") else False
                    )
                    ttl = (
                        60_000
                        if (has_pos or has_oo)
                        else int(getattr(self, "inactive_coin_candle_ttl_ms", 600_000))
                    )
                res = await self.cm.get_latest_ema_metrics(
                    symbol,
                    {"qv": span},
                    max_age_ms=ttl,
                    window_candles=window_candles,
                    timeframe=None,
                )
                val = float(res.get("qv", float("nan")))
                return float(val) if np.isfinite(val) else 0.0
            except Exception:
                return 0.0

        syms = list(symbols)
        tasks = {s: asyncio.create_task(one(s)) for s in syms}
        out = {}
        n = len(syms)
        started_ms = utc_ms()
        for sym, task in tasks.items():
            try:
                out[sym] = await task
            except Exception:
                out[sym] = 0.0
        elapsed_s = max(0.001, (utc_ms() - started_ms) / 1000.0)
        now_ms = utc_ms()
        ema_log_throttle_ms = 300_000  # 5 minutes between logs per metric
        if out:
            top_n = min(8, len(out))
            top = sorted(out.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
            top_syms = tuple(sym for sym, _ in top)
            # Throttle to at most once per 5 minutes per metric.
            if not hasattr(self, "_volume_top_cache"):
                self._volume_top_cache = {}
            if not hasattr(self, "_volume_top_last_log_ms"):
                self._volume_top_last_log_ms = {}
            cache_key = (pside, span)
            last_top = self._volume_top_cache.get(cache_key)
            last_log_ms = self._volume_top_last_log_ms.get(cache_key, 0)
            if last_top != top_syms and (now_ms - last_log_ms) >= ema_log_throttle_ms:
                self._volume_top_cache[cache_key] = top_syms
                self._volume_top_last_log_ms[cache_key] = now_ms
                summary = ", ".join(f"{symbol_to_coin(sym)}={val:.2f}" for sym, val in top)
                logging.info(
                    f"[ranking] volume EMA span {span}: {n} coins elapsed={int(elapsed_s)}s, top{top_n}: {summary}"
                )
        return out

    async def execute_multiple(self, orders: [dict], type_: str):
        """Execute a list of order operations sequentially while tracking failures."""
        if not orders:
            return []
        executions = []
        any_exceptions = False
        for order in orders:  # sorted by PA dist
            task = None
            try:
                task = asyncio.create_task(getattr(self, type_)(order))
                executions.append((order, task))
            except Exception as e:
                logging.error(f"error executing {type_} {order} {e}")
                print_async_exception(task)
                traceback.print_exc()
                executions.append((order, e))
                any_exceptions = True
        results = []
        for order, execution in executions:
            if isinstance(execution, Exception):
                # Already failed at task creation time
                results.append(execution)
                continue
            result = None
            try:
                result = await execution
                results.append(result)
            except Exception as e:
                logging.error(f"error executing {type_} {execution} {e}")
                print_async_exception(result)
                results.append(e)
                traceback.print_exc()
                any_exceptions = True
        if any_exceptions:
            await self.restart_bot_on_too_many_errors()
        return results

    # Legacy maintain_ohlcvs_1m_REST removed; CandlestickManager handles caching and TTL

    # Legacy update_ohlcvs_1m_single_from_exchange removed

    # Legacy update_ohlcvs_1m_single_from_disk removed

    # Legacy update_ohlcvs_1m_single removed

    # Legacy file lock helpers removed

    async def close(self):
        """Stop background tasks and close exchange clients."""
        logging.info(f"Stopped data maintainers: {self.stop_data_maintainers()}")
        await self.cca.close()
        if self.ccp is not None:
            await self.ccp.close()

    def add_to_coins_lists(self, content, k_coins, log_psides=None):
        """Update approved/ignored coin sets from configuration content."""
        if log_psides is None:
            log_psides = set(content.keys())
        symbols = None
        result = {"added": {}, "removed": {}}
        psides_equal = content["long"] == content["short"]
        for pside in content:
            if not psides_equal or symbols is None:
                coins = content[pside]
                if k_coins == "approved_coins" and _coins_source_side_is_all(coins):
                    symbols = set(getattr(self, "eligible_symbols", set()))
                else:
                    # Check if coins is a single string that needs to be split
                    if isinstance(coins, str):
                        coins = coins.split(",")
                    # Handle case where list contains comma-separated values in its elements
                    elif isinstance(coins, (list, tuple)):
                        expanded_coins = []
                        for item in coins:
                            if isinstance(item, str) and "," in item:
                                expanded_coins.extend(item.split(","))
                            else:
                                expanded_coins.append(item)
                        coins = expanded_coins

                    symbols = [self.coin_to_symbol(coin, verbose=False) for coin in coins if coin]
                    symbols = {s for s in symbols if s}
                    eligible = getattr(self, "eligible_symbols", None)
                    if eligible:
                        skipped = [sym for sym in symbols if sym not in eligible]
                        if skipped:
                            coin_list = ", ".join(
                                sorted(symbol_to_coin(sym, verbose=False) or sym for sym in skipped)
                            )
                            symbol_list = ", ".join(sorted(skipped))
                            warned = getattr(self, "_unsupported_coin_warnings", None)
                            if warned is None:
                                warned = set()
                                setattr(self, "_unsupported_coin_warnings", warned)
                            warn_key = (self.exchange, coin_list, symbol_list, k_coins)
                            if warn_key not in warned:
                                logging.info(
                                    "[config] skipping unsupported markets for %s: coins=%s symbols=%s exchange=%s",
                                    k_coins,
                                    coin_list,
                                    symbol_list,
                                    getattr(self, "exchange", "?"),
                                )
                                warned.add(warn_key)
                            symbols = symbols - set(skipped)
            symbols_already = getattr(self, k_coins)[pside]
            if symbols_already != symbols:
                added = symbols - symbols_already
                removed = symbols_already - symbols
                if added and pside in log_psides:
                    result["added"][pside] = added
                if removed and pside in log_psides:
                    result["removed"][pside] = removed
                getattr(self, k_coins)[pside] = symbols
        return result

    def refresh_approved_ignored_coins_lists(self):
        """Reload approved and ignored coin lists from config sources."""
        try:
            added_summary = {}
            removed_summary = {}
            for k in ("approved_coins", "ignored_coins"):
                if not hasattr(self, k):
                    setattr(self, k, {"long": set(), "short": set()})
                config_sources = self.config.get("_coins_sources", {})
                if k in config_sources:
                    raw_source = config_sources[k]
                else:
                    raw_source = self.live_value(k)
                parsed = normalize_coins_source(raw_source, allow_all=(k == "approved_coins"))
                if k == "approved_coins":
                    log_psides = {ps for ps in parsed if self.is_pside_enabled(ps)}
                else:
                    log_psides = set(parsed.keys())
                add_res = self.add_to_coins_lists(parsed, k, log_psides=log_psides)
                if add_res:
                    added_summary.setdefault(k, {}).update(add_res.get("added", {}))
                    removed_summary.setdefault(k, {}).update(add_res.get("removed", {}))
            self.approved_coins_minus_ignored_coins = {}
            for pside in self.approved_coins:
                if not self.is_pside_enabled(pside):
                    if pside not in self._disabled_psides_logged:
                        if self.approved_coins[pside]:
                            logging.info(
                                f"{pside} side disabled (zero exposure or positions); clearing approved list."
                            )
                        else:
                            logging.info(
                                f"{pside} side disabled (zero exposure or positions); approved list already empty."
                            )
                        self._disabled_psides_logged.add(pside)
                    self.approved_coins[pside] = set()
                    self.approved_coins_minus_ignored_coins[pside] = set()
                    continue
                else:
                    if pside in self._disabled_psides_logged:
                        logging.info(f"{pside} side re-enabled; restoring approved coin handling.")
                        self._disabled_psides_logged.discard(pside)
                self.approved_coins_minus_ignored_coins[pside] = self._filter_approved_symbols(
                    pside, self.approved_coins[pside] - self.ignored_coins[pside]
                )
            # aggregate add/remove logs for readability
            for k, summary in (("added", added_summary.get("approved_coins", {})),):
                if summary:
                    parts = []
                    for pside, coins in summary.items():
                        if coins:
                            parts.append(
                                f"{pside}: {','.join(sorted(symbol_to_coin(x) for x in coins))}"
                            )
                    if parts:
                        logging.info("added to approved_coins | %s", " | ".join(parts))
            for k, summary in (("removed", removed_summary.get("approved_coins", {})),):
                if summary:
                    parts = []
                    for pside, coins in summary.items():
                        if coins:
                            parts.append(
                                f"{pside}: {','.join(sorted(symbol_to_coin(x) for x in coins))}"
                            )
                    if parts:
                        logging.info("removed from approved_coins | %s", " | ".join(parts))
            for k, summary in (("added", added_summary.get("ignored_coins", {})),):
                if summary:
                    parts = []
                    for pside, coins in summary.items():
                        if coins:
                            parts.append(
                                f"{pside}: {','.join(sorted(symbol_to_coin(x) for x in coins))}"
                            )
                    if parts:
                        logging.info("added to ignored_coins | %s", " | ".join(parts))
            for k, summary in (("removed", removed_summary.get("ignored_coins", {})),):
                if summary:
                    parts = []
                    for pside, coins in summary.items():
                        if coins:
                            parts.append(
                                f"{pside}: {','.join(sorted(symbol_to_coin(x) for x in coins))}"
                            )
                    if parts:
                        logging.info("removed from ignored_coins | %s", " | ".join(parts))
            try:
                if not getattr(self, "_stock_perps_warning_logged", False):
                    stock_syms = set()
                    for syms in self.approved_coins_minus_ignored_coins.values():
                        for sym in syms:
                            base = sym.split("/")[0] if "/" in sym else sym
                            if base.startswith(("xyz:", "XYZ-", "XYZ:")) or sym.startswith(
                                ("xyz:", "XYZ-", "XYZ:")
                            ):
                                stock_syms.add(sym)
                    if stock_syms:
                        coins = sorted(
                            {
                                symbol_to_coin(s) or (s.split("/")[0] if "/" in s else s)
                                for s in stock_syms
                            }
                        )
                        logging.warning(
                            "Stock perps detected in approved_coins (%s). HIP-3 isolated margin is currently unsupported; isolated-only symbols will be skipped and existing isolated live state will fail loudly.",
                            ",".join(coins),
                        )
                        self._stock_perps_warning_logged = True
            except Exception:  # error-contract: allow - warning emission must not block refresh
                pass
            self._log_coin_symbol_fallback_summary()
        except Exception as e:
            logging.error(f"error with refresh_approved_ignored_coins_lists {e}")
            traceback.print_exc()

    def _log_coin_symbol_fallback_summary(self):
        """Emit a brief summary of symbol/coin mapping fallbacks (once per change)."""
        counts = coin_symbol_warning_counts()
        if counts != self._last_coin_symbol_warning_counts:
            if counts["symbol_to_coin_fallbacks"] or counts["coin_to_symbol_fallbacks"]:
                logging.info(
                    "[mapping] fallbacks: symbol->coin=%d | coin->symbol=%d (unique)",
                    counts["symbol_to_coin_fallbacks"],
                    counts["coin_to_symbol_fallbacks"],
                )
            self._last_coin_symbol_warning_counts = dict(counts)

def setup_bot(config):
    """Instantiate the correct exchange bot implementation based on configuration."""
    user_info = load_user_info(require_live_value(config, "user"))
    if user_info["exchange"] == "bybit":
        from exchanges.bybit import BybitBot

        bot = BybitBot(config)
    elif user_info["exchange"] == "bitget":
        from exchanges.bitget import BitgetBot

        bot = BitgetBot(config)
    elif user_info["exchange"] == "binance":
        from exchanges.binance import BinanceBot

        bot = BinanceBot(config)
    elif user_info["exchange"] == "okx":
        from exchanges.okx import OKXBot

        bot = OKXBot(config)
    elif user_info["exchange"] == "hyperliquid":
        from exchanges.hyperliquid import HyperliquidBot

        bot = HyperliquidBot(config)
    elif user_info["exchange"] == "gateio":
        from exchanges.gateio import GateIOBot

        bot = GateIOBot(config)
    elif user_info["exchange"] == "defx":
        from exchanges.defx import DefxBot

        bot = DefxBot(config)
    elif user_info["exchange"] == "kucoin":
        from exchanges.kucoin import KucoinBot

        bot = KucoinBot(config)
    elif user_info["exchange"] == "paradex":
        from exchanges.paradex import ParadexBot

        bot = ParadexBot(config)
    elif user_info["exchange"] == "fake":
        from exchanges.fake import FakeBot

        bot = FakeBot(config)
    else:
        # Generic CCXTBot for any CCXT-supported exchange
        from exchanges.ccxt_bot import CCXTBot

        bot = CCXTBot(config)
        logging.info(
            f"Using generic CCXTBot for '{user_info['exchange']}' (no custom implementation)"
        )
    return bot


async def main():
    """Entry point: parse CLI args, load config, and launch the bot lifecycle."""
    pb_runtime.register_signal_handlers()
    raw_argv = sys.argv[1:]
    help_all = help_all_requested(raw_argv)
    parser = build_command_parser(
        prog=get_cli_prog("passivbot"),
        description="run passivbot",
        usage="%(prog)s [config_path] [options]",
        epilog=(
            "Examples:\n"
            "  passivbot live configs/live/my_account.json\n"
            "  passivbot live configs/live/my_account.json -s BTC,ETH --log-level info\n"
            "\n"
            "Use --help-all to show every config override flag."
        ),
    )
    parser.add_argument(
        "config_path",
        type=str,
        nargs="?",
        default=None,
        help="path to json/hjson passivbot config (defaults to in-code schema defaults if omitted)",
    )
    add_help_all_argument(
        parser,
        help_all=help_all,
        help_text="Show all live-trading override flags, including advanced config overrides.",
    )

    logging_group = parser.add_argument_group("Logging")
    logging_group.add_argument(
        "--log-level",
        dest="log_level",
        default=None,
        help="Logging verbosity (warning, info, debug, trace or 0-3).",
    )
    logging_group.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="Enable verbose (debug) logging. Equivalent to --log-level debug.",
    )

    runtime_group = parser.add_argument_group("Runtime")
    runtime_group.add_argument(
        "--custom-endpoints",
        dest="custom_endpoints",
        default=None,
        help=(
            "Path to custom endpoints JSON for this run. "
            "Use 'none' to disable overrides even if a default file exists."
        ),
    )

    group_map = {
        "Coin Selection": parser.add_argument_group("Coin Selection"),
        "Behavior": parser.add_argument_group("Behavior"),
        "Runtime": runtime_group,
        "Logging": logging_group,
        "Advanced Overrides": parser.add_argument_group("Advanced Overrides"),
    }

    template_config = get_template_config()
    del template_config["optimize"]
    del template_config["backtest"]
    if "logging" in template_config and isinstance(template_config["logging"], dict):
        template_config["logging"].pop("level", None)
    allowed_config_keys = add_config_arguments(
        parser,
        template_config,
        command="live",
        help_all=help_all,
        group_map=group_map,
    )
    raw_args = merge_negative_cli_values(expand_help_all_argv(raw_argv))
    args = parser.parse_args(raw_args)
    # --verbose flag overrides --log-level to debug (level 2)
    cli_log_level = "debug" if args.verbose else args.log_level
    initial_log_level = resolve_log_level(cli_log_level, None, fallback=1)
    configure_logging(debug=initial_log_level)
    source_config, base_config_path, raw_snapshot = load_input_config(args.config_path)
    update_config_with_args(source_config, args, verbose=True, allowed_keys=allowed_config_keys)
    config = prepare_config(
        source_config,
        base_config_path=base_config_path,
        live_only=True,
        verbose=True,
        target="live",
        runtime="live",
        raw_snapshot=raw_snapshot,
    )
    config_logging_value = get_optional_config_value(config, "logging.level", None)
    effective_log_level = resolve_log_level(cli_log_level, config_logging_value, fallback=1)
    logging_section = config.get("logging")
    if not isinstance(logging_section, dict):
        logging_section = {}
    config["logging"] = logging_section
    logging_section["level"] = effective_log_level
    live_user = require_live_value(config, "user")
    log_file_settings = resolve_live_log_file_settings(
        config,
        user=live_user,
        command_args=[sys.argv[0], *raw_argv],
    )
    if effective_log_level != initial_log_level or log_file_settings["log_file"]:
        configure_logging(debug=effective_log_level, **log_file_settings)

    custom_endpoints_cli = args.custom_endpoints
    live_section = config.get("live") if isinstance(config.get("live"), dict) else {}
    custom_endpoints_cfg = live_section.get("custom_endpoints_path") if live_section else None

    override_path = None
    autodiscover = True
    preloaded_override = None

    def _sanitize(value):
        if value is None:
            return None
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return "none"
            return stripped
        return str(value)

    cli_value = _sanitize(custom_endpoints_cli) if custom_endpoints_cli is not None else None
    cfg_value = _sanitize(custom_endpoints_cfg) if custom_endpoints_cfg is not None else None

    if cli_value is not None:
        if cli_value.lower() in {"none", "off", "disable"}:
            override_path = None
            autodiscover = False
            logging.info("Custom endpoints disabled via CLI argument.")
        else:
            override_path = cli_value
            autodiscover = False
            preloaded_override = load_custom_endpoint_config(override_path)
            logging.info("Using custom endpoints from CLI path: %s", override_path)
    elif cfg_value:
        if cfg_value.lower() in {"none", "off", "disable"}:
            override_path = None
            autodiscover = False
            logging.info("Custom endpoints disabled via config live.custom_endpoints_path.")
        else:
            override_path = cfg_value
            autodiscover = False
            preloaded_override = load_custom_endpoint_config(override_path)
            logging.info(
                "Using custom endpoints from config live.custom_endpoints_path: %s", override_path
            )
    else:
        logging.debug("Custom endpoints not specified; falling back to auto-discovery.")

    configure_custom_endpoint_loader(
        override_path,
        autodiscover=autodiscover,
        preloaded=preloaded_override,
    )

    user_info = load_user_info(live_user)
    # Reconfigure logging with exchange prefix now that we know the exchange
    exchange_prefix = user_info["exchange"]
    configure_logging(debug=effective_log_level, prefix=exchange_prefix, **log_file_settings)
    await load_markets(user_info["exchange"], verbose=True)

    config = parse_overrides(config, verbose=True)
    cooldown_secs = 60
    restarts = []
    while True:

        bot = setup_bot(config)
        try:
            await bot.start_bot()
        except Exception as e:
            logging.error(f"passivbot error {e}")
            traceback.print_exc()
        finally:
            await pb_runtime.close_bot_clients(bot)
        if bot.stop_signal_received:
            logging.info("Bot stopped via signal; exiting main loop.")
            break

        logging.info(f"restarting bot...")
        print()
        for z in range(cooldown_secs, -1, -1):
            print(f"\rcountdown {z}...  ")
            await asyncio.sleep(1)
        print()

        restarts.append(utc_ms())
        restarts = [x for x in restarts if x > utc_ms() - 1000 * 60 * 60 * 24]
        max_restarts = int(require_live_value(bot.config, "max_n_restarts_per_day"))
        if len(restarts) > max_restarts:
            logging.info(f"n restarts exceeded {max_restarts} last 24h")
            break


Passivbot._build_order_params = pb_execution._build_order_params
Passivbot.execute_order = pb_execution.execute_order
Passivbot.execute_orders = pb_execution.execute_orders
Passivbot.execute_cancellation = pb_execution.execute_cancellation
Passivbot.execute_cancellations = pb_execution.execute_cancellations
Passivbot.order_to_order_tuple = order_to_order_tuple
Passivbot.has_open_unstuck_order = has_open_unstuck_order
signal_handler = pb_runtime.signal_handler
register_signal_handlers = pb_runtime.register_signal_handlers
shutdown_bot = pb_runtime.shutdown_bot
close_bot_clients = pb_runtime.close_bot_clients
Passivbot.update_exchange_configs = pb_exchange_config.update_exchange_configs
Passivbot._is_rate_limit_like_exception = pb_exchange_config.is_rate_limit_like_exception
Passivbot._exchange_config_backoff_seconds = pb_exchange_config.exchange_config_backoff_seconds
Passivbot._exchange_config_success_pause_seconds = (
    pb_exchange_config.exchange_config_success_pause_seconds
)
Passivbot.get_hysteresis_snapped_balance = pb_balance_utils.get_hysteresis_snapped_balance
Passivbot.get_raw_balance = pb_balance_utils.get_raw_balance
Passivbot._calc_effective_min_cost_at_price = pb_balance_utils.calc_effective_min_cost_at_price
Passivbot.calc_upnl_sum = pb_balance_utils.calc_upnl_sum
Passivbot.handle_balance_update = pb_balance_utils.handle_balance_update
Passivbot.update_effective_min_cost = pb_balance_utils.update_effective_min_cost
Passivbot._build_ccxt_options = pb_client_utils.build_ccxt_options
Passivbot._apply_endpoint_override = pb_client_utils.apply_endpoint_override
Passivbot.log_once = pb_debug_utils.log_once
Passivbot._is_rate_limit_like_exception = pb_exchange_config.is_rate_limit_like_exception
Passivbot._exchange_config_backoff_seconds = pb_exchange_config.exchange_config_backoff_seconds
Passivbot._exchange_config_success_pause_seconds = pb_exchange_config.exchange_config_success_pause_seconds
Passivbot._update_single_symbol_exchange_config = pb_exchange_config.update_single_symbol_exchange_config
Passivbot.update_exchange_configs = pb_exchange_config.update_exchange_configs
Passivbot.get_exchange_time = pb_runtime_ops.get_exchange_time
Passivbot.get_current_n_positions = pb_mode_utils.get_current_n_positions
Passivbot._assert_supported_live_state = pb_hook_utils.assert_supported_live_state
Passivbot.effective_min_cost_is_low_enough = pb_exposure_utils.effective_min_cost_is_low_enough
Passivbot._filter_approved_symbols = pb_hook_utils.filter_approved_symbols
Passivbot._ensure_exchange_config_ready_for_market_init = (
    pb_market_init_utils.ensure_exchange_config_ready_for_market_init
)
Passivbot._log_ema_gating = pb_logging_utils.log_ema_gating
Passivbot.maybe_log_ema_debug = pb_logging_utils.maybe_log_ema_debug
Passivbot.get_max_n_positions = pb_mode_utils.get_max_n_positions
Passivbot.get_forced_PB_mode = pb_mode_utils.get_forced_PB_mode
Passivbot.is_forager_mode = pb_mode_utils.is_forager_mode
Passivbot._forager_refresh_budget = pb_mode_utils._forager_refresh_budget
Passivbot._split_forager_budget_by_side = pb_mode_utils._split_forager_budget_by_side
Passivbot._forager_target_staleness_ms = pb_mode_utils._forager_target_staleness_ms
Passivbot._maybe_log_candle_refresh = pb_mode_utils._maybe_log_candle_refresh
Passivbot._pside_blocks_new_entries = pb_mode_utils.pside_blocks_new_entries
Passivbot._mode_override_to_orchestrator_mode = pb_mode_utils.mode_override_to_orchestrator_mode
Passivbot._python_mode_from_orchestrator_state = pb_mode_utils.python_mode_from_orchestrator_state
Passivbot._build_orchestrator_mode_overrides_fallback = pb_mode_utils.build_orchestrator_mode_overrides_fallback
Passivbot._build_live_symbol_universe = pb_mode_utils.build_live_symbol_universe
Passivbot.config_get = pb_override_utils.config_get
Passivbot.bp = pb_override_utils.bp
Passivbot.init_coin_overrides = pb_override_utils.init_coin_overrides
Passivbot.live_value = pb_override_utils.live_value
Passivbot.bot_value = pb_override_utils.bot_value
Passivbot.init_pnls = pb_pnls_utils.init_pnls
Passivbot._apply_loaded_markets = pb_market_init_utils.apply_loaded_markets
Passivbot._apply_post_market_load_setup = pb_market_init_utils.apply_post_market_load_setup
Passivbot.set_wallet_exposure_limits = pb_exposure_utils.set_wallet_exposure_limits
Passivbot._compute_fetch_budget_ttls = pb_fetch_budget_utils.compute_fetch_budget_ttls
Passivbot._format_duration = pb_format_utils.format_duration
Passivbot.get_exchange_time = pb_runtime_ops.get_exchange_time
Passivbot._get_fetch_delay_seconds = pb_runtime_ops.get_fetch_delay_seconds
Passivbot._set_log_silence_watchdog_context = pb_runtime_ops.set_log_silence_watchdog_context
Passivbot._maybe_log_silence_watchdog = pb_runtime_ops.maybe_log_silence_watchdog
Passivbot._run_log_silence_watchdog = pb_runtime_ops.run_log_silence_watchdog
Passivbot._start_log_silence_watchdog = pb_runtime_ops.start_log_silence_watchdog
Passivbot._stop_log_silence_watchdog = pb_runtime_ops.stop_log_silence_watchdog
Passivbot._log_memory_snapshot = pb_runtime_ops.log_memory_snapshot
Passivbot.get_wallet_exposure_limit = pb_exposure_utils.get_wallet_exposure_limit
Passivbot._log_health_summary = pb_runtime_ops.log_health_summary
Passivbot._maybe_log_health_summary = pb_runtime_ops.maybe_log_health_summary
Passivbot._maybe_apply_boot_stagger = pb_startup_utils.maybe_apply_boot_stagger
Passivbot._run_startup_preloop = pb_startup_utils.run_startup_preloop
Passivbot._handle_startup_error = pb_startup_utils.handle_startup_error
Passivbot._finalize_startup_ready = pb_startup_utils.finalize_startup_ready
Passivbot._log_startup_banner = pb_startup_utils.log_startup_banner
Passivbot.get_first_timestamp = pb_timestamp_utils.get_first_timestamp
Passivbot.update_first_timestamps = pb_timestamp_utils.update_first_timestamps
Passivbot._calc_unstuck_allowance_for_logging = pb_unstuck_utils.calc_unstuck_allowance_for_logging
Passivbot._calc_unstuck_allowances = pb_unstuck_utils.calc_unstuck_allowances
Passivbot._calc_unstuck_allowances_live = pb_unstuck_utils.calc_unstuck_allowances_live
Passivbot._log_unstuck_status = pb_unstuck_utils.log_unstuck_status
Passivbot._maybe_log_unstuck_status = pb_unstuck_utils.maybe_log_unstuck_status
Passivbot._equity_hard_stop_realized_pnl_now = pb_hsl._equity_hard_stop_realized_pnl_now
Passivbot.add_new_order = pb_order_update_utils.add_new_order
Passivbot.add_to_recent_order_cancellations = pb_order_update_utils.add_to_recent_order_cancellations
Passivbot.add_to_recent_order_executions = pb_order_update_utils.add_to_recent_order_executions
Passivbot.remove_order = pb_order_update_utils.remove_order
Passivbot.handle_order_update = pb_order_update_utils.handle_order_update
Passivbot.is_pside_enabled = pb_exposure_utils.is_pside_enabled
Passivbot.has_position = pb_position_utils.has_position
Passivbot.order_was_recently_cancelled = pb_order_update_utils.order_was_recently_cancelled
Passivbot.order_was_recently_updated = pb_order_update_utils.order_was_recently_updated
Passivbot.get_last_position_changes = pb_trailing_utils.get_last_position_changes
Passivbot.log_position_changes = pb_position_logging_utils.log_position_changes
Passivbot.is_approved = pb_approval_utils.is_approved
Passivbot.is_old_enough = pb_approval_utils.is_old_enough
Passivbot.is_trailing = pb_position_utils.is_trailing
Passivbot.symbol_is_eligible = pb_hook_utils.symbol_is_eligible
Passivbot.debug_print = pb_debug_utils.debug_print
Passivbot.pad_sym = pb_format_utils.pad_sym
Passivbot.stop_data_maintainers = pb_runtime_ops.stop_data_maintainers
Passivbot.coin_to_symbol = pb_symbol_utils.coin_to_symbol
Passivbot.get_symbol_id = pb_symbol_utils.get_symbol_id
Passivbot.to_ccxt_symbol = pb_symbol_utils.to_ccxt_symbol
Passivbot.get_symbol_id_inv = pb_symbol_utils.get_symbol_id_inv
Passivbot.set_market_specific_settings = pb_symbol_utils.set_market_specific_settings
Passivbot.update_tickers = pb_ticker_utils.update_tickers


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot shutdown complete.")
