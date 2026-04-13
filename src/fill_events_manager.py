"""Fill events management module.

Provides a reusable manager that keeps local cache of canonicalised fill events,
fetches fresh data from the exchange when requested, and exposes convenient query
APIs (PnL summaries, cumulative PnL, last fill timestamps, etc.).

Currently implements a Bitget fetcher; the design is extensible to other
exchanges.
"""

from __future__ import annotations

import argparse
import asyncio
import fill_events_binance_utils as binance_utils
import fill_events_bitget_utils as bitget_utils
import fill_events_bybit_utils as bybit_utils
import fill_events_cache_utils as cache_utils
import fill_events_cli_utils as cli_utils
import fill_events_fee_utils as fee_utils
import fill_events_fetcher_utils as fetcher_utils
import fill_events_gateio_utils as gateio_utils
import fill_events_hyperliquid_utils as hyperliquid_utils
import fill_events_kucoin_utils as kucoin_utils
import fill_events_okx_utils as okx_utils
import fill_events_pagination_utils as pagination_utils
import fill_events_parse_utils as parse_utils
import fill_events_position_utils as position_utils
import fill_events_query_utils as query_utils
import fill_events_time_utils as time_utils
import json
import math
import sys

if sys.platform.startswith("win"):
    try:
        import fcntl
    except ImportError:
        class _FcntlStub:
            LOCK_EX = None
            LOCK_SH = None
            LOCK_UN = None

            def lockf(self, *args, **kwargs):
                pass

            def ioctl(self, *args, **kwargs):
                pass

            def flock(self, *args, **kwargs):
                pass

        sys.modules["fcntl"] = _FcntlStub()
        import fcntl
else:
    import fcntl

import logging
import os
import random
import tempfile
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from importlib import import_module
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple, TypedDict

from ccxt.base.errors import RateLimitExceeded
from config import load_input_config, prepare_config

try:
    import fill_events_data_model as data_model
except ImportError:  # pragma: no cover - package-relative fallback
    from . import fill_events_data_model as data_model

try:
    from utils import ts_to_date  # type: ignore
except ImportError:  # pragma: no cover - fallback for package-relative execution
    from .utils import ts_to_date

from logging_setup import configure_logging
from procedures import load_user_info
from pure_funcs import ensure_millis

logger = logging.getLogger(__name__)

_REQUIRED_FILL_EVENT_KEYS = (
    "id",
    "timestamp",
    "symbol",
    "side",
    "qty",
    "price",
    "pnl",
    "pb_order_type",
    "position_side",
    "client_order_id",
)


def _validate_required_fill_event_payload(data: Dict[str, object]) -> None:
    missing = [key for key in _REQUIRED_FILL_EVENT_KEYS if key not in data]
    if missing:
        raise ValueError(f"missing required keys: {missing}")

    invalid: List[str] = []
    if data.get("id") is None or not str(data.get("id")).strip():
        invalid.append("id(empty)")

    try:
        timestamp = int(data["timestamp"])
    except (TypeError, ValueError):
        invalid.append(f"timestamp(invalid={data.get('timestamp')!r})")
    else:
        if timestamp <= 0:
            invalid.append(f"timestamp(non-positive={timestamp})")

    symbol = str(data["symbol"] or "").strip()
    if not symbol:
        invalid.append("symbol(empty)")

    side = str(data["side"] or "").strip().lower()
    if side not in {"buy", "sell"}:
        invalid.append(f"side(invalid={data.get('side')!r})")

    try:
        qty = float(data["qty"])
    except (TypeError, ValueError):
        invalid.append(f"qty(invalid={data.get('qty')!r})")
    else:
        if not math.isfinite(qty):
            invalid.append(f"qty(non-finite={data.get('qty')!r})")
        elif qty == 0.0:
            invalid.append("qty(zero)")

    try:
        price = float(data["price"])
    except (TypeError, ValueError):
        invalid.append(f"price(invalid={data.get('price')!r})")
    else:
        if not math.isfinite(price):
            invalid.append(f"price(non-finite={data.get('price')!r})")
        elif price <= 0.0:
            invalid.append(f"price(non-positive={price})")

    try:
        pnl = float(data["pnl"])
    except (TypeError, ValueError):
        invalid.append(f"pnl(invalid={data.get('pnl')!r})")
    else:
        if not math.isfinite(pnl):
            invalid.append(f"pnl(non-finite={data.get('pnl')!r})")

    if not str(data["pb_order_type"] or "").strip():
        invalid.append("pb_order_type(empty)")

    position_side = str(data["position_side"] or "").strip().lower()
    if position_side not in {"long", "short"}:
        invalid.append(f"position_side(invalid={data.get('position_side')!r})")

    if invalid:
        raise ValueError(f"invalid required fields: {invalid}")

# Throttle state for spammy warnings
_pnl_discrepancy_last_log: Dict[str, float] = {}  # exchange:user -> last log time
_pnl_discrepancy_last_delta: Dict[str, float] = {}  # exchange:user -> last delta value
_PNL_DISCREPANCY_THROTTLE_SECONDS = 3600.0  # Log at most once per hour if delta unchanged
_PNL_DISCREPANCY_CHANGE_THRESHOLD = 0.10  # Consider delta "changed" if >10%
_PNL_DISCREPANCY_MIN_SECONDS = 900.0  # Minimum seconds between logs even if delta changes


# ---------------------------------------------------------------------------
# Rate Limit Coordination
# ---------------------------------------------------------------------------

# Default rate limits per exchange (calls per minute)
_DEFAULT_RATE_LIMITS: Dict[str, Dict[str, int]] = {
    "binance": {"fetch_my_trades": 1200, "fetch_income_history": 120, "default": 1200},
    "bybit": {"fetch_my_trades": 120, "fetch_positions_history": 120, "default": 120},
    "bitget": {"fill_history": 120, "fetch_order": 60, "default": 120},
    "hyperliquid": {"fetch_my_trades": 120, "default": 120},
    "gateio": {"fetch_closed_orders": 120, "default": 120},
    "kucoin": {
        "fetch_my_trades": 120,
        "fetch_positions_history": 120,
        "fetch_order": 60,
        "default": 120,
    },
    # OKX: /fills = 60 req/2s, /fills-history = 10 req/2s (conservative estimates)
    "okx": {"fetch_my_trades": 1800, "fills_history": 300, "default": 300},
}

# Window for rate limit tracking (ms)
_RATE_LIMIT_WINDOW_MS = 60_000

# Default jitter range for staggered startup (seconds)
_STARTUP_JITTER_MIN = 0.0
_STARTUP_JITTER_MAX = 30.0


class RateLimitCoordinator:
    """Coordinates rate limiting across multiple bot instances via shared temp file.

    Each exchange has a temp file that logs recent API calls. Instances check this
    file before making API calls and add jitter if approaching rate limits.
    """

    def __init__(
        self,
        exchange: str,
        user: str,
        *,
        temp_dir: Optional[Path] = None,
        window_ms: int = _RATE_LIMIT_WINDOW_MS,
        limits: Optional[Dict[str, int]] = None,
    ) -> None:
        self.exchange = exchange.lower()
        self.user = user
        self.window_ms = window_ms
        self.limits = limits or _DEFAULT_RATE_LIMITS.get(self.exchange, {"default": 120})

        if temp_dir is None:
            temp_dir = Path(tempfile.gettempdir()) / "passivbot_rate_limits"
        self.temp_dir = temp_dir
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.temp_file = self.temp_dir / f"{self.exchange}.json"

    def _load_calls(self) -> List[Dict[str, object]]:
        """Load recent API calls from temp file."""
        if not self.temp_file.exists():
            return []
        try:
            with self.temp_file.open("r") as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                try:
                    data = json.load(f)
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                return data.get("calls", [])
        except Exception as exc:
            logger.debug("RateLimitCoordinator: failed to load %s: %s", self.temp_file, exc)
            return []

    def _save_calls(self, calls: List[Dict[str, object]]) -> None:
        """Save API calls to temp file atomically."""
        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

        # Prune old entries
        cutoff = now_ms - self.window_ms
        calls = [c for c in calls if c.get("timestamp_ms", 0) > cutoff]

        data = {
            "calls": calls,
            "window_ms": self.window_ms,
            "limits": self.limits,
            "last_update": now_ms,
        }

        tmp_file = self.temp_file.with_suffix(".tmp")
        try:
            with tmp_file.open("w") as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                try:
                    json.dump(data, f)
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            os.replace(tmp_file, self.temp_file)
        except Exception as exc:
            logger.debug("RateLimitCoordinator: failed to save %s: %s", self.temp_file, exc)

    def get_current_usage(self, endpoint: str) -> int:
        """Get current call count for an endpoint in the current window."""
        calls = self._load_calls()
        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        cutoff = now_ms - self.window_ms
        return sum(
            1 for c in calls if c.get("endpoint") == endpoint and c.get("timestamp_ms", 0) > cutoff
        )

    def get_limit(self, endpoint: str) -> int:
        """Get rate limit for an endpoint."""
        return self.limits.get(endpoint, self.limits.get("default", 120))

    def record_call(self, endpoint: str) -> None:
        """Record an API call."""
        calls = self._load_calls()
        calls.append(
            {
                "endpoint": endpoint,
                "timestamp_ms": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
                "user": self.user,
            }
        )
        self._save_calls(calls)

    async def wait_if_needed(self, endpoint: str) -> float:
        """Check rate limit and wait if needed. Returns time waited (seconds)."""
        current = self.get_current_usage(endpoint)
        limit = self.get_limit(endpoint)

        if current >= limit:
            # At or over limit - wait for full window
            wait_time = self.window_ms / 1000.0
            logger.info(
                "RateLimitCoordinator: %s:%s at limit (%d/%d), waiting %.1fs",
                self.exchange,
                endpoint,
                current,
                limit,
                wait_time,
            )
            await asyncio.sleep(wait_time)
            return wait_time
        elif current >= limit * 0.8:
            # Approaching limit - add jitter
            jitter = random.uniform(0.1, 2.0)
            logger.debug(
                "RateLimitCoordinator: %s:%s approaching limit (%d/%d), jitter %.2fs",
                self.exchange,
                endpoint,
                current,
                limit,
                jitter,
            )
            await asyncio.sleep(jitter)
            return jitter

        return 0.0

    @staticmethod
    async def startup_jitter(
        min_seconds: float = _STARTUP_JITTER_MIN,
        max_seconds: float = _STARTUP_JITTER_MAX,
    ) -> float:
        """Apply random jitter at startup to stagger multiple bot launches."""
        jitter = random.uniform(min_seconds, max_seconds)
        if jitter > 0:
            logger.info("RateLimitCoordinator: startup jitter %.2fs", jitter)
            await asyncio.sleep(jitter)
        return jitter


def _format_ms(ts: Optional[int]) -> str:
    return time_utils.format_ms(ts)


def _day_key(timestamp_ms: int) -> str:
    return time_utils.day_key(timestamp_ms)


def _merge_fee_lists(
    fees_a: Optional[Sequence], fees_b: Optional[Sequence]
) -> Optional[List[Dict[str, object]]]:
    return fee_utils.merge_fee_lists(fees_a, fees_b)


def _fee_cost(fees: Optional[Sequence]) -> float:
    return fee_utils.fee_cost(fees)


def ensure_qty_signage(events: List[Dict[str, object]]) -> None:
    return position_utils.ensure_qty_signage(events)


def _compute_add_reduce(pos_side: str, qty_signed: float) -> Tuple[float, float]:
    return position_utils.compute_add_reduce(pos_side, qty_signed)


def compute_psize_pprice(
    events: List[Dict[str, object]],
    initial_state: Optional[Dict[Tuple[str, str], Tuple[float, float]]] = None,
) -> Dict[Tuple[str, str], Tuple[float, float]]:
    return position_utils.compute_psize_pprice(events, initial_state)


def annotate_positions_inplace(
    events: List[Dict[str, object]],
    state: Optional[Dict[Tuple[str, str], Tuple[float, float]]] = None,
    *,
    recompute_pnl: bool = False,
) -> Dict[Tuple[str, str], Tuple[float, float]]:
    return position_utils.annotate_positions_inplace(events, state, recompute_pnl=recompute_pnl)


def compute_realized_pnls_from_trades(
    trades: List[Dict[str, object]],
) -> Tuple[Dict[str, float], Dict[Tuple[str, str], Tuple[float, float]]]:
    return position_utils.compute_realized_pnls_from_trades(trades)


def _coalesce_events(events: List[Dict[str, object]]) -> List[Dict[str, object]]:
    return data_model.coalesce_events(events, _merge_fee_lists)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


_normalize_raw_field = data_model.normalize_raw_field


_extract_source_ids = data_model.extract_source_ids


_bybit_trade_dedupe_key = data_model.bybit_trade_dedupe_key


_bybit_trade_qty_abs = data_model.bybit_trade_qty_abs


_bybit_trade_qty_signed = data_model.bybit_trade_qty_signed


_bybit_event_group_key = data_model.bybit_event_group_key


_check_pagination_progress = pagination_utils.check_pagination_progress


FillEvent = data_model.FillEvent


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

# Maximum retry attempts before marking gap as persistent
_GAP_MAX_RETRIES = 3

# Gap confidence levels
GAP_CONFIDENCE_UNKNOWN = 0.0
GAP_CONFIDENCE_SUSPICIOUS = 0.3
GAP_CONFIDENCE_LIKELY_LEGITIMATE = 0.7
GAP_CONFIDENCE_CONFIRMED = 1.0

# Gap reasons
GAP_REASON_AUTO = "auto_detected"
GAP_REASON_FETCH_FAILED = "fetch_failed"
GAP_REASON_CONFIRMED = "confirmed_legitimate"
GAP_REASON_MANUAL = "manual"


class KnownGap(TypedDict, total=False):
    """Gap metadata stored in metadata.json known_gaps."""

    start_ts: int  # Gap start timestamp (ms)
    end_ts: int  # Gap end timestamp (ms)
    retry_count: int  # Number of fetch attempts (max 3)
    reason: str  # auto_detected, fetch_failed, confirmed_legitimate, manual
    added_at: int  # Timestamp when gap was first detected
    confidence: float  # 0.0=unknown, 0.3=suspicious, 0.7=likely_ok, 1.0=confirmed


class CacheMetadata(TypedDict, total=False):
    """Cache metadata stored in metadata.json."""

    last_refresh_ms: int  # Timestamp of last successful refresh
    oldest_event_ts: int  # Oldest event timestamp in cache
    newest_event_ts: int  # Newest event timestamp in cache
    covered_start_ms: int  # Earliest open-ended lookback start confirmed against exchange
    known_gaps: List[KnownGap]  # List of known gaps
    history_scope: str  # unknown, window, all


class FillEventCache:
    """JSON cache storing fills split by UTC day."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self._metadata: Optional[CacheMetadata] = None
        self.logger = logger

    def load(self) -> List[FillEvent]:
        return cache_utils.load_events(self, FillEvent)

    def save(self, events: Sequence[FillEvent]) -> None:
        return cache_utils.save_events(self, events, _day_key)

    def save_days(self, day_events: Dict[str, Sequence[FillEvent]]) -> None:
        return cache_utils.save_days(self, day_events)

    @property
    def metadata_path(self) -> Path:
        return cache_utils.metadata_path(self)

    def load_metadata(self) -> CacheMetadata:
        return cache_utils.load_metadata(self)

    def save_metadata(self, metadata: Optional[CacheMetadata] = None) -> None:
        return cache_utils.save_metadata(self, metadata)

    def update_metadata_from_events(self, events: Sequence[FillEvent]) -> None:
        return cache_utils.update_metadata_from_events(self, events)

    def get_known_gaps(self) -> List[KnownGap]:
        return cache_utils.get_known_gaps(self)

    def get_covered_start_ms(self) -> int:
        return cache_utils.get_covered_start_ms(self)

    def mark_covered_start(self, start_ts: int) -> None:
        return cache_utils.mark_covered_start(self, start_ts)

    def get_history_scope(self) -> str:
        return cache_utils.get_history_scope(self)

    def set_history_scope(self, scope: str) -> None:
        return cache_utils.set_history_scope(self, scope)

    def add_known_gap(
        self,
        start_ts: int,
        end_ts: int,
        *,
        reason: str = GAP_REASON_AUTO,
        confidence: float = GAP_CONFIDENCE_UNKNOWN,
    ) -> None:
        return cache_utils.add_known_gap(
            self,
            start_ts,
            end_ts,
            reason=reason,
            confidence=confidence,
            gap_max_retries=_GAP_MAX_RETRIES,
            likely_legitimate_confidence=GAP_CONFIDENCE_LIKELY_LEGITIMATE,
        )

    def clear_gap(self, start_ts: int, end_ts: int) -> bool:
        return cache_utils.clear_gap(self, start_ts, end_ts)

    def should_retry_gap(self, gap: KnownGap) -> bool:
        return cache_utils.should_retry_gap(gap, _GAP_MAX_RETRIES)

    def get_coverage_summary(self) -> Dict[str, object]:
        return cache_utils.get_coverage_summary(self, gap_max_retries=_GAP_MAX_RETRIES)


# ---------------------------------------------------------------------------
# Fetcher infrastructure
# ---------------------------------------------------------------------------


class BaseFetcher:
    """Abstract interface for exchange-specific fill fetchers."""

    async def fetch(
        self,
        since_ms: Optional[int],
        until_ms: Optional[int],
        detail_cache: Dict[str, Tuple[str, str]],
        on_batch: Optional[Callable[[List[Dict[str, object]]], None]] = None,
    ) -> List[Dict[str, object]]:
        raise NotImplementedError


class FakeFetcher(BaseFetcher):
    """Fetch canonical fill events from the fake exchange ledger."""

    def __init__(self, api) -> None:
        self.api = api

    async def fetch(
        self,
        since_ms: Optional[int],
        until_ms: Optional[int],
        detail_cache: Dict[str, Tuple[str, str]],
        on_batch: Optional[Callable[[List[Dict[str, object]]], None]] = None,
    ) -> List[Dict[str, object]]:
        events = list(self.api.get_fill_events(since_ms, until_ms))
        for event in events:
            cache_entry = detail_cache.get(event["id"])
            if cache_entry:
                event["client_order_id"], event["pb_order_type"] = cache_entry
            elif event["client_order_id"]:
                event["pb_order_type"] = custom_id_to_snake(event["client_order_id"])
            if not event["pb_order_type"]:
                event["pb_order_type"] = "unknown"
        if on_batch and events:
            on_batch(events)
        return events


class BitgetFetcher(BaseFetcher):
    """Fetches and enriches fill events from Bitget."""

    def __init__(
        self,
        api,
        *,
        product_type: str = "USDT-FUTURES",
        history_limit: int = 100,
        detail_calls_per_minute: int = 120,
        detail_concurrency: int = 10,
        now_func: Optional[Callable[[], int]] = None,
        symbol_resolver: Optional[Callable[[Optional[str]], str]] = None,
    ) -> None:
        self.api = api
        self.product_type = product_type
        self.history_limit = history_limit
        self.detail_calls_per_minute = max(1, detail_calls_per_minute)
        self._detail_call_timestamps: deque[int] = deque()
        self.detail_concurrency = max(1, detail_concurrency)
        self._rate_lock = asyncio.Lock()
        self._now_func = now_func or (lambda: int(datetime.now(tz=timezone.utc).timestamp() * 1000))
        if symbol_resolver is None:
            raise ValueError("BitgetFetcher requires a symbol_resolver callable")
        self._symbol_resolver = symbol_resolver

    async def fetch(
        self,
        since_ms: Optional[int],
        until_ms: Optional[int],
        detail_cache: Dict[str, Tuple[str, str]],
        on_batch: Optional[Callable[[List[Dict[str, object]]], None]] = None,
    ) -> List[Dict[str, object]]:
        buffer_step_ms = 24 * 60 * 60 * 1000
        end_time = int(until_ms) if until_ms is not None else self._now_func() + buffer_step_ms
        params: Dict[str, object] = {
            "productType": self.product_type,
            "limit": self.history_limit,
            "endTime": end_time,
        }
        events: Dict[str, Dict[str, object]] = {}

        detail_hits = 0
        detail_fetches = 0
        max_fetches = 400
        fetch_count = 0

        logger.debug(
            "BitgetFetcher.fetch: start (since=%s, until=%s, limit=%d)",
            _format_ms(since_ms),
            _format_ms(until_ms),
            self.history_limit,
        )

        while True:
            if fetch_count >= max_fetches:
                logger.warning(
                    "BitgetFetcher.fetch: reached maximum pagination depth (%d)",
                    max_fetches,
                )
                break
            fetch_count += 1
            payload = await self.api.private_mix_get_v2_mix_order_fill_history(dict(params))
            fill_list = payload.get("data", {}).get("fillList") or []
            if fetch_count > 1:
                logger.debug(
                    "BitgetFetcher.fetch: fetch #%d endTime=%s size=%d",
                    fetch_count,
                    _format_ms(params.get("endTime")),
                    len(fill_list),
                )
            if not fill_list:
                end_param = int(params.get("endTime", self._now_func()))
                new_end_time = bitget_utils.next_end_time_for_empty_batch(
                    end_param, since_ms, buffer_step_ms
                )
                if new_end_time is None:
                    if since_ms is None:
                        logger.debug("BitgetFetcher.fetch: empty batch without start bound; stopping")
                    else:
                        logger.debug(
                            "BitgetFetcher.fetch: empty batch and cursor reached start; stopping"
                        )
                    break
                params["endTime"] = new_end_time
                logger.debug(
                    "BitgetFetcher.fetch: empty batch, continuing with endTime=%s",
                    _format_ms(params["endTime"]),
                )
                continue
            logger.debug(
                "BitgetFetcher.fetch: received batch size=%d endTime=%s",
                len(fill_list),
                params.get("endTime"),
            )
            batch_ids, batch_detail_hits, batch_detail_fetches = await bitget_utils.process_fill_batch(
                self, fill_list, detail_cache, events
            )
            detail_hits += batch_detail_hits
            detail_fetches += batch_detail_fetches
            if on_batch:
                batch_events = bitget_utils.build_batch_events(events, batch_ids)
                if batch_events:
                    on_batch(batch_events)
            oldest = bitget_utils.oldest_fill_timestamp(fill_list)
            if len(fill_list) < self.history_limit:
                end_param = int(params.get("endTime", oldest))
                new_end_time = bitget_utils.next_end_time_for_short_batch(
                    fill_list, end_param, since_ms, buffer_step_ms
                )
                if new_end_time is None:
                    if since_ms is None:
                        logger.debug(
                            "BitgetFetcher.fetch: short batch size=%d without start bound; stopping",
                            len(fill_list),
                        )
                    elif end_param - since_ms < buffer_step_ms:
                        logger.debug(
                            "BitgetFetcher.fetch: short batch size=%d close to requested start; stopping",
                            len(fill_list),
                        )
                    else:
                        logger.debug(
                            "BitgetFetcher.fetch: rewound endTime to start boundary; stopping",
                        )
                    break
                params["endTime"] = new_end_time
                logger.debug(
                    "BitgetFetcher.fetch: short batch size=%d, continuing with endTime=%s",
                    len(fill_list),
                    _format_ms(params["endTime"]),
                )
                continue
            first_ts = min(ev["timestamp"] for ev in events.values()) if events else None
            if since_ms is not None and first_ts is not None and first_ts <= since_ms:
                break
            params["endTime"] = max(since_ms or oldest, oldest - 1)

        ordered = sorted(events.values(), key=lambda ev: ev["timestamp"])
        if since_ms is not None:
            ordered = [ev for ev in ordered if ev["timestamp"] >= since_ms]
        if until_ms is not None:
            ordered = [ev for ev in ordered if ev["timestamp"] <= until_ms]
        logger.debug(
            "BitgetFetcher.fetch: done (events=%d, detail_cache_hits=%d, detail_fetches=%d)",
            len(ordered),
            detail_hits,
            detail_fetches,
        )
        return ordered

    async def _enrich_with_details(
        self,
        event: Dict[str, object],
        cache: Dict[str, Tuple[str, str]],
    ) -> int:
        if not event.get("order_id"):
            return 0
        logger.debug(
            "BitgetFetcher._enrich_with_details: fetching detail for order %s %s",
            event["order_id"],
            event.get("datetime"),
        )
        await self._respect_rate_limit()
        try:
            order_details = await self.api.private_mix_get_v2_mix_order_detail(
                {
                    "productType": self.product_type,
                    "orderId": event["order_id"],
                    "symbol": event["symbol_external"],
                }
            )
        except Exception as exc:
            logger.error(
                "BitgetFetcher._enrich_with_details: detail fetch failed for %s (%s)",
                event.get("order_id"),
                exc,
            )
            return 0
        client_oid = (
            order_details.get("data", {}).get("clientOid")
            if isinstance(order_details, dict)
            else None
        )
        if client_oid:
            pb_type = custom_id_to_snake(client_oid)
            bitget_utils.apply_detail_result(event, cache, (client_oid, pb_type))
            logger.debug(
                "BitgetFetcher._enrich_with_details: cached clientOid=%s for trade %s, pb_order_type %s",
                client_oid,
                event["id"],
                pb_type,
            )
            return 1
        else:
            logger.debug(
                "BitgetFetcher._enrich_with_details: no clientOid returned for order %s",
                event["order_id"],
            )
            return 1

    async def _respect_rate_limit(self) -> None:
        window_ms = 60_000
        max_calls = self.detail_calls_per_minute
        q = self._detail_call_timestamps
        while True:
            async with self._rate_lock:
                now = self._now_func()
                window_start = now - window_ms
                while q and q[0] <= window_start:
                    q.popleft()
                if len(q) < max_calls:
                    q.append(now)
                    return
                wait_ms = q[0] + window_ms - now
            if wait_ms > 0:
                logger.debug(
                    "BitgetFetcher._respect_rate_limit: sleeping %.3fs to respect %d calls/min",
                    wait_ms / 1000,
                    max_calls,
                )
                await asyncio.sleep(wait_ms / 1000)
            else:
                await asyncio.sleep(0)

    async def _flush_detail_tasks(self, tasks: List[asyncio.Task[int]]) -> int:
        if not tasks:
            return 0
        results = await asyncio.gather(*tasks)
        tasks.clear()
        total = 0
        for res in results:
            total += res or 0
        return total

    def _normalize_fill(self, raw: Dict[str, object]) -> Dict[str, object]:
        return bitget_utils.normalize_fill(self, raw, deduce_side_pside)

    def _resolve_symbol(self, market_symbol: Optional[str]) -> str:
        return bitget_utils.resolve_symbol(self, market_symbol)


class BinanceFetcher(BaseFetcher):
    """Fetch realised PnL events for Binance by combining income and trade history."""

    def __init__(
        self,
        api,
        *,
        symbol_resolver: Callable[[str], str],
        now_func: Optional[Callable[[], int]] = None,
        positions_provider: Optional[Callable[[], Iterable[str]]] = None,
        open_orders_provider: Optional[Callable[[], Iterable[str]]] = None,
        income_limit: int = 1000,
        trade_limit: int = 1000,
    ) -> None:
        self.api = api
        if symbol_resolver is None:
            raise ValueError("BinanceFetcher requires a symbol_resolver callable")
        self._symbol_resolver = symbol_resolver
        self._positions_provider = positions_provider or (lambda: ())
        self._open_orders_provider = open_orders_provider or (lambda: ())
        self.income_limit = min(1000, max(1, income_limit))  # cap to max 1000
        self._now_func = now_func or (lambda: int(datetime.now(tz=timezone.utc).timestamp() * 1000))
        self.trade_limit = max(1, trade_limit)
        self._unsupported_symbols: set[str] = set()
        self._market_symbols: Optional[set[str]] = None
        self._markets_loaded = False

    async def _get_market_symbols(self) -> Optional[set[str]]:
        if self._market_symbols is not None:
            return self._market_symbols
        symbols = getattr(self.api, "symbols", None)
        markets = getattr(self.api, "markets", None)
        if (not symbols and not markets) and not self._markets_loaded:
            try:
                await self.api.load_markets()
                self._markets_loaded = True
            except Exception:
                return None
            symbols = getattr(self.api, "symbols", None)
            markets = getattr(self.api, "markets", None)
        if symbols:
            self._market_symbols = set(symbols)
        elif markets:
            self._market_symbols = set(markets.keys())
        else:
            self._market_symbols = None
        return self._market_symbols

    def _note_unsupported_symbol(self, symbol: str) -> None:
        if symbol in self._unsupported_symbols:
            return
        self._unsupported_symbols.add(symbol)
        logger.debug("[fills] BinanceFetcher skipping unsupported symbol %s", symbol)

    async def fetch(
        self,
        since_ms: Optional[int],
        until_ms: Optional[int],
        detail_cache: Dict[str, Tuple[str, str]],
        on_batch: Optional[Callable[[List[Dict[str, object]]], None]] = None,
    ) -> List[Dict[str, object]]:
        logger.debug(
            "BinanceFetcher.fetch: start since=%s until=%s",
            _format_ms(since_ms),
            _format_ms(until_ms),
        )
        income_events = await self._fetch_income(since_ms, until_ms)
        symbol_pool = set(self._collect_symbols(self._positions_provider))
        symbol_pool.update(self._collect_symbols(self._open_orders_provider))
        symbol_pool.update(ev["symbol"] for ev in income_events if ev.get("symbol"))
        if detail_cache is None:
            detail_cache = {}

        supported_symbols = await self._get_market_symbols()
        if supported_symbols:
            unsupported = [sym for sym in symbol_pool if sym not in supported_symbols]
            for sym in unsupported:
                self._note_unsupported_symbol(sym)
            symbol_pool = {sym for sym in symbol_pool if sym in supported_symbols}

        trade_events: Dict[str, Dict[str, object]] = {}
        trade_tasks: Dict[str, asyncio.Task[List[Dict[str, object]]]] = {}
        for symbol in sorted(symbol_pool):
            if not symbol:
                continue
            trade_tasks[symbol] = asyncio.create_task(
                self._fetch_symbol_trades(symbol, since_ms, until_ms)
            )
        for symbol, task in trade_tasks.items():
            try:
                trades = await task
            except RateLimitExceeded as exc:  # pragma: no cover - depends on live API
                logger.warning(
                    "BinanceFetcher.fetch: rate-limited fetching trades for %s (%s)", symbol, exc
                )
                trades = []
            except Exception as exc:
                logger.error("BinanceFetcher.fetch: error fetching trades for %s (%s)", symbol, exc)
                trades = []
            for trade in trades:
                event = self._normalize_trade(trade)
                cached = detail_cache.get(event["id"])
                if cached:
                    event.setdefault("client_order_id", cached[0])
                    if cached[1]:
                        event.setdefault("pb_order_type", cached[1])
                trade_events[event["id"]] = event

        merged: Dict[str, Dict[str, object]] = {}
        for ev in income_events:
            merged[ev["id"]] = ev

        def _event_from_trade(trade: Dict[str, object]) -> Dict[str, object]:
            symbol = trade.get("symbol") or self._resolve_symbol(trade.get("info", {}).get("symbol"))
            timestamp = int(trade.get("timestamp") or 0)
            client_oid = trade.get("client_order_id") or ""
            event: Dict[str, object] = {
                "id": str(trade.get("id")),
                "timestamp": timestamp,
                "datetime": ts_to_date(timestamp) if timestamp else "",
                "symbol": symbol or "",
                "side": trade.get("side") or "",
                "qty": float(trade.get("qty") or 0.0),
                "price": float(trade.get("price") or 0.0),
                "pnl": float(trade.get("pnl") or 0.0),
                "fees": trade.get("fees"),
                "pb_order_type": trade.get("pb_order_type") or "",
                "position_side": trade.get("position_side") or "unknown",
                "client_order_id": client_oid,
                "order_id": trade.get("order_id") or "",
                "info": trade.get("info"),
            }
            return event

        def _merge_trade_into_event(event: Dict[str, object], trade: Dict[str, object]) -> None:
            if not event.get("symbol") and trade.get("symbol"):
                event["symbol"] = trade["symbol"]
            if not event.get("side") and trade.get("side"):
                event["side"] = trade["side"]
            if float(event.get("qty", 0.0)) == 0.0 and trade.get("qty") is not None:
                event["qty"] = float(trade.get("qty", 0.0))
            if float(event.get("price", 0.0)) == 0.0 and trade.get("price") is not None:
                event["price"] = float(trade.get("price", 0.0))
            if not event.get("fees") and trade.get("fees"):
                event["fees"] = trade["fees"]
            if (event.get("position_side") in (None, "", "unknown")) and trade.get("position_side"):
                event["position_side"] = trade["position_side"]
            if trade.get("client_order_id"):
                event["client_order_id"] = trade["client_order_id"]
            if trade.get("order_id"):
                event["order_id"] = trade["order_id"]
            if trade.get("info"):
                event["info"] = trade["info"]
            if trade.get("pb_order_type"):
                event["pb_order_type"] = trade["pb_order_type"]

        if trade_events:
            for event_id, trade in trade_events.items():
                if event_id not in merged:
                    merged[event_id] = _event_from_trade(trade)
                event = merged[event_id]
                _merge_trade_into_event(event, trade)

        for event_id, event in merged.items():
            cached = detail_cache.get(event_id)
            if cached:
                client_oid, pb_type = cached
                if client_oid:
                    event["client_order_id"] = client_oid
                if pb_type and pb_type != "unknown":
                    event["pb_order_type"] = pb_type

        enrichment_targets = binance_utils.collect_enrichment_targets(merged, trade_events)
        if enrichment_targets:
            async def enrich_target(target: Tuple[Dict[str, object], str, str, str]):
                event, event_id, order_id, symbol = target
                try:
                    result = await self._enrich_with_order_details(order_id, symbol)
                except Exception as exc:
                    logger.debug(
                        "BinanceFetcher.fetch: fetch_order failed for %s (%s)",
                        event.get("id"),
                        exc,
                    )
                    result = None
                return event, event_id, result

            detail_results = await asyncio.gather(
                *(enrich_target(target) for target in enrichment_targets)
            )
            for event, event_id, result in detail_results:
                binance_utils.apply_enrichment_result(event, event_id, result, detail_cache)

        binance_utils.finalize_merged_events(merged, detail_cache, custom_id_to_snake)

        ordered = sorted(merged.values(), key=lambda ev: ev["timestamp"])
        if since_ms is not None:
            ordered = [ev for ev in ordered if ev["timestamp"] >= since_ms]
        if until_ms is not None:
            ordered = [ev for ev in ordered if ev["timestamp"] <= until_ms]

        if on_batch and ordered:
            on_batch(ordered)

        logger.debug(
            "BinanceFetcher.fetch: done events=%d (symbols=%d)",
            len(ordered),
            len(symbol_pool),
        )
        return ordered

    async def _enrich_with_order_details(
        self,
        order_id: Optional[str],
        symbol: Optional[str],
    ) -> Optional[Tuple[str, str]]:
        if not order_id or not symbol:
            return None
        try:
            detail = await self.api.fetch_order(order_id, symbol)
        except Exception as exc:  # pragma: no cover - live API dependent
            logger.debug(
                "BinanceFetcher._enrich_with_order_details: fetch_order failed for %s (%s)",
                order_id,
                exc,
            )
            return None
        info = detail.get("info") if isinstance(detail, dict) else detail
        if not isinstance(info, dict):
            return None
        client_oid = info.get("clientOrderId") or info.get("clientOrderID")
        if not client_oid:
            return None
        client_oid = str(client_oid)
        return client_oid, custom_id_to_snake(client_oid)

    async def _fetch_income(
        self,
        since_ms: Optional[int],
        until_ms: Optional[int],
    ) -> List[Dict[str, object]]:
        params: Dict[str, object] = {"incomeType": "REALIZED_PNL", "limit": self.income_limit}
        if until_ms is None:
            if since_ms is None:
                logger.debug(f"BinanceFetcher._fetch_income.fapiprivate_get_income params={params}")
                payload = await self.api.fapiprivate_get_income(params=params)
                return sorted(
                    [self._normalize_income(x) for x in payload], key=lambda x: x["timestamp"]
                )
            until_ms = self._now_func() + 1000 * 60 * 60
        week_buffer_ms = 1000 * 60 * 60 * 24 * 6.95
        params["startTime"] = int(since_ms)
        params["endTime"] = int(min(until_ms, since_ms + week_buffer_ms))
        events = []
        previous_key: Optional[Tuple[Tuple[str, object], ...]] = None
        fetch_count = 0
        while True:
            key = _check_pagination_progress(
                previous_key,
                params,
                "BinanceFetcher._fetch_income",
            )
            if key is None:
                break
            previous_key = key
            fetch_count += 1
            payload = await self.api.fapiprivate_get_income(params=params)
            if fetch_count > 1:
                payload_size = len(payload) if payload else 0
                # Only log at INFO when there's actual data; DEBUG otherwise
                log_fn = logger.info if payload_size > 0 else logger.debug
                log_fn(
                    "BinanceFetcher._fetch_income: fetch #%d startTime=%s endTime=%s size=%d",
                    fetch_count,
                    _format_ms(params.get("startTime")),
                    _format_ms(params.get("endTime")),
                    payload_size,
                )
            if payload == []:
                if params["startTime"] + week_buffer_ms >= until_ms:
                    break
                params["startTime"] = int(params["startTime"] + week_buffer_ms)
                params["endTime"] = int(min(until_ms, params["startTime"] + week_buffer_ms))
                continue
            events.extend(
                sorted([self._normalize_income(x) for x in payload], key=lambda x: x["timestamp"])
            )
            params["startTime"] = int(events[-1]["timestamp"]) + 1
            params["endTime"] = int(min(until_ms, params["startTime"] + week_buffer_ms))
            if params["startTime"] > until_ms:
                break
        return events

    async def _fetch_symbol_trades(
        self,
        ccxt_symbol: str,
        since_ms: Optional[int],
        until_ms: Optional[int],
    ) -> List[Dict[str, object]]:
        limit = min(1000, max(1, self.trade_limit))
        try:
            if since_ms is None and until_ms is None:
                return await self.api.fetch_my_trades(ccxt_symbol, limit=limit)

            end_bound = until_ms or self._now_func()
            start_bound = since_ms or max(0, end_bound - 7 * 24 * 60 * 60 * 1000)
            week_span = int(7 * 24 * 60 * 60 * 1000 * 0.99)
            params: Dict[str, object] = {}
            fetched: Dict[str, Dict[str, object]] = {}
            previous_key: Optional[Tuple[Tuple[str, object], ...]] = None
            fetch_count = 0

            cursor = int(start_bound)
            while cursor <= end_bound:
                window_end = int(min(end_bound, cursor + week_span))
                params["startTime"] = cursor
                params["endTime"] = window_end
                param_key = _check_pagination_progress(
                    previous_key,
                    params,
                    f"BinanceFetcher._fetch_symbol_trades({ccxt_symbol})",
                )
                if param_key is None:
                    break
                previous_key = param_key
                fetch_count += 1
                batch = await self.api.fetch_my_trades(
                    ccxt_symbol,
                    limit=limit,
                    params=dict(params),
                )
                if fetch_count > 1:
                    batch_size = len(batch) if batch else 0
                    # Only log at INFO when there's actual data; DEBUG otherwise
                    log_fn = logger.info if batch_size > 0 else logger.debug
                    log_fn(
                        "BinanceFetcher._fetch_symbol_trades: fetch #%d symbol=%s start=%s end=%s size=%d",
                        fetch_count,
                        ccxt_symbol,
                        _format_ms(params["startTime"]),
                        _format_ms(params["endTime"]),
                        batch_size,
                    )
                if not batch:
                    cursor = window_end + 1
                    continue
                for trade in batch:
                    trade_id = str(
                        trade.get("id")
                        or (trade.get("info") or {}).get("id")
                        or f"{trade.get('order')}-{trade.get('timestamp')}"
                    )
                    fetched[trade_id] = trade
                last_ts = int(
                    batch[-1].get("timestamp")
                    or (batch[-1].get("info") or {}).get("time")
                    or params["endTime"]
                )
                if last_ts >= end_bound or len(batch) < limit:
                    cursor = last_ts + 1
                    if cursor > end_bound:
                        break
                else:
                    cursor = last_ts + 1

            ordered = sorted(
                fetched.values(),
                key=lambda tr: int(tr.get("timestamp") or (tr.get("info") or {}).get("time") or 0),
            )
            return ordered
        except Exception as exc:  # pragma: no cover - depends on live API
            msg = str(exc).lower() if exc else ""
            if "does not have market symbol" in msg or "market symbol" in msg:
                self._note_unsupported_symbol(ccxt_symbol)
                return []
            logger.error("BinanceFetcher._fetch_symbol_trades: error %s (%s)", ccxt_symbol, exc)
            return []

    def _normalize_income(self, entry: Dict[str, object]) -> Dict[str, object]:
        return binance_utils.normalize_income(self, entry)

    def _normalize_trade(self, trade: Dict[str, object]) -> Dict[str, object]:
        return binance_utils.normalize_trade(self, trade)

    def _collect_symbols(self, provider: Callable[[], Iterable[str]]) -> List[str]:
        return binance_utils.collect_symbols(self, provider)

    def _resolve_symbol(self, value: Optional[str]) -> str:
        return binance_utils.resolve_symbol(self, value)


# ---------------------------------------------------------------------------
# Manager
# ---------------------------------------------------------------------------


class FillEventsManager:
    """High-level interface around cached/fetched fill events."""

    def __init__(
        self,
        *,
        exchange: str,
        user: str,
        fetcher: BaseFetcher,
        cache_path: Path,
        rate_limit_coordinator: Optional[RateLimitCoordinator] = None,
    ) -> None:
        self.exchange = exchange
        self.user = user
        self.fetcher = fetcher
        self.cache = FillEventCache(cache_path)
        self.rate_limiter = rate_limit_coordinator or RateLimitCoordinator(exchange, user)
        self._events: List[FillEvent] = []
        self._loaded = False
        self._lock = asyncio.Lock()

    async def ensure_loaded(self) -> None:
        if self._loaded:
            return
        async with self._lock:
            if self._loaded:
                return
            cached = self.cache.load()
            filtered = []
            dropped = 0
            for ev in cached:
                if getattr(ev, "raw", None) is None:
                    dropped += 1
                    continue
                filtered.append(ev)
            self._events = sorted(filtered, key=lambda ev: ev.timestamp)

            # Annotate psize/pprice for legacy caches that may lack these values
            if self._events:
                payload = [ev.to_dict() for ev in self._events]
                ensure_qty_signage(payload)
                compute_psize_pprice(payload)
                self._events = [FillEvent.from_dict(ev) for ev in payload]

            logger.info(
                "[fills] ensure_loaded: %d cached events (dropped %d without raw)",
                len(self._events),
                dropped,
            )
            self._loaded = True

    @staticmethod
    def _bybit_event_trade_rows(event: FillEvent) -> List[Dict[str, object]]:
        rows: List[Dict[str, object]] = []
        for item in _normalize_raw_field(getattr(event, "raw", None)):
            if not isinstance(item, dict):
                continue
            if item.get("source") != "fetch_my_trades":
                continue
            data = item.get("data")
            if isinstance(data, dict):
                rows.append(data)
        return rows

    @staticmethod
    def _bybit_event_non_trade_raw(event: FillEvent) -> List[Dict[str, object]]:
        rows: List[Dict[str, object]] = []
        for item in _normalize_raw_field(getattr(event, "raw", None)):
            if not isinstance(item, dict):
                continue
            if item.get("source") == "fetch_my_trades":
                continue
            rows.append(item)
        return rows

    @staticmethod
    def _bybit_group_stats(events: Sequence[FillEvent]) -> Dict[str, object]:
        unique_rows: Dict[Tuple[object, ...], Dict[str, object]] = {}
        fallback_idx = 0
        duplicate_rows = 0
        for ev in events:
            for row in FillEventsManager._bybit_event_trade_rows(ev):
                key = _bybit_trade_dedupe_key(row)
                if key is None:
                    key = ("__fallback__", fallback_idx)
                    fallback_idx += 1
                if key in unique_rows:
                    duplicate_rows += 1
                    continue
                unique_rows[key] = row

        unique_qty_abs = sum(_bybit_trade_qty_abs(row) for row in unique_rows.values())
        side = str(events[0].side).lower() if events else "buy"
        unique_qty_signed = -unique_qty_abs if side == "sell" else unique_qty_abs
        group_qty = sum(float(ev.qty) for ev in events)
        return {
            "duplicate_rows": duplicate_rows,
            "group_size": len(events),
            "group_qty": group_qty,
            "unique_qty_signed": unique_qty_signed,
            "unique_row_count": len(unique_rows),
        }

    @staticmethod
    def _scan_bybit_qty_inflation(events: Sequence[FillEvent]) -> List[Dict[str, object]]:
        anomalies: List[Dict[str, object]] = []
        tolerance = 1e-9

        grouped: Dict[Tuple[int, str, str, str, str], List[FillEvent]] = defaultdict(list)
        for ev in events:
            grouped[_bybit_event_group_key(ev)].append(ev)

        for key, group in grouped.items():
            stats = FillEventsManager._bybit_group_stats(group)
            group_size = int(stats["group_size"])
            duplicate_rows = int(stats["duplicate_rows"])
            group_qty = float(stats["group_qty"])
            unique_qty_signed = float(stats["unique_qty_signed"])
            if group_size <= 1 and duplicate_rows <= 0:
                continue
            if abs(group_qty - unique_qty_signed) <= tolerance and group_size == 1:
                continue
            anomalies.append(
                {
                    "key": key,
                    "event_ids": [ev.id for ev in group],
                    "group_size": group_size,
                    "duplicate_rows": duplicate_rows,
                    "group_qty": group_qty,
                    "expected_qty": unique_qty_signed,
                    "unique_trade_rows": int(stats["unique_row_count"]),
                }
            )
        return anomalies

    @staticmethod
    def _normalize_fee_dict(fee: Optional[Dict[str, object]]) -> Optional[Dict[str, object]]:
        return fee_utils.normalize_fee_dict(fee)

    @staticmethod
    def _extract_bybit_fee_from_trade_row(row: Dict[str, object]) -> Optional[Dict[str, object]]:
        return fee_utils.extract_bybit_fee_from_trade_row(
            row, normalize_fee_dict_fn=FillEventsManager._normalize_fee_dict
        )

    @staticmethod
    def _dedupe_raw_payloads(items: Sequence[Dict[str, object]]) -> List[Dict[str, object]]:
        deduped: List[Dict[str, object]] = []
        seen: set[str] = set()
        for item in items:
            try:
                marker = json.dumps(item, sort_keys=True, separators=(",", ":"))
            except Exception:
                marker = str(item)
            if marker in seen:
                continue
            seen.add(marker)
            deduped.append(item)
        return deduped

    @staticmethod
    def _build_consolidated_bybit_event(group: Sequence[FillEvent]) -> FillEvent:
        # Pick best baseline event (prefer internally deduped, then largest unique coverage).
        best_event = group[0]
        best_rank: Tuple[int, int, float] = (-1, -1, float("-inf"))
        mt_unique_by_key: Dict[Tuple[object, ...], Dict[str, object]] = {}
        non_mt_rows: List[Dict[str, object]] = []
        fallback_idx = 0

        for ev in group:
            mt_rows = FillEventsManager._bybit_event_trade_rows(ev)
            keys_seen: set[Tuple[object, ...]] = set()
            duplicates = 0
            unique_count = 0
            signed_qty_sum = 0.0
            for row in mt_rows:
                key = _bybit_trade_dedupe_key(row)
                if key is None:
                    key = ("__fallback__", fallback_idx)
                    fallback_idx += 1
                if key in keys_seen:
                    duplicates += 1
                    continue
                keys_seen.add(key)
                unique_count += 1
                signed_qty_sum += _bybit_trade_qty_signed(row)
                if key not in mt_unique_by_key:
                    mt_unique_by_key[key] = row
            qty_delta = -abs(float(ev.qty) - signed_qty_sum)
            rank = (1 if duplicates == 0 else 0, unique_count, qty_delta)
            if rank > best_rank:
                best_rank = rank
                best_event = ev
            non_mt_rows.extend(FillEventsManager._bybit_event_non_trade_raw(ev))

        mt_rows_unique = list(mt_unique_by_key.values())
        side = str(best_event.side).lower()
        qty_abs_sum = sum(_bybit_trade_qty_abs(row) for row in mt_rows_unique)
        qty_signed_sum = -qty_abs_sum if side == "sell" else qty_abs_sum

        price_num = 0.0
        for row in mt_rows_unique:
            info = row.get("info")
            info = info if isinstance(info, dict) else {}
            price = float(row.get("price") or info.get("execPrice") or 0.0)
            price_num += price * _bybit_trade_qty_abs(row)
        price = float(best_event.price)
        if qty_abs_sum > 0.0:
            price = price_num / qty_abs_sum

        fees_merged = None
        for row in mt_rows_unique:
            fee = FillEventsManager._extract_bybit_fee_from_trade_row(row)
            fees_merged = _merge_fee_lists(fees_merged, fee)
        fees_out: Optional[Sequence]
        if isinstance(fees_merged, list) and len(fees_merged) == 1:
            fees_out = fees_merged[0]
        else:
            fees_out = fees_merged

        source_ids: set[str] = set(best_event.source_ids or [])
        for row in mt_rows_unique:
            info = row.get("info")
            info = info if isinstance(info, dict) else {}
            trade_id = row.get("id") or info.get("execId")
            if trade_id:
                source_ids.add(str(trade_id))
        source_ids_sorted = sorted(source_ids)
        event_id = "+".join(source_ids_sorted) if source_ids_sorted else best_event.id

        # Recompute close PnL when possible from positions_history + unique fills.
        pnl: Optional[float] = None
        positions_items = [
            row
            for row in non_mt_rows
            if isinstance(row, dict) and str(row.get("source")) == "positions_history"
        ]
        for pos_item in positions_items:
            data = pos_item.get("data")
            if not isinstance(data, dict):
                continue
            info = data.get("info")
            info = info if isinstance(info, dict) else {}
            avg_entry = float(info.get("avgEntryPrice") or data.get("entryPrice") or 0.0)
            total_closed = float(info.get("closedSize") or data.get("contracts") or 0.0)
            if avg_entry <= 0.0 or total_closed <= 0.0:
                continue
            total_fees = float(info.get("closeFee") or 0.0) + float(info.get("openFee") or 0.0)
            recomputed = 0.0
            used = False
            for row in mt_rows_unique:
                info_row = row.get("info")
                info_row = info_row if isinstance(info_row, dict) else {}
                closed_size = float(info_row.get("closedSize") or info_row.get("closeSize") or 0.0)
                if closed_size <= 0.0:
                    continue
                exit_price = float(row.get("price") or info_row.get("execPrice") or 0.0)
                if exit_price <= 0.0:
                    continue
                if str(best_event.position_side).lower() == "long":
                    gross = (exit_price - avg_entry) * closed_size
                else:
                    gross = (avg_entry - exit_price) * closed_size
                fee_portion = (closed_size / total_closed) * total_fees if total_closed > 0.0 else 0.0
                recomputed += gross - fee_portion
                used = True
            if used:
                pnl = recomputed
                break
        if pnl is None:
            if abs(float(best_event.qty)) > 1e-12:
                pnl = float(best_event.pnl) * (qty_signed_sum / float(best_event.qty))
            else:
                pnl = float(best_event.pnl)

        raw_payload = [
            {"source": "fetch_my_trades", "data": dict(row)} for row in mt_rows_unique
        ] + FillEventsManager._dedupe_raw_payloads(non_mt_rows)

        return FillEvent(
            id=event_id,
            source_ids=source_ids_sorted,
            timestamp=int(best_event.timestamp),
            datetime=str(best_event.datetime),
            symbol=str(best_event.symbol),
            side=str(best_event.side).lower(),
            qty=float(qty_signed_sum),
            price=float(price),
            pnl=float(pnl),
            fees=fees_out,
            pb_order_type=str(best_event.pb_order_type),
            position_side=str(best_event.position_side).lower(),
            client_order_id=str(best_event.client_order_id),
            psize=float(best_event.psize),
            pprice=float(best_event.pprice),
            raw=raw_payload,
        )

    async def run_doctor(self, *, auto_repair: bool = False) -> Dict[str, object]:
        """Detect and optionally auto-repair known fill-event cache anomalies."""
        await self.ensure_loaded()
        report: Dict[str, object] = {
            "exchange": self.exchange,
            "user": self.user,
            "events_scanned": len(self._events),
            "anomaly_events": 0,
            "anomaly_examples": [],
            "auto_repair": bool(auto_repair),
            "repaired": False,
        }
        if self.exchange.lower() != "bybit":
            return report

        anomalies = self._scan_bybit_qty_inflation(self._events)
        report["anomaly_events"] = len(anomalies)
        report["anomaly_examples"] = anomalies[:5]
        if not anomalies or not auto_repair:
            return report

        grouped: Dict[Tuple[int, str, str, str, str], List[FillEvent]] = defaultdict(list)
        for ev in self._events:
            grouped[_bybit_event_group_key(ev)].append(ev)

        repaired_events: List[FillEvent] = []
        for key in sorted(grouped.keys()):
            group = grouped[key]
            if len(group) == 1:
                stats = self._bybit_group_stats(group)
                if int(stats["duplicate_rows"]) <= 0:
                    repaired_events.extend(group)
                    continue
            repaired_events.append(self._build_consolidated_bybit_event(group))

        repaired_events.sort(key=lambda ev: ev.timestamp)
        payload = [ev.to_dict() for ev in repaired_events]
        ensure_qty_signage(payload)
        compute_psize_pprice(payload)
        self._events = [FillEvent.from_dict(ev) for ev in payload]
        self.cache.save(self._events)
        self.cache.update_metadata_from_events(self._events)

        remaining = self._scan_bybit_qty_inflation(self._events)
        report["anomaly_events_after"] = len(remaining)
        report["anomaly_examples_after"] = remaining[:5]
        report["repaired"] = len(remaining) == 0
        if remaining:
            logger.warning(
                "[fills-doctor] repair incomplete: %d anomalies remain (continuing)",
                len(remaining),
            )
        else:
            logger.info("[fills-doctor] repair complete; no remaining Bybit anomalies")
        return report

    async def refresh(
        self,
        *,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
    ) -> None:
        await self.ensure_loaded()
        logger.debug(
            "[fills] refresh: start=%s end=%s current_cache=%d",
            _format_ms(start_ms),
            _format_ms(end_ms),
            len(self._events),
        )
        detail_cache = {
            ev.id: (ev.client_order_id, ev.pb_order_type) for ev in self._events if ev.client_order_id
        }
        updated_map: Dict[str, FillEvent] = {ev.id: ev for ev in self._events}
        source_ids_index: Dict[Tuple[str, ...], set[str]] = defaultdict(set)
        for ev in self._events:
            if ev.source_ids:
                source_ids_index[tuple(ev.source_ids)].add(ev.id)
        added_ids: set[str] = set()
        all_days_persisted: set[str] = set()

        def handle_batch(batch: List[Dict[str, object]]) -> None:
            ensure_qty_signage(batch)
            days_touched: set[str] = set()
            for raw in batch:
                raw.setdefault("raw", [])
                try:
                    _validate_required_fill_event_payload(raw)
                    event = FillEvent.from_dict(raw)
                except ValueError as exc:
                    raise ValueError(
                        "[fills] malformed canonical event "
                        f"exchange={self.exchange} event_id={raw.get('id')!r}: {exc}"
                    )
                source_key = tuple(event.source_ids) if event.source_ids else tuple()
                replaced_ids: set[str] = set()
                if source_key and source_key in source_ids_index:
                    replaced_ids = {eid for eid in source_ids_index[source_key] if eid != event.id}
                    for replaced_id in replaced_ids:
                        updated_map.pop(replaced_id, None)
                    source_ids_index[source_key] = {event.id}
                prev = updated_map.get(event.id)
                if prev is not None and event.timestamp < prev.timestamp:
                    continue
                updated_map[event.id] = event
                if source_key:
                    source_ids_index[source_key].add(event.id)
                if prev is None and not replaced_ids:
                    added_ids.add(event.id)
                day = _day_key(event.timestamp)
                days_touched.add(day)
            if not days_touched:
                return
            day_payload = self._events_for_days(updated_map.values(), days_touched)
            self.cache.save_days(day_payload)
            all_days_persisted.update(days_touched)

        try:
            await self.fetcher.fetch(start_ms, end_ms, detail_cache, on_batch=handle_batch)
        except RateLimitExceeded:
            # Preserve bounded-range failures as known gaps so retry logic can
            # revisit them.  We still re-raise to fail loudly on critical input.
            if start_ms is not None and end_ms is not None:
                self.cache.add_known_gap(
                    start_ms,
                    end_ms,
                    reason=GAP_REASON_FETCH_FAILED,
                    confidence=GAP_CONFIDENCE_UNKNOWN,
                )
            raise

        self._events = sorted(updated_map.values(), key=lambda ev: ev.timestamp)

        # Annotate psize/pprice for all events
        if self._events:
            payload = [ev.to_dict() for ev in self._events]
            ensure_qty_signage(payload)
            compute_psize_pprice(payload)
            self._events = [FillEvent.from_dict(ev) for ev in payload]

            # Re-persist touched days with annotated psize/pprice values
            if all_days_persisted:
                day_payload = self._events_for_days(self._events, all_days_persisted)
                self.cache.save_days(day_payload)

        # Update cache metadata with timestamps
        if self._events:
            self.cache.update_metadata_from_events(self._events)

            # If we successfully fetched data for a gap range, clear it
            if start_ms is not None and end_ms is not None and added_ids:
                self.cache.clear_gap(start_ms, end_ms)

        # Consolidated refresh summary log
        # Only log at INFO when there are actually new fills; routine refreshes go to DEBUG
        if added_ids:
            days_list = sorted(all_days_persisted)
            days_preview = ", ".join(days_list[:5])
            if len(days_list) > 5:
                days_preview += f", ... ({len(days_list)} total)"
            logger.info(
                "[fills] refresh: events=%d (+%d) | persisted %d days (%s)",
                len(self._events),
                len(added_ids),
                len(all_days_persisted),
                days_preview,
            )
        else:
            logger.debug("[fills] refresh: events=%d (no changes)", len(self._events))

    async def refresh_latest(self, *, overlap: int = 20) -> None:
        """Fetch only the most recent fills, overlapping by `overlap` events."""
        await self.ensure_loaded()
        if not self._events:
            logger.debug("[fills] refresh_latest: cache empty, falling back to full refresh")
        start_ms = None
        if self._events:
            idx = max(0, len(self._events) - overlap)
            start_ms = self._events[idx].timestamp
        await self.refresh(start_ms=start_ms, end_ms=None)

    async def refresh_for_lookback(
        self,
        start_ms: int,
        *,
        end_ms: Optional[int] = None,
        overlap: int = 20,
        gap_hours: float = 12.0,
        force_refetch_gaps: bool = False,
    ) -> None:
        """Refresh fills for a requested lookback window using cache-derived coverage.

        Open-ended lookbacks are tracked in cache metadata so bots can avoid
        re-running the same expensive history bootstrap after restart when the
        early portion of the lookback legitimately contains no fills.
        """
        await self.ensure_loaded()
        start_ms = int(start_ms)
        if end_ms is not None:
            await self.refresh_range(
                start_ms=start_ms,
                end_ms=end_ms,
                gap_hours=gap_hours,
                overlap=overlap,
                force_refetch_gaps=force_refetch_gaps,
            )
            return

        metadata = self.cache.load_metadata()
        covered_start_ms = int(metadata.get("covered_start_ms", 0) or 0)
        oldest_event_ts = int(self._events[0].timestamp) if self._events else 0
        metadata_oldest_event_ts = int(metadata.get("oldest_event_ts", 0) or 0)
        metadata_newest_event_ts = int(metadata.get("newest_event_ts", 0) or 0)
        metadata_indicates_no_cached_fills = (
            metadata_oldest_event_ts <= 0 and metadata_newest_event_ts <= 0
        )
        metadata_claims_history_without_events = (
            not self._events
            and (metadata_oldest_event_ts > 0 or metadata_newest_event_ts > 0)
            and covered_start_ms > 0
            and covered_start_ms <= start_ms
        )
        lookback_covered = (
            covered_start_ms > 0 and covered_start_ms <= start_ms and bool(self._events)
        ) or (oldest_event_ts > 0 and oldest_event_ts <= start_ms) or (
            covered_start_ms > 0
            and covered_start_ms <= start_ms
            and metadata_indicates_no_cached_fills
        )

        if lookback_covered:
            logger.debug(
                "[fills] lookback already covered from %s (covered_start=%s oldest_event=%s); refreshing latest",
                _format_ms(start_ms),
                _format_ms(covered_start_ms) if covered_start_ms else "None",
                _format_ms(oldest_event_ts) if oldest_event_ts else "None",
            )
            await self.refresh_latest(overlap=overlap)
            return

        if metadata_claims_history_without_events:
            logger.warning(
                "[fills] cache metadata claims lookback coverage from %s, but no cached events were loaded; rebuilding from requested lookback",
                _format_ms(covered_start_ms),
            )

        if self._events:
            logger.info(
                "[fills] lookback uncovered from %s; refreshing missing range before latest",
                _format_ms(start_ms),
            )
            await self.refresh_range(
                start_ms=start_ms,
                end_ms=None,
                gap_hours=gap_hours,
                overlap=overlap,
                force_refetch_gaps=force_refetch_gaps,
            )
        else:
            logger.info("[fills] cache empty; refreshing full lookback from %s", _format_ms(start_ms))
            await self.refresh(start_ms=start_ms, end_ms=None)

        self.cache.mark_covered_start(start_ms)

    async def refresh_range(
        self,
        start_ms: int,
        end_ms: Optional[int],
        *,
        gap_hours: float = 12.0,
        overlap: int = 20,
        force_refetch_gaps: bool = False,
    ) -> None:
        """Fill missing data between `start_ms` and `end_ms` using gap heuristics.

        Args:
            start_ms: Start timestamp in milliseconds
            end_ms: End timestamp in milliseconds (or None for now)
            gap_hours: Threshold for detecting gaps (default 12 hours)
            overlap: Number of events to overlap when fetching latest
            force_refetch_gaps: If True, retry even persistent gaps
        """
        await self.ensure_loaded()
        intervals: List[Tuple[int, int]] = []

        # Get known gaps from cache metadata
        known_gaps = self.cache.get_known_gaps()

        def is_in_persistent_gap(ts_start: int, ts_end: int) -> bool:
            """Check if interval is fully within a persistent (max retries) gap."""
            if force_refetch_gaps:
                return False
            for gap in known_gaps:
                if ts_start >= gap["start_ts"] and ts_end <= gap["end_ts"]:
                    if not self.cache.should_retry_gap(gap):
                        return True
            return False

        if not self._events:
            logger.debug("[fills] refresh_range: cache empty, refreshing entire interval")
            await self.refresh(start_ms=start_ms, end_ms=end_ms)
            await self.refresh_latest(overlap=overlap)
            return

        events_sorted = self._events
        earliest = events_sorted[0].timestamp
        latest = events_sorted[-1].timestamp
        gap_ms = max(1, int(gap_hours * 60.0 * 60.0 * 1000.0))

        # Fetch older data before earliest cached if requested
        if start_ms < earliest:
            upper = earliest if end_ms is None else min(earliest, end_ms)
            if start_ms < upper and not is_in_persistent_gap(start_ms, upper):
                intervals.append((start_ms, upper))

        # Detect large gaps in cached data
        prev_ts = earliest
        for ev in events_sorted[1:]:
            cur_ts = ev.timestamp
            if end_ms is not None and cur_ts > end_ms:
                break
            if cur_ts - prev_ts >= gap_ms:
                gap_start = max(prev_ts, start_ms)
                gap_end = cur_ts
                if gap_start < gap_end:
                    if is_in_persistent_gap(gap_start, gap_end):
                        logger.debug(
                            "FillEventsManager.refresh_range: skipping persistent gap %s → %s",
                            _format_ms(gap_start),
                            _format_ms(gap_end),
                        )
                    else:
                        intervals.append((gap_start, gap_end))
                        # Record as potential gap for tracking
                        self.cache.add_known_gap(
                            gap_start,
                            gap_end,
                            reason=GAP_REASON_AUTO,
                            confidence=GAP_CONFIDENCE_SUSPICIOUS,
                        )
            prev_ts = cur_ts

        # Fetch newer data after latest cached if requested (if not already covered)
        if end_ms is not None and end_ms > latest and (not intervals or intervals[-1][1] != end_ms):
            lower = max(latest, start_ms)
            if lower < end_ms and not is_in_persistent_gap(lower, end_ms):
                intervals.append((lower, end_ms))

        merged = self._merge_intervals(intervals)
        if merged:
            logger.debug(
                "[fills] refresh_range: refreshing %d intervals: %s",
                len(merged),
                ", ".join(f"{_format_ms(start)} → {_format_ms(end)}" for start, end in merged),
            )
        else:
            logger.debug("[fills] refresh_range: no gaps detected in requested interval")

        for start, end in merged:
            await self.refresh(start_ms=start, end_ms=end)

        await self.refresh_latest(overlap=overlap)

    def get_events(
        self,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        symbol: Optional[str] = None,
    ) -> List[FillEvent]:
        return query_utils.get_events(self, start_ms, end_ms, symbol)

    def get_pnl_sum(
        self,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        symbol: Optional[str] = None,
    ) -> float:
        return query_utils.get_pnl_sum(self, start_ms, end_ms, symbol)

    def get_pnl_cumsum(
        self,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        symbol: Optional[str] = None,
    ) -> List[Tuple[int, float]]:
        return query_utils.get_pnl_cumsum(self, start_ms, end_ms, symbol)

    def get_last_timestamp(self, symbol: Optional[str] = None) -> Optional[int]:
        return query_utils.get_last_timestamp(self, symbol)

    def reconstruct_positions(
        self, current_positions: Optional[Dict[str, float]] = None
    ) -> Dict[str, float]:
        return query_utils.reconstruct_positions(self, current_positions)

    def reconstruct_equity_curve(self, starting_equity: float = 0.0) -> List[Tuple[int, float]]:
        return query_utils.reconstruct_equity_curve(self, starting_equity)

    def get_coverage_summary(self) -> Dict[str, object]:
        return query_utils.get_coverage_summary(self)

    def get_history_scope(self) -> str:
        return self.cache.get_history_scope()

    def set_history_scope(self, scope: str) -> None:
        self.cache.set_history_scope(scope)

    @staticmethod
    def _events_for_days(
        events: Iterable[FillEvent], days: Iterable[str]
    ) -> Dict[str, List[FillEvent]]:
        return query_utils.events_for_days(events, days)

    @staticmethod
    def _merge_intervals(intervals: Sequence[Tuple[int, int]]) -> List[Tuple[int, int]]:
        return query_utils.merge_intervals(intervals)


class BybitFetcher(BaseFetcher):
    """Fetches fill events from Bybit using trades + positions history."""

    def __init__(
        self,
        api,
        *,
        category: str = "linear",
        trade_limit: int = 100,
        position_limit: int = 100,
        overlap_days: float = 3.0,
        max_span_days: float = 6.5,
    ) -> None:
        self.api = api
        self.category = category
        self.trade_limit = max(1, min(trade_limit, 100))
        self.position_limit = max(1, min(position_limit, 100))
        self._default_span_ms = int(overlap_days * 24 * 60 * 60 * 1000)
        self._max_span_ms = int(max_span_days * 24 * 60 * 60 * 1000)

    async def fetch(
        self,
        since_ms: Optional[int],
        until_ms: Optional[int],
        detail_cache: Dict[str, Tuple[str, str]],
        on_batch: Optional[Callable[[List[Dict[str, object]]], None]] = None,
    ) -> List[Dict[str, object]]:
        end_ms = until_ms or (self._now_ms() + 60 * 60 * 1000)
        start_ms = since_ms or max(0, end_ms - self._default_span_ms)

        trades = await self._fetch_my_trades(start_ms, end_ms)
        positions = await self._fetch_positions_history(start_ms, end_ms)

        events = self._combine(trades, positions, detail_cache)
        events = [
            ev
            for ev in events
            if (since_ms is None or ev["timestamp"] >= since_ms)
            and (until_ms is None or ev["timestamp"] <= until_ms)
        ]
        events.sort(key=lambda ev: ev["timestamp"])
        events = _coalesce_events(events)

        if on_batch and events:
            day_map = defaultdict(list)
            for ev in events:
                day_map[_day_key(ev["timestamp"])].append(ev)
            for day in sorted(day_map):
                on_batch(day_map[day])

        logger.debug(
            "BybitFetcher.fetch: done (events=%d, trades=%d, positions=%d)",
            len(events),
            len(trades),
            len(positions),
        )
        return events

    async def _fetch_my_trades(self, start_ms: int, end_ms: int) -> List[Dict[str, object]]:
        params = {
            "type": "swap",
            "subType": self.category,
            "limit": self.trade_limit,
            "endTime": int(end_ms),
        }
        results: List[Dict[str, object]] = []
        max_fetches = 200
        fetch_count = 0
        prev_params = None
        while True:
            new_key = _check_pagination_progress(
                prev_params,
                params,
                "BybitFetcher._fetch_my_trades",
            )
            if new_key is None:
                break
            prev_params = new_key
            fetch_count += 1
            batch = await self.api.fetch_my_trades(params=params)
            if fetch_count > 1:
                logger.debug(
                    "BybitFetcher._fetch_my_trades: fetch #%d endTime=%s size=%d",
                    fetch_count,
                    _format_ms(params.get("endTime")),
                    len(batch) if batch else 0,
                )
            if not batch:
                break
            batch.sort(key=lambda x: x["timestamp"])
            results.extend(batch)
            if len(batch) < self.trade_limit:
                if params["endTime"] - start_ms < self._max_span_ms:
                    break
                params["endTime"] = max(start_ms, params["endTime"] - self._max_span_ms)
                continue
            first_ts = batch[0]["timestamp"]
            if first_ts <= start_ms:
                break
            if params["endTime"] == first_ts:
                break
            params["endTime"] = int(first_ts)
            if fetch_count >= max_fetches:
                logger.warning("BybitFetcher._fetch_my_trades: max fetches reached")
                break
        ordered = sorted(
            results,
            key=lambda x: int(x.get("info", {}).get("updatedTime") or x.get("timestamp") or 0),
        )
        deduped: List[Dict[str, object]] = []
        seen_keys: set[Tuple[object, ...]] = set()
        duplicate_rows = 0
        for trade in ordered:
            key = _bybit_trade_dedupe_key(trade)
            if key is None:
                deduped.append(trade)
                continue
            if key in seen_keys:
                duplicate_rows += 1
                continue
            seen_keys.add(key)
            deduped.append(trade)
        if duplicate_rows:
            logger.debug(
                "BybitFetcher._fetch_my_trades: dropped %d duplicate fill rows before canonicalization",
                duplicate_rows,
            )
        return deduped

    async def _fetch_positions_history(self, start_ms: int, end_ms: int) -> List[Dict[str, object]]:
        """Fetch closed-pnl records using Bybit's raw API with hybrid pagination.

        Uses a two-phase approach:
        1. Cursor pagination for recent records (more efficient, no missed records)
        2. Time-based sliding window for older records (cursor doesn't go back far enough)

        This is necessary because:
        - CCXT's fetch_positions_history uses time-based pagination which can miss records
        - Bybit's cursor pagination only covers ~7 days of recent data
        """
        results: Dict[str, Dict[str, object]] = {}  # Dedupe by orderId
        max_fetches = 500
        fetch_count = 0

        # Phase 1: Use cursor pagination for recent records
        params: Dict[str, object] = {
            "category": "linear",
            "limit": self.position_limit,
            "endTime": int(end_ms),
        }

        cursor_oldest_ts = end_ms

        while True:
            fetch_count += 1
            if fetch_count > max_fetches:
                logger.warning(
                    "BybitFetcher._fetch_positions_history: max fetches reached (%d)", max_fetches
                )
                break

            try:
                response = await self.api.private_get_v5_position_closed_pnl(params)
            except Exception as exc:
                logger.warning("BybitFetcher._fetch_positions_history: API error: %s", exc)
                break

            batch = response.get("result", {}).get("list", [])
            if not batch:
                break

            self._process_closed_pnl_batch(batch, start_ms, results)

            oldest_ts = int(batch[-1].get("updatedTime", 0)) if batch else 0
            cursor_oldest_ts = oldest_ts

            if oldest_ts <= start_ms:
                break

            cursor = response.get("result", {}).get("nextPageCursor")
            if not cursor:
                # Cursor exhausted - switch to time-based sliding window
                break
            params["cursor"] = cursor

        # Phase 2: Time-based sliding window for older records (if cursor didn't reach start)
        if cursor_oldest_ts > start_ms:
            logger.debug(
                "BybitFetcher._fetch_positions_history: cursor exhausted at %s, switching to time-based",
                _format_ms(cursor_oldest_ts),
            )
            # Remove cursor and continue with time-based pagination
            current_end = cursor_oldest_ts

            while current_end > start_ms and fetch_count < max_fetches:
                fetch_count += 1
                params = {
                    "category": "linear",
                    "limit": self.position_limit,
                    "endTime": int(current_end),
                }

                try:
                    response = await self.api.private_get_v5_position_closed_pnl(params)
                except Exception as exc:
                    logger.warning("BybitFetcher._fetch_positions_history: API error: %s", exc)
                    break

                batch = response.get("result", {}).get("list", [])
                if not batch:
                    # No more records, slide window back
                    current_end = max(start_ms, current_end - self._max_span_ms)
                    continue

                self._process_closed_pnl_batch(batch, start_ms, results)

                oldest_ts = int(batch[-1].get("updatedTime", 0)) if batch else 0
                if oldest_ts <= start_ms:
                    break

                # Slide window: if batch was full, use oldest ts; otherwise jump back
                if len(batch) >= self.position_limit:
                    current_end = oldest_ts
                else:
                    current_end = max(start_ms, oldest_ts - self._max_span_ms)

        logger.debug(
            "BybitFetcher._fetch_positions_history: fetched %d records in %d requests",
            len(results),
            fetch_count,
        )
        return list(results.values())

    def _process_closed_pnl_batch(
        self,
        batch: List[Dict[str, object]],
        start_ms: int,
        results: Dict[str, Dict[str, object]],
    ) -> None:
        return bybit_utils.process_closed_pnl_batch(self, batch, start_ms, results)

    def _combine(
        self,
        trades: List[Dict[str, object]],
        positions: List[Dict[str, object]],
        detail_cache: Dict[str, Tuple[str, str]],
    ) -> List[Dict[str, object]]:
        return bybit_utils.combine(self, trades, positions, detail_cache, custom_id_to_snake)

    @staticmethod
    def _normalize_trade(trade: Dict[str, object]) -> Dict[str, object]:
        return bybit_utils.normalize_trade(trade)

    @staticmethod
    def _determine_position_side(side: str, closed_size: float) -> str:
        return bybit_utils.determine_position_side(side, closed_size)

    @staticmethod
    def _now_ms() -> int:
        return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


class HyperliquidFetcher(BaseFetcher):
    """Fetches fill events via ccxt.fetch_my_trades for Hyperliquid."""

    def __init__(
        self,
        api,
        *,
        trade_limit: int = 500,
        symbol_resolver: Optional[Callable[[Optional[str]], str]] = None,
    ) -> None:
        self.api = api
        self.trade_limit = max(1, trade_limit)
        self._symbol_resolver = symbol_resolver

    async def fetch(
        self,
        since_ms: Optional[int],
        until_ms: Optional[int],
        detail_cache: Dict[str, Tuple[str, str]],
        on_batch: Optional[Callable[[List[Dict[str, object]]], None]] = None,
    ) -> List[Dict[str, object]]:
        params: Dict[str, object] = {"limit": self.trade_limit}
        if since_ms is not None:
            params["since"] = int(since_ms)

        collected: Dict[str, Dict[str, object]] = {}
        max_fetches = 200
        fetch_count = 0

        prev_params = None
        rate_limit_retries = 0
        max_rate_limit_retries = 5
        while True:
            check_params = dict(params)
            check_params["_page"] = fetch_count
            new_key = _check_pagination_progress(
                prev_params,
                check_params,
                "HyperliquidFetcher.fetch",
            )
            if new_key is None:
                break
            prev_params = new_key
            try:
                trades = await self.api.fetch_my_trades(params=params)
            except RateLimitExceeded as exc:
                rate_limit_retries += 1
                if rate_limit_retries >= max_rate_limit_retries:
                    msg = (
                        "HyperliquidFetcher.fetch: too many consecutive rate-limit retries "
                        f"({rate_limit_retries}/{max_rate_limit_retries}); aborting fetch"
                    )
                    logger.warning("%s", msg)
                    raise RateLimitExceeded(msg) from exc
                logger.debug(
                    "HyperliquidFetcher.fetch: rate limit exceeded (retry %d/%d), sleeping (%s)",
                    rate_limit_retries,
                    max_rate_limit_retries,
                    exc,
                )
                await asyncio.sleep(min(30.0, 2.0 ** rate_limit_retries))
                # Reset prev_params so the retry is not flagged as repeated
                prev_params = None
                continue
            rate_limit_retries = 0
            fetch_count += 1
            if fetch_count > 1:
                logger.debug(
                    "HyperliquidFetcher.fetch: fetch #%d since=%s size=%d",
                    fetch_count,
                    _format_ms(params.get("since")),
                    len(trades) if trades else 0,
                )
            if not trades:
                break
            before_count = len(collected)
            for trade in trades:
                event = self._normalize_trade(trade)
                ts = event["timestamp"]
                if since_ms is not None and ts < since_ms:
                    continue
                if until_ms is not None and ts > until_ms:
                    continue
                collected[event["id"]] = event
            added = len(collected) - before_count
            if len(trades) < self.trade_limit:
                break
            last_ts = int(
                trades[-1].get("timestamp")
                or trades[-1].get("info", {}).get("time")
                or trades[-1].get("info", {}).get("updatedTime")
                or 0
            )
            if last_ts <= 0:
                break
            if until_ms is not None and last_ts >= until_ms:
                break
            if added <= 0:
                logger.debug(
                    "HyperliquidFetcher.fetch: no new trades added on page (last_ts=%s), stopping",
                    last_ts,
                )
                break
            params["since"] = last_ts
            if fetch_count >= max_fetches:
                logger.warning(
                    "HyperliquidFetcher.fetch: reached maximum pagination depth (%d)",
                    max_fetches,
                )
                break

        events = sorted(collected.values(), key=lambda ev: ev["timestamp"])
        events = _coalesce_events(events)
        # Note: psize/pprice annotation is done centrally in FillEventsManager.refresh()

        for event in events:
            cache_entry = detail_cache.get(event["id"])
            if cache_entry:
                event["client_order_id"], event["pb_order_type"] = cache_entry
            elif event["client_order_id"]:
                event["pb_order_type"] = custom_id_to_snake(event["client_order_id"])
            else:
                event["pb_order_type"] = "unknown"
            if not event["pb_order_type"]:
                event["pb_order_type"] = "unknown"

        if on_batch and events:
            on_batch(events)

        return events

    @staticmethod
    def _normalize_trade(trade: Dict[str, object]) -> Dict[str, object]:
        return hyperliquid_utils.normalize_trade(trade)


class GateioFetcher(BaseFetcher):
    """Fetches fill events for Gate.io using trades + order PnL.

    Uses the my_trades_timerange endpoint for fill-level data (fees, exact prices)
    since the standard my_trades endpoint has a 7-day hard limit. Uses
    fetch_closed_orders for PnL. Distributes order-level PnL proportionally
    across fills when an order has multiple trades.
    """

    def __init__(
        self,
        api,
        *,
        trade_limit: int = 100,
        now_func: Optional[Callable[[], int]] = None,
    ) -> None:
        self.api = api
        self.trade_limit = max(1, min(100, trade_limit))
        self._now_func = now_func or (lambda: int(datetime.now(tz=timezone.utc).timestamp() * 1000))

    async def fetch(
        self,
        since_ms: Optional[int],
        until_ms: Optional[int],
        detail_cache: Dict[str, Tuple[str, str]],
        on_batch: Optional[Callable[[List[Dict[str, object]]], None]] = None,
    ) -> List[Dict[str, object]]:
        logger.debug(
            "GateioFetcher.fetch: start (since=%s, until=%s)",
            _format_ms(since_ms),
            _format_ms(until_ms),
        )

        # Step 1: Fetch trades (fill-level data with fees)
        trades = await self._fetch_trades(since_ms, until_ms)
        if not trades:
            logger.debug("GateioFetcher.fetch: no trades found")
            return []

        # Step 2: Collect unique order IDs
        order_ids: set[str] = set()
        for t in trades:
            oid = str(t.get("order") or t.get("info", {}).get("order_id") or "")
            if oid:
                order_ids.add(oid)

        # Step 3: Fetch closed orders for PnL
        orders_by_id = await self._fetch_orders_for_pnl(order_ids)

        # Step 4: Merge trades with order PnL
        events = self._merge_trades_with_orders(trades, orders_by_id, detail_cache)

        # Filter by time bounds
        if since_ms is not None:
            events = [ev for ev in events if ev["timestamp"] >= since_ms]
        if until_ms is not None:
            events = [ev for ev in events if ev["timestamp"] <= until_ms]

        ordered = sorted(events, key=lambda ev: ev["timestamp"])

        if on_batch and ordered:
            on_batch(ordered)

        logger.debug(
            "GateioFetcher.fetch: done (events=%d, trades=%d, orders=%d)",
            len(ordered),
            len(trades),
            len(orders_by_id),
        )
        return ordered

    async def _fetch_trades(
        self, since_ms: Optional[int], until_ms: Optional[int]
    ) -> List[Dict[str, object]]:
        """Fetch trades using the my_trades_timerange endpoint.

        The standard my_trades endpoint has a ~7 day hard limit, so we use
        the timerange endpoint which allows fetching historical data by
        specifying from/to timestamps.
        """
        now_ms = self._now_func()
        # Default to 30 days if no since_ms provided
        default_lookback_ms = 30 * 24 * 60 * 60 * 1000
        from_s = int((since_ms or (now_ms - default_lookback_ms)) / 1000)
        to_s = int((until_ms or now_ms) / 1000)

        collected: Dict[str, Dict[str, object]] = {}
        max_fetches = 400
        fetch_count = 0
        offset = 0
        consecutive_rate_limits = 0

        while fetch_count < max_fetches:
            fetch_count += 1
            try:
                # Use the timerange endpoint directly via CCXT's private API
                batch = await self.api.private_futures_get_settle_my_trades_timerange(
                    {
                        "settle": "usdt",
                        "from": from_s,
                        "to": to_s,
                        "limit": self.trade_limit,
                        "offset": offset,
                    }
                )
                consecutive_rate_limits = 0
            except RateLimitExceeded as exc:
                consecutive_rate_limits += 1
                sleep_time = min(2**consecutive_rate_limits, 30)
                logger.debug(
                    "GateioFetcher._fetch_trades: rate-limited (%s); sleeping %.1fs", exc, sleep_time
                )
                await asyncio.sleep(sleep_time)
                continue
            except Exception as exc:
                # Check if it's a rate limit error in disguise
                if "TOO_MANY_REQUESTS" in str(exc):
                    consecutive_rate_limits += 1
                    sleep_time = min(2**consecutive_rate_limits, 30)
                    logger.debug(
                        "GateioFetcher._fetch_trades: rate-limited (%s); sleeping %.1fs",
                        exc,
                        sleep_time,
                    )
                    await asyncio.sleep(sleep_time)
                    continue
                raise

            if fetch_count > 1:
                logger.info(
                    "GateioFetcher._fetch_trades: fetch #%d offset=%s size=%d",
                    fetch_count,
                    offset,
                    len(batch) if batch else 0,
                )

            if not batch:
                break

            for raw_trade in batch:
                # Convert raw Gate.io response to CCXT-like format
                trade = self._normalize_raw_trade(raw_trade)
                ts = trade.get("timestamp", 0)
                # Skip trades outside time bounds (safety check)
                if since_ms is not None and ts < since_ms:
                    continue
                if until_ms is not None and ts > until_ms:
                    continue
                trade_id = str(trade.get("id") or "")
                if trade_id:
                    collected[trade_id] = trade

            if len(batch) < self.trade_limit:
                break

            offset += self.trade_limit
            # Small delay to avoid rate limits
            await asyncio.sleep(0.15)

        if fetch_count >= max_fetches:
            logger.warning("GateioFetcher._fetch_trades: reached pagination cap (%d)", max_fetches)

        return list(collected.values())

    def _normalize_raw_trade(self, raw: Dict[str, object]) -> Dict[str, object]:
        return gateio_utils.normalize_raw_trade(raw)

    async def _fetch_orders_for_pnl(self, order_ids: set[str]) -> Dict[str, Dict[str, object]]:
        return await gateio_utils.fetch_orders_for_pnl(self, order_ids)

    def _merge_trades_with_orders(
        self,
        trades: List[Dict[str, object]],
        orders_by_id: Dict[str, Dict[str, object]],
        detail_cache: Dict[str, Tuple[str, str]],
    ) -> List[Dict[str, object]]:
        return gateio_utils.merge_trades_with_orders(self, trades, orders_by_id, detail_cache)

    def _normalize_trade(
        self,
        trade: Dict[str, object],
        order: Dict[str, object],
        order_pnl: float,
        total_qty: float,
        detail_cache: Dict[str, Tuple[str, str]],
    ) -> Dict[str, object]:
        return gateio_utils.normalize_trade(self, trade, order, order_pnl, total_qty, detail_cache)

    @staticmethod
    def _determine_position_side(side: str, is_close: bool) -> str:
        return gateio_utils.determine_position_side(side, is_close)


class KucoinFetcher(BaseFetcher):
    """Fetches fill events for Kucoin by combining trade and position history."""

    def __init__(
        self, api, *, trade_limit: int = 1000, now_func: Optional[Callable[[], int]] = None
    ) -> None:
        self.api = api
        self.trade_limit = max(1, trade_limit)
        self._symbol_resolver = None
        self._now_func = now_func or (lambda: int(datetime.now(tz=timezone.utc).timestamp() * 1000))

    async def fetch(
        self,
        since_ms: Optional[int],
        until_ms: Optional[int],
        detail_cache: Dict[str, Tuple[str, str]],
        on_batch: Optional[Callable[[List[Dict[str, object]]], None]] = None,
    ) -> List[Dict[str, object]]:
        trades = await self._fetch_trades(since_ms, until_ms)
        if not trades:
            return []

        # Compute local realized PnL from trades (gross), subtract fees when available
        local_pnls, _ = compute_realized_pnls_from_trades(trades)

        closes = [
            t
            for t in trades
            if (t["side"] == "sell" and t["position_side"] == "long")
            or (t["side"] == "buy" and t["position_side"] == "short")
        ]
        events: Dict[str, Dict[str, object]] = {}
        for t in trades:
            ev = dict(t)
            fee_cost = _fee_cost(ev.get("fees"))
            ev["pnl"] = local_pnls.get(ev["id"], 0.0) - fee_cost
            events[ev["id"]] = ev

        if closes:
            ph = await self._fetch_positions_history(
                start_ms=closes[0]["timestamp"] - 60_000,
                end_ms=closes[-1]["timestamp"] + 60_000,
            )
            self._match_pnls(closes, ph, events)
            self._log_discrepancies(local_pnls, ph)

        ordered = sorted(events.values(), key=lambda ev: ev["timestamp"])
        await self._enrich_with_order_details_bulk(ordered, detail_cache)
        if on_batch and ordered:
            on_batch(ordered)
        return ordered

    async def _fetch_trades(
        self, since_ms: Optional[int], until_ms: Optional[int]
    ) -> List[Dict[str, object]]:
        now_ms = self._now_func()
        until_ts = int(until_ms) if until_ms is not None else now_ms + 3_600_000
        since_ts = int(since_ms) if since_ms is not None else until_ts - 24 * 60 * 60 * 1000
        buffer_ms = int(24 * 60 * 60 * 1000 * 0.99)
        limit = min(self.trade_limit, 1000)

        collected: Dict[str, Dict[str, object]] = {}
        max_fetches = 400
        start_at = since_ts
        prev_params = None
        fetch_count = 0

        while start_at < until_ts and fetch_count < max_fetches:
            fetch_count += 1
            end_at = min(start_at + buffer_ms, until_ts)
            params: Dict[str, object] = {
                "startAt": int(start_at),
                "endAt": int(end_at),
                "limit": limit,
            }
            key = _check_pagination_progress(prev_params, dict(params), "KucoinFetcher._fetch_trades")
            if key is None:
                break
            prev_params = key
            batch = await self.api.fetch_my_trades(params=params)
            if fetch_count > 1:
                logger.debug(
                    "KucoinFetcher._fetch_trades: fetch #%d startAt=%s endAt=%s size=%d",
                    fetch_count,
                    _format_ms(params["startAt"]),
                    _format_ms(params["endAt"]),
                    len(batch) if batch else 0,
                )
            if not batch:
                start_at += buffer_ms
                continue

            last_ts = kucoin_utils.collect_trade_batch(
                batch, self._normalize_trade, since_ts, until_ts, collected
            )
            if last_ts <= start_at:
                start_at = start_at + buffer_ms
            else:
                start_at = last_ts + 1

        if fetch_count >= max_fetches:
            logger.warning("KucoinFetcher._fetch_trades: reached pagination cap (%d)", max_fetches)

        return sorted(collected.values(), key=lambda ev: ev["timestamp"])

    async def _fetch_positions_history(self, start_ms: int, end_ms: int) -> List[Dict[str, object]]:
        results: Dict[str, Dict[str, object]] = {}
        max_fetches = 400
        fetch_count = 0
        buffer_ms = int(24 * 60 * 60 * 1000 * 0.99)
        limit = 200
        now_ms = self._now_func()
        until_ts = int(end_ms) if end_ms is not None else now_ms + 3_600_000
        since_ts = int(start_ms) if start_ms is not None else until_ts - 24 * 60 * 60 * 1000

        start_at = since_ts
        prev_params = None
        while start_at < until_ts and fetch_count < max_fetches:
            end_at = min(start_at + buffer_ms, until_ts)
            params: Dict[str, object] = {"from": int(start_at), "to": int(end_at), "limit": limit}
            key = _check_pagination_progress(
                prev_params, dict(params), "KucoinFetcher._fetch_positions_history"
            )
            if key is None:
                break
            prev_params = key
            fetch_count += 1
            batch = await self.api.fetch_positions_history(params=params)
            if fetch_count > 1:
                logger.debug(
                    "KucoinFetcher._fetch_positions_history: fetch #%d from=%s to=%s size=%d",
                    fetch_count,
                    _format_ms(params.get("from")),
                    _format_ms(params.get("to")),
                    len(batch) if batch else 0,
                )
            if not batch:
                start_at += buffer_ms
                continue
            last_ts = kucoin_utils.collect_positions_history_batch(batch, results, end_at)
            if last_ts <= start_at:
                start_at += buffer_ms
            else:
                start_at = last_ts + 1

        if fetch_count >= max_fetches:
            logger.warning(
                "KucoinFetcher._fetch_positions_history: reached pagination cap (%d)", max_fetches
            )

        return sorted(results.values(), key=lambda x: x.get("lastUpdateTimestamp", 0))

    def _match_pnls(
        self,
        closes: List[Dict[str, object]],
        positions: List[Dict[str, object]],
        events: Dict[str, Dict[str, object]],
    ) -> None:
        """Match position close PnL from positions_history to trade fills.

        Uses a 5-minute window to find all fills that could be part of a position close.
        When multiple fills match a single position close:
        - The PnL is distributed proportionally by fill quantity
        - This ensures the total PnL sums correctly regardless of how many fills closed the position
        """
        unmatched_positions = kucoin_utils.match_pnls(closes, positions, events)

        if unmatched_positions:
            unmatched_count, total_unmatched_pnl = kucoin_utils.summarize_unmatched_positions(
                unmatched_positions
            )
            logger.debug(
                "[pnl] KucoinFetcher._match_pnls: %d position closes (%s total PnL) "
                "could not be matched to any trade fills",
                unmatched_count,
                f"{total_unmatched_pnl:.4f}",
            )

    def _log_discrepancies(
        self, local_pnls: Dict[str, float], positions: List[Dict[str, object]]
    ) -> None:
        if not positions or not local_pnls:
            return
        pos_sum = kucoin_utils.aggregate_position_pnls_by_symbol(positions)
        if not pos_sum:
            return
        local_total = sum(local_pnls.values())
        remote_total = sum(pos_sum.values())
        now = time.time()
        throttle_key = f"kucoin:{id(self.api)}"
        last_log = _pnl_discrepancy_last_log.get(throttle_key, 0.0)
        last_delta = _pnl_discrepancy_last_delta.get(throttle_key)
        current_delta = local_total - remote_total
        should_log = kucoin_utils.should_log_discrepancy(
            local_total,
            remote_total,
            current_delta,
            last_log,
            last_delta,
            now,
            min_ratio=0.05,
            change_threshold=_PNL_DISCREPANCY_CHANGE_THRESHOLD,
            min_seconds=_PNL_DISCREPANCY_MIN_SECONDS,
            throttle_seconds=_PNL_DISCREPANCY_THROTTLE_SECONDS,
        )
        if should_log:
            _pnl_discrepancy_last_log[throttle_key] = now
            _pnl_discrepancy_last_delta[throttle_key] = current_delta
            logger.warning(
                "[pnl] KucoinFetcher: local sum %.2f differs from positions_history %.2f (delta=%.2f)",
                local_total,
                remote_total,
                current_delta,
            )

    @staticmethod
    def _normalize_trade(trade: Dict[str, object]) -> Dict[str, object]:
        return kucoin_utils.normalize_trade(trade)

    @staticmethod
    def _determine_position_side(side: str, reduce_only: bool, close_fee_pay: float) -> str:
        return kucoin_utils.determine_position_side(side, reduce_only, close_fee_pay)

    async def _enrich_with_order_details_bulk(
        self, events: List[Dict[str, object]], detail_cache: Dict[str, Tuple[str, str]]
    ) -> None:
        """Enrich events with clientOid from order details.

        Optimized to:
        1. Check cache by both tradeId and orderId
        2. Group events by orderId to avoid duplicate fetch_order calls
        3. Share results across events with the same orderId
        """
        if events is None:
            return
        detail_cache = detail_cache or {}
        order_id_cache = kucoin_utils.apply_cached_order_details(events, detail_cache)
        events_by_order = kucoin_utils.collect_events_requiring_order_details(
            events, detail_cache, order_id_cache
        )

        unique_orders = list(events_by_order.keys())
        if unique_orders:
            # Get symbol for each orderId (use first event's symbol)
            order_symbols = {oid: evs[0].get("symbol") for oid, evs in events_by_order.items()}

            # Limit concurrency to avoid overwhelming the API
            sem = asyncio.Semaphore(8)
            total = len(unique_orders)
            completed = 0
            last_log_time = time.time()
            log_interval = 5.0

            async def throttled_fetch(order_id: str) -> Tuple[str, Optional[Tuple[str, str]]]:
                nonlocal completed, last_log_time
                async with sem:
                    symbol = order_symbols.get(order_id)
                    try:
                        result = await self._enrich_with_order_details(order_id, symbol)
                    except Exception as exc:
                        logger.debug(
                            "KucoinFetcher._enrich_with_order_details_bulk: enrichment failed for %s (%s)",
                            order_id,
                            exc,
                        )
                        result = None
                    completed += 1
                    now = time.time()
                    if total > 50 and (now - last_log_time >= log_interval):
                        last_log_time = now
                        pct = int(100 * completed / total)
                        logger.info(
                            "KucoinFetcher: enriching order details %d/%d (%d%%)",
                            completed,
                            total,
                            pct,
                        )
                    return order_id, result

            total_events = sum(len(evs) for evs in events_by_order.values())
            if total > 50:
                logger.info(
                    "KucoinFetcher: enriching %d events via %d unique orders (concurrency=8)...",
                    total_events,
                    total,
                )

            tasks = [throttled_fetch(oid) for oid in unique_orders]
            results = await asyncio.gather(*tasks)

            if total > 50:
                logger.info(
                    "KucoinFetcher: enrichment complete (%d orders, %d events)", total, total_events
                )

            # Apply results to all events sharing the same orderId
            for res in results:
                order_id, detail = res
                kucoin_utils.apply_order_detail_result(
                    order_id, detail, events_by_order, detail_cache, order_id_cache
                )

        kucoin_utils.ensure_order_detail_defaults(events)

    async def _enrich_with_order_details(
        self, order_id: Optional[str], symbol: Optional[str]
    ) -> Optional[Tuple[str, str]]:
        if not order_id:
            return None
        try:
            detail = await self.api.fetch_order(order_id, symbol)
        except Exception as exc:  # pragma: no cover - live API dependent
            logger.debug(
                "KucoinFetcher._enrich_with_order_details: fetch_order failed for %s (%s)",
                order_id,
                exc,
            )
            return None
        return kucoin_utils.parse_order_detail(detail, custom_id_to_snake)


# ---------------------------------------------------------------------------
# Utilities for Bitget integration
# ---------------------------------------------------------------------------


class OkxFetcher(BaseFetcher):
    """Fetches fill events from OKX using fills and fills-history endpoints.

    OKX provides all required fields in a single endpoint:
    - tradeId: unique fill identifier
    - fillPnl: realized PnL
    - posSide: position side (long/short/net)
    - clOrdId: client order ID (passivbot order type)
    - fillSz: fill quantity
    - fillPx: fill price

    Endpoints:
    - /api/v5/trade/fills: last 3 days (higher rate limit)
    - /api/v5/trade/fills-history: last 3 months (lower rate limit)

    Pagination: Returns newest first; use 'after' param with billId for backward pagination.
    """

    # 3 days in ms - threshold for choosing between /fills and /fills-history
    _THREE_DAYS_MS = 3 * 24 * 60 * 60 * 1000

    def __init__(
        self,
        api,
        *,
        trade_limit: int = 100,
        inst_type: str = "SWAP",
    ) -> None:
        self.api = api
        self.trade_limit = max(1, min(100, trade_limit))  # OKX max is 100
        self.inst_type = inst_type

    async def fetch(
        self,
        since_ms: Optional[int],
        until_ms: Optional[int],
        detail_cache: Dict[str, Tuple[str, str]],
        on_batch: Optional[Callable[[List[Dict[str, object]]], None]] = None,
    ) -> List[Dict[str, object]]:
        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        until_ms = until_ms or now_ms

        # Determine which endpoint(s) to use based on time range
        three_days_ago = now_ms - self._THREE_DAYS_MS

        collected: Dict[str, Dict[str, object]] = {}
        max_fetches = 400
        fetch_count = 0

        logger.debug(
            "OkxFetcher.fetch: start (since=%s, until=%s)",
            _format_ms(since_ms),
            _format_ms(until_ms),
        )

        # If we need data older than 3 days, start with fills-history
        if since_ms is not None and since_ms < three_days_ago:
            # Use fills-history for older data
            fetch_count, collected = await self._fetch_from_endpoint(
                endpoint="history",
                since_ms=since_ms,
                until_ms=min(until_ms, three_days_ago),
                collected=collected,
                max_fetches=max_fetches,
                start_fetch_count=fetch_count,
                on_batch=on_batch,
                detail_cache=detail_cache,
            )

        # Use /fills for recent data (last 3 days)
        recent_since = max(since_ms or 0, three_days_ago) if since_ms else three_days_ago
        if until_ms > three_days_ago:
            fetch_count, collected = await self._fetch_from_endpoint(
                endpoint="recent",
                since_ms=recent_since if since_ms else None,
                until_ms=until_ms,
                collected=collected,
                max_fetches=max_fetches,
                start_fetch_count=fetch_count,
                on_batch=on_batch,
                detail_cache=detail_cache,
            )

        events = okx_utils.finalize_events(
            collected,
            detail_cache,
            custom_id_to_snake,
            _coalesce_events,
            since_ms,
            until_ms,
        )
        # Note: psize/pprice annotation is done centrally in FillEventsManager.refresh()

        logger.debug(
            "OkxFetcher.fetch: done (events=%d, fetches=%d)",
            len(events),
            fetch_count,
        )
        return events

    async def _fetch_from_endpoint(
        self,
        endpoint: str,
        since_ms: Optional[int],
        until_ms: int,
        collected: Dict[str, Dict[str, object]],
        max_fetches: int,
        start_fetch_count: int,
        on_batch: Optional[Callable[[List[Dict[str, object]]], None]],
        detail_cache: Dict[str, Tuple[str, str]],
    ) -> Tuple[int, Dict[str, Dict[str, object]]]:
        """Fetch fills from either /fills (recent) or /fills-history (history) endpoint."""
        fetch_count = start_fetch_count
        after_cursor: Optional[str] = None

        endpoint_name = "fills" if endpoint == "recent" else "fills-history"
        logger.debug(
            "OkxFetcher: using /%s endpoint (since=%s, until=%s)",
            endpoint_name,
            _format_ms(since_ms),
            _format_ms(until_ms),
        )

        while fetch_count < max_fetches:
            params = okx_utils.build_fetch_params(
                self.inst_type, self.trade_limit, since_ms, until_ms, after_cursor
            )

            try:
                if endpoint == "recent":
                    response = await self.api.private_get_trade_fills(params)
                else:
                    response = await self.api.private_get_trade_fills_history(params)
            except RateLimitExceeded as exc:
                logger.debug("OkxFetcher: rate limit hit, sleeping (%s)", exc)
                await asyncio.sleep(2.0)
                continue

            fetch_count += 1
            fills = response.get("data", [])

            if fetch_count > 1:
                logger.debug(
                    "OkxFetcher.fetch: /%s #%d after=%s size=%d",
                    endpoint_name,
                    fetch_count,
                    after_cursor,
                    len(fills),
                )

            if not fills:
                break

            batch_events, oldest_ts = okx_utils.process_fill_batch(
                fills,
                self._normalize_fill,
                detail_cache,
                custom_id_to_snake,
                collected,
                since_ms,
                until_ms,
            )

            # Callback for incremental processing
            if on_batch and batch_events:
                on_batch(batch_events)

            # Check if we've reached the start boundary
            if okx_utils.reached_since_boundary(oldest_ts, since_ms):
                logger.debug("OkxFetcher: reached since_ms boundary, stopping")
                break

            # Short batch means no more data
            if okx_utils.short_batch(fills, self.trade_limit):
                break

            # Get pagination cursor for next batch (use billId from oldest fill)
            after_cursor = okx_utils.next_after_cursor(fills)
            if not after_cursor:
                break

        return fetch_count, collected

    @staticmethod
    def _normalize_fill(raw: Dict[str, object]) -> Dict[str, object]:
        return okx_utils.normalize_fill(raw)


def custom_id_to_snake(client_oid: str) -> str:
    return parse_utils.custom_id_to_snake(client_oid)


def deduce_side_pside(elm: dict) -> Tuple[str, str]:
    return parse_utils.deduce_side_pside(elm)


# ---------------------------------------------------------------------------
# CLI helpers
# ---------------------------------------------------------------------------


EXCHANGE_BOT_CLASSES: Dict[str, Tuple[str, str]] = {
    "binance": ("exchanges.binance", "BinanceBot"),
    "bitget": ("exchanges.bitget", "BitgetBot"),
    "bybit": ("exchanges.bybit", "BybitBot"),
    "fake": ("exchanges.fake", "FakeBot"),
    "hyperliquid": ("exchanges.hyperliquid", "HyperliquidBot"),
    "gateio": ("exchanges.gateio", "GateIOBot"),
    "kucoin": ("exchanges.kucoin", "KucoinBot"),
    "okx": ("exchanges.okx", "OKXBot"),
}


def _parse_time_arg(value: Optional[str]) -> Optional[int]:
    return cli_utils.parse_time_arg(value)


def _parse_log_level(value: str) -> int:
    return cli_utils.parse_log_level(value)


def _extract_symbol_pool(config: dict, override: Optional[List[str]]) -> List[str]:
    return fetcher_utils.extract_symbol_pool(config, override)


def _symbol_resolver(bot) -> Callable[[Optional[str]], str]:
    return fetcher_utils.symbol_resolver(bot)


def _build_fetcher_for_bot(bot, symbols: List[str]) -> BaseFetcher:
    return fetcher_utils.build_fetcher_for_bot(
        bot,
        symbols,
        {
            "BinanceFetcher": BinanceFetcher,
            "BitgetFetcher": BitgetFetcher,
            "BybitFetcher": BybitFetcher,
            "FakeFetcher": FakeFetcher,
            "HyperliquidFetcher": HyperliquidFetcher,
            "GateioFetcher": GateioFetcher,
            "KucoinFetcher": KucoinFetcher,
            "OkxFetcher": OkxFetcher,
        },
    )


def _instantiate_bot(config: dict):
    return cli_utils.instantiate_bot(
        config,
        load_user_info=load_user_info,
        exchange_bot_classes=EXCHANGE_BOT_CLASSES,
    )


async def _run_cli(args: argparse.Namespace) -> None:
    await cli_utils.run_cli(
        args,
        load_input_config=load_input_config,
        prepare_config=prepare_config,
        instantiate_bot_fn=_instantiate_bot,
        extract_symbol_pool=_extract_symbol_pool,
        build_fetcher_for_bot=_build_fetcher_for_bot,
        manager_cls=FillEventsManager,
        parse_time_arg_fn=_parse_time_arg,
        format_ms_fn=_format_ms,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Fill events cache refresher")
    parser.add_argument(
        "--config",
        "-c",
        type=str,
        default=None,
        help="Config path (defaults to in-code schema defaults)",
    )
    parser.add_argument("--user", "-u", type=str, required=True, help="Live user identifier")
    parser.add_argument("--start", "-s", type=str, help="Start datetime (ms or ISO)")
    parser.add_argument("--end", "-e", type=str, help="End datetime (ms or ISO)")
    parser.add_argument(
        "--lookback-days",
        "-d",
        type=float,
        default=30.0,
        help="Default lookback window in days when start is omitted",
    )
    parser.add_argument(
        "--log-level",
        "-l",
        type=str,
        default="info",
        help="Logging verbosity (warning/info/debug/trace or 0-3)",
    )
    parser.add_argument(
        "--cache-root",
        "-r",
        type=str,
        default="caches/fill_events",
        help="Root directory for fill events cache (default: caches/fill_events)",
    )
    parser.add_argument(
        "--symbols",
        "-S",
        nargs="*",
        default=None,
        help="Optional explicit symbol list to fetch",
    )
    args = parser.parse_args()
    configure_logging(debug=_parse_log_level(args.log_level))
    try:
        asyncio.run(_run_cli(args))
    except KeyboardInterrupt:
        logger.info("fill_events_manager CLI interrupted by user")


if __name__ == "__main__":
    main()
