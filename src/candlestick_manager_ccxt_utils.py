from __future__ import annotations

import numpy as np

from typing import Any


def exchange_retry_config(ex_id: str) -> tuple[bool, bool, int, float, float]:
    exid = (ex_id or "").lower()
    is_bybit = "bybit" in exid
    is_hyperliquid = "hyperliquid" in exid
    max_attempts = 9 if is_bybit else 5
    backoff = 1.0 if is_bybit else 0.5
    backoff_cap = 20.0 if is_bybit else 8.0
    return is_bybit, is_hyperliquid, max_attempts, backoff, backoff_cap


def build_fetch_params(ex_id: str, end_exclusive_ms: int | None) -> dict[str, object]:
    params: dict[str, object] = {}
    exid = (ex_id or "").lower()
    if end_exclusive_ms is not None and all(x not in exid for x in ("bitget", "okx", "bybit", "kucoin")):
        params["until"] = int(end_exclusive_ms) - 1
    if "bybit" in exid:
        params.setdefault("category", "linear")
    return params


def summarize_fetch_result(res: list) -> tuple[int | None, int | None, int]:
    first_ts = None
    last_ts = None
    rows = int(len(res) if res else 0)
    if res:
        try:
            first_ts = int(res[0][0])
        except Exception:
            first_ts = None
        try:
            last_ts = int(res[-1][0])
        except Exception:
            last_ts = None
    return first_ts, last_ts, rows


def classify_fetch_error(error: BaseException) -> tuple[str, str, bool]:
    err_type = type(error).__name__
    msg = str(error) or ""
    msg_l = msg.lower()
    is_rate_limit = any(token in msg_l for token in ("rate limit", "too many", "429", "10006"))
    return err_type, msg_l, is_rate_limit


def adjusted_sleep_seconds(
    *,
    current_backoff: float,
    is_rate_limit: bool,
    is_bybit: bool,
    is_hyperliquid: bool,
    err_type: str,
    msg_l: str,
) -> tuple[float, float | None]:
    sleep_s = current_backoff
    global_backoff = None
    if is_rate_limit:
        global_backoff = 10.0 if is_hyperliquid else 5.0
        sleep_s = max(sleep_s, global_backoff)
    if is_bybit and (
        err_type in {"RequestTimeout", "NetworkError", "ExchangeNotAvailable", "DDoSProtection"}
        or any(
            token in msg_l
            for token in ("timed out", "timeout", "etimedout", "econnreset", "502", "503", "504")
        )
    ):
        sleep_s = max(sleep_s, 2.0)
    return sleep_s, global_backoff


def error_tf_value(tf: str | None) -> str | None:
    return str(tf) if tf is not None else None


def remote_fetch_start_payload(
    *,
    exchange: str,
    symbol: str,
    tf: str,
    since_ts: int,
    limit: int,
    attempt: int,
    params: dict[str, object],
) -> dict[str, object]:
    return {
        "kind": "ccxt_fetch_ohlcv",
        "stage": "start",
        "exchange": exchange,
        "symbol": symbol,
        "tf": tf,
        "since_ts": since_ts,
        "limit": limit,
        "attempt": attempt,
        "params": dict(params),
    }


def remote_fetch_ok_payload(
    *,
    exchange: str,
    symbol: str,
    tf: str,
    since_ts: int,
    rows: int,
    first_ts: int | None,
    last_ts: int | None,
    elapsed_ms: int,
) -> dict[str, object]:
    return {
        "kind": "ccxt_fetch_ohlcv",
        "stage": "ok",
        "exchange": exchange,
        "symbol": symbol,
        "tf": tf,
        "since_ts": since_ts,
        "rows": rows,
        "first_ts": first_ts,
        "last_ts": last_ts,
        "elapsed_ms": elapsed_ms,
    }


def remote_fetch_error_payload(
    *,
    exchange: str,
    symbol: str,
    tf: str | None,
    since_ts: int,
    attempt: int,
    elapsed_ms: int | None,
    params: dict[str, object] | None,
    err_type: str,
    error: str,
    error_repr: str,
) -> dict[str, object]:
    return {
        "kind": "ccxt_fetch_ohlcv",
        "stage": "error",
        "exchange": exchange,
        "symbol": symbol,
        "tf": tf,
        "since_ts": since_ts,
        "attempt": attempt,
        "elapsed_ms": elapsed_ms,
        "params": dict(params) if params is not None else None,
        "error_type": err_type,
        "error": error,
        "error_repr": error_repr,
    }


async def fetch_ohlcv_with_optional_semaphore(
    *,
    exchange,
    symbol: str,
    tf: str,
    since_ms: int,
    limit: int,
    params: dict[str, object],
    net_sem,
    apply_rate_limit_backoff_fn,
) -> list:
    if net_sem is not None:
        async with net_sem:
            await apply_rate_limit_backoff_fn()
            return await exchange.fetch_ohlcv(
                symbol,
                timeframe=tf,
                since=since_ms,
                limit=limit,
                params=params,
            )
    return await exchange.fetch_ohlcv(
        symbol,
        timeframe=tf,
        since=since_ms,
        limit=limit,
        params=params,
    )


def fetch_error_context(
    error: BaseException,
    *,
    elapsed_ms: int | None,
    params: dict[str, object] | None,
) -> dict[str, object]:
    err_type, msg_l, is_rate_limit = classify_fetch_error(error)
    return {
        "err_type": err_type,
        "msg_l": msg_l,
        "is_rate_limit": is_rate_limit,
        "error": str(error),
        "error_repr": repr(error),
        "elapsed_ms": elapsed_ms,
        "params": dict(params) if params is not None else None,
    }


def normalize_ccxt_ohlcv_rows(
    rows: list,
    *,
    ex_id: str,
    one_min_ms: int,
    candle_dtype,
    floor_minute_fn,
    normalize_ccxt_volume_to_base_fn,
) -> np.ndarray:
    if not rows:
        return np.empty((0,), dtype=candle_dtype)
    out = []
    for row in rows:
        try:
            ts = int(row[0])
            if ts % one_min_ms != 0:
                ts = floor_minute_fn(ts)
            o, h, l, c = map(float, (row[1], row[2], row[3], row[4]))
            bv = float(row[5]) if len(row) > 5 else 0.0
            bv = normalize_ccxt_volume_to_base_fn(ex_id, c, bv)
            out.append((ts, o, h, l, c, bv))
        except Exception:
            continue
    if not out:
        return np.empty((0,), dtype=candle_dtype)
    arr = np.array(out, dtype=candle_dtype)
    arr = np.sort(arr, order="ts")
    ts = arr["ts"].astype(np.int64)
    keep = np.ones(len(arr), dtype=bool)
    last = None
    for i in range(len(arr)):
        if last is not None and ts[i] == last:
            keep[i - 1] = False
        last = ts[i]
    return arr[keep]


def paginated_fetch_state(
    *,
    since_ms: int,
    end_exclusive_ms: int,
    period_ms: int,
    since_exclusive: bool,
    overlap_candles: int,
) -> tuple[int, int, int, int, int | None]:
    since_start = int(since_ms)
    since = int(since_ms)
    end_excl = int(end_exclusive_ms)
    if since_exclusive and overlap_candles > 0 and since > 0:
        overlap_ms = period_ms * int(overlap_candles)
        since = max(0, since - overlap_ms)
    total_span = max(1, end_excl - since_start)
    return since_start, since, end_excl, total_span, None


def compute_next_since(last_ts: int, *, period_ms: int, overlap_candles: int, current_since: int) -> int:
    new_since = last_ts + period_ms
    if overlap_candles > 0:
        overlap_ms = period_ms * int(overlap_candles)
        new_since = max(last_ts - overlap_ms, current_since + period_ms)
    return new_since


def progress_percent(last_ts: int, since_start: int, total_span: int) -> float:
    try:
        return max(0.0, min(100.0, 100.0 * float(last_ts - since_start) / float(total_span)))
    except Exception:
        return 0.0
