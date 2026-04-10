from __future__ import annotations

import os
from pathlib import Path
from typing import List


def legacy_coin_from_symbol(symbol: str) -> str:
    symbol = str(symbol or "")
    if not symbol:
        return ""
    if "/" in symbol:
        base = symbol.split("/", 1)[0]
    elif ":" in symbol:
        base = symbol.split(":", 1)[0]
    else:
        base = symbol
    base = base.strip()
    if "_" in base:
        left, right = base.rsplit("_", 1)
        if right in {"USDT", "USDC", "USD", "BUSD"}:
            base = left
    return base


def legacy_symbol_code_from_symbol(archive_symbol_code_fn, symbol: str) -> str:
    try:
        return archive_symbol_code_fn(symbol)
    except Exception:
        return ""


def legacy_shard_candidates(
    exchange_name: str,
    symbol: str,
    date_key: str,
    tf: str,
    archive_symbol_code_fn,
) -> List[str]:
    if tf != "1m":
        return []
    ex = str(exchange_name or "").lower()
    coin = legacy_coin_from_symbol(symbol)
    sym_code = legacy_symbol_code_from_symbol(archive_symbol_code_fn, symbol)
    out: List[str] = []
    if coin:
        out.append(os.path.join("historical_data", f"ohlcvs_{ex}", coin, f"{date_key}.npy"))
    if ex == "binanceusdm" and sym_code:
        out.append(os.path.join("historical_data", "ohlcvs_futures", sym_code, f"{date_key}.npy"))
    if ex == "bybit" and sym_code:
        out.append(os.path.join("historical_data", "ohlcvs_bybit", sym_code, f"{date_key}.npy"))
    return out


def legacy_shard_dirs(exchange_name: str, symbol: str, tf: str, archive_symbol_code_fn) -> List[str]:
    if tf != "1m":
        return []
    ex = str(exchange_name or "").lower()
    coin = legacy_coin_from_symbol(symbol)
    sym_code = legacy_symbol_code_from_symbol(archive_symbol_code_fn, symbol)
    out: List[str] = []
    if coin:
        out.append(os.path.join("historical_data", f"ohlcvs_{ex}", coin))
    if ex == "binanceusdm" and sym_code:
        out.append(os.path.join("historical_data", "ohlcvs_futures", sym_code))
    if ex == "bybit" and sym_code:
        out.append(os.path.join("historical_data", "ohlcvs_bybit", sym_code))
    return out


def scan_legacy_shard_paths(directories: List[str]) -> tuple[dict[str, str], list[str]]:
    mapping: dict[str, str] = {}
    scanned_dirs: list[str] = []
    for directory in directories:
        try:
            dp = Path(directory)
            if not dp.exists():
                continue
            scanned_dirs.append(str(dp))
            for path in dp.glob("*.npy"):
                name = path.stem
                if len(name) == 10 and name[4] == "-" and name[7] == "-":
                    mapping.setdefault(name, str(path))
        except Exception:
            continue
    return mapping, scanned_dirs
