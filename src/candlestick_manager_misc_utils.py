from __future__ import annotations

import inspect
import logging
import os
import re
import time
import shutil
from datetime import datetime, timezone
from pathlib import Path


def looks_like_daily_shard_filename(name: str) -> bool:
    if not isinstance(name, str) or not name.endswith(".npy"):
        return False
    stem = name[:-4]
    if len(stem) != 10 or stem[4] != "-" or stem[7] != "-":
        return False
    try:
        datetime.strptime(stem, "%Y-%m-%d")
    except ValueError:
        return False
    return True


def quarantine_root_level_timeframe_debris(cache_base: str) -> int:
    root = Path(cache_base)
    if not root.is_dir():
        return 0

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    moved = 0

    for exchange_dir in root.iterdir():
        if not exchange_dir.is_dir() or exchange_dir.name.startswith("."):
            continue
        if exchange_dir.name.startswith("_"):
            continue

        for tf_dir in exchange_dir.iterdir():
            if not tf_dir.is_dir():
                continue

            debris = []
            for child in tf_dir.iterdir():
                if not child.is_file():
                    continue
                if child.name == "index.json" or looks_like_daily_shard_filename(child.name):
                    debris.append(child)

            if not debris:
                continue

            quarantine_dir = root / "_quarantine_root_level" / stamp / exchange_dir.name / tf_dir.name
            quarantine_dir.mkdir(parents=True, exist_ok=True)

            for child in debris:
                shutil.move(str(child), str(quarantine_dir / child.name))
                moved += 1

            logging.warning(
                "Quarantined %d invalid root-level OHLCV cache artifact(s) from %s -> %s",
                len(debris),
                tf_dir,
                quarantine_dir,
            )

    return moved


def tf_to_ms(s: str | None, one_min_ms: int) -> int:
    if not isinstance(s, str) or not s:
        return one_min_ms
    st = s.strip().lower()
    match = re.fullmatch(r"(\d+)([smhd])", st)
    if not match:
        return one_min_ms
    n, unit = int(match.group(1)), match.group(2)
    if unit == "s":
        return max(one_min_ms, (n // 60) * one_min_ms)
    if unit == "m":
        return n * one_min_ms
    if unit == "h":
        return n * 60 * one_min_ms
    if unit == "d":
        return n * 1440 * one_min_ms
    return one_min_ms


def utc_now_ms() -> int:
    return int(time.time() * 1000)


def utc_now_datetime() -> datetime:
    return datetime.utcnow()


def get_caller_name(depth: int = 2, logger: logging.Logger | None = None) -> str:
    def frame_to_name(fr) -> str:
        try:
            func = getattr(fr.f_code, "co_name", "unknown")
            mod = fr.f_globals.get("__name__", None)
            cls = None
            if "self" in fr.f_locals and fr.f_locals["self"] is not None:
                cls = type(fr.f_locals["self"]).__name__
            elif "cls" in fr.f_locals and fr.f_locals["cls"] is not None:
                cls = getattr(fr.f_locals["cls"], "__name__", None)
            parts = []
            if isinstance(mod, str) and mod:
                parts.append(mod)
            if isinstance(cls, str) and cls:
                parts.append(cls)
            if isinstance(func, str) and func:
                parts.append(func)
            return ".".join(parts) if parts else "unknown"
        except Exception:
            return "unknown"

    frame = inspect.currentframe()
    target = frame
    fallback_name = "unknown"
    preferred = None
    try:
        for _ in range(max(0, int(depth))):
            if target is None:
                break
            target = target.f_back
        if target is not None:
            fallback_name = frame_to_name(target)

        cur = target
        for _ in range(20):
            if cur is None:
                break
            try:
                slf = cur.f_locals.get("self") if hasattr(cur, "f_locals") else None
                is_cm = slf is not None and type(slf).__name__ == "CandlestickManager"
            except Exception:
                is_cm = False
            func = getattr(getattr(cur, "f_code", None), "co_name", "")
            try:
                mod = cur.f_globals.get("__name__")
            except Exception:
                mod = None

            skip_names = {
                "one",
                "<listcomp>",
                "<dictcomp>",
                "<lambda>",
                "_run",
                "gather",
                "create_task",
            }
            is_asyncio = isinstance(mod, str) and (
                mod.startswith("asyncio.") or mod == "asyncio.events"
            )
            if not is_cm and func not in skip_names and not is_asyncio:
                name = frame_to_name(cur)
                if isinstance(mod, str) and "passivbot" in mod and name and name != "unknown":
                    preferred = name
                    break
                if name and name != "unknown" and preferred is None:
                    preferred = name
            cur = cur.f_back
    finally:
        try:
            del frame
        except Exception:
            pass
        try:
            del target
        except Exception:
            pass
    return preferred or fallback_name


def quarantine_gateio_cache_if_stale(cache_base: str, cutoff_date: str) -> None:
    try:
        cutoff = datetime.strptime(cutoff_date, "%Y-%m-%d").date()
    except ValueError:
        logging.warning(
            "Invalid GATEIO_CACHE_CUTOFF_DATE=%r; skipping gateio cache check", cutoff_date
        )
        return

    gateio_root = os.path.join(cache_base, "gateio")
    if not os.path.isdir(gateio_root):
        return

    tf_root = os.path.join(gateio_root, "1m")
    if not os.path.isdir(tf_root):
        return

    for sym in os.listdir(tf_root):
        sym_dir = os.path.join(tf_root, sym)
        if not os.path.isdir(sym_dir):
            continue
        for fname in os.listdir(sym_dir):
            if not fname.endswith(".npy"):
                continue
            try:
                day = datetime.strptime(fname[:10], "%Y-%m-%d").date()
            except ValueError:
                continue
            if day < cutoff:
                stamp = utc_now_datetime().strftime("%Y%m%dT%H%M%SZ")
                backup = f"{gateio_root}_backup_{stamp}"
                logging.warning(
                    "GateIO cache has shards before %s; moving %s -> %s. Delete backup after confirming volumes are correct.",
                    cutoff_date,
                    gateio_root,
                    backup,
                )
                try:
                    os.rename(gateio_root, backup)
                except OSError as exc:
                    logging.error("Failed to move gateio cache to backup: %s", exc)
                return
