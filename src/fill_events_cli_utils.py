from __future__ import annotations

from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path
from typing import Optional


def parse_time_arg(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        ts = int(value)
        if ts < 10**11:
            ts *= 1000
        return ts
    except ValueError:
        pass
    try:
        if value.lower() == "now":
            dt = datetime.now(tz=timezone.utc)
        else:
            if value.endswith("Z"):
                value = value[:-1] + "+00:00"
            dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        raise ValueError(f"Unable to parse datetime '{value}'")


def parse_log_level(value: str) -> int:
    mapping = {"warning": 0, "warn": 0, "info": 1, "debug": 2, "trace": 3}
    if value is None:
        return 1
    value = str(value).strip().lower()
    if value in mapping:
        return mapping[value]
    try:
        lvl = int(float(value))
        return max(0, min(3, lvl))
    except Exception:
        return 1


def instantiate_bot(config: dict, *, load_user_info, exchange_bot_classes):
    live = config.get("live", {})
    user = str(live.get("user") or "").strip()
    if not user:
        raise ValueError("Config missing live.user to determine bot exchange")
    user_info = load_user_info(user)
    exchange = str(user_info.get("exchange") or "").lower()
    if not exchange:
        raise ValueError(f"User '{user}' has no exchange configured in api-keys.json")
    bot_cls_info = exchange_bot_classes.get(exchange)
    if bot_cls_info is None:
        raise ValueError(f"No bot class registered for exchange '{exchange}'")
    module = import_module(bot_cls_info[0])
    bot_cls = getattr(module, bot_cls_info[1])
    return bot_cls(config)


async def run_cli(
    args,
    *,
    load_input_config,
    prepare_config,
    instantiate_bot_fn,
    extract_symbol_pool,
    build_fetcher_for_bot,
    manager_cls,
    parse_time_arg_fn,
    format_ms_fn,
    logger,
):
    source_config, base_config_path, raw_snapshot = load_input_config(args.config)
    config = prepare_config(
        source_config,
        base_config_path=base_config_path,
        verbose=False,
        target="live",
        runtime="live",
        raw_snapshot=raw_snapshot,
    )
    live = config.setdefault("live", {})
    if args.user:
        live["user"] = args.user
    bot = instantiate_bot_fn(config)
    try:
        symbol_pool = extract_symbol_pool(config, args.symbols)
        fetcher = build_fetcher_for_bot(bot, symbol_pool)
        cache_root = Path(args.cache_root)
        cache_path = cache_root / bot.exchange / bot.user
        manager = manager_cls(
            exchange=bot.exchange,
            user=bot.user,
            fetcher=fetcher,
            cache_path=cache_path,
        )
        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        start_ms = parse_time_arg_fn(args.start) or (now_ms - int(args.lookback_days * 24 * 60 * 60 * 1000))
        end_ms = parse_time_arg_fn(args.end) or now_ms
        if start_ms >= end_ms:
            raise ValueError("start time must be earlier than end time")
        logger.info(
            "fill_events_manager CLI | exchange=%s user=%s start=%s end=%s cache=%s",
            bot.exchange,
            bot.user,
            format_ms_fn(start_ms),
            format_ms_fn(end_ms),
            cache_path,
        )
        await manager.refresh_range(start_ms, end_ms)
        events = manager.get_events(start_ms, end_ms)
        logger.info("fill_events_manager CLI: events=%d written to %s", len(events), cache_path)
    finally:
        try:
            await bot.close()
        except Exception:
            pass
