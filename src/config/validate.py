from .access import require_config_dict
from .bot import BOT_POSITION_SIDES, validate_forager_config
from .coerce import normalize_hsl_cooldown_position_policy, normalize_hsl_signal_mode
import math


def _require_float_in_range(section_cfg: dict, key: str, *, path_prefix: str, min_value=None, max_value=None, inclusive_min=True, inclusive_max=True):
    raw = section_cfg[key]
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise TypeError(f"{path_prefix}.{key} must be numeric") from exc
    if not math.isfinite(value):
        raise ValueError(f"{path_prefix}.{key} must be finite")
    if min_value is not None:
        if inclusive_min and value < min_value:
            raise ValueError(f"{path_prefix}.{key} must be >= {min_value}")
        if not inclusive_min and value <= min_value:
            raise ValueError(f"{path_prefix}.{key} must be > {min_value}")
    if max_value is not None:
        if inclusive_max and value > max_value:
            raise ValueError(f"{path_prefix}.{key} must be <= {max_value}")
        if not inclusive_max and value >= max_value:
            raise ValueError(f"{path_prefix}.{key} must be < {max_value}")
    return value


def _require_optional_int(section_cfg: dict, key: str, *, path_prefix: str, min_value=None):
    raw = section_cfg[key]
    if raw is None:
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise TypeError(f"{path_prefix}.{key} must be an integer or null") from exc
    if min_value is not None and value < min_value:
        raise ValueError(f"{path_prefix}.{key} must be >= {min_value} when set")
    return value


def _validate_pnls_max_lookback_days(section_cfg: dict, key: str, *, path_prefix: str) -> None:
    raw = section_cfg[key]
    if isinstance(raw, str) and raw.strip().lower() == "all":
        return
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{path_prefix}.{key} must be >= 0 or 'all', got {raw!r}") from exc
    if not math.isfinite(value) or value < 0.0:
        raise ValueError(f"{path_prefix}.{key} must be >= 0 or 'all', got {raw!r}")


def validate_live_config(config: dict) -> None:
    live_cfg = require_config_dict(config, "live")

    _require_float_in_range(live_cfg, "leverage", path_prefix="config.live", min_value=0.0, inclusive_min=False)
    _require_float_in_range(live_cfg, "execution_delay_seconds", path_prefix="config.live", min_value=0.0)
    _validate_pnls_max_lookback_days(live_cfg, "pnls_max_lookback_days", path_prefix="live")
    _require_float_in_range(live_cfg, "max_realized_loss_pct", path_prefix="config.live", min_value=0.0, max_value=1.0)
    _require_float_in_range(live_cfg, "warmup_ratio", path_prefix="config.live", min_value=0.0, max_value=1.0)
    _require_float_in_range(live_cfg, "warmup_jitter_seconds", path_prefix="config.live", min_value=0.0)
    _require_float_in_range(live_cfg, "max_warmup_minutes", path_prefix="config.live", min_value=0.0)
    _require_float_in_range(live_cfg, "recv_window_ms", path_prefix="config.live", min_value=0.0, inclusive_min=False)
    _require_float_in_range(live_cfg, "candle_lock_timeout_seconds", path_prefix="config.live", min_value=0.0, inclusive_min=False)
    _require_float_in_range(live_cfg, "market_order_near_touch_threshold", path_prefix="config.live", min_value=0.0)
    _require_float_in_range(live_cfg, "order_match_tolerance_pct", path_prefix="config.live", min_value=0.0)
    _require_float_in_range(live_cfg, "price_distance_threshold", path_prefix="config.live", min_value=0.0)
    _require_float_in_range(live_cfg, "balance_hysteresis_snap_pct", path_prefix="config.live", min_value=0.0)

    _require_optional_int(live_cfg, "max_concurrent_api_requests", path_prefix="config.live", min_value=1)
    _require_optional_int(live_cfg, "max_n_cancellations_per_batch", path_prefix="config.live", min_value=1)
    _require_optional_int(live_cfg, "max_n_creations_per_batch", path_prefix="config.live", min_value=1)
    _require_optional_int(live_cfg, "max_n_restarts_per_day", path_prefix="config.live", min_value=0)
    _require_optional_int(live_cfg, "max_ohlcv_fetches_per_minute", path_prefix="config.live", min_value=1)

    if live_cfg["margin_mode_preference"] not in {"cross", "isolated"}:
        raise ValueError("config.live.margin_mode_preference must be 'cross' or 'isolated'")
    if live_cfg["time_in_force"] not in {
        "good_till_cancelled",
        "post_only",
        "immediate_or_cancel",
    }:
        raise ValueError(
            "config.live.time_in_force must be one of: good_till_cancelled, post_only, immediate_or_cancel"
        )


def validate_bot_risk_config(config: dict) -> None:
    bot_cfg = require_config_dict(config, "bot")
    for pside in BOT_POSITION_SIDES:
        side_cfg = require_config_dict(bot_cfg, pside)

        path_prefix = f"config.bot.{pside}"
        twel = _require_float_in_range(side_cfg, "total_wallet_exposure_limit", path_prefix=path_prefix, min_value=0.0)
        n_positions = _require_optional_int(side_cfg, "n_positions", path_prefix=path_prefix, min_value=0)
        _require_float_in_range(side_cfg, "ema_span_0", path_prefix=path_prefix, min_value=0.0, inclusive_min=False)
        _require_float_in_range(side_cfg, "ema_span_1", path_prefix=path_prefix, min_value=0.0, inclusive_min=False)
        _require_float_in_range(side_cfg, "entry_initial_qty_pct", path_prefix=path_prefix, min_value=0.0, max_value=1.0, inclusive_min=False)
        _require_float_in_range(side_cfg, "close_grid_qty_pct", path_prefix=path_prefix, min_value=0.0, max_value=1.0, inclusive_min=False)
        _require_float_in_range(side_cfg, "unstuck_close_pct", path_prefix=path_prefix, min_value=0.0, max_value=1.0)
        _require_float_in_range(side_cfg, "unstuck_loss_allowance_pct", path_prefix=path_prefix, min_value=0.0, max_value=1.0)
        _require_float_in_range(side_cfg, "unstuck_threshold", path_prefix=path_prefix, min_value=0.0, max_value=1.0)
        _require_float_in_range(side_cfg, "hsl_red_threshold", path_prefix=path_prefix, min_value=0.0, max_value=1.0)
        _require_float_in_range(side_cfg, "risk_twel_enforcer_threshold", path_prefix=path_prefix, min_value=0.0, inclusive_min=False)
        _require_float_in_range(side_cfg, "risk_wel_enforcer_threshold", path_prefix=path_prefix, min_value=0.0, inclusive_min=False)

        if twel > 0.0 and (n_positions is None or n_positions <= 0):
            raise ValueError(
                f"config.bot.{pside}.n_positions must be > 0 when total_wallet_exposure_limit is enabled"
            )


def validate_config(config: dict, *, raw_optimize=None, verbose: bool = True, tracker=None) -> None:
    from analysis_visibility import validate_visible_metrics_config

    del raw_optimize
    require_config_dict(config, "monitor")
    normalize_hsl_signal_mode(config["live"]["hsl_signal_mode"])
    normalize_hsl_cooldown_position_policy(config["live"]["hsl_position_during_cooldown_policy"])
    monitor_cfg = require_config_dict(config, "monitor")
    if not str(monitor_cfg["root_dir"]).strip():
        raise ValueError("config.monitor.root_dir must be a non-empty string")
    validate_live_config(config)
    validate_bot_risk_config(config)
    validate_visible_metrics_config(config)
    validate_forager_config(config, verbose=verbose, tracker=tracker)
