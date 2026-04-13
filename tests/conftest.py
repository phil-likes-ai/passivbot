import math
import os
import sys
import types
from typing import Any, cast

# Ensure we can import modules from the src/ directory as "downloader"
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(ROOT_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# Test runs verify Rust behavior explicitly through dedicated cargo checks/tests, so bypass
# import-time runtime-stamp enforcement inside Python module collection.
os.environ.setdefault("PASSIVBOT_SKIP_RUNTIME_EXTENSION_VERIFY", "1")


def _install_passivbot_rust_stub():
    if "passivbot_rust" in sys.modules:
        return

    try:
        import importlib

        importlib.import_module("passivbot_rust")
        return
    except Exception:
        pass

    # If pytest is launched outside the venv, try the project venv site-packages
    # before falling back to the lightweight stub.
    try:
        import importlib

        pyver = f"python{sys.version_info.major}.{sys.version_info.minor}"
        candidate_sites = [
            os.path.join(ROOT_DIR, "venv", "lib", pyver, "site-packages"),
            os.path.join(ROOT_DIR, "venv", "Lib", "site-packages"),
        ]
        for venv_site in candidate_sites:
            if os.path.isdir(venv_site) and venv_site not in sys.path:
                sys.path.insert(0, venv_site)
        importlib.import_module("passivbot_rust")
        return
    except Exception:
        pass

    stub = types.ModuleType("passivbot_rust")
    stub_any = cast(Any, stub)
    stub_any.__is_stub__ = True

    def _identity(x, *_args, **_kwargs):
        return x

    def _round(value, step):
        if step == 0:
            return value
        return round(value / step) * step

    def _round_up(value, step):
        if step == 0:
            return value
        return math.ceil(value / step) * step

    def _round_dn(value, step):
        if step == 0:
            return value
        return math.floor(value / step) * step

    stub_any.calc_diff = lambda price, reference: price - reference
    stub_any.calc_order_price_diff = lambda side, price, market: (
        (0.0 if not market else (1 - price / market))
        if str(side).lower() in ("buy", "long")
        else (0.0 if not market else (price / market - 1))
    )
    stub_any.calc_min_entry_qty = lambda *args, **kwargs: 0.0
    stub_any.calc_min_entry_qty_py = stub_any.calc_min_entry_qty
    stub_any.round_ = _round
    stub_any.round_dn = _round_dn
    stub_any.round_up = _round_up
    stub_any.round_dynamic = _identity
    stub_any.round_dynamic_up = _identity
    stub_any.round_dynamic_dn = _identity
    stub_any.calc_pnl_long = (
        lambda entry_price, close_price, qty, c_mult=1.0: (close_price - entry_price) * qty
    )
    stub_any.calc_pnl_short = (
        lambda entry_price, close_price, qty, c_mult=1.0: (entry_price - close_price) * qty
    )
    stub_any.calc_pprice_diff_int = lambda *args, **kwargs: 0

    def _calc_auto_unstuck_allowance(balance, loss_allowance_pct, pnl_cumsum_max, pnl_cumsum_last):
        balance_peak = balance + (pnl_cumsum_max - pnl_cumsum_last)
        drop_since_peak_pct = balance / balance_peak - 1.0
        return max(0.0, balance_peak * (loss_allowance_pct + drop_since_peak_pct))

    stub_any.calc_auto_unstuck_allowance = _calc_auto_unstuck_allowance
    stub_any.calc_wallet_exposure = (
        lambda c_mult, balance, size, price: abs(size) * price / max(balance, 1e-12)
    )
    stub_any.cost_to_qty = lambda cost, price, c_mult=1.0: (
        0.0 if price == 0 else cost / (price * (c_mult if c_mult else 1.0))
    )
    stub_any.qty_to_cost = lambda qty, price, c_mult=1.0: qty * price * (c_mult if c_mult else 1.0)

    stub_any.hysteresis = _identity
    stub_any.calc_entries_long_py = lambda *args, **kwargs: []
    stub_any.calc_entries_short_py = lambda *args, **kwargs: []
    stub_any.calc_closes_long_py = lambda *args, **kwargs: []
    stub_any.calc_closes_short_py = lambda *args, **kwargs: []
    stub_any.calc_unstucking_close_py = lambda *args, **kwargs: None

    # Order type IDs must match passivbot_rust exactly
    _order_map = {
        "entry_initial_normal_long": 0,
        "entry_initial_partial_long": 1,
        "entry_trailing_normal_long": 2,
        "entry_trailing_cropped_long": 3,
        "entry_grid_normal_long": 4,
        "entry_grid_cropped_long": 5,
        "entry_grid_inflated_long": 6,
        "close_grid_long": 7,
        "close_trailing_long": 8,
        "close_unstuck_long": 9,
        "close_auto_reduce_twel_long": 10,
        "entry_initial_normal_short": 11,
        "entry_initial_partial_short": 12,
        "entry_trailing_normal_short": 13,
        "entry_trailing_cropped_short": 14,
        "entry_grid_normal_short": 15,
        "entry_grid_cropped_short": 16,
        "entry_grid_inflated_short": 17,
        "close_grid_short": 18,
        "close_trailing_short": 19,
        "close_unstuck_short": 20,
        "close_auto_reduce_twel_short": 21,
        "close_panic_long": 22,
        "close_panic_short": 23,
        "close_auto_reduce_wel_long": 24,
        "close_auto_reduce_wel_short": 25,
        "empty": 65535,
    }
    stub_any.get_order_id_type_from_string = lambda name: _order_map.get(name, 0)
    stub_any.order_type_id_to_snake = lambda type_id: {v: k for k, v in _order_map.items()}.get(
        type_id, "other"
    )
    stub_any.order_type_snake_to_id = lambda name: _order_map.get(name, 0)

    stub_any.run_backtest = lambda *args, **kwargs: {}
    stub_any.gate_entries_by_twel_py = lambda *args, **kwargs: []
    stub_any.calc_twel_enforcer_orders_py = lambda *args, **kwargs: []

    # Minimal stub for orchestrator JSON API
    def _compute_ideal_orders_json(input_json: str) -> str:
        """Stub orchestrator that returns empty orders."""
        import json

        return json.dumps({"orders": []})

    stub_any.compute_ideal_orders_json = _compute_ideal_orders_json

    sys.modules["passivbot_rust"] = stub


_install_passivbot_rust_stub()


import pytest


@pytest.fixture(autouse=True)
def _restore_passivbot_rust_module_after_test():
    original = sys.modules.get("passivbot_rust")
    original_attrs = dict(getattr(original, "__dict__", {})) if original is not None else None
    yield
    current = sys.modules.get("passivbot_rust")
    if original is None:
        if current is not None and getattr(current, "__is_stub__", False):
            sys.modules.pop("passivbot_rust", None)
    elif sys.modules.get("passivbot_rust") is not original:
        sys.modules["passivbot_rust"] = original
    elif current is original and original_attrs is not None:
        for key in list(current.__dict__.keys()):
            if key not in original_attrs:
                delattr(current, key)
        for key, value in original_attrs.items():
            setattr(current, key, value)
