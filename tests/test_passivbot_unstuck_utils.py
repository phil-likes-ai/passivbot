import logging
import types
from importlib import import_module


pb_unstuck_utils = import_module("passivbot_unstuck_utils")


def test_calc_unstuck_allowance_for_logging_reports_disabled_states():
    bot = types.SimpleNamespace(
        bot_value=lambda pside, key: 0.0,
        _pnls_manager=None,
    )
    assert pb_unstuck_utils.calc_unstuck_allowance_for_logging(bot, "long") == {"status": "disabled"}

    bot2 = types.SimpleNamespace(
        bot_value=lambda pside, key: 1.0 if key == "total_wallet_exposure_limit" else 0.0,
        _pnls_manager=None,
    )
    assert pb_unstuck_utils.calc_unstuck_allowance_for_logging(bot2, "long") == {"status": "unstuck_disabled"}


def test_maybe_log_unstuck_status_respects_interval(monkeypatch):
    monkeypatch.setattr(pb_unstuck_utils, "utc_ms", lambda: 10_000)
    calls = []
    bot = types.SimpleNamespace(
        _unstuck_last_log_ms=9_500,
        _unstuck_log_interval_ms=1_000,
        _log_unstuck_status=lambda: calls.append("logged"),
    )

    pb_unstuck_utils.maybe_log_unstuck_status(bot)
    assert calls == []

    bot._unstuck_last_log_ms = 8_000
    pb_unstuck_utils.maybe_log_unstuck_status(bot)
    assert calls == ["logged"]


def test_log_unstuck_status_formats_output(caplog):
    caplog.set_level(logging.INFO)
    bot = types.SimpleNamespace(
        _calc_unstuck_allowance_for_logging=lambda pside: {
            "long": {"status": "disabled"},
            "short": {"status": "ok", "allowance": -1.5, "peak": 120.0, "pct_from_peak": -2.5},
        }[pside]
    )

    pb_unstuck_utils.log_unstuck_status(bot)

    text = caplog.text
    assert "long: disabled" in text
    assert "short: allowance=-1.50 (over budget) | peak=120.00 | pct_from_peak=-2.5%" in text


def test_calc_unstuck_allowances_live_delegates_to_internal_calc():
    bot = types.SimpleNamespace(_calc_unstuck_allowances=lambda allow_new_unstuck: {"long": 1.0, "short": 2.0})

    assert pb_unstuck_utils.calc_unstuck_allowances_live(bot, True) == {"long": 1.0, "short": 2.0}


def test_calc_unstuck_allowances_handles_disabled_and_history_cases(monkeypatch):
    stub = types.SimpleNamespace(calc_auto_unstuck_allowance=lambda balance, pct, maxv, lastv: 42.0)
    monkeypatch.setattr(pb_unstuck_utils, "import_module", lambda name: stub)

    bot = types.SimpleNamespace(_pnls_manager=None)
    assert pb_unstuck_utils.calc_unstuck_allowances(bot, True) == {"long": 0.0, "short": 0.0}

    events = [types.SimpleNamespace(pnl=1.0), types.SimpleNamespace(pnl=2.0)]
    bot2 = types.SimpleNamespace(
        _pnls_manager=object(),
        _get_effective_pnl_events=lambda: events,
        get_raw_balance=lambda: 100.0,
        bot_value=lambda pside, key: 0.5 if key == "unstuck_loss_allowance_pct" else 1.0,
    )
    assert pb_unstuck_utils.calc_unstuck_allowances(bot2, True) == {"long": 42.0, "short": 42.0}
