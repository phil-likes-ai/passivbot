import logging

from passivbot import Passivbot
import passivbot as pb_mod
import passivbot_symbol_utils as pb_symbol_utils


def test_add_to_coins_lists_skips_symbols_not_in_eligible_markets(caplog):
    bot = Passivbot.__new__(Passivbot)
    bot.exchange = "bitget"
    bot.markets_dict = {"AAA/USDT:USDT": {"swap": True}}
    bot.eligible_symbols = {"AAA/USDT:USDT"}
    bot.approved_coins = {"long": set(), "short": set()}
    bot.ignored_coins = {"long": set(), "short": set()}

    def fake_coin_to_symbol(self, coin, verbose=True):
        mapping = {"AAA": "AAA/USDT:USDT", "BBB": "BBB/USDT:USDT"}
        return mapping.get(coin, f"{coin}/USDT:USDT")

    bot.coin_to_symbol = fake_coin_to_symbol.__get__(bot, Passivbot)

    with caplog.at_level(logging.INFO):
        bot.add_to_coins_lists(
            {"long": ["AAA", "BBB"], "short": []},
            "approved_coins",
            log_psides={"long"},
        )
        bot.add_to_coins_lists(
            {"long": ["AAA", "BBB"], "short": []},
            "approved_coins",
            log_psides={"long"},
        )

    assert bot.approved_coins["long"] == {"AAA/USDT:USDT"}
    assert bot.approved_coins["short"] == set()
    warnings = [
        rec.message for rec in caplog.records if "skipping unsupported markets" in rec.message.lower()
    ]
    assert len(warnings) == 1


def test_refresh_approved_ignored_coin_lists_supports_explicit_all_per_side():
    bot = Passivbot.__new__(Passivbot)
    bot.exchange = "bitget"
    bot.eligible_symbols = {"AAA/USDT:USDT", "BBB/USDT:USDT"}
    bot.approved_coins = {"long": set(), "short": set()}
    bot.ignored_coins = {"long": set(), "short": set()}
    bot.approved_coins_minus_ignored_coins = {"long": set(), "short": set()}
    bot._disabled_psides_logged = set()
    bot._unsupported_coin_warnings = set()
    bot.config = {
        "_coins_sources": {
            "approved_coins": {"long": ["AAA"], "short": "all"},
            "ignored_coins": {"long": [], "short": []},
        },
        "live": {},
    }

    def fake_coin_to_symbol(self, coin, verbose=True):
        mapping = {"AAA": "AAA/USDT:USDT", "BBB": "BBB/USDT:USDT"}
        return mapping.get(coin, f"{coin}/USDT:USDT")

    bot.coin_to_symbol = fake_coin_to_symbol.__get__(bot, Passivbot)
    bot.is_pside_enabled = lambda pside: True
    bot.live_value = lambda key: bot.config["live"][key]
    bot._filter_approved_symbols = lambda pside, symbols: symbols

    bot.refresh_approved_ignored_coins_lists()

    assert bot.approved_coins["long"] == {"AAA/USDT:USDT"}
    assert bot.approved_coins["short"] == {"AAA/USDT:USDT", "BBB/USDT:USDT"}
    assert bot.approved_coins_minus_ignored_coins["short"] == {"AAA/USDT:USDT", "BBB/USDT:USDT"}


def test_refresh_approved_ignored_coin_lists_supports_migrated_global_all():
    bot = Passivbot.__new__(Passivbot)
    bot.exchange = "bitget"
    bot.eligible_symbols = {"AAA/USDT:USDT", "BBB/USDT:USDT"}
    bot.approved_coins = {"long": set(), "short": set()}
    bot.ignored_coins = {"long": set(), "short": set()}
    bot.approved_coins_minus_ignored_coins = {"long": set(), "short": set()}
    bot._disabled_psides_logged = set()
    bot._unsupported_coin_warnings = set()
    bot.config = {
        "_coins_sources": {
            "approved_coins": "all",
            "ignored_coins": {"long": [], "short": []},
        },
        "live": {},
    }

    def fake_coin_to_symbol(self, coin, verbose=True):
        mapping = {"AAA": "AAA/USDT:USDT", "BBB": "BBB/USDT:USDT"}
        return mapping.get(coin, f"{coin}/USDT:USDT")

    bot.coin_to_symbol = fake_coin_to_symbol.__get__(bot, Passivbot)
    bot.is_pside_enabled = lambda pside: True
    bot.live_value = lambda key: bot.config["live"][key]
    bot._filter_approved_symbols = lambda pside, symbols: symbols

    bot.refresh_approved_ignored_coins_lists()

    assert bot.approved_coins["long"] == {"AAA/USDT:USDT", "BBB/USDT:USDT"}
    assert bot.approved_coins["short"] == {"AAA/USDT:USDT", "BBB/USDT:USDT"}


def test_get_symbols_with_pos_returns_expected_sets():
    bot = Passivbot.__new__(Passivbot)
    bot.positions = {
        "AAA/USDT:USDT": {"long": {"size": 1.0}, "short": {"size": 0.0}},
        "BBB/USDT:USDT": {"long": {"size": 0.0}, "short": {"size": -2.0}},
        "CCC/USDT:USDT": {"long": {"size": 0.0}, "short": {"size": 0.0}},
    }

    assert bot.get_symbols_with_pos("long") == {"AAA/USDT:USDT"}
    assert bot.get_symbols_with_pos("short") == {"BBB/USDT:USDT"}
    assert bot.get_symbols_with_pos() == {"AAA/USDT:USDT", "BBB/USDT:USDT"}


def test_get_symbols_approved_or_has_pos_includes_forced_normal_overrides():
    bot = Passivbot.__new__(Passivbot)
    bot.approved_coins_minus_ignored_coins = {
        "long": {"AAA/USDT:USDT"},
        "short": {"SHORT/USDT:USDT"},
    }
    bot.positions = {
        "BBB/USDT:USDT": {"long": {"size": 1.0}, "short": {"size": 0.0}},
        "CCC/USDT:USDT": {"long": {"size": 0.0}, "short": {"size": 0.0}},
    }
    bot.coin_overrides = {"CCC/USDT:USDT": {}, "DDD/USDT:USDT": {}}

    def forced_mode(pside, symbol=None):
        return "normal" if symbol == "CCC/USDT:USDT" else "manual"

    bot.get_forced_PB_mode = forced_mode

    assert bot.get_symbols_approved_or_has_pos("long") == {
        "AAA/USDT:USDT",
        "BBB/USDT:USDT",
        "CCC/USDT:USDT",
    }
    assert bot.get_symbols_approved_or_has_pos("short") == {"SHORT/USDT:USDT", "CCC/USDT:USDT"}
    assert bot.get_symbols_approved_or_has_pos() == {
        "AAA/USDT:USDT",
        "BBB/USDT:USDT",
        "CCC/USDT:USDT",
        "SHORT/USDT:USDT",
    }


def test_log_coin_symbol_fallback_summary_logs_only_when_counts_change(
    monkeypatch, caplog
):
    bot = Passivbot.__new__(Passivbot)
    bot._last_coin_symbol_warning_counts = {
        "symbol_to_coin_fallbacks": 0,
        "coin_to_symbol_fallbacks": 0,
    }
    counts_sequence = iter(
        [
            {"symbol_to_coin_fallbacks": 1, "coin_to_symbol_fallbacks": 2},
            {"symbol_to_coin_fallbacks": 1, "coin_to_symbol_fallbacks": 2},
            {"symbol_to_coin_fallbacks": 2, "coin_to_symbol_fallbacks": 2},
        ]
    )

    def next_counts():
        return next(counts_sequence)

    monkeypatch.setattr(pb_mod, "coin_symbol_warning_counts", next_counts, raising=False)
    monkeypatch.setattr(pb_symbol_utils, "coin_symbol_warning_counts", next_counts, raising=False)

    with caplog.at_level(logging.INFO):
        bot._log_coin_symbol_fallback_summary()
        bot._log_coin_symbol_fallback_summary()
        bot._log_coin_symbol_fallback_summary()

    messages = [record.message for record in caplog.records if "[mapping] fallbacks" in record.message]
    assert len(messages) == 2
    assert "symbol->coin=1 | coin->symbol=2" in messages[0]
    assert "symbol->coin=2 | coin->symbol=2" in messages[1]
