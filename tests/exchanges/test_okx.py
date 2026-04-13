import pytest

from exchanges.okx import OKXBot


class _DummyOKX(OKXBot):
    pass


def test_get_position_side_for_order_raises_when_pos_side_missing():
    bot = _DummyOKX.__new__(_DummyOKX)
    bot.exchange = "okx"

    with pytest.raises(KeyError, match="missing posSide"):
        bot._get_position_side_for_order({"info": {}})


def test_normalize_positions_raises_when_side_missing_on_open_position():
    bot = _DummyOKX.__new__(_DummyOKX)
    bot.exchange = "okx"
    bot._record_live_margin_mode = lambda symbol, margin_mode: None
    bot._extract_live_margin_mode = lambda elm: None

    with pytest.raises(KeyError, match="missing side"):
        bot._normalize_positions(
            [{"symbol": "BTC/USDT:USDT", "contracts": 1, "entryPrice": 100.0}]
        )


def test_normalize_positions_raises_when_contracts_is_boolean():
    bot = _DummyOKX.__new__(_DummyOKX)
    bot.exchange = "okx"
    bot._record_live_margin_mode = lambda symbol, margin_mode: None
    bot._extract_live_margin_mode = lambda elm: None

    with pytest.raises(TypeError, match="invalid boolean contracts"):
        bot._normalize_positions(
            [{"symbol": "BTC/USDT:USDT", "side": "long", "contracts": True, "entryPrice": 100.0}]
        )


def test_normalize_positions_raises_when_entry_price_is_non_positive_for_open_position():
    bot = _DummyOKX.__new__(_DummyOKX)
    bot.exchange = "okx"
    bot._record_live_margin_mode = lambda symbol, margin_mode: None
    bot._extract_live_margin_mode = lambda elm: None

    with pytest.raises(ValueError, match="non-positive entryPrice"):
        bot._normalize_positions(
            [{"symbol": "BTC/USDT:USDT", "side": "long", "contracts": 1, "entryPrice": 0.0}]
        )
