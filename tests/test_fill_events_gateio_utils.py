from importlib import import_module
import types

import pytest


gateio_utils = import_module("fill_events_gateio_utils")


def test_normalize_raw_trade_missing_price_raises():
    with pytest.raises(ValueError, match="Gate.io fill missing required 'price' field"):
        gateio_utils.normalize_raw_trade(
            {"trade_id": "1", "order_id": "o1", "create_time": 1.0, "contract": "BTC_USDT", "size": -2, "fee": 1}
        )


def test_normalize_raw_trade_builds_ccxt_like_payload():
    result = gateio_utils.normalize_raw_trade(
        {"trade_id": "1", "order_id": "o1", "create_time": 1.0, "contract": "BTC_USDT", "size": -2, "price": 100, "fee": 1}
    )

    assert result["id"] == "1"
    assert result["order"] == "o1"
    assert result["symbol"] == "BTC/USDT:USDT"
    assert result["side"] == "sell"
    assert result["amount"] == 2.0


def test_normalize_trade_missing_qty_raises(monkeypatch):
    monkeypatch.setattr(gateio_utils, "ts_to_date", lambda ts: f"T{ts}")
    fetcher = type("F", (), {"ensure_millis": staticmethod(lambda x: x)})()
    detail_cache = {}

    trade = {"id": "t1", "order": "o1", "timestamp": 1000, "symbol": "BTC/USDT:USDT", "side": "buy", "price": 100.0, "fee": {"cost": 1.0}, "info": {}}
    order = {"info": {}}

    with pytest.raises(ValueError, match="Gate.io fill missing required 'qty' field"):
        gateio_utils.normalize_trade(fetcher, trade, order, 4.0, 2.0, detail_cache)


def test_normalize_trade_missing_price_raises(monkeypatch):
    monkeypatch.setattr(gateio_utils, "ts_to_date", lambda ts: f"T{ts}")
    fetcher = type("F", (), {"ensure_millis": staticmethod(lambda x: x)})()
    detail_cache = {}

    trade = {"id": "t1", "order": "o1", "timestamp": 1000, "symbol": "BTC/USDT:USDT", "side": "buy", "amount": 2.0, "fee": {"cost": 1.0}, "info": {}}
    order = {"info": {}}

    with pytest.raises(ValueError, match="Gate.io fill missing required 'price' field"):
        gateio_utils.normalize_trade(fetcher, trade, order, 4.0, 2.0, detail_cache)


def test_merge_trades_with_orders_groups_by_order():
    fetcher = types.SimpleNamespace(_normalize_trade=lambda t, order, pnl, total_qty, detail_cache: {"id": t["id"], "pnl": pnl, "qty": total_qty})
    trades = [
        {"id": "t1", "order": "o1", "amount": 1.0, "info": {}},
        {"id": "t2", "order": "o1", "amount": 2.0, "info": {}},
    ]
    orders_by_id = {"o1": {"info": {"pnl": 5.0}}}

    events = gateio_utils.merge_trades_with_orders(fetcher, trades, orders_by_id, {})

    assert len(events) == 2
    assert events[0]["pnl"] == 5.0
    assert events[0]["qty"] == 3.0


def test_determine_position_side_and_normalize_trade(monkeypatch):
    monkeypatch.setattr(gateio_utils, "ts_to_date", lambda ts: f"T{ts}")
    fetcher = type("F", (), {"ensure_millis": staticmethod(lambda x: x)})()
    detail_cache = {}

    assert gateio_utils.determine_position_side("buy", True) == "short"
    assert gateio_utils.determine_position_side("sell", False) == "short"

    trade = {"id": "t1", "order": "o1", "timestamp": 1000, "symbol": "BTC/USDT:USDT", "side": "buy", "amount": 2.0, "price": 100.0, "fee": {"cost": 1.0}, "info": {"text": "cid", "close_size": 0.0}}
    order = {"info": {"text": "cid"}}
    result = gateio_utils.normalize_trade(fetcher, trade, order, 4.0, 2.0, detail_cache)

    assert result["datetime"] == "T1000"
    assert result["position_side"] == "short"
    assert result["pb_order_type"] in {"unknown", "cid"}


@pytest.mark.asyncio
async def test_fetch_orders_for_pnl_collects_pages():
    class API:
        def __init__(self):
            self.calls = 0

        async def fetch_closed_orders(self, params=None):
            self.calls += 1
            if self.calls == 1:
                return [{"id": "o1"}, {"id": "o2"}]
            return []

    fetcher = type("F", (), {"api": API()})()
    result = await gateio_utils.fetch_orders_for_pnl(fetcher, {"o1", "o2"})

    assert sorted(result) == ["o1", "o2"]
