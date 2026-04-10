from importlib import import_module


fee_utils = import_module("fill_events_fee_utils")


def test_merge_fee_lists_merges_by_currency_and_sums_cost():
    result = fee_utils.merge_fee_lists(
        [{"currency": "USDT", "cost": 1.0}],
        [{"currency": "USDT", "cost": 2.5}, {"currency": "BTC", "cost": 0.1}],
    )

    assert sorted(result, key=lambda x: x["currency"]) == [
        {"currency": "BTC", "cost": 0.1},
        {"currency": "USDT", "cost": 3.5},
    ]


def test_fee_cost_sums_defensively():
    assert fee_utils.fee_cost(None) == 0.0
    assert fee_utils.fee_cost([{"cost": 1.0}, {"cost": "2.5"}, "bad"]) == 3.5


def test_normalize_fee_dict_and_extract_bybit_fee_row():
    assert fee_utils.normalize_fee_dict({"currency": "USDT", "cost": "1.5", "rate": "0.1"}) == {
        "currency": "USDT",
        "cost": 1.5,
        "rate": 0.1,
    }

    row = {"info": {"execFee": "2.0", "feeCurrency": "USDT", "feeRate": "0.2"}}
    assert fee_utils.extract_bybit_fee_from_trade_row(row) == {
        "cost": 2.0,
        "currency": "USDT",
        "rate": 0.2,
    }
