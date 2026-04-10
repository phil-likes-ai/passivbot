from importlib import import_module


pb_orchestrator_utils = import_module("passivbot_orchestrator_utils")


def test_build_ema_pairs_sorts_and_casts():
    pairs = pb_orchestrator_utils.build_ema_pairs({2.0: 3.0, 1.0: 4.0})
    assert pairs == [[1.0, 4.0], [2.0, 3.0]]


def test_build_side_input_uses_defaults_and_overrides():
    mode_overrides = {"long": {"BTC/USDT:USDT": "tp_only"}, "short": {}}
    positions = {"BTC/USDT:USDT": {"long": {"size": 1.5, "price": 100.0}}}
    trailing_prices = {}

    side = pb_orchestrator_utils.build_side_input(
        pside="long",
        symbol="BTC/USDT:USDT",
        mode_overrides=mode_overrides,
        positions=positions,
        trailing_prices=trailing_prices,
        bot_params_to_rust_dict_fn=lambda pside, symbol: {"pside": pside, "symbol": symbol},
        mode_override_to_orchestrator_mode_fn=lambda mode: f"mode:{mode}",
        trailing_bundle_default_fn=lambda: {"min_since_open": 0.0},
    )

    assert side["mode"] == "mode:tp_only"
    assert side["position"] == {"size": 1.5, "price": 100.0}
    assert side["trailing"]["min_since_open"] == 0.0
    assert side["bot_params"]["symbol"] == "BTC/USDT:USDT"


def test_build_symbol_input_wires_fields():
    symbol_input = pb_orchestrator_utils.build_symbol_input(
        symbol="BTC/USDT:USDT",
        idx=3,
        mprice=101.5,
        active=True,
        qty_step=0.1,
        price_step=0.01,
        min_qty=0.001,
        min_cost=5.0,
        c_mult=1.0,
        maker_fee=0.0002,
        taker_fee=0.0006,
        effective_min_cost=6.0,
        m1_close_emas={2.0: 3.0},
        m1_volume_emas={1.0: 2.0},
        m1_log_range_emas={4.0: 5.0},
        h1_log_range_emas={6.0: 7.0},
        side_input_fn=lambda pside: {"mode": pside},
    )

    assert symbol_input["symbol_idx"] == 3
    assert symbol_input["order_book"]["bid"] == 101.5
    assert symbol_input["exchange"]["min_cost"] == 5.0
    assert symbol_input["emas"]["m1"]["close"] == [[2.0, 3.0]]
    assert symbol_input["emas"]["h1"]["log_range"] == [[6.0, 7.0]]
    assert symbol_input["long"]["mode"] == "long"


def test_build_orchestrator_input_base_wires_global():
    base = pb_orchestrator_utils.build_orchestrator_input_base(
        balance=10.0,
        balance_raw=11.0,
        filter_by_min_effective_cost=True,
        market_orders_allowed=False,
        market_order_near_touch_threshold=0.5,
        panic_close_market=True,
        unstuck_allowance_long=1.0,
        unstuck_allowance_short=2.0,
        max_realized_loss_pct=0.3,
        realized_pnl_cumsum_max=4.0,
        realized_pnl_cumsum_last=5.0,
        global_bp={"long": {}, "short": {}},
        effective_hedge_mode=True,
    )

    assert base["balance"] == 10.0
    assert base["balance_raw"] == 11.0
    assert base["global"]["panic_close_market"] is True
    assert base["global"]["unstuck_allowance_short"] == 2.0
    assert base["symbols"] == []


def test_build_ideal_orders_by_symbol_groups_and_converts():
    orders = [
        {"symbol_idx": 1, "qty": 1, "price": 2, "order_type": "close", "execution_type": "market"},
        {"symbol_idx": 2, "qty": 3, "price": 4, "order_type": "entry"},
    ]
    ideal = pb_orchestrator_utils.build_ideal_orders_by_symbol(
        orders=orders,
        idx_to_symbol={1: "BTC/USDT:USDT", 2: "ETH/USDT:USDT"},
        order_type_snake_to_id_fn=lambda order_type: 7 if order_type == "close" else 8,
    )

    assert ideal["BTC/USDT:USDT"][0][0] == 1.0
    assert ideal["BTC/USDT:USDT"][0][3] == 7
    assert ideal["ETH/USDT:USDT"][0][4] == "limit"


def test_extract_unstuck_log_payload_returns_first_match():
    payload = pb_orchestrator_utils.extract_unstuck_log_payload(
        orders=[{"order_type": "close_unstuck_long", "symbol_idx": 0}],
        idx_to_symbol={0: "BTC/USDT:USDT"},
        positions={"BTC/USDT:USDT": {"long": {"price": 100.0}}},
        last_prices={"BTC/USDT:USDT": 110.0},
        unstuck_allowances={"long": 3.5},
    )

    assert payload["coin"] == "BTC"
    assert payload["pside"] == "long"
    assert payload["allowance"] == 3.5


def test_log_missing_ema_error_logs_symbol():
    calls = []

    class Logger:
        @staticmethod
        def error(msg, *args):
            calls.append((msg, args))

    pb_orchestrator_utils.log_missing_ema_error(
        error=RuntimeError("MissingEma symbol_idx: 2"),
        idx_to_symbol={2: "BTC/USDT:USDT"},
        logger=Logger,
    )

    assert calls
    assert calls[0][0] == "[ema] Missing EMA for %s (symbol_idx=%d)"
