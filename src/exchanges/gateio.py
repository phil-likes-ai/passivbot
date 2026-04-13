from exchanges.ccxt_bot import CCXTBot
from passivbot import logging

from utils import ts_to_date, utc_ms
from config.access import require_live_value


class GateIOBot(CCXTBot):
    def __init__(self, config: dict):
        super().__init__(config)
        self.ohlcvs_1m_init_duration_seconds = (
            120  # gateio has stricter rate limiting on fetching ohlcvs
        )
        self.hedge_mode = False
        max_cancel = int(require_live_value(config, "max_n_cancellations_per_batch"))
        self.config["live"]["max_n_cancellations_per_batch"] = min(max_cancel, 20)
        max_create = int(require_live_value(config, "max_n_creations_per_batch"))
        self.config["live"]["max_n_creations_per_batch"] = min(max_create, 10)
        self.custom_id_max_length = 28

    def create_ccxt_sessions(self):
        """GateIO: Add broker header to CCXT config."""
        super().create_ccxt_sessions()
        # Add broker header to both clients
        headers = {"X-Gate-Channel-Id": self.broker_code} if self.broker_code else {}
        for client in [self.cca, self.ccp]:
            if client is not None:
                client.headers.update(headers)

    # ═══════════════════ HOOK OVERRIDES ═══════════════════

    def _get_position_side_for_order(self, order: dict) -> str:
        """GateIO: Derive position side from order side + reduceOnly (one-way mode)."""
        return self.determine_pos_side(order)

    def determine_pos_side(self, order):
        """GateIO-specific logic for one-way mode position side derivation."""
        if order["side"] == "buy":
            return "short" if order["reduceOnly"] else "long"
        if order["side"] == "sell":
            return "long" if order["reduceOnly"] else "short"
        raise Exception(f"unsupported order side {order['side']}")

    # ═══════════════════ GATEIO-SPECIFIC METHODS ═══════════════════

    async def fetch_balance(self) -> float:
        """GateIO: Fetch balance with special UID logic for websockets.

        GateIO requires UID for websocket subscriptions, which is obtained
        from the balance response. Also handles classic vs multi_currency
        margin modes.
        """
        balance_fetched = await self.cca.fetch_balance()
        info = balance_fetched.get("info")
        if not isinstance(info, list) or not info or not isinstance(info[0], dict):
            raise KeyError(f"{self.exchange}: missing info payload in fetch_balance response")
        info0 = info[0]
        if not hasattr(self, "uid") or not self.uid:
            if "user" not in info0:
                raise KeyError(f"{self.exchange}: missing user in fetch_balance response")
            self.uid = info0["user"]
            self.cca.uid = self.uid
            if self.ccp is not None:
                self.ccp.uid = self.uid
        if "margin_mode_name" not in info0:
            raise KeyError(f"{self.exchange}: missing margin_mode_name in fetch_balance response")
        margin_mode_name = info0["margin_mode_name"]
        self.log_once(f"account margin mode: {margin_mode_name}")
        if margin_mode_name == "classic":
            quote_payload = balance_fetched.get(self.quote)
            if not isinstance(quote_payload, dict) or "total" not in quote_payload:
                raise KeyError(
                    f"{self.exchange}: missing classic quote balance for {self.quote}"
                )
            balance = self._coerce_required_numeric_value(
                quote_payload["total"],
                field="total",
                symbol=self.quote,
                allow_zero=True,
                payload_kind="balance payload",
            )
        elif margin_mode_name == "multi_currency":
            if "cross_available" not in info0:
                raise KeyError(
                    f"{self.exchange}: missing cross_available in fetch_balance response"
                )
            balance = self._coerce_required_numeric_value(
                info0["cross_available"],
                field="cross_available",
                symbol=self.quote,
                allow_zero=True,
                payload_kind="balance payload",
            )
        else:
            raise Exception(f"unknown margin_mode_name {balance_fetched}")
        return balance

    async def fetch_pnls(
        self,
        start_time: int = None,
        end_time: int = None,
        limit=None,
    ):
        if start_time is None:
            return await self.fetch_pnl(limit=limit)
        all_fetched = {}
        if limit is None:
            limit = 1000
        offset = 0
        while True:
            fetched = await self.fetch_pnl(offset=offset, limit=limit)
            if not fetched:
                break
            for elm in fetched:
                all_fetched[elm["id"]] = elm
            if len(fetched) < limit:
                break
            if fetched[0]["timestamp"] <= start_time:
                break
            logging.debug(f"fetching pnls {ts_to_date(fetched[-1]['timestamp'])}")
            offset += limit
        return sorted(all_fetched.values(), key=lambda x: x["timestamp"])

    async def gather_fill_events(self, start_time=None, end_time=None, limit=None):
        """Return canonical fill events for Gate.io."""
        events = []
        fills = await self.fetch_pnls(start_time=start_time, end_time=end_time, limit=limit)
        for fill in fills:
            events.append(
                {
                    "id": fill.get("id"),
                    "timestamp": fill.get("timestamp"),
                    "symbol": fill.get("symbol"),
                    "side": fill.get("side"),
                    "position_side": fill.get("position_side"),
                    "qty": fill.get("amount") or fill.get("filled"),
                    "price": fill.get("price"),
                    "pnl": fill.get("pnl"),
                    "fee": fill.get("fee"),
                    "info": fill.get("info"),
                }
            )
        return events

    async def fetch_pnl(
        self,
        offset=0,
        limit=None,
    ):
        n_pnls_limit = 1000 if limit is None else limit
        fetched = await self.cca.fetch_closed_orders(limit=n_pnls_limit, params={"offset": offset})
        for i in range(len(fetched)):
            fetched[i]["pnl"] = float(fetched[i]["info"]["pnl"])
            fetched[i]["position_side"] = self.determine_pos_side(fetched[i])
        return sorted(fetched, key=lambda x: x["timestamp"])

    def did_cancel_order(self, executed, order=None):
        if isinstance(executed, list) and len(executed) == 1:
            return self.did_cancel_order(executed[0], order)
        try:
            return executed.get("id", "") == order["id"] and executed.get("status", "") == "canceled"
        except Exception:
            return False

    def _build_order_params(self, order: dict) -> dict:
        order_type = order["type"] if "type" in order else "limit"
        params = {
            "reduce_only": order["reduce_only"],
            "text": order["custom_id"],
        }
        if order_type == "limit":
            params["timeInForce"] = (
                "poc" if require_live_value(self.config, "time_in_force") == "post_only" else "gtc"
            )
        return params

    def did_create_order(self, executed):
        try:
            return "status" in executed and executed["status"] != "rejected"
        except Exception:
            return False

    async def update_exchange_config_by_symbols(self, symbols):
        """GateIO: No per-symbol configuration needed."""
        pass

    async def update_exchange_config(self):
        """GateIO: No exchange-level configuration needed."""
        pass
