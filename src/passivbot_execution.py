from __future__ import annotations

import logging


def _build_order_params(self, order: dict) -> dict:
    """Hook: Build execution parameters for order placement.

    Override in subclass with exchange-specific logic.
    """
    return {}


async def execute_order(self, order: dict) -> dict:
    """Place a single order via the exchange client."""
    if "type" not in order:
        raise KeyError(f"missing required order field 'type' for {order.get('symbol', '?')}")
    params = {
        "symbol": order["symbol"],
        "type": order["type"],
        "side": order["side"],
        "amount": abs(order["qty"]),
        "price": order["price"],
        "params": self._build_order_params(order),
    }
    executed = await self.cca.create_order(**params)
    return executed


async def execute_orders(self, orders: list[dict]) -> list[dict]:
    """Execute a batch of order creations using the helper pipeline."""
    return await self.execute_multiple(orders, "execute_order")


async def execute_cancellation(self, order: dict) -> dict:
    """Cancel a single order via the exchange client."""
    executed = None
    try:
        executed = await self.cca.cancel_order(order["id"], symbol=order["symbol"])
        return executed
    except Exception as e:
        err_str = str(e).lower()
        already_gone_indicators = [
            "100004",
            "order does not exist",
            "order not found",
            "already filled",
            "already cancelled",
            "already canceled",
            "-2011",
            "51400",
            "order_not_found",
        ]
        if any(ind in err_str for ind in already_gone_indicators):
            logging.info(
                "[order] cancel skipped: %s %s - order likely already filled or cancelled",
                order.get("symbol", "?"),
                order.get("id", "?")[:12],
            )
        else:
            logging.exception(
                "[order] cancel failed: %s %s",
                order.get("symbol", "?"),
                order.get("id", "?")[:12],
            )
        return {}


async def execute_cancellations(self, orders: list[dict]) -> list[dict]:
    """Execute a batch of cancellations using the helper pipeline."""
    return await self.execute_multiple(orders, "execute_cancellation")
