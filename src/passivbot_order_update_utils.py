from __future__ import annotations

from utils import utc_ms
from passivbot_utils import order_has_match


def add_new_order(self, order, source="WS"):
    """No-op placeholder; subclasses update open orders through REST synchronisation."""
    del self, order, source
    return


def remove_order(self, order: dict, source="WS", reason="cancelled"):
    """No-op placeholder; subclasses remove open orders through REST synchronisation."""
    del self, order, source, reason
    return


def handle_order_update(self, upd_list):
    """Mark the execution loop dirty when websocket order updates arrive."""
    if upd_list:
        self.execution_scheduled = True
    return


def add_to_recent_order_cancellations(self, order):
    """Record a recently cancelled order to throttle repeated cancellations."""
    self.recent_order_cancellations.append({**order, **{"execution_timestamp": utc_ms()}})


def order_was_recently_cancelled(self, order, max_age_ms=15_000) -> float:
    """Return remaining throttle delay if the order was cancelled within `max_age_ms`."""
    age_limit = utc_ms() - max_age_ms
    self.recent_order_cancellations = [
        x for x in self.recent_order_cancellations if x["execution_timestamp"] > age_limit
    ]
    if matching := order_has_match(
        order, self.recent_order_cancellations, tolerance_price=0.0, tolerance_qty=0.0
    ):
        return max(0.0, (matching["execution_timestamp"] + max_age_ms) - utc_ms())
    return 0.0


def add_to_recent_order_executions(self, order):
    """Track newly created orders to limit duplicate submissions."""
    self.recent_order_executions.append({**order, **{"execution_timestamp": utc_ms()}})


def order_was_recently_updated(self, order, max_age_ms=15_000) -> float:
    """Return throttle delay if the order was placed within `max_age_ms`."""
    age_limit = utc_ms() - max_age_ms
    self.recent_order_executions = [
        x for x in self.recent_order_executions if x["execution_timestamp"] > age_limit
    ]
    if matching := order_has_match(order, self.recent_order_executions):
        return max(0.0, (matching["execution_timestamp"] + max_age_ms) - utc_ms())
    return 0.0
