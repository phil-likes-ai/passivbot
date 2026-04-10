from __future__ import annotations

import logging
from typing import Optional

from config.access import get_optional_live_value
from custom_endpoint_overrides import apply_rest_overrides_to_ccxt


def build_ccxt_options(self, overrides: Optional[dict] = None) -> dict:
    options: dict[str, object] = {"adjustForTimeDifference": True}
    recv_window = get_optional_live_value(self.config, "recv_window_ms", None)
    if recv_window not in (None, ""):
        try:
            recv_value = recv_window if isinstance(recv_window, (int, float, str)) else None
            if recv_value is None:
                raise TypeError(recv_window)
            recv_int = int(float(recv_value))
            if recv_int > 0:
                options["recvWindow"] = int(recv_int)
        except (TypeError, ValueError):
            logging.warning("Unable to parse live.recv_window_ms=%r; ignoring", recv_window)
    if overrides:
        options.update(overrides)
    return options


def apply_endpoint_override(self, client) -> None:
    """Apply configured REST endpoint overrides to a ccxt client."""
    if client is None:
        return
    apply_rest_overrides_to_ccxt(client, self.endpoint_override)
