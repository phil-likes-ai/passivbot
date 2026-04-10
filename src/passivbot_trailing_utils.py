from __future__ import annotations

import logging
from collections import defaultdict

from utils import utc_ms


def get_last_position_changes(self, symbol=None):
    """Return the most recent fill timestamp per symbol/side for trailing logic."""
    del symbol
    last_position_changes = defaultdict(dict)
    if self._pnls_manager is None:
        return last_position_changes

    events = self._pnls_manager.get_events()
    for sym in self.positions:
        for pside in ["long", "short"]:
            if self.has_position(pside, sym) and self.is_trailing(sym, pside):
                last_position_changes[sym][pside] = utc_ms() - 1000 * 60 * 60 * 24 * 7
                for ev in reversed(events):
                    try:
                        if ev.symbol == sym and ev.position_side == pside:
                            last_position_changes[sym][pside] = ev.timestamp
                            break
                    except Exception as e:
                        logging.error(f"Error in get_last_position_changes: {e}")
    return last_position_changes
