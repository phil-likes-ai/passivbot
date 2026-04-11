from __future__ import annotations

import logging


def log_once(self, msg: str):
    if not hasattr(self, "log_once_set"):
        self.log_once_set = set()
    if msg in self.log_once_set:
        return
    logging.info(msg)
    self.log_once_set.add(msg)


def debug_print(self, *args):
    """Emit debug output only when the instance is in debug mode."""
    if hasattr(self, "debug_mode") and self.debug_mode:
        logging.debug(" ".join(str(arg) for arg in args))
