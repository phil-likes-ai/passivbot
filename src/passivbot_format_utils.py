from __future__ import annotations


def pad_sym(self, symbol):
    """Return the symbol left-aligned to the configured log width."""
    return f"{symbol: <{self.sym_padding}}"


def format_duration(ms) -> str:
    """Format milliseconds as a compact human-readable duration."""
    total_seconds = max(0, int(ms // 1000))
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    if days > 0:
        return f"{days}d{hours}h{minutes}m"
    if hours > 0:
        return f"{hours}h{minutes}m"
    if minutes > 0:
        return f"{minutes}m{seconds}s"
    return f"{seconds}s"
