from __future__ import annotations

from typing import Dict, Optional, Tuple

from utils import utc_ms


CACHE_ONLY_TTL_MS = 365 * 24 * 3600 * 1000


def compute_fetch_budget_ttls(
    self, syms: list, max_age_ms: Optional[int], max_network_fetches: Optional[int]
) -> Tuple[Dict[str, int], set]:
    """Compute per-symbol TTLs with fetch budget."""
    per_sym_ttl: Dict[str, int] = {}
    if max_network_fetches is not None and max_network_fetches >= 0 and max_age_ms is not None:
        now = utc_ms()
        staleness = []
        for s in syms:
            try:
                last_ref = self.cm.get_last_refresh_ms(s)
            except Exception:
                last_ref = 0
            staleness.append((s, int(now - last_ref) if last_ref > 0 else now))
        staleness.sort(key=lambda x: x[1], reverse=True)
        fetch_set = {s for s, _ in staleness[:max_network_fetches]}
        for s in syms:
            per_sym_ttl[s] = int(max_age_ms) if s in fetch_set else CACHE_ONLY_TTL_MS
    else:
        for s in syms:
            per_sym_ttl[s] = int(max_age_ms) if max_age_ms is not None else 0

    cache_only_never_fetched: set = set()
    for s in syms:
        if per_sym_ttl.get(s) == CACHE_ONLY_TTL_MS:
            try:
                if self.cm.get_last_refresh_ms(s) == 0:
                    cache_only_never_fetched.add(s)
            except Exception:
                cache_only_never_fetched.add(s)

    return per_sym_ttl, cache_only_never_fetched
