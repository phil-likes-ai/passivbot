from __future__ import annotations

import logging
from typing import Dict, Optional, Tuple


logger = logging.getLogger("fill_events_manager")


def check_pagination_progress(
    previous: Optional[Tuple[Tuple[str, object], ...]],
    params: Dict[str, object],
    context: str,
) -> Optional[Tuple[Tuple[str, object], ...]]:
    params_key = tuple(sorted(params.items()))
    if previous == params_key:
        logger.warning(
            "%s: repeated params detected; aborting pagination (%s)",
            context,
            dict(params),
        )
        return None
    logger.debug("%s: fetching with params %s", context, dict(params))
    return params_key
