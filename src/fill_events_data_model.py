from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, cast

try:
    import fill_events_coalesce_utils as coalesce_utils
    import fill_events_model_utils as model_utils
    import fill_events_parse_utils as parse_utils
except ImportError:  # pragma: no cover - package-relative fallback
    from . import fill_events_coalesce_utils as coalesce_utils
    from . import fill_events_model_utils as model_utils
    from . import fill_events_parse_utils as parse_utils


def normalize_raw_field(raw: object) -> List[Dict[str, object]]:
    return parse_utils.normalize_raw_field(raw)


def extract_source_ids(raw: object, fallback_id: Optional[object]) -> List[str]:
    return parse_utils.extract_source_ids(raw, fallback_id)


def bybit_trade_dedupe_key(trade: Dict[str, object]) -> Optional[Tuple[object, ...]]:
    return parse_utils.bybit_trade_dedupe_key(trade)


def bybit_trade_qty_abs(trade: Dict[str, object]) -> float:
    return parse_utils.bybit_trade_qty_abs(trade)


def bybit_trade_qty_signed(trade: Dict[str, object]) -> float:
    return parse_utils.bybit_trade_qty_signed(trade)


@dataclass(frozen=True)
class FillEvent:
    """Canonical representation of a single fill event."""

    id: str
    timestamp: int
    datetime: str
    symbol: str
    side: str
    qty: float
    price: float
    pnl: float
    fees: Optional[Sequence]
    pb_order_type: str
    position_side: str
    client_order_id: str
    source_ids: List[str] = field(default_factory=list)
    psize: float = 0.0
    pprice: float = 0.0
    raw: Optional[List[Dict[str, object]]] = None

    @property
    def key(self) -> str:
        return model_utils.fill_event_key(self)

    def to_dict(self) -> Dict[str, object]:
        return model_utils.fill_event_to_dict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "FillEvent":
        payload = cast(
            Any,
            model_utils.fill_event_from_dict_kwargs(
                data,
                extract_source_ids=extract_source_ids,
                normalize_raw_field=normalize_raw_field,
            ),
        )
        return cls(
            **payload,
        )


def bybit_event_group_key(event: FillEvent) -> Tuple[int, str, str, str, str]:
    return parse_utils.bybit_event_group_key(event)


def coalesce_events(
    events: List[Dict[str, object]],
    merge_fee_lists: Callable[[object, object], object],
) -> List[Dict[str, object]]:
    return coalesce_utils.coalesce_events(events, merge_fee_lists, normalize_raw_field)
