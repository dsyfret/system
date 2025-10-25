from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Optional

@dataclass(frozen=True)
class StrategyContext:
    """
    Explicit context passed to every edge in ctx-only mode.

    Required:
      - market_id: target market
      - selection_id: optional runner filter (None => edge iterates all runners)
      - snapshot: OrderBookSnapshot
      - cfg: the edge-specific config subtree (dict-like)
      - book: read-only BookBuilder (p_fill_at/ttf_ms_at/size_ahead_at/get_mbr)
      - now_ms: loop clock in milliseconds

    Optional (future):
      - edge_id: canonical edge name (e.g., "maker")
      - telemetry: logger/handle
      - risk_tags: default tags for intents
    """
    market_id: str
    selection_id: Optional[int]
    snapshot: Any
    cfg: Any
    book: Any
    now_ms: int
    edge_id: Optional[str] = None
    telemetry: Optional[Any] = None
    risk_tags: Optional[dict] = None
