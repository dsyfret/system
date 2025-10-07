# src/strategy/edges/maker_thin.py
"""
Location : src/strategy/edges/maker_thin.py
Purpose  : Inside-spread maker tuned for thin books:
           - spread ≥ min_spread_ticks (default 3)
           - RAW best-2 depth 'thin band' per runner (sum back+lay ≤ threshold)
           - smaller default size, longer TTL, higher slippage cushion (via FeeGate)

Rollout   : Start with feature flag 'maker_thin' = 'observer'.
            Trader logs proposals and does NOT emit intents until promoted to 'live'.

Config (configs/edges.yaml):
  edges:
    maker_thin:
      enabled: false
      default_size: 1.0
      min_spread_ticks: 3
      thin_best2_depth_sum_max: 200
      min_lifetime_ms: 2500
      slippage_cushion_ticks: 2
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
import time

from ...core.schemas import EdgeProposal
from ...betfair.ticks import one_tick_up, one_tick_down, distance_in_ticks


def _now_ms() -> int:
    return int(time.time() * 1000)


def _runner_touch(rb: Any) -> Optional[Tuple[float, float]]:
    """(best_back, best_lay) using DISPLAY ladders."""
    if not getattr(rb, "best_back", None) or not getattr(rb, "best_lay", None):
        return None
    try:
        return float(rb.best_back[0].price), float(rb.best_lay[0].price)
    except Exception:
        return None


def _runner_raw_best2_sum(rb: Any) -> float:
    """Sum sizes across RAW best-2 back+lay (fallback to DISPLAY if RAW missing)."""
    backs = getattr(rb, "best_back_raw", None) or getattr(rb, "best_back", []) or []
    lays  = getattr(rb, "best_lay_raw", None) or getattr(rb, "best_lay", []) or []
    b2 = sum(float(getattr(x, "size", 0.0) or 0.0) for x in backs[:2])
    l2 = sum(float(getattr(x, "size", 0.0) or 0.0) for x in lays[:2])
    return float(b2 + l2)


def propose(snapshot: Any, book: Any, cfg: Dict[str, Any], now_ms: Optional[int] = None) -> List[EdgeProposal]:
    """
    Build inside-spread quoting proposals for runners that satisfy thin-band gates.
    Returns a list of EdgeProposal with edge_id="maker_thin".
    """
    if not snapshot or not getattr(snapshot, "runners", None):
        return []

    now = int(now_ms or _now_ms())
    min_spread = int(cfg.get("min_spread_ticks", 3))
    thin_sum_max = float(cfg.get("thin_best2_depth_sum_max", 200.0))
    default_size = float(cfg.get("default_size", 1.0))
    ttl_ms = int(cfg.get("min_lifetime_ms", 2500))

    out: List[EdgeProposal] = []

    for sel_id, rb in getattr(snapshot, "runners", {}).items():
        touch = _runner_touch(rb)
        if not touch:
            continue
        best_back, best_lay = touch
        sp_ticks = distance_in_ticks(best_back, best_lay)
        if sp_ticks is None or sp_ticks < min_spread:
            continue

        raw_sum = _runner_raw_best2_sum(rb)
        if raw_sum > thin_sum_max:
            continue

        # BACK: quote one tick inside lay
        back_price = one_tick_down(best_lay)
        if back_price and back_price > best_back:
            out.append(EdgeProposal(
                edge_id="maker_thin",
                market_id=str(getattr(snapshot.market, "market_id", getattr(snapshot, "market_id", ""))),
                selection_id=int(sel_id),
                side="BACK",
                price=float(back_price),
                size_hint=default_size,
                ttl_ms=ttl_ms,
                weight=1.0,
                ev_net_ticks=0.0,
                rationale=f"inside-spread BACK thin (spread_ticks={sp_ticks}, raw2_sum={raw_sum:.1f})",
            ))

        # LAY: quote one tick inside back
        lay_price = one_tick_up(best_back)
        if lay_price and lay_price < best_lay:
            out.append(EdgeProposal(
                edge_id="maker_thin",
                market_id=str(getattr(snapshot.market, "market_id", getattr(snapshot, "market_id", ""))),
                selection_id=int(sel_id),
                side="LAY",
                price=float(lay_price),
                size_hint=default_size,
                ttl_ms=ttl_ms,
                weight=1.0,
                ev_net_ticks=0.0,
                rationale=f"inside-spread LAY thin (spread_ticks={sp_ticks}, raw2_sum={raw_sum:.1f})",
            ))

    return out