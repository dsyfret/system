# src/strategy/edges/mean_revert.py
"""
Location : src/strategy/edges/mean_revert.py
Purpose  : Short-horizon mean reversion (pre-off only). Fades small deviations
           from micro-mid; requires healthy RAW depth; suppresses during strong slope.
Inputs   : OrderBookSnapshot (with micro_mid / micro_mid_raw), optional TrendEngine
           features (slope), edge config.
Outputs  : list[EdgeProposal]
Priority : Arbiter rank above maker, below suspend→reopen and parity.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
import time
import uuid

from ...core.schemas import EdgeProposal
from ...betfair.ticks import (
    distance_in_ticks,
    one_tick_up,
    one_tick_down,
    snap,
)

# ------------- helpers ------------- #

def _now_ms() -> int:
    return int(time.time() * 1000)


def _runner_touch(rb: Any) -> Optional[Tuple[float, float]]:
    """DISPLAY touch as (best_back, best_lay)."""
    if not getattr(rb, "best_back", None) or not getattr(rb, "best_lay", None):
        return None
    try:
        return float(rb.best_back[0].price), float(rb.best_lay[0].price)
    except Exception:
        return None


def _runner_raw_best2_sum(rb: Any) -> float:
    """Sum of RAW best-2 sizes across both sides (fallback to DISPLAY if RAW missing)."""
    backs = getattr(rb, "best_back_raw", None) or getattr(rb, "best_back", []) or []
    lays  = getattr(rb, "best_lay_raw", None) or getattr(rb, "best_lay", []) or []
    b2 = sum(float(getattr(x, "size", 0.0) or 0.0) for x in backs[:2])
    l2 = sum(float(getattr(x, "size", 0.0) or 0.0) for x in lays[:2])
    return float(b2 + l2)


def _move_ticks_down(price: float, n: int) -> float:
    out = price
    for _ in range(max(0, n)):
        nxt = one_tick_down(out)
        if not nxt:  # guard
            break
        out = nxt
    return out


def _move_ticks_up(price: float, n: int) -> float:
    out = price
    for _ in range(max(0, n)):
        nxt = one_tick_up(out)
        if not nxt:
            break
        out = nxt
    return out


# ------------- main entry ------------- #

def propose(
    snapshot: Any,
    book: Any,
    cfg: Dict[str, Any],
    *,
    now_ms: Optional[int] = None,
    trend_feats: Optional[Dict[str, Any]] = None,
) -> List[EdgeProposal]:
    """
    Build small, inside-spread fading quotes when DISPLAY mid deviates slightly from micro-mid.
    - Only when not in-play
    - Require healthy RAW best-2 depth on both sides
    - Suppress during strong slope (from TrendEngine features if provided)

    Also stages *bundled* exits for each entry:
      - Take-profit at +/- tp_ticks (toward getting done)
      - Protective stop at +/- stop_ticks (against)
    Both exits share the entry's bundle_id so Router enforces atomicity.
    """
    if not snapshot or not getattr(snapshot, "runners", None):
        return []

    ms = getattr(snapshot, "market", None) or getattr(snapshot, "market_state", None)
    if ms and bool(getattr(ms, "in_play", False)):
        return []  # pre-off only

    # Config (entries)
    default_size = float(cfg.get("default_size", 1.0))
    ttl_ms = int(cfg.get("ttl_ms", 1500))
    band_min = float(cfg.get("band_ticks_min", 1.0))   # start reacting at ≥ this many ticks
    band_max = float(cfg.get("band_ticks_max", 1.5))   # avoid chasing big moves (trend regime)
    max_spread = int(cfg.get("max_spread_ticks", 6))
    raw2_min = float(cfg.get("min_raw_best2_sum", 250.0))
    slope_suppress = float(cfg.get("slope_suppress_abs", 0.8))  # ticks/s threshold
    max_candidates = int(cfg.get("max_candidates", 2))

    # Config (exits)
    tp_ticks = int(cfg.get("tp_ticks", 1))                       # toward profit
    stop_ticks = int(cfg.get("stop_ticks", 2))                   # protective
    exit_time_budget_ms = int(cfg.get("exit_time_budget_ms", 3000))
    # Policy: MR defaults to BEST_EFFORT (matches your scratchpad); overrideable in config
    default_bundle_policy = str(cfg.get("bundle_policy", "BEST_EFFORT")).upper()

    # Optional trend slope gate
    if trend_feats:
        try:
            slope = float(trend_feats.get("slope_ticks_s", 0.0) or 0.0)
            if abs(slope) >= slope_suppress:
                return []
        except Exception:
            pass

    # Choose base micro-mid
    base_mid = getattr(snapshot, "micro_mid_raw", None) or getattr(snapshot, "micro_mid", None)
    if base_mid is None:
        return []

    # Shared bundle metadata (per propose() call so paired legs can align)
    mid = str(getattr(getattr(snapshot, "market", None), "market_id", getattr(snapshot, "market_id", "")))
    b_id = f"mr:{mid}:{int(now_ms or _now_ms())}:{uuid.uuid4().hex[:8]}"
    b_policy = default_bundle_policy  # 'BEST_EFFORT' by default, configurable

    # Gather candidates by absolute deviation within [band_min, band_max]
    cands: List[Tuple[float, int, Any, float, float, int]] = []
    for sel_id, rb in getattr(snapshot, "runners", {}).items():
        touch = _runner_touch(rb)
        if not touch:
            continue
        bb, bl = touch
        sp_ticks = distance_in_ticks(bb, bl)
        if sp_ticks is None or sp_ticks <= 0 or sp_ticks > max_spread:
            continue

        raw2 = _runner_raw_best2_sum(rb)
        if raw2 < raw2_min:
            continue

        disp_mid = 0.5 * (bb + bl)
        dev_ticks = distance_in_ticks(base_mid, disp_mid)
        if dev_ticks is None:
            continue
        dev = abs(float(dev_ticks))
        if dev < band_min or dev > band_max:
            continue

        # Keep candidate: larger deviation first
        cands.append((dev, int(sel_id), rb, bb, bl, int(sp_ticks)))

    if not cands:
        return []

    cands.sort(key=lambda x: x[0], reverse=True)
    cands = cands[:max(1, max_candidates)]

    out: List[EdgeProposal] = []

    for dev, sel_id, rb, bb, bl, sp_ticks in cands:
        disp_mid = 0.5 * (bb + bl)

        if disp_mid > base_mid:
            # Displayed mid above micro-mid → fade downward → ENTRY LAY near back
            entry_price = snap(one_tick_up(bb) or bb)
            if entry_price < bl:  # ensure inside spread
                # ENTRY
                out.append(EdgeProposal(
                    edge_id="mean_revert",
                    market_id=mid,
                    selection_id=int(sel_id),
                    side="LAY",
                    price=float(entry_price),
                    size_hint=default_size,
                    ttl_ms=ttl_ms,
                    weight=1.0,
                    ev_net_ticks=0.0,
                    rationale=f"MR entry LAY (dev={dev:.2f}t, spread={sp_ticks})",
                    bundle_id=b_id,
                    bundle_policy=b_policy,
                ))
                # EXITS for the same bundle (opposite side BACK)
                # Take-profit: towards getting done (lower odds for BACK)
                tp_price = snap(_move_ticks_down(entry_price, tp_ticks))
                # Protective stop: against (higher odds for BACK)
                st_price = snap(_move_ticks_up(entry_price, stop_ticks))
                out.append(EdgeProposal(
                    edge_id="mean_revert_tp",
                    market_id=mid,
                    selection_id=int(sel_id),
                    side="BACK",
                    price=float(tp_price),
                    size_hint=default_size,
                    ttl_ms=exit_time_budget_ms,
                    weight=1.0,
                    ev_net_ticks=0.0,
                    rationale=f"MR TP BACK (tp={tp_ticks}t from entry)",
                    bundle_id=b_id,
                    bundle_policy=b_policy,
                ))
                out.append(EdgeProposal(
                    edge_id="mean_revert_stop",
                    market_id=mid,
                    selection_id=int(sel_id),
                    side="BACK",
                    price=float(st_price),
                    size_hint=default_size,
                    ttl_ms=exit_time_budget_ms,
                    weight=1.0,
                    ev_net_ticks=0.0,
                    rationale=f"MR STOP BACK (stop={stop_ticks}t from entry)",
                    bundle_id=b_id,
                    bundle_policy=b_policy,
                ))
        else:
            # Displayed mid below micro-mid → fade upward → ENTRY BACK near lay
            entry_price = snap(one_tick_down(bl) or bl)
            if entry_price > bb:
                # ENTRY
                out.append(EdgeProposal(
                    edge_id="mean_revert",
                    market_id=mid,
                    selection_id=int(sel_id),
                    side="BACK",
                    price=float(entry_price),
                    size_hint=default_size,
                    ttl_ms=ttl_ms,
                    weight=1.0,
                    ev_net_ticks=0.0,
                    rationale=f"MR entry BACK (dev={dev:.2f}t, spread={sp_ticks})",
                    bundle_id=b_id,
                    bundle_policy=b_policy,
                ))
                # EXITS for the same bundle (opposite side LAY)
                # Take-profit: towards getting done (higher odds for LAY)
                tp_price = snap(_move_ticks_up(entry_price, tp_ticks))
                # Protective stop: against (lower odds for LAY)
                st_price = snap(_move_ticks_down(entry_price, stop_ticks))
                out.append(EdgeProposal(
                    edge_id="mean_revert_tp",
                    market_id=mid,
                    selection_id=int(sel_id),
                    side="LAY",
                    price=float(tp_price),
                    size_hint=default_size,
                    ttl_ms=exit_time_budget_ms,
                    weight=1.0,
                    ev_net_ticks=0.0,
                    rationale=f"MR TP LAY (tp={tp_ticks}t from entry)",
                    bundle_id=b_id,
                    bundle_policy=b_policy,
                ))
                out.append(EdgeProposal(
                    edge_id="mean_revert_stop",
                    market_id=mid,
                    selection_id=int(sel_id),
                    side="LAY",
                    price=float(st_price),
                    size_hint=default_size,
                    ttl_ms=exit_time_budget_ms,
                    weight=1.0,
                    ev_net_ticks=0.0,
                    rationale=f"MR STOP LAY (stop={stop_ticks}t from entry)",
                    bundle_id=b_id,
                    bundle_policy=b_policy,
                ))

    return out
