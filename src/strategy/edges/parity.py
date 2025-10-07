"""
Location : src/strategy/edges/parity.py
Purpose  : WIN ↔ PLACE cross-market parity (observer-first).
           Detect divergences between linked markets when both books are fresh & liquid.
           Propose a single inside-spread quote on WIN to lean toward parity.

Rollout   : Start with feature flag 'cross_market' = 'observer'.
            Trader logs proposals and does NOT emit intents until promoted to 'live'.

Config (configs/edges.yaml):
  edges:
    cross_market:
      enabled: true
      freshness_ms: 100
      cushion_ticks: 2
      min_dual_best2_sum: 200
      ttl_ms: 1200
      top_runners: 3
      place_terms_k: 3
      max_spread_ticks: 6
      default_size: 1.0    # optional; if absent, defaults to 1.0
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
import time
import uuid

from ...core.schemas import EdgeProposal
from ...betfair.ticks import distance_in_ticks, one_tick_up, one_tick_down


# ----------------- utilities ----------------- #

def _now_ms() -> int:
    return int(time.time() * 1000)


def _market_type(snap: Any) -> str:
    m = getattr(snap, "market", snap)
    for k in ("market_type", "marketType", "type", "market_type_code"):
        v = getattr(m, k, None)
        if v:
            return str(v).upper()
    name = getattr(m, "name", "") or getattr(m, "market_name", "")
    if isinstance(name, str):
        if "PLACE" in name.upper():
            return "PLACE"
        if "WIN" in name.upper():
            return "WIN"
    return ""


def _event_id(snap: Any) -> Optional[str]:
    m = getattr(snap, "market", snap)
    for k in ("event_id", "eventId", "eventid"):
        v = getattr(m, k, None)
        if v:
            return str(v)
    return None


def _ts_ms(snap: Any) -> Optional[int]:
    for k in ("ts_ms", "timestamp_ms", "last_ts_ms"):
        v = getattr(snap, k, None)
        if v is not None:
            try:
                return int(v)
            except Exception:
                return None
    return None


def _runner_touch(rb: Any) -> Optional[Tuple[float, float]]:
    if not getattr(rb, "best_back", None) or not getattr(rb, "best_lay", None):
        return None
    try:
        return float(rb.best_back[0].price), float(rb.best_lay[0].price)
    except Exception:
        return None


def _raw_best2_sum(rb: Any) -> float:
    backs = getattr(rb, "best_back_raw", None) or getattr(rb, "best_back", []) or []
    lays  = getattr(rb, "best_lay_raw", None) or getattr(rb, "best_lay", []) or []
    b2 = sum(float(getattr(x, "size", 0.0) or 0.0) for x in backs[:2])
    l2 = sum(float(getattr(x, "size", 0.0) or 0.0) for x in lays[:2])
    return float(b2 + l2)


def _find_sibling_place(book: Any, win_snap: Any) -> Optional[Any]:
    """Find a PLACE market snapshot for the same event (best-effort)."""
    want_type = "PLACE"
    ev = _event_id(win_snap)
    for _mid, s in getattr(book, "all_snapshots")().items():  # type: ignore[attr-defined]
        if s is win_snap:
            continue
        if _market_type(s) != want_type:
            continue
        if ev is not None and _event_id(s) != ev:
            continue
        return s
    return None


def _map_selection_id(win_rb: Any, place_snap: Any, sel_id: int) -> Optional[int]:
    """
    Map runner selection_id from WIN to PLACE. On Betfair, selection_id is typically
    stable across WIN/PLACE for the same event; fall back to runner name match if needed.
    """
    if sel_id in getattr(place_snap, "runners", {}):
        return int(sel_id)
    win_name = getattr(win_rb, "runner_name", None) or getattr(win_rb, "name", None)
    if not win_name:
        return None
    for pid, prb in getattr(place_snap, "runners", {}).items():
        pname = getattr(prb, "runner_name", None) or getattr(prb, "name", None)
        if pname and str(pname).strip().lower() == str(win_name).strip().lower():
            try:
                return int(pid)
            except Exception:
                return None
    return None


# ----------------- main entry ----------------- #

def propose(
    snapshot: Any,
    market_id: str,
    book: Any,
    cfg: Dict[str, Any],
    now_ms: Optional[int] = None,
    *,
    alerts: Optional[Any] = None,  # NEW (duck-typed; Alerts instance)
) -> List[EdgeProposal]:
    """
    Compare WIN vs PLACE parity for top runners.
    - Freshness gate on both books (alerts if stale)
    - Dual liquidity gate (RAW best-2)
    - Short TTL quotes placed in WIN only (one inside-spread BACK or LAY)
    - Conservative cushion in ticks to clear fees and microstructure noise
    """
    now = int(now_ms or _now_ms())
    out: List[EdgeProposal] = []

    if not snapshot or not getattr(snapshot, "runners", None):
        return out

    if _market_type(snapshot) != "WIN":
        return out

    # Pre-off bias: skip in-play to avoid bet-delay dynamics
    m = getattr(snapshot, "market", None) or getattr(snapshot, "market_state", None)
    if m and bool(getattr(m, "in_play", False)):
        return out

    # Shared bundle metadata (strict pairing with opposite leg if/when we add it)
    mid = str(market_id)  # prefer explicit arg over nested getattr
    bundle_id = f"bundle:{mid}:{now}:{uuid.uuid4().hex[:8]}"
    bundle_policy = (cfg.get("bundle_policy")
                     or cfg.get("edges", {}).get("bundle_policy")
                     or "ALL_OR_CANCEL")

    # Config
    freshness_ms = int(cfg.get("freshness_ms", 100))
    cushion_ticks = int(cfg.get("cushion_ticks", 2))
    min_dual_best2 = float(cfg.get("min_dual_best2_sum", 200.0))
    ttl_ms = int(cfg.get("ttl_ms", 1200))
    top_n = int(cfg.get("top_runners", 3))
    k_places = int(cfg.get("place_terms_k", 3))
    max_spread = int(cfg.get("max_spread_ticks", 6))
    default_size = float(cfg.get("default_size", 1.0))

    # Find sibling PLACE market
    place_snap = _find_sibling_place(book, snapshot)
    if place_snap is None or not getattr(place_snap, "runners", None):
        return out

    # Freshness gate (+ alert if stale)
    ts_win = _ts_ms(snapshot)
    ts_plc = _ts_ms(place_snap)
    win_age = (now - ts_win) if ts_win is not None else None
    plc_age = (now - ts_plc) if ts_plc is not None else None
    if (win_age is not None and win_age > freshness_ms) or (plc_age is not None and plc_age > freshness_ms):
        if alerts is not None and hasattr(alerts, "parity_freshness"):
            try:
                alerts.parity_freshness(win_age, plc_age, freshness_ms)
            except Exception:
                pass
        return out

    # Rank WIN runners by best back (lowest price first)
    win_order: List[Tuple[float, int, Any]] = []
    for sid, wrb in getattr(snapshot, "runners", {}).items():
        t = _runner_touch(wrb)
        if not t:
            continue
        bb, bl = t
        sp = distance_in_ticks(bb, bl)
        if sp is None or sp <= 0 or sp > max_spread:
            continue
        win_order.append((float(bb), int(sid), wrb))
    if not win_order:
        return out
    win_order.sort(key=lambda x: x[0])
    win_order = win_order[:max(1, top_n)]

    # Evaluate parity per runner
    win_mid_by_sel: Dict[int, float] = {}
    for _, sid, wrb in win_order:
        t = _runner_touch(wrb)
        if not t:
            continue
        bb, bl = t
        win_mid_by_sel[sid] = 0.5 * (bb + bl)

    for _, sel_id, wrb in win_order:
        # Map to PLACE selection
        plc_sel = _map_selection_id(wrb, place_snap, int(sel_id))
        if plc_sel is None:
            continue
        prb = getattr(place_snap, "runners", {}).get(plc_sel)
        if not prb:
            continue

        # Touch / spread gates on both markets
        t_win = _runner_touch(wrb)
        t_plc = _runner_touch(prb)
        if not t_win or not t_plc:
            continue
        w_bb, w_bl = t_win
        p_bb, p_bl = t_plc
        sp_w = distance_in_ticks(w_bb, w_bl)
        sp_p = distance_in_ticks(p_bb, p_bl)
        if (sp_w is None or sp_w <= 0 or sp_w > max_spread) or (sp_p is None or sp_p <= 0 or sp_p > max_spread):
            continue

        # Liquidity: require both books healthy
        from typing import Dict as _Dict  # avoid shadowing above
        raw_w = _raw_best2_sum(wrb)
        raw_p = _raw_best2_sum(prb)
        if raw_w < min_dual_best2 or raw_p < min_dual_best2:
            continue

        # Parity calc (approximation): implied_win_mid ≈ k_places * place_mid
        win_mid = win_mid_by_sel[int(sel_id)]
        plc_mid = 0.5 * (p_bb + p_bl)
        implied_win_mid = float(k_places) * float(plc_mid)

        diff = distance_in_ticks(win_mid, implied_win_mid)
        if diff is None:
            continue

        # If WIN mid > implied (rich) by cushion → LAY WIN inside-spread
        if diff >= cushion_ticks:
            lay_px = one_tick_up(w_bb) or w_bb
            if lay_px < w_bl:
                out.append(EdgeProposal(
                    edge_id="cross_market",
                    market_id=mid,
                    selection_id=int(sel_id),
                    side="LAY",
                    price=float(lay_px),
                    size_hint=default_size,
                    ttl_ms=ttl_ms,
                    weight=1.0,
                    ev_net_ticks=0.0,
                    rationale=f"WIN rich vs PLACE (Δ={diff}t, k={k_places})",
                    bundle_id=bundle_id,
                    bundle_policy=bundle_policy,
                ))
        # If WIN mid < implied (cheap) by cushion → BACK WIN inside-spread
        elif diff <= -cushion_ticks:
            back_px = one_tick_down(w_bl) or w_bl
            if back_px > w_bb:
                out.append(EdgeProposal(
                    edge_id="cross_market",
                    market_id=mid,
                    selection_id=int(sel_id),
                    side="BACK",
                    price=float(back_px),
                    size_hint=default_size,
                    ttl_ms=ttl_ms,
                    weight=1.0,
                    ev_net_ticks=0.0,
                    rationale=f"WIN cheap vs PLACE (Δ={diff}t, k={k_places})",
                    bundle_id=bundle_id,
                    bundle_policy=bundle_policy,
                ))

    return out
