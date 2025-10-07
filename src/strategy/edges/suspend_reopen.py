"""
Location : src/strategy/edges/suspend_reopen.py
Purpose  : Micro-clip "queue jump" right after a market reopens from SUSPENDED.
           We place tiny KEEP quotes just inside the touch for a very short window.

Rollout   : Start in 'observer'. Trader logs counts and never emits intents until
            feature_flags.flags.edges.suspend_reopen == 'live'.

Signals   :
- Triggered only within `reopen_window_ms` after a SUSPENDED→OPEN transition.
- Disarmed for `disarm_flap_ms` after any rapid flap or bet-delay model change.
- Optional side preference toward current depth imbalance (prefer_with_imbalance).
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
import time
import uuid

from ...core.schemas import EdgeProposal
from ...betfair.ticks import one_tick_up, one_tick_down, distance_in_ticks


class _ReopenState:
    __slots__ = ("last_status", "last_reopen_ms", "disarm_until_ms", "last_bet_delay", "last_models")

    def __init__(self) -> None:
        self.last_status: Optional[str] = None
        self.last_reopen_ms: Optional[int] = None
        self.disarm_until_ms: int = 0
        self.last_bet_delay: Optional[int] = None
        self.last_models: Tuple[str, ...] = ()


_STATE: Dict[str, _ReopenState] = {}


def _st(mid: str) -> _ReopenState:
    s = _STATE.get(mid)
    if s is None:
        s = _ReopenState()
        _STATE[mid] = s
    return s


def _now_ms() -> int:
    return int(time.time() * 1000)


def on_market_def(market_id: str, md: Dict[str, Any], now_ms: Optional[int] = None) -> None:
    """
    Receive market-definition deltas so we can detect SUSPENDED→OPEN transitions.
    Trader calls this from its _on_market_def() callback.
    """
    now = int(now_ms or _now_ms())
    s = _st(str(market_id))

    status = str(md.get("status", "OPEN")).upper()
    bet_delay = int(md.get("bet_delay", 0) or 0)
    models = tuple(md.get("bet_delay_models") or ())

    # flap & model-change disarm
    if s.last_bet_delay is not None and bet_delay != s.last_bet_delay:
        s.disarm_until_ms = max(s.disarm_until_ms, now + int(md.get("disarm_flap_ms", 4000)))
    if s.last_models and models and models != s.last_models:
        s.disarm_until_ms = max(s.disarm_until_ms, now + int(md.get("disarm_flap_ms", 4000)))

    # detect SUSPENDED -> OPEN
    if s.last_status == "SUSPENDED" and status == "OPEN":
        s.last_reopen_ms = now

    s.last_status = status
    s.last_bet_delay = bet_delay
    s.last_models = models


def _runner_best2_totals(rb: Any) -> Tuple[float, float]:
    backs = getattr(rb, "best_back_raw", None) or getattr(rb, "best_back", []) or []
    lays  = getattr(rb, "best_lay_raw", None) or getattr(rb, "best_lay", []) or []
    b2 = sum(float(getattr(x, "size", 0.0) or 0.0) for x in backs[:2])
    l2 = sum(float(getattr(x, "size", 0.0) or 0.0) for x in lays[:2])
    return b2, l2


def _runner_touch(rb: Any) -> Optional[Tuple[float, float]]:
    if not getattr(rb, "best_back", None) or not getattr(rb, "best_lay", None):
        return None
    try:
        return float(rb.best_back[0].price), float(rb.best_lay[0].price)
    except Exception:
        return None


def propose(
    snapshot: Any,
    market_id: str,
    cfg: Dict[str, Any],
    now_ms: Optional[int] = None,
    *,
    alerts: Optional[Any] = None,  # NEW (duck-typed; Alerts instance)
) -> List[EdgeProposal]:
    """
    Return at most `max_clips_per_reopen` proposals inside the reopen window.
    Also emits an alert if a potential fire would be outside the configured window.
    """
    now = int(now_ms or _now_ms())
    mid = str(market_id)
    s = _st(mid)

    if not snapshot or not getattr(snapshot, "runners", None):
        return []

    # Shared bundle metadata (paired legs within this reopen pulse)
    bundle_id = f"bundle:{mid}:{now}:{uuid.uuid4().hex[:8]}"
    bundle_policy = (cfg.get("bundle_policy")
                     or cfg.get("edges", {}).get("bundle_policy")
                     or "ALL_OR_CANCEL")

    # Guard: must be OPEN and within the strict reopen window
    status = str(getattr(snapshot.market, "status", "OPEN")).upper() if getattr(snapshot, "market", None) else "OPEN"
    if status != "OPEN":
        return []
    ro_ms = s.last_reopen_ms or 0
    if ro_ms == 0:
        return []
    reopen_window_ms = int(cfg.get("reopen_window_ms", 1200))
    delta_ms = now - ro_ms
    if delta_ms > reopen_window_ms:
        # Fire alert for misuse (outside window) and return
        if alerts is not None and hasattr(alerts, "reopen_window_breach"):
            try:
                alerts.reopen_window_breach(delta_ms, reopen_window_ms)
            except Exception:
                pass
        return []
    if now < s.disarm_until_ms:
        return []

    # Optional spread guard (avoid zero-spread jitters)
    min_spread_ticks = int(cfg.get("min_spread_ticks", 1))

    # Choose up to N runners (by balanced depth)
    max_runners = int(cfg.get("max_runners", 1))
    candidates: List[Tuple[float, int, Any]] = []
    for sel_id, rb in getattr(snapshot, "runners", {}).items():
        touch = _runner_touch(rb)
        if not touch:
            continue
        bb, bl = touch
        sp = distance_in_ticks(bb, bl)
        if sp is None or sp < min_spread_ticks:
            continue
        b2, l2 = _runner_best2_totals(rb)
        bal = min(b2, l2)
        if bal > 0:
            candidates.append((bal, int(sel_id), rb))
    candidates.sort(key=lambda x: x[0], reverse=True)
    picks = candidates[:max(1, max_runners)]
    if not picks:
        return []

    prefer_imb = bool(cfg.get("prefer_with_imbalance", True))
    ttl_ms = int(cfg.get("ttl_ms", 1500))

    out: List[EdgeProposal] = []
    max_clips = int(cfg.get("max_clips_per_reopen", 1))
    clips = 0

    for _, sel_id, rb in picks:
        if clips >= max_clips:
            break
        bb, bl = _runner_touch(rb)  # re-read; safe after checks
        b2, l2 = _runner_best2_totals(rb)

        side_pref = "BACK" if (prefer_imb and l2 >= b2) else "LAY"

        if side_pref == "BACK":
            price = one_tick_down(bl) or bl
            if price > bb:  # ensure inside-spread
                out.append(EdgeProposal(
                    edge_id="suspend_reopen",
                    market_id=mid,
                    selection_id=int(sel_id),
                    side="BACK",
                    price=float(price),
                    size_hint=0.0,  # observer; sizer decides when live
                    ttl_ms=ttl_ms,
                    weight=1.0,
                    ev_net_ticks=0.0,
                    rationale=f"reopen BACK (Δt={delta_ms}ms, b2={b2:.0f}, l2={l2:.0f})",
                    bundle_id=bundle_id,
                    bundle_policy=bundle_policy,
                ))
                clips += 1
        else:
            price = one_tick_up(bb) or bb
            if price < bl:
                out.append(EdgeProposal(
                    edge_id="suspend_reopen",
                    market_id=mid,
                    selection_id=int(sel_id),
                    side="LAY",
                    price=float(price),
                    size_hint=0.0,
                    ttl_ms=ttl_ms,
                    weight=1.0,
                    ev_net_ticks=0.0,
                    rationale=f"reopen LAY (Δt={delta_ms}ms, b2={b2:.0f}, l2={l2:.0f})",
                    bundle_id=bundle_id,
                    bundle_policy=bundle_policy,
                ))
                clips += 1

    return out
