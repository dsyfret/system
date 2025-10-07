# src/strategy/selector.py
"""
Location : src/strategy/selector.py
Purpose  : Decide which markets are admissible now, and which are "hot" (active).
Plain    : Uses the profile’s time windows (e.g., racing T-15 → 0) and admission
           rules (min spread, min raw depth). Excludes in-play unless policy allows.
Config   : configs/profiles/<profile>.yaml (windows, admission, in_play)
Inputs   : Book snapshots from BookBuilder (virtual best + raw ladders if present)
Outputs  : admit(snap) -> AdmissionResult(eligible, hot, reason, score)
           pick_hot(book) -> list[str] (market_ids ordered by score)
Notes    : - Decision uses virtual best for spread, RAW for depth (per scratchpad).
           - Hot window = within preoff_hot_min (e.g., last 5 minutes) and eligible.

Time-base convention (FIXED):
- We use "minutes_since_off": negative before the scheduled off (e.g., -15 at T-15),
  zero at the off, positive after the off. This matches config values like -15..0.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, Optional, List, Tuple
import time
import math

from ..core.config import load_config
from ..core.logging import log_event
from ..betfair.ticks import distance_in_ticks
from ..execution.policies import allow_maker_in_play, bet_delay_models_allowed


@dataclass
class AdmissionResult:
    eligible: bool
    hot: bool
    reason: str
    score: float  # for allocator ordering


class MarketSelector:
    def __init__(self, profile_name: str) -> None:
        snap = load_config(profile_name)
        self.profile_name = profile_name
        self.profile = getattr(snap, "profile", {}) or {}

        adm = (self.profile.get("admission") or {})
        win = (self.profile.get("windows") or {})

        # Admission hygiene
        self.min_spread_ticks = int(adm.get("min_spread_ticks", 2))
        self.min_depth_best2 = float(adm.get("min_depth_best2", 200.0))

        # Time windows (minutes_since_off: negative pre-off, 0 at off)
        self.preoff_open_min = int(win.get("preoff_open_min", -15))  # e.g., -15
        self.preoff_hot_min = int(win.get("preoff_hot_min", -5))     # e.g., -5

    # ----- public API -----

    def admit(self, snap: Any) -> AdmissionResult:
        """Evaluate a single market snapshot."""
        # Basic guards
        if not snap or not getattr(snap, "market", None):
            return AdmissionResult(False, False, "no_snapshot", 0.0)
        st = snap.market
        if st.status != "OPEN":
            return AdmissionResult(False, False, f"status_{st.status.lower()}", 0.0)

        # In-play check (policy driven)
        if st.in_play:
            bet_delay = int(getattr(st, "bet_delay", 0) or 0)
            if not allow_maker_in_play(self.profile_name, bet_delay):
                return AdmissionResult(False, False, "in_play_blocked", 0.0)

            # (NEW) Disallowed bet-delay model(s) gate
            models = getattr(st, "bet_delay_models", None) or getattr(snap, "bet_delay_models", None)
            if models and not bet_delay_models_allowed(self.profile_name, models):
                return AdmissionResult(False, False, "bet_delay_model_blocked", 0.0)

        # Time window: minutes_since_off (negative pre-off)
        mso = self._minutes_since_off(st.off_dt)
        if mso is None:
            # No off-time yet; allow only if not in-play and treat as "not hot"
            window_ok = True
            hot_window = False
        else:
            # Open window: [preoff_open_min, 0]; Hot window: [preoff_hot_min, 0]
            window_ok = (self.preoff_open_min <= mso <= 0)
            hot_window = (self.preoff_hot_min <= mso <= 0)

        if not window_ok:
            return AdmissionResult(False, False, "outside_window", 0.0)

        # Spread (virtual best)
        vb = self._top_virtual(snap)
        if not vb:
            return AdmissionResult(False, False, "no_virtual_top", 0.0)
        v_back, v_lay = vb
        sp_ticks = distance_in_ticks(v_back, v_lay)
        if sp_ticks is None or sp_ticks < self.min_spread_ticks:
            return AdmissionResult(False, False, "spread_tight", 0.0)

        # Depth (RAW ladders best-2)
        raw_depth = self._raw_depth_best2_total(snap)
        if raw_depth < self.min_depth_best2:
            return AdmissionResult(False, False, "raw_depth_low", 0.0)

        # Eligible. Compute a simple score for ordering (allocator): wider spread + deeper raw
        score = float(sp_ticks) * math.log1p(raw_depth)
        return AdmissionResult(True, bool(hot_window), "ok", score)

    def pick_hot(self, book: Any, max_hot: int) -> List[str]:
        """
        Rank eligible markets by score and return up to max_hot market_ids that are 'hot'.
        """
        results: List[Tuple[str, AdmissionResult]] = []
        # Robust snapshot iteration:
        snapshots_fn = getattr(book, "all_snapshots", None)
        if callable(snapshots_fn):
            iterator = snapshots_fn().items()
        else:
            snapshots_dict = getattr(book, "snapshots", {}) or {}
            iterator = snapshots_dict.items()
        for mid, snap in iterator:  # type: ignore
            res = self.admit(snap)
            if res.eligible and res.hot:
                results.append((mid, res))
        results.sort(key=lambda x: x[1].score, reverse=True)
        mids = [mid for mid, _ in results[:max_hot]]
        log_event("selector", "hot.pick", "ok", count=len(mids))
        return mids

    # ----- helpers -----

    @staticmethod
    def _minutes_since_off(off_dt_ms: Optional[int]) -> Optional[int]:
        """
        Return minutes since scheduled off:
          - negative before the off (e.g., -15 at T-15)
          - 0 at the off time
          - positive after the off
        """
        if not off_dt_ms:
            return None
        now_ms = int(time.time() * 1000)
        delta_min = int((off_dt_ms - now_ms) / 60000)
        # Make negative pre-off, positive post-off
        return -delta_min

    @staticmethod
    def _top_virtual(snap: Any) -> Optional[Tuple[float, float]]:
        """
        Return (best_back_price, best_lay_price) using the displayed (virtualised) ladders.
        We compute market touch by taking best lay min across runners and best back max across runners.
        """
        best_back = None
        best_lay = None
        for rb in snap.runners.values():
            if getattr(rb, "best_back", None):
                p = rb.best_back[0].price
                best_back = p if best_back is None else max(best_back, p)
            if getattr(rb, "best_lay", None):
                p = rb.best_lay[0].price
                best_lay = p if best_lay is None else min(best_lay, p)
        if best_back is None or best_lay is None:
            return None
        return float(best_back), float(best_lay)

    @staticmethod
    def _raw_depth_best2_total(snap: Any) -> float:
        """
        Sum of RAW depth at best-2 across runners, taking the *smaller* of back-side
        and lay-side totals so we don't over-trust one-sided ladders.
        """
        back_total = 0.0
        lay_total = 0.0
        for rb in snap.runners.values():
            # Prefer explicit raw ladders if present; fall back to displayed if not available
            back_lvls = getattr(rb, "best_back_raw", None) or getattr(rb, "best_back", [])
            lay_lvls = getattr(rb, "best_lay_raw", None) or getattr(rb, "best_lay", [])
            # take up to 2 levels
            back2 = back_lvls[:2]
            lay2 = lay_lvls[:2]
            back_total += sum(float(getattr(x, "size", 0.0) or 0.0) for x in back2)
            lay_total += sum(float(getattr(x, "size", 0.0) or 0.0) for x in lay2)
        return float(min(back_total, lay_total))


def is_thin_candidate(
    snap: Any,
    *,
    min_spread_ticks: int = 3,
    best2_depth_sum_max: float = 200.0
) -> Tuple[bool, Dict[str, Any]]:
    """
    Observer-only helper for 'thin-band' admission.
    Conditions:
      - Spread (virtual touch) >= min_spread_ticks
      - RAW best-2 depth sum across sides <= best2_depth_sum_max
    Returns: (ok, telemetry)
    """
    vb = MarketSelector._top_virtual(snap)
    if not vb:
        return False, {"reason": "no_virtual_top"}
    v_back, v_lay = vb
    sp_ticks = distance_in_ticks(v_back, v_lay)
    if sp_ticks is None or sp_ticks < min_spread_ticks:
        return False, {"reason": "spread_tight", "spread": float(sp_ticks or 0.0)}
    raw_best2 = MarketSelector._raw_depth_best2_total(snap)
    if raw_best2 > best2_depth_sum_max:
        return False, {"reason": "depth_heavy", "best2_sum": float(raw_best2)}
    return True, {"reason": "ok", "spread": float(sp_ticks), "best2_sum": float(raw_best2)}
