# src/strategy/arbiter.py
"""
Location : src/strategy/arbiter.py
Purpose  : Merge multiple (already fee-gated, sized) EdgeProposals into
           lane plans per (market, selection, side), with quote hygiene
           (min lifetime, replace pacing) and clean logs.

Pipeline position:
  edges/* → FeeGate → Sizer → Arbiter → QuoteIntent(s)

Outputs:
  list[QuotePlan]
    - By default (allow_opposite_side_stack = false): at most ONE plan per
      (market_id, selection_id), choosing the better side.
    - If allow_opposite_side_stack = true: up to TWO plans per selection
      (one BACK lane, one LAY lane) provided hygiene allows.

Config it auto-loads (with safe defaults):
  profiles/<profile>.yaml:
    persistence.default                   (KEEP | CANCEL | TAKE_SP)

  edges.yaml (optional, all safe defaults if absent):
    edges.maker.min_lifetime_ms
    edges.arbiter.allow_opposite_side_stack   (bool, default false)
    edges.arbiter.max_per_lane                (int, default 1 — effectively one plan)
    edges.arbiter.lane_priorities (or arbiter.lane_priority/arbiter.lane_priorities)             (list[str], default:
                                               [suspend_reopen, cross_market, mean_revert, maker])

  throttles.yaml:
    replaces.min_interval_ms
    replaces.widen_trigger_ticks

Notes:
  - No network calls; tiny in-memory state to enforce hygiene.
  - Compatible with existing pure-maker flow: if only maker proposals
    are present, behavior is essentially unchanged unless you enable
    opposite-side stacking.

(ENH) MBR logging:
  If `snapshots` has a callable `get_mbr(market_id) -> (pct, source)`,
  logs will include `applied_mbr_pct` and `mbr_source` for each plan.

(ENH 2025-10) Exposure-aware per-runner cap:
  - Optional resolver hook set via set_exposure_resolver(fn) where
    fn(market_id, selection_id, side) -> current unmatched liability.
  - If a per-runner liability cap is configured, we subtract existing
  exposure and scale/skip plans accordingly.
"""

from __future__ import annotations
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any, Callable

from ..core.config import load_config
from ..core.logging import log_event
from ..core.schemas import EdgeProposal, OrderBookSnapshot
from ..betfair.ticks import distance_in_ticks, one_tick_up, one_tick_down


# --------- Small data object returned by Arbiter (pre-intents) --------- #
@dataclass(frozen=True)
class QuotePlan:
    market_id: str
    selection_id: int
    side: str                  # "BACK" | "LAY"
    price: float
    # Provide BOTH for compatibility: Trader reads .size today; legacy used .size_hint
    size: float
    size_hint: float
    min_lifetime_ms: int
    persistence: str           # "KEEP" | "CANCEL" | "TAKE_SP"
    edge_contribs: List[dict]  # [{edge_id, weight, ev_net_ticks, price, size}]
    rationale: str             # short text
    # NEW: explicit identifiers for tests/diagnostics
    edge_id: str               # primary contributing edge for this plan
    lane: str                  # normalized lane name for this plan


class Arbiter:
    def __init__(self, profile_name: str, cfg_override: Optional[Dict] = None) -> None:
        self.profile_name = profile_name
        snap = load_config(profile_name)

        # --- Persistence policy (profile-level) ---
        self.persistence: str = "KEEP"
        try:
            self.persistence = (snap.profile.get("persistence", {}) or {}).get("default", "KEEP")
        except Exception:
            pass

        # --- Hygiene knobs (edge-agnostic) ---
        # Preferred source: arbiter.* ; Fallbacks: edges.maker.* (legacy) and throttles.replaces.*
        maker_cfg = {}
        try:
            maker_cfg = (getattr(snap, "edges", {}) or {}).get("maker", {})
        except Exception:
            maker_cfg = {}

        arb_top = {}
        try:
            arb_top = (getattr(snap, "arbiter", {}) or {})
        except Exception:
            arb_top = {}

        hygiene = {}
        try:
            hygiene = (arb_top.get("hygiene", {}) or {})
        except Exception:
            hygiene = {}

        self.min_lifetime_ms: int = int(
            hygiene.get("min_lifetime_ms",
                        maker_cfg.get("min_lifetime_ms", 2000))
        )

        throttles = getattr(snap, "throttles", None)
        replaces_throttles = {}
        try:
            if throttles:
                replaces_throttles = throttles.get("replaces", {})  # type: ignore[attr-defined]
        except Exception:
            replaces_throttles = {}

        replaces_arb = {}
        try:
            replaces_arb = (arb_top.get("replaces", {}) or {})
        except Exception:
            replaces_arb = {}

        self.min_replace_interval_ms: int = int(
            replaces_arb.get("min_interval_ms",
                             replaces_throttles.get("min_interval_ms", 2000))
        )
        self.widen_trigger_ticks: int = int(
            replaces_arb.get("widen_trigger_ticks",
                             replaces_throttles.get("widen_trigger_ticks", 1))
        )

        # --- Lane / budgets config (prefer top-level arbiter.*, fallback to legacy edges.arbiter.*) ---
        arb_cfg_legacy = {}
        try:
            arb_cfg_legacy = (getattr(snap, "edges", {}) or {}).get("arbiter", {}) or {}
        except Exception:
            arb_cfg_legacy = {}

        self.allow_opposite_side_stack: bool = bool(
            arb_top.get("allow_opposite_side_stack",
                        arb_cfg_legacy.get("allow_opposite_side_stack", False))
        )
        self.max_per_lane: int = int(
            arb_top.get("max_per_lane",
                        arb_cfg_legacy.get("max_per_lane", 1))
        )

        default_prios = ["suspend_reopen", "cross_market", "mean_revert", "maker"]
        # lane priorities: support top-level `arbiter.lane_priority(s)` first, then legacy `edges.arbiter.lane_priorities`
        pr_list = default_prios
        try:
            arb_top = (getattr(snap, "arbiter", {}) or {})
            if isinstance(arb_top, dict):
                pr_list = list(arb_top.get("lane_priority", arb_top.get("lane_priorities", pr_list)) or pr_list)
        except Exception:
            pass
        if pr_list is default_prios or not pr_list:
            pr_list = arb_cfg.get("lane_priorities", default_prios)

        self._lane_rank: Dict[str, int] = {str(name).lower(): i for i, name in enumerate(pr_list)}
        # Lane mapping (config-driven). Supports both top-level `arbiter.lane_of` and legacy `edges.arbiter.lane_of`.
        lane_map_top = {}
        try:
            lane_map_top = (getattr(snap, "arbiter", {}) or {}).get("lane_of", {}) or {}
        except Exception:
            lane_map_top = {}
        lane_map_legacy = {}
        try:
            lane_map_legacy = (getattr(snap, "edges", {}) or {}).get("arbiter", {}) or {}
            lane_map_legacy = (lane_map_legacy.get("lane_of", {}) or {})
        except Exception:
            lane_map_legacy = {}
        # Normalize keys/values to lowercase strings
        lane_map = {}
        try:
            lane_map = {str(k).lower(): str(v).lower() for k, v in (lane_map_top or lane_map_legacy or {}).items() if k}
        except Exception:
            lane_map = {}
        self._lane_of: Dict[str, str] = lane_map

        # --- Exposure-aware per-runner liability cap (optional) ---
        # Primary source: risk.per_runner_liability_cap
        # Fallbacks: edges.maker.overlays.stacking.liability_cap_per_runner (new)
        #            edges.maker.stack.liability_cap_per_runner (legacy)
        self._liability_cap_per_runner: Optional[float] = None
        try:
            cap = (getattr(snap, "risk", {}) or {}).get("per_runner_liability_cap", None)
            if cap is None:
                # NEW canonical path
                cap = ((((getattr(snap, "edges", {}) or {}).get("maker", {}) or {}).get("overlays", {}) or {})
                       .get("stacking", {}) or {}).get("liability_cap_per_runner", None)
            if cap is None:
                # Legacy path retained
                cap = ((((getattr(snap, "edges", {}) or {}).get("maker", {}) or {}).get("stack", {}) or {})
                       .get("liability_cap_per_runner", None))
            if cap is not None:
                self._liability_cap_per_runner = float(cap)
        except Exception:
            self._liability_cap_per_runner = None

        # Resolver hook to read current unmatched exposure from OrderManager
        self._exposure_resolver: Optional[Callable[[str, int, str], float]] = None

        if cfg_override:
            self.persistence = cfg_override.get("persistence", self.persistence)
            self.min_lifetime_ms = int(cfg_override.get("min_lifetime_ms", self.min_lifetime_ms))
            self.min_replace_interval_ms = int(cfg_override.get("min_replace_interval_ms", self.min_replace_interval_ms))
            self.widen_trigger_ticks = int(cfg_override.get("widen_trigger_ticks", self.widen_trigger_ticks))
            if "allow_opposite_side_stack" in cfg_override:
                self.allow_opposite_side_stack = bool(cfg_override["allow_opposite_side_stack"])
            if "max_per_lane" in cfg_override:
                self.max_per_lane = int(cfg_override["max_per_lane"])
            if "lane_priorities" in cfg_override and isinstance(cfg_override["lane_priorities"], list):
                self._lane_rank = {str(n).lower(): i for i, n in enumerate(cfg_override["lane_priorities"])}

        # State: last decision per (market, selection, side)
        self._last: Dict[Tuple[str, int, str], Dict] = {}

    # Allow Trader to wire OrderManager.exposure_for(...)
    def set_exposure_resolver(self, fn: Callable[[str, int, str], float]) -> None:
        self._exposure_resolver = fn

    # --------------------------- Public API --------------------------- #

    def decide(
        self,
        proposals: List[EdgeProposal],
        snapshots: Any | None = None,
        now_ms: Optional[int] = None,
    ) -> List[QuotePlan]:
        """
        Merge proposals into per-lane plans with priorities and hygiene.
        If `snapshots` has a `.get_mbr(market_id)` method, logs will include MBR context.
        """
        now = now_ms or _now_ms()
        if not proposals:
            return []

        # Small local cache to avoid repeated MBR lookups per market in this call
        mbr_cache: Dict[str, Tuple[Optional[float], Optional[str]]] = {}

        def mbr_ctx(market_id: str) -> Dict[str, Any]:
            if not market_id or snapshots is None or not hasattr(snapshots, "get_mbr"):
                return {}
            if market_id not in mbr_cache:
                try:
                    mbr_cache[market_id] = snapshots.get_mbr(market_id)  # type: ignore[attr-defined]
                except Exception:
                    mbr_cache[market_id] = (None, None)
            pct, src = mbr_cache.get(market_id, (None, None))
            if pct is None:
                return {}
            return {"applied_mbr_pct": round(float(pct), 6), "mbr_source": src or "unknown"}

        # 1) Group proposals by (market_id, selection_id, side)
        grouped: Dict[Tuple[str, int, str], List[EdgeProposal]] = {}
        for p in proposals:
            mid = getattr(p, "market_id", None)
            sid = getattr(p, "selection_id", None)
            side = str(getattr(p, "side", "BACK")).upper()
            if not mid or sid is None:
                continue
            grouped.setdefault((str(mid), int(sid), side), []).append(p)

        plans: List[QuotePlan] = []

        # 2) For each lane, select according to lane priority, then EV within lane
        for (mid, sel, side), lst in grouped.items():
            lane_buckets: Dict[str, List[EdgeProposal]] = {}
            for p in lst:
                lane = self._classify_lane(getattr(p, "edge_id", "maker"))
                lane_buckets.setdefault(lane, []).append(p)

            if not lane_buckets:
                continue

            ordered_lanes = sorted(
                lane_buckets.keys(),
                key=lambda ln: self._lane_rank.get(ln, 9999),
            )

            lane_winners: List[Tuple[str, EdgeProposal, List[EdgeProposal]]] = []
            for ln in ordered_lanes:
                cand = lane_buckets[ln]
                winner = max(
                    cand,
                    key=lambda p: float(getattr(p, "ev_net_ticks", 0.0) or 0.0)
                    * float(getattr(p, "weight", 1.0) or 1.0),
                )
                same_price = [
                    q for q in cand
                    if float(getattr(q, "price", 0.0) or 0.0) == float(getattr(winner, "price", 0.0) or 0.0)
                ]
                lane_winners.append((ln, winner, same_price))

            lane_winners = lane_winners[: max(1, int(self.max_per_lane))]

            for ln, winner, same_price_list in lane_winners:
                target_price = float(getattr(winner, "price", 0.0) or 0.0)

                key = (mid, sel, side)
                last = self._last.get(key)
                action = "new"

                if last:
                    dt = now - int(last.get("ts", 0))
                    tick_move = abs(
                        distance_in_ticks(float(last.get("price", target_price)), target_price)
                    )
                    if dt < self.min_lifetime_ms and tick_move < self.widen_trigger_ticks:
                        target_price = float(last["price"])
                        action = "hold_min_lifetime"

                    dr = now - int(last.get("last_replace_ts", 0))
                    price_changed = (target_price != float(last.get("price", target_price)))
                    if price_changed and dr < self.min_replace_interval_ms:
                        target_price = float(last["price"])
                        action = "hold_min_interval"

                total_size = float(
                    sum(float(getattr(p, "size_hint", 0.0) or 0.0) for p in same_price_list)
                )
                contribs = [
                    {
                        "edge_id": getattr(p, "edge_id", None),
                        "weight": float(getattr(p, "weight", 1.0) or 1.0),
                        "ev_net_ticks": float(getattr(p, "ev_net_ticks", 0.0) or 0.0),
                        "price": float(getattr(p, "price", 0.0) or 0.0),
                        "size": float(getattr(p, "size_hint", 0.0) or 0.0),
                    }
                    for p in same_price_list
                ]

                # ---- Exposure-aware per-runner cap (optional) ----
                new_total_size = total_size
                cap = self._liability_cap_per_runner
                if cap is not None and cap > 0.0:
                    liab_per_unit = (max(0.0, target_price - 1.0) if side == "LAY" else 1.0)
                    if liab_per_unit > 0.0:
                        existing = 0.0
                        try:
                            if self._exposure_resolver:
                                existing = float(self._exposure_resolver(mid, sel, side)) or 0.0
                        except Exception:
                            existing = 0.0
                        remaining_liab = float(cap) - max(0.0, existing)
                        if remaining_liab <= 0.0:
                            # Already at/over cap → skip this plan
                            log_event(
                                "arbiter", "arbiter.cap", "skip",
                                market_id=mid, selection_id=sel, side=side,
                                cap=cap, existing_liability=existing, price=target_price
                            )
                            continue
                        allowed_size = remaining_liab / liab_per_unit
                        if new_total_size > allowed_size:
                            if allowed_size <= 0.0:
                                log_event(
                                    "arbiter", "arbiter.cap", "skip_zero",
                                    market_id=mid, selection_id=sel, side=side,
                                    cap=cap, existing_liability=existing, price=target_price
                                )
                                continue
                            scale = allowed_size / max(1e-12, new_total_size)
                            for c in contribs:
                                c["size"] = float(c.get("size", 0.0)) * scale
                            log_event(
                                "arbiter", "arbiter.cap", "trim",
                                market_id=mid, selection_id=sel, side=side,
                                cap=cap, existing_liability=existing, allowed_size=allowed_size,
                                original_size=new_total_size, price=target_price
                            )
                            new_total_size = allowed_size

                rationale = (getattr(winner, "rationale", "") or f"{ln} lane").strip()
                primary_edge = str(getattr(winner, "edge_id", ln)) or ln

                plan = QuotePlan(
                    market_id=mid,
                    selection_id=sel,
                    side=side,
                    price=target_price,
                    size=max(0.0, new_total_size),
                    size_hint=max(0.0, new_total_size),
                    min_lifetime_ms=self.min_lifetime_ms,
                    persistence=self.persistence,
                    edge_contribs=contribs,
                    rationale=rationale[:160],
                    edge_id=primary_edge,
                    lane=ln,
                )

                # State + logs (with MBR context if available)
                ctx = mbr_ctx(mid)
                changed = (not last) or (float(last.get("price", -1.0)) != plan.price)
                if not last:
                    self._last[key] = {"price": plan.price, "ts": now, "last_replace_ts": now}
                    log_event(
                        "arbiter", "arbiter.plan", "new",
                        market_id=mid, selection_id=sel, side=side, price=plan.price, size=plan.size, lane=ln, **ctx
                    )
                else:
                    if changed:
                        self._last[key].update({"price": plan.price, "last_replace_ts": now})
                        log_event(
                            "arbiter", "arbiter.replace", "update",
                            market_id=mid, selection_id=sel, side=side, price=plan.price, action=action, lane=ln, **ctx
                        )
                    self._last[key]["ts"] = now
                    if action.startswith("hold"):
                        log_event(
                            "arbiter", "arbiter.hold", action,
                            market_id=mid, selection_id=sel, side=side, price=plan.price, lane=ln, **ctx
                        )

                plans.append(plan)

        plans = self._maybe_add_opposite_stack(plans, snapshots, now)

        if not self.allow_opposite_side_stack:
            plans = self._collapse_to_single_side(plans)

        return plans

    # --------------------------- Back-compat shims --------------------------- #
    @property
    def lane_priorities(self) -> List[str]:
        return [ln for ln, _ in sorted(self._lane_rank.items(), key=lambda kv: kv[1])]

    def prioritize(
        self,
        cfg_unused,
        proposals: List[EdgeProposal],
        snapshots: Dict[str, OrderBookSnapshot] | None = None,
        now_ms: Optional[int] = None,
    ) -> List[QuotePlan]:
        return self.decide(proposals, snapshots=snapshots, now_ms=now_ms)

    # --------------------------- Internal --------------------------- #

    def _collapse_to_single_side(self, plans: List[QuotePlan]) -> List[QuotePlan]:
        by_sel: Dict[Tuple[str, int], Dict[str, List[QuotePlan]]] = {}
        for pl in plans:
            by_sel.setdefault((pl.market_id, pl.selection_id), {}).setdefault(pl.side, []).append(pl)

        out: List[QuotePlan] = []
        for (mid, sel), sides in by_sel.items():
            if len(sides) == 1:
                out.extend(next(iter(sides.values())))
                continue
            best_side = None
            best_score = float("-inf")
            keep: List[QuotePlan] = []
            for side, pls in sides.items():
                score = 0.0
                for pl in pls:
                    for c in pl.edge_contribs:
                        score += float(c.get("ev_net_ticks", 0.0)) * float(c.get("weight", 1.0))
                if score > best_score:
                    best_score = score
                    best_side = side
                    keep = pls
            out.extend(keep)
        return out

    def _maybe_add_opposite_stack(self, plans: List[QuotePlan], snapshots: Any | None, now: int) -> List[QuotePlan]:
        """For maker lanes in thick books, add a small opposite-side helper plan.
        Conservative: skip if book thickness cannot be verified from snapshots.
        """
        if not (self.allow_opposite_side_stack and getattr(self, "stack_enabled", False)):
            return plans
        # Per (market, selection) cap counting
        per_sel_counts: Dict[Tuple[str,int], int] = {}
        out: List[QuotePlan] = list(plans)
        for pl in plans:
            key = (pl.market_id, pl.selection_id)
            per_sel_counts[key] = per_sel_counts.get(key, 0) + 1

        def _thick(mid: str, sel: int) -> bool:
            if snapshots is None:
                return False
            try:
                snap = None
                # Try common access patterns
                if hasattr(snapshots, "get"):
                    snap = snapshots.get(mid)  # type: ignore[attr-defined]
                if snap is None and hasattr(snapshots, "get_snapshot"):
                    snap = snapshots.get_snapshot(mid)  # type: ignore[attr-defined]
                if snap is None and hasattr(snapshots, "all_snapshots"):
                    snap = (snapshots.all_snapshots() or {}).get(mid)  # type: ignore[attr-defined]
                if snap is None:
                    return False
                rb = getattr(snap, "runners", {}).get(sel)
                if not rb:
                    return False
                backs = getattr(rb, "best_back_raw", None) or getattr(rb, "best_back", []) or []
                lays  = getattr(rb, "best_lay_raw", None) or getattr(rb, "best_lay", []) or []
                b2 = sum(float(getattr(x, "size", 0.0) or 0.0) for x in backs[:2])
                l2 = sum(float(getattr(x, "size", 0.0) or 0.0) for x in lays[:2])
                raw2 = float(b2 + l2)
                if raw2 < self.stack_min_raw_best2_sum:
                    return False
                # Spread gate
                try:
                    bb = float(backs[0].price) if backs else None
                    bl = float(lays[0].price) if lays else None
                    if bb is None or bl is None:
                        return False
                    sp = distance_in_ticks(bb, bl)
                    if sp is None or sp <= 0 or sp > self.stack_max_spread_ticks:
                        return False
                except Exception:
                    return False
                return True
            except Exception:
                return False

        for pl in plans:
            if per_sel_counts.get((pl.market_id, pl.selection_id), 0) >= self.stack_max_quotes_per_lane_per_market:
                continue
            # Only for maker lane
            if getattr(pl, "lane", "maker") != "maker":
                continue
            # Verify thickness
            if not _thick(pl.market_id, pl.selection_id):
                continue
            # Build opposite-side plan
            opp_side = "BACK" if pl.side == "LAY" else "LAY"
            # Adjust price 1 tick toward getting done
            price = pl.price
            try:
                price = one_tick_down(pl.price) if opp_side == "BACK" else one_tick_up(pl.price)
                price = float(price or pl.price)
            except Exception:
                price = pl.price
            # Size with caps
            base = float(getattr(pl, "size", 0.0) or getattr(pl, "size_hint", 0.0) or 0.0)
            stake = max(0.0, self.stack_size_mult * base)
            if opp_side == "LAY":
                liab = stake * max(0.0, (price - 1.0))
                if liab > self.stack_liability_cap_per_runner and (price - 1.0) > 0:
                    stake = self.stack_liability_cap_per_runner / (price - 1.0)
            else:
                # BACK stake is not liability-limited here; stake is already small
                pass
            if stake <= 0.0:
                continue
            plan2 = QuotePlan(
                market_id=pl.market_id,
                selection_id=pl.selection_id,
                side=opp_side,
                price=price,
                size=stake,
                size_hint=stake,
                min_lifetime_ms=int(pl.min_lifetime_ms + self.stack_ttl_bump_ms),
                persistence=pl.persistence,
                edge_contribs=[
                    {"edge_id": "maker_stack", "weight": 0.8, "ev_net_ticks": 0.0, "price": price, "size": stake}
                ],
                rationale="maker opposite-side stack",
                edge_id="maker_stack",
                lane="maker",
            )
            out.append(plan2)
            per_sel_counts[(pl.market_id, pl.selection_id)] = per_sel_counts.get((pl.market_id, pl.selection_id), 0) + 1
        return out

    def _classify_lane(self, edge_id: Optional[str]) -> str:
        """Classify edge into lane using config map if provided; fallback to legacy heuristics."""
        try:
            eid = (edge_id or "").lower()
            # exact match on provided name (e.g., "maker", "parity", "suspend_reopen", "xmsr")
            mapped = self._lane_of.get(eid) if hasattr(self, "_lane_of") and isinstance(self._lane_of, dict) else None
            if mapped:
                return mapped
        except Exception:
            pass
        # Fallback to old substring heuristic
        return _classify_lane(edge_id)


# ----------------------- helpers ----------------------- #
def _classify_lane(edge_id: Optional[str]) -> str:
    eid = (edge_id or "").lower()
    if eid.startswith("suspend") or "reopen" in eid:
        return "suspend_reopen"
    if "cross" in eid or "parity" in eid:
        return "cross_market"
    if "mean" in eid or "revert" in eid:
        return "mean_revert"
    return "maker"


def _now_ms() -> int:
    return int(time.time() * 1000)
