# src/strategy/fee_gate.py
"""
Location : src/strategy/fee_gate.py
Purpose  : Allow or block proposed quotes based on a simple cost floor:
           EV (in ticks) must beat fees + slippage + a small safety cushion.
           (NEW) Use per-market Market Base Rate (MBR) when available.

Plain summary
- Think in "ticks": proposals carry EV in ticks (ev_net_ticks).
- We subtract an estimated tick cost for fees (from MBR if known) + slippage + cushion.
- If EV >= total_cost → PASS, else BLOCK.
- Emits clear audit logs for every pass/block (includes commission source).

Config sources (precedence, highest wins):
1) cfg_override.edges.fee_gate.*            (optional runtime override)
2) profile.edges.fee_gate.*                 (per-profile YAML)
3) edges.fee_gate.*                         (global defaults in edges.yaml)
PLUS: commission % fallback from profile.fees.commission_pct, then base.betfair.commission_pct_default.
"""

from __future__ import annotations
from typing import Dict, List, Optional, Tuple, Any

from ..core.config import load_config
from ..core.logging import log_event
from ..core.schemas import EdgeProposal, OrderBookSnapshot


class FeeGate:
    def __init__(self, profile_name: str, cfg_override: Optional[Dict] = None) -> None:
        """
        Auto-load fees and fee-gate knobs with correct precedence.
        """
        self.profile_name = profile_name
        snap = load_config(profile_name)

        # --- Commission % (fallback only; per-market MBR preferred in allow()) ---
        self._commission_pct_fallback: float = _read_commission_pct(snap)

        # --- Fee-gate knobs in ticks, with precedence ---
        global_cfg = {}
        try:
            global_cfg = getattr(snap, "edges", {}).get("fee_gate", {})  # edges.yaml defaults
        except Exception:
            global_cfg = {}

        profile_cfg = {}
        try:
            profile_cfg = (getattr(snap, "profile", {}) or {}).get("edges", {}).get("fee_gate", {})
        except Exception:
            profile_cfg = {}

        merged = _deep_merge(global_cfg, profile_cfg)  # profile overrides global
        if cfg_override:
            try:
                merged = _deep_merge(merged, (cfg_override.get("edges", {}) or {}).get("fee_gate", {}))
            except Exception:
                pass

        # Non-commission costs in ticks
        self.slippage_ticks: float = float(merged.get("slippage_ticks", 0.3))
        self.cushion_ticks: float = float(merged.get("cushion_ticks", 0.2))

        # Backward-compat: some installs used fee_ticks as a static total. We'll treat it as
        # a minimum cost floor (ticks) that can exceed dynamic commission-derived ticks.
        self._min_cost_ticks: float = float(merged.get("fee_ticks", 0.0))

    # ---------------- Public API ---------------- #

    def ok(self, obj: Any, snapshot: Optional[Any] = None) -> bool:
        """
        Convenience for callers that may pass either an EdgeProposal or a plan-like object.
        - If obj has ev_net_ticks/market_id/selection_id/side/price → treat as EdgeProposal.
        - Otherwise, fail-open (True) and log a one-liner (keeps legacy codepaths safe).
        """
        try:
            if hasattr(obj, "ev_net_ticks") and hasattr(obj, "market_id"):
                ok, _reason, _ = self.allow(obj, snapshot=snapshot)
                return bool(ok)
        except Exception:
            pass
        log_event("fee_gate", "fee_gate.skip", "unknown_obj_type")
        return True

    def allow(self, proposal: EdgeProposal, snapshot: Optional[Any] = None) -> Tuple[bool, str, Dict]:
        """
        Decide if a single proposal passes the fee gate.
        Returns (ok, reason, details_dict).
        Uses per-market MBR if available via BookBuilder (snapshot) or falls back to config.
        """
        ev_ticks = float(getattr(proposal, "ev_net_ticks", 0.0))

        # Commission %: prefer BookBuilder.get_mbr(...) if provided
        commission_pct, commission_src = self._resolve_commission_pct(getattr(proposal, "market_id", None), snapshot)

        # Convert commission to ticks. Heuristic: ~12 * commission% ticks at 5% ≈ 0.6 ticks
        commission_ticks = 12.0 * float(commission_pct or 0.0)

        # Total cost = max(min static floor, commission_ticks) + slippage + cushion
        commission_floored = max(self._min_cost_ticks, commission_ticks)
        total_cost = commission_floored + self.slippage_ticks + self.cushion_ticks

        ok = ev_ticks >= total_cost

        details = {
            "ev_ticks": round(ev_ticks, 4),
            "commission_pct": round(float(commission_pct or 0.0), 6),
            "commission_src": commission_src or "fallback",
            "commission_ticks": round(commission_ticks, 4),
            "min_cost_ticks": round(self._min_cost_ticks, 4),
            "slip_ticks": round(self.slippage_ticks, 4),
            "cushion_ticks": round(self.cushion_ticks, 4),
            "cost_ticks": round(total_cost, 4),
            "market_id": getattr(proposal, "market_id", None),
            "selection_id": getattr(proposal, "selection_id", None),
            "edge": getattr(proposal, "edge_id", None),
            "side": getattr(proposal, "side", None),
            "price": getattr(proposal, "price", None),
            "profile": self.profile_name,
        }

        if ok:
            log_event("fee_gate", "fee_gate.pass", "fee gate pass", **details)
            return True, "pass", details
        else:
            log_event("fee_gate", "fee_gate.block", "fee gate block", **details)
            return False, "block", details

    def filter(self, proposals: List[EdgeProposal], snapshot: Optional[OrderBookSnapshot] = None) -> List[EdgeProposal]:
        """
        Keep only proposals that pass. Emits a log per proposal.
        """
        passed: List[EdgeProposal] = []
        for p in proposals:
            ok, _reason, _d = self.allow(p, snapshot=snapshot)
            if ok:
                passed.append(p)
        return passed

    # -------------- helpers -------------- #

    def _resolve_commission_pct(self, market_id: Optional[str], snapshot: Optional[Any]) -> Tuple[float, Optional[str]]:
        """
        If snapshot has get_mbr(market_id) (BookBuilder), use it; else fallback to config.
        Returns (pct_fraction, source_str).
        """
        # Try BookBuilder
        try:
            if market_id and snapshot and hasattr(snapshot, "get_mbr"):
                pct, src = snapshot.get_mbr(market_id)  # type: ignore[attr-defined]
                if pct is not None:
                    return float(pct), (src or "book")
        except Exception:
            pass
        # Fallback
        return float(self._commission_pct_fallback), "fallback"


def _deep_merge(dst: Dict, src: Dict) -> Dict:
    """Recursive dict merge (src over dst)."""
    out = dict(dst or {})
    for k, v in (src or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out

def _read_commission_pct(snap) -> float:
    # profile fees first
    try:
        pct = float((getattr(snap, "profile", {}) or {}).get("fees", {}).get("commission_pct"))
        if pct:
            return pct
    except Exception:
        pass
    # fallback to base default
    try:
        return float(getattr(snap, "betfair", {}).get("commission_pct_default", 0.05))
    except Exception:
        return 0.05
