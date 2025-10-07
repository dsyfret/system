# src/strategy/sizing.py
"""
Location : src/strategy/sizing.py
Purpose  : Turn an approved proposal into a stake size that respects:
           - fractional-Kelly based on edge (from EV in ticks),
           - per-market max % of bankroll,
           - per-minute new-risk cap,
           - optional drawdown risk multiplier.
           - (NEW) Option to auto-use live bankroll via Accounts API.
           - (NEW) Absolute per-minute new-risk cap (risk.new_risk_per_min_abs).
           - (ENH) Optional per-market MBR context logging via resolver hook.
           - (ENH 2025-09) Safety clamps: price bounds, tick alignment, min stake with explicit reasons.

Plain summary
- EV (ticks) -> edge% at the quoted price -> Kelly-lite -> clamp by caps.
- BACK liability = stake; LAY liability = (price-1)*stake.
- Emits one audit log per sizing decision, optionally with MBR info when available.
- Two ways to use:
    1) size(. bankroll=NUMBER)          # you supply bankroll
    2) await size_async(. )              # it fetches live bankroll (with haircut) from config

MBR logging
- Call `sizer.set_mbr_resolver(book.get_mbr)` at init time (optional).
- If set, logs will include: applied_mbr_pct, mbr_source.
"""
import time


from __future__ import annotations
import time
from typing import Callable, Dict, Optional, Tuple, Any

from ..core.config import load_config
from ..core.logging import log_event
from ..core.schemas import EdgeProposal
from ..betfair.ticks import tick_size as _tick_size
# Optional: if 'snap' is present in ticks, use it to compute a suggested on-tick price.
try:
    from ..betfair.ticks import snap as _snap  # type: ignore
except Exception:  # pragma: no cover
    _snap = None  # type: ignore

from .bankroll import get_bankroll  # live bankroll helper


_MIN_PRICE = 1.01
_MAX_PRICE = 1000.0
_EPS = 1e-9


class Sizer:
    def size_plan(self, plan, bankroll, book):
        """Return a QuoteIntent from a QuotePlan using existing math."""
        from ...core.schemas import EdgeProposal
        from ...execution.interfaces.intents import QuoteIntent
        side = str(getattr(plan, 'side', 'BACK')).upper()
        proposal = EdgeProposal(
            edge_id=str(getattr(plan, 'edge_id', 'arbiter')),
            market_id=str(getattr(plan, 'market_id', '')),
            selection_id=int(getattr(plan, 'selection_id', 0)),
            side=side,
            price=float(getattr(plan, 'price', 0.0)),
            ttl_ms=int(getattr(plan, 'min_lifetime_ms', 1500)),
            size_hint=float(getattr(plan, 'size', 0.0) or getattr(plan, 'size_hint', 0.0)),
            weight=1.0,
            ev_net_ticks=float(next(iter([c.get('ev_net_ticks', 0.0) for c in getattr(plan, 'edge_contribs', [])] or [0.0]))),
            rationale=str(getattr(plan, 'rationale', ''))[:160],
            bundle_id=getattr(plan, 'bundle_id', None),
            bundle_policy=getattr(plan, 'bundle_policy', None),
        )
        stake, reason, _details = self.size(proposal, bankroll=bankroll)
        if stake <= 0.0:
            return None
        intent = QuoteIntent(
            intent_id=f"ARB:{getattr(plan, 'edge_id', 'arbiter')}:{int(self._now()*1000)}",
            market_id=proposal.market_id,
            selection_id=proposal.selection_id,
            side=side,
            price=proposal.price,
            size=stake,
            tif='GTC',
            persistence=str(getattr(plan, 'persistence', 'KEEP')).upper(),
            min_lifetime_ms=int(getattr(plan, 'min_lifetime_ms', 1500)),
            max_replace_rate_per_min=60,
            edge_contribs=list(getattr(plan, 'edge_contribs', []) or []),
            risk_tags={'source': 'arbiter'},
            client_ts_ms=int(self._now()*1000),
            bundle_id=proposal.bundle_id,
            bundle_policy=proposal.bundle_policy,
        )
        # P2-8: emit SizeDecisionRecord (guarded by config)
        try:
            if (self.cfg.get('risk', {}).get('size_decision_records', {}).get('enabled', False)):
                _clamps = list(getattr(self, '_last_clamp_reasons', [])) if hasattr(self, '_last_clamp_reasons') else []
                _rec = SizeDecisionRecord(
                    intent_id=str(getattr(intent, 'intent_id', '')),
                    market_id=str(getattr(intent, 'market_id', getattr(plan, 'market_id', ''))),
                    selection_id=int(getattr(intent, 'selection_id', getattr(plan, 'selection_id', 0))),
                    side=str(getattr(intent, 'side', getattr(plan, 'side', ''))),
                    proposed_price=float(getattr(plan, 'price', 0.0)),
                    snapped_price=float(getattr(intent, 'price', getattr(plan, 'price', 0.0))),
                    bankroll_source=str(getattr(self, '_bankroll_source', 'unknown')),
                    bankroll_amount=float(getattr(self, '_bankroll_amount', 0.0)),
                    raw_kelly_stake=float(locals().get('kelly_stake', 0.0)),
                    clamps_applied=_clamps,
                    mbr_pct=float(locals().get('applied_mbr_pct', 0.0)),
                    final_size=float(getattr(intent, 'size', 0.0)),
                    ts_ms=int(time.time()*1000),
                )
                if hasattr(self.ledger, 'record_size_decision'):
                    self.ledger.record_size_decision(_rec)
        except Exception:
            pass
        return intent.snapped() if hasattr(intent, 'snapped') else intent
    def __init__(self, profile_name: str, cfg_override: Optional[Dict] = None) -> None:
        self.profile_name = profile_name
        snap = load_config(profile_name)

        # --- Risk caps ---
        rb = getattr(snap, "risk", {}).get("bankroll", {})  # type: ignore[attr-defined]
        self.per_market_max_pct: float = float(rb.get("per_market_max_pct", 0.008))          # 0.8%
        self.per_minute_new_risk_pct: float = float(rb.get("per_minute_new_risk_pct", 0.03)) # 3%

        # Absolute cap (root-level risk.new_risk_per_min_abs; also supported in base.yaml)
        self.per_minute_new_risk_abs: float = float(getattr(snap, "risk", {}).get("new_risk_per_min_abs", 0.0))

        # --- Kelly fraction (optional knob) ---
        rz = getattr(snap, "risk", {}).get("sizing", {})  # type: ignore[attr-defined]
        self.kelly_fraction: float = float(rz.get("kelly_fraction", 0.25))  # quarter-Kelly by default

        # --- Safety floor: min stake ---
        self.min_stake: float = float(getattr(snap, "risk", {}).get("min_stake", 2.0))  # default 2.0 unless configured

        # Allow runtime overrides if provided
        if cfg_override:
            self.per_market_max_pct = float(cfg_override.get("per_market_max_pct", self.per_market_max_pct))
            self.per_minute_new_risk_pct = float(cfg_override.get("per_minute_new_risk_pct", self.per_minute_new_risk_pct))
            self.kelly_fraction = float(cfg_override.get("kelly_fraction", self.kelly_fraction))
            if "per_minute_new_risk_abs" in cfg_override:
                self.per_minute_new_risk_abs = float(cfg_override["per_minute_new_risk_abs"])
            if "min_stake" in cfg_override:
                self.min_stake = float(cfg_override["min_stake"])

        # Per-minute accounting for new risk
        self._bucket_start_ms: int = _now_ms()
        self._bucket_new_risk: float = 0.0  # absolute currency units

        # (ENH) Optional MBR resolver callable: market_id -> (pct_fraction, source)
        self._mbr_resolver: Optional[Callable[[str], Tuple[Optional[float], Optional[str]]]] = None

    # ---------------- Optional hook ---------------- #

    def set_mbr_resolver(self, resolver: Callable[[str], Tuple[Optional[float], Optional[str]]]) -> None:
        """
        Set a function that returns (pct_fraction, source) for a given market_id.
        Example: sizer.set_mbr_resolver(book.get_mbr)
        """
        self._mbr_resolver = resolver

    # ---------------- Public API ---------------- #

    async def size_async(
        self,
        proposal: EdgeProposal,
        risk_multiplier: float = 1.0,
        now_ms: Optional[int] = None,
    ) -> Tuple[float, str, Dict]:
        """
        Auto-fetches live bankroll per config (risk.bankroll.source/haircut_pct).
        Returns (stake, reason, details).
        """
        bankroll = await get_bankroll(self.profile_name)
        return self.size(proposal, bankroll=bankroll, risk_multiplier=risk_multiplier, now_ms=now_ms)

    def size(
        self,
        proposal: EdgeProposal,
        bankroll: float,
        risk_multiplier: float = 1.0,
        now_ms: Optional[int] = None,
    ) -> Tuple[float, str, Dict]:
        """
        Sizing using an explicit bankroll (float). Returns (stake, reason, details).
        """
        now = now_ms or _now_ms()
        self._roll_minute_bucket(now)

        price = float(getattr(proposal, "price", 0.0) or 0.0)
        side = str(getattr(proposal, "side", "BACK")).upper()
        ev_ticks = float(getattr(proposal, "ev_net_ticks", 0.0) or 0.0)

        # ---------- Safety clamps: price bounds & tick alignment ----------
        # Hard bounds: 1.01 .. 1000
        if price < _MIN_PRICE - _EPS:
            details = _details(self, proposal, bankroll, 0.0, 0.0, 0.0, 0.0, 0.0, side, price, risk_multiplier)
            details.update({"clamp_reason": "PRICE_BELOW_MIN", "min_price": _MIN_PRICE})
            details.update(self._mbr_context(getattr(proposal, "market_id", None)))
            log_event("sizing", "size.block", "price_below_min", **details)
            return 0.0, "price_below_min", details
        if price > _MAX_PRICE + _EPS:
            details = _details(self, proposal, bankroll, 0.0, 0.0, 0.0, 0.0, 0.0, side, price, risk_multiplier)
            details.update({"clamp_reason": "PRICE_ABOVE_MAX", "max_price": _MAX_PRICE})
            details.update(self._mbr_context(getattr(proposal, "market_id", None)))
            log_event("sizing", "size.block", "price_above_max", **details)
            return 0.0, "price_above_max", details

        # Tick alignment check (suggest a snapped price if available)
        if not _is_on_tick(price):
            details = _details(self, proposal, bankroll, 0.0, 0.0, 0.0, 0.0, 0.0, side, price, risk_multiplier)
            suggested = _suggest_snap(price)
            if suggested is not None:
                details["suggested_price"] = float(suggested)
            details["clamp_reason"] = "PRICE_OFF_TICK"
            details.update(self._mbr_context(getattr(proposal, "market_id", None)))
            log_event("sizing", "size.block", "price_off_tick", **details)
            return 0.0, "price_off_tick", details

        # ---------- Basic validity ----------
        if ev_ticks <= 0.0 or bankroll <= 0.0:
            details = _details(self, proposal, bankroll, 0.0, 0.0, 0.0, 0.0, 0.0, side, price, risk_multiplier)
            details.update(self._mbr_context(getattr(proposal, "market_id", None)))
            log_event("sizing", "size.block", "invalid inputs", **details)
            return 0.0, "invalid", details

        # --- Convert ticks → edge% at this price ---
        tick_w = _tick_size(price)
        edge_pct = max(0.0, ev_ticks * tick_w / price)  # fractional edge (e.g. 0.003 = 0.3%)

        # --- Kelly-lite fraction ---
        kelly_f = self.kelly_fraction * edge_pct  # risk-averse fraction of bankroll

        base_stake = max(0.0, bankroll * kelly_f * float(risk_multiplier))

        # --- Clamp by per-market cap ---
        per_market_cap = bankroll * self.per_market_max_pct
        stake_capped_market = min(base_stake, per_market_cap)

        # --- Clamp by per-minute new risk cap (pct and absolute) ---
        liab_per_unit = 1.0 if side == "BACK" else max(0.0, price - 1.0)

        cap_pct_abs = bankroll * self.per_minute_new_risk_pct
        minute_cap_abs = max(cap_pct_abs, float(self.per_minute_new_risk_abs or 0.0))
        remaining_minute_abs = max(0.0, minute_cap_abs - self._bucket_new_risk)

        if liab_per_unit <= 0.0:
            minute_capped_stake = 0.0
        else:
            minute_capped_stake = min(stake_capped_market, remaining_minute_abs / liab_per_unit)

        stake = max(0.0, minute_capped_stake)

        # ---------- Final min-stake clamp ----------
        if 0.0 < stake < self.min_stake:
            # Block and annotate (executor will not place micro-stakes)
            details = _details(
                self, proposal, bankroll, edge_pct, kelly_f, base_stake,
                per_market_cap, minute_cap_abs, side, price, risk_multiplier,
                liab_per_unit=liab_per_unit, new_risk_abs=stake * liab_per_unit, stake=stake
            )
            details["clamp_reason"] = "STAKE_BELOW_MIN"
            details["min_stake"] = float(self.min_stake)
            details.update(self._mbr_context(getattr(proposal, "market_id", None)))
            log_event("sizing", "size.block", "stake_below_min", **details)
            return 0.0, "stake_below_min", details

        # --- Record new risk consumed this minute ---
        new_risk_abs = stake * liab_per_unit
        self._bucket_new_risk += new_risk_abs

        # --- Emit audit log (with optional MBR context) ---
        details = _details(
            self, proposal, bankroll, edge_pct, kelly_f, base_stake,
            per_market_cap, minute_cap_abs, side, price, risk_multiplier,
            liab_per_unit=liab_per_unit, new_risk_abs=new_risk_abs, stake=stake
        )
        details.update(self._mbr_context(getattr(proposal, "market_id", None)))

        event = "size.pass" if stake > 0 else "size.block"
        msg = "sized" if stake > 0 else "blocked"
        log_event("sizing", event, msg, **details)

        reason = "ok" if stake > 0 else "capped_or_no_edge"
        return float(stake), reason, details

    def reset_minute(self, now_ms: Optional[int] = None) -> None:
        """Manually reset the per-minute accounting (usually not needed)."""
        self._bucket_start_ms = (now_ms or _now_ms())
        self._bucket_new_risk = 0.0

    # ---------------- Internal ---------------- #

    def _roll_minute_bucket(self, now_ms: int) -> None:
        if now_ms - self._bucket_start_ms >= 60_000:
            self._bucket_start_ms = now_ms
            self._bucket_new_risk = 0.0

    def _mbr_context(self, market_id: Optional[str]) -> Dict[str, Any]:
        """
        Return {'applied_mbr_pct': float, 'mbr_source': str} if resolver is set and MBR is known; else {}.
        """
        if not market_id or self._mbr_resolver is None:
            return {}
        try:
            pct, src = self._mbr_resolver(str(market_id))
            if pct is None:
                return {}
            return {"applied_mbr_pct": round(float(pct), 6), "mbr_source": (src or "unknown")}
        except Exception:
            return {}


# ---------------- Helpers ---------------- #

def _now_ms() -> int:
    return int(time.time() * 1000)

def _details(
    self_ref: Sizer,
    proposal: EdgeProposal,
    bankroll: float,
    edge_pct: float,
    kelly_f: float,
    base_stake: float,
    per_market_cap: float,
    minute_cap_abs: float,
    side: str,
    price: float,
    risk_multiplier: float,
    **extra,
) -> Dict:
    d = {
        "market_id": getattr(proposal, "market_id", None),
        "selection_id": getattr(proposal, "selection_id", None),
        "edge": getattr(proposal, "edge_id", None),
        "ev_ticks": float(getattr(proposal, "ev_net_ticks", 0.0) or 0.0),
        "edge_pct": round(edge_pct, 6),
        "kelly_fraction": round(self_ref.kelly_fraction, 6),
        "kelly_f_effective": round(kelly_f, 6),
        "bankroll": round(bankroll, 2),
        "per_market_cap": round(per_market_cap, 2),
        "per_minute_cap_abs": round(minute_cap_abs, 2),
        "risk_multiplier": round(float(risk_multiplier), 3),
        "side": side,
        "price": round(price, 2),
        "stake_base": round(base_stake, 2),
        "min_stake": round(self_ref.min_stake, 2),
    }
    d.update({k: (round(v, 6) if isinstance(v, float) else v) for k, v in extra.items()})
    return d

def _is_on_tick(price: float) -> bool:
    """Check if price is aligned to the current tick size band."""
    try:
        ts = float(_tick_size(price))
        # Normalise to 2 ULP around the band
        snapped_units = round(price / ts)
        snapped = snapped_units * ts
        return abs(snapped - price) <= max(1e-9, ts * 1e-9)
    except Exception:
        return True  # be permissive if tick_size fails

def _suggest_snap(price: float) -> Optional[float]:
    """If ticks.snap exists, suggest the on-tick price; else None."""
    if _snap is None:
        return None
    try:
        return float(_snap(price))
    except Exception:
        return None