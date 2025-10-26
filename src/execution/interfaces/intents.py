# src/execution/interfaces/intents.py
"""
Location : src/execution/interfaces/intents.py
Purpose  : Canonical QuoteIntent (strategy -> executor) with stable wire format.
Notes    : Keep fields stable. Rust/Go executors must use the same keys/types.

(ENH 2025-09) Bundles foundation:
  - bundle_id: Optional[str]
  - bundle_policy: Optional["ALL_OR_CANCEL" | "BEST_EFFORT"]

(ENH 2025-09) Price hygiene helpers (opt-in):
  - snapped_price(price: float) -> float          # clamp to [1.01, 1000], snap to tick if ticks.snap available
  - QuoteIntent.snapped() -> QuoteIntent         # returns a NEW instance with price sanitized
"""

from __future__ import annotations
from dataclasses import dataclass, asdict, replace
from typing import Literal, Dict, List, Any, Optional
import time

# Optional import of tick helpers; keep this file independent of betfair/ticks at runtime.
_MIN_PRICE = 1.01
_MAX_PRICE = 1000.0
try:
    from ...betfair.ticks import snap as _tick_snap  # type: ignore
except Exception:  # pragma: no cover
    _tick_snap = None  # type: ignore

Side = Literal["BACK", "LAY"]
TIF = Literal["GTC", "GTD"]
Persistence = Literal["CANCEL", "KEEP", "TAKE_SP"]
BundlePolicy = Literal["ALL_OR_CANCEL", "BEST_EFFORT"]


def now_ms() -> int:
    return int(time.time() * 1000)


def snapped_price(price: float) -> float:
    """
    Clamp price to exchange bounds and snap to the nearest valid tick if ticks.snap is available.
    No-ops if tick lib not present.
    """
    try:
        p = float(price)
    except Exception:
        return _MIN_PRICE
    # bounds first
    if p < _MIN_PRICE:
        p = _MIN_PRICE
    elif p > _MAX_PRICE:
        p = _MAX_PRICE
    # snap if function exists
    if _tick_snap is not None:
        try:
            p = float(_tick_snap(p))
        except Exception:
            pass
    return p


@dataclass(frozen=True)
class QuoteIntent:
    # Wire schema version (freeze seam). Increment on breaking changes.
    v: int = 1
    # Identity
    intent_id: str
    market_id: str
    selection_id: int

    # Action
    side: Side
    price: float
    size: float
    tif: TIF
    persistence: Persistence

    # Hygiene & pacing (executor may enforce stricter)
    min_lifetime_ms: int               # don't churn price before this age
    max_replace_rate_per_min: int      # soft limiter

    # Attribution / risk
    edge_contribs: List[Dict[str, Any]]  # [{edge_id, weight, ev_net_ticks}]
    risk_tags: Dict[str, Any]            # {profile, region, cap_bucket, ...}

    # Client clocks
    client_ts_ms: int

    # Optional expiry for GTD (milliseconds since epoch)
    expiry_ts_ms: Optional[int] = None

    # Optional flags (future-proofing)
    flags: Dict[str, Any] = None

    # -------- Bundles (NEW) --------
    bundle_id: Optional[str] = None
    bundle_policy: Optional[BundlePolicy] = None  # "ALL_OR_CANCEL" | "BEST_EFFORT"

    # ---------- Wire helpers ---------- #

    def to_wire(self) -> Dict[str, Any]:
        """
        JSON-serialisable dict. Keep keys/shape stable.
        """
        d = asdict(self)
        d["v"] = int(self.v)
        # Ensure canonical ENUM strings
        d["side"] = str(self.side)
        d["tif"] = str(self.tif)
        d["persistence"] = str(self.persistence)
        if d.get("bundle_policy") is not None:
            d["bundle_policy"] = str(d["bundle_policy"])

        # None → omit optional fields
        if d.get("expiry_ts_ms") is None:
            d.pop("expiry_ts_ms", None)
        if not d.get("flags"):
            d.pop("flags", None)
        if not d.get("bundle_id"):
            d.pop("bundle_id", None)
        if not d.get("bundle_policy"):
            d.pop("bundle_policy", None)
        return d

    @staticmethod
    def from_wire(d: Dict[str, Any]) -> "QuoteIntent":
        """
        Construct from a dict (e.g., JSON decoded).
        """
        bp = d.get("bundle_policy")
        bp_norm: Optional[BundlePolicy] = None
        if bp is not None:
            s = str(bp).upper()
            if s in ("ALL_OR_CANCEL", "BEST_EFFORT"):
                bp_norm = s  # type: ignore[assignment]

        return QuoteIntent(
            v=int(d.get("v", 1)),
            intent_id=str(d["intent_id"]),
            market_id=str(d["market_id"]),
            selection_id=int(d["selection_id"]),
            side=str(d["side"]).upper(),               # type: ignore[arg-type]
            price=float(d["price"]),
            size=float(d["size"]),
            tif=str(d["tif"]).upper(),                 # type: ignore[arg-type]
            persistence=str(d["persistence"]).upper(), # type: ignore[arg-type]
            min_lifetime_ms=int(d["min_lifetime_ms"]),
            max_replace_rate_per_min=int(d["max_replace_rate_per_min"]),
            edge_contribs=list(d.get("edge_contribs", [])),
            risk_tags=dict(d.get("risk_tags", {})),
            client_ts_ms=int(d.get("client_ts_ms", now_ms())),
            expiry_ts_ms=int(d["expiry_ts_ms"]) if d.get("expiry_ts_ms") is not None else None,
            flags=dict(d["flags"]) if d.get("flags") else None,
            bundle_id=str(d["bundle_id"]) if d.get("bundle_id") else None,
            bundle_policy=bp_norm,
        )

    # ---------- Price hygiene helper (opt-in) ---------- #

    def snapped(self) -> "QuoteIntent":
        """
        Return a NEW QuoteIntent with price clamped to bounds and snapped to a valid tick if possible.
        Does not mutate the original (frozen dataclass).
        """
        try:
            p = snapped_price(self.price)
            if p == self.price:
                return self
            return replace(self, price=p)
        except Exception:
            return self

