# src/strategy/trend/engine.py
"""
Location : src/strategy/trend/engine.py
Purpose  : Shared TrendEngine (advisory/defensive). Production-ready.

Summary
- Computes lightweight, robust features per snapshot (slope, depth imbalance, burst).
- Suggests conservative modifiers (pause / size_mult / extra_min_life_ms) when enabled.
- Gateable via mode: "off" | "observer" | "live" (config-driven).
- Zero hard deps; tolerant to partial book shapes (DISPLAY/RAW).

Usage
    trend = TrendEngine(policy="thick", mode="observer", cfg=config.get("trend", {}))
    feats = trend.features(snapshot)
    mods  = trend.modifiers(feats)   # neutral unless mode == "live"
    intent = trend.annotate_intent(intent, feats)

Notes
- Keeps behaviour unchanged unless you set mode="live" and consume modifiers.
- All thresholds live in cfg so you can tune via YAML, not code.
"""

from __future__ import annotations
from typing import Any, Dict, Optional
import time
import logging

log = logging.getLogger(__name__)


def _now_ms() -> int:
    return int(time.time() * 1000)


class TrendEngine:
    """
    Advisory trend module with three modes:
      - "off":      compute features; return neutral modifiers
      - "observer": compute features; annotate intents; neutral modifiers
      - "live":     compute features; may suggest pause/size/min-life modifiers

    Policy hint:
      - "thick" vs "thin" only affects default windows; kept in metadata.
    """

    def __init__(self, policy: str = "thick", mode: str = "off", cfg: Optional[Dict[str, Any]] = None):
        if mode not in ("off", "observer", "live"):
            raise ValueError("mode must be off|observer|live")
        if policy not in ("thin", "thick"):
            raise ValueError("policy must be thin|thick")

        self.policy = policy
        self.mode = mode
        self.cfg = cfg or {}

        # Defaults (conservative; YAML can override)
        self.window_s: float = float(self.cfg.get("slope_window_s", 6 if policy == "thick" else 10))
        self.burst_window_s: float = float(self.cfg.get("burst_window_s", 1.0))
        self.burst_vs_baseline: float = float(self.cfg.get("burst_vs_baseline", 3.0))  # 1s vs 30s
        self.max_adverse_slope: float = float(self.cfg.get("max_adverse_slope", 0.0))  # ticks/sec; 0 disables
        self.cooloff_ms: int = int(self.cfg.get("cooloff_ms", 2000))
        self.size_scale_live: float = float(self.cfg.get("size_scale_live", 0.5))
        self.extra_min_life_ms_live: int = int(self.cfg.get("extra_min_life_ms_live", 750))

    def set_mode(self, mode: str) -> None:
        if mode not in ("off", "observer", "live"):
            raise ValueError("mode must be off|observer|live")
        self.mode = mode

    # ----------------------------- Features ----------------------------- #

    def features(self, snapshot: Any, *, now_ms: Optional[int] = None) -> Dict[str, Any]:
        """
        Compute robust features from a book snapshot.

        Returns:
          {
            "policy": "thin"|"thick",
            "slope_ticks_s": float,
            "depth_imbalance": float in [-1,+1],
            "burst_flag": bool,
            "micro_mid": float|None,
            "cooloff_until_ms": int
          }
        """
        now = now_ms or _now_ms()

        # Best-effort micro-mid from market-wide virtual touch OR runner ladders
        best_back, best_lay = None, None
        try:
            if getattr(snapshot, "best_back_price", None) and getattr(snapshot, "best_lay_price", None):
                best_back = float(snapshot.best_back_price)
                best_lay = float(snapshot.best_lay_price)
            else:
                # derive from runner ladders (max back, min lay)
                for rb in getattr(snapshot, "runners", {}).values():
                    bb = getattr(rb, "best_back", None)
                    bl = getattr(rb, "best_lay", None)
                    if bb and len(bb) > 0:
                        p = float(getattr(bb[0], "price", 0.0) or 0.0)
                        best_back = p if best_back is None else max(best_back, p)
                    if bl and len(bl) > 0:
                        p = float(getattr(bl[0], "price", 0.0) or 0.0)
                        best_lay = p if best_lay is None else min(best_lay, p)
        except Exception:
            pass

        micro_mid = None
        if best_back and best_lay:
            try:
                # micro-mid via harmonic mean of odds (more stable near extremes)
                micro_mid = (best_back * best_lay) / (best_back + best_lay - best_back * best_lay)
            except Exception:
                micro_mid = (best_back + best_lay) / 2.0

        # slope (ticks/sec) — neutral unless you wire a short history buffer on snapshot
        slope_ticks_s = 0.0
        try:
            hist = list(getattr(snapshot, "mid_hist", []) or [])
            if len(hist) >= 2:
                t2, p2 = hist[-1]
                cutoff = now - int(self.window_s * 1000)
                t1, p1 = None, None
                for t, p in hist:
                    if t >= cutoff:
                        t1, p1 = t, p
                        break
                if t1 is None:
                    t1, p1 = hist[0]
                dt_s = max(1e-3, (t2 - t1) / 1000.0)
                slope_ticks_s = (float(p2) - float(p1)) / dt_s
        except Exception:
            slope_ticks_s = 0.0

        # depth imbalance (best-2 per side across runners)
        back2 = lay2 = 0.0
        try:
            for rb in getattr(snapshot, "runners", {}).values():
                bb = (getattr(rb, "best_back_raw", None) or getattr(rb, "best_back", []))[:2]
                bl = (getattr(rb, "best_lay_raw", None) or getattr(rb, "best_lay", []))[:2]
                back2 += sum(float(getattr(x, "size", 0.0) or 0.0) for x in bb)
                lay2  += sum(float(getattr(x, "size", 0.0) or 0.0) for x in bl)
        except Exception:
            pass
        denom = max(1e-9, back2 + lay2)
        depth_imbalance = (back2 - lay2) / denom if denom > 0 else 0.0

        # burst vs 30s baseline (optional turnover counters on snapshot)
        v1 = float(getattr(snapshot, "turnover_1s", 0.0) or 0.0)
        v30 = float(getattr(snapshot, "turnover_30s", 0.0) or 0.0)
        burst_flag = (v1 > 0.0 and v30 > 0.0 and (v1 / max(1e-9, v30 / 30.0)) >= self.burst_vs_baseline)

        return {
            "policy": self.policy,
            "slope_ticks_s": float(slope_ticks_s),
            "depth_imbalance": float(depth_imbalance),
            "burst_flag": bool(burst_flag),
            "micro_mid": float(micro_mid) if micro_mid is not None else None,
            "cooloff_until_ms": 0,
        }

    # ---------------------------- Modifiers ------------------------------ #

    def modifiers(self, feats: Dict[str, Any]) -> Dict[str, Any]:
        """Return conservative modifiers based on features and current mode."""
        neutral = {"pause": False, "size_mult": 1.0, "extra_min_life_ms": 0}
        if self.mode in ("off", "observer"):
            return neutral

        adverse = (self.max_adverse_slope and feats.get("slope_ticks_s", 0.0) <= -abs(self.max_adverse_slope))
        burst = bool(feats.get("burst_flag", False))
        if adverse or burst:
            return {
                "pause": True,
                "size_mult": max(0.0, min(1.0, self.size_scale_live)),
                "extra_min_life_ms": max(0, self.extra_min_life_ms_live),
            }
        return neutral

    # ----------------------------- Metadata ------------------------------ #

    def annotate_intent(self, intent: Any, feats: Dict[str, Any]) -> Any:
        """Attach trend metadata to a QuoteIntent-like object (safe no-op on failure)."""
        try:
            meta = getattr(intent, "meta", {}) or {}
            meta["trend"] = {
                "mode": self.mode,
                "policy": self.policy,
                "slope_ticks_s": float(feats.get("slope_ticks_s", 0.0)),
                "depth_imbalance": float(feats.get("depth_imbalance", 0.0)),
                "burst_flag": bool(feats.get("burst_flag", False)),
            }
            setattr(intent, "meta", meta)
        except Exception:
            log.debug("trend.annotate_intent.failed", exc_info=True)
        return intent