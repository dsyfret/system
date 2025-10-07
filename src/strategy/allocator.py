# src/strategy/allocator.py
"""
Location : src/strategy/allocator.py
Purpose  : Cap how many markets run "hot" concurrently and pick the best ones.
Plain    : Takes (market_id, score) from selector; returns up to max_hot markets.
Config   : betfair.max_concurrent_hot in base.yaml (profile.profile.max_hot optional override)
Inputs   : List of (market_id, score), optional congestion flag
Outputs  : List[str] of selected hot market_ids
Notes    : If the system is congested (SLO breach), we reduce capacity ~25%.
"""

from __future__ import annotations
from typing import List, Tuple

from ..core.config import load_config
from ..core.logging import log_event


class Allocator:
    def __init__(self, profile_name: str) -> None:
        snap = load_config(profile_name)
        self.profile_name = profile_name
        # capacity: profile override -> base fallback
        prof = getattr(snap, "profile", {}) or {}
        self.max_hot = int(prof.get("max_hot", getattr(snap, "betfair", {}).get("max_concurrent_hot", 12)))
        # adaptive multiplier (reduced when congested)
        self._capacity_mult = 1.0

    def set_congested(self, congested: bool) -> None:
        """Lower capacity when the system is under load; restore when healthy."""
        self._capacity_mult = 0.75 if congested else 1.0

    def allocate(self, ranked: List[Tuple[str, float]]) -> List[str]:
        """
        ranked: list of (market_id, score) in any order. We sort descending by score,
                then take up to capacity.
        """
        capacity = max(1, int(self.max_hot * self._capacity_mult))
        ranked = sorted(ranked, key=lambda x: x[1], reverse=True)
        chosen = [mid for mid, _ in ranked[:capacity]]
        log_event("allocator", "hot.allocate", "ok", capacity=capacity, chosen=len(chosen))
        return chosen