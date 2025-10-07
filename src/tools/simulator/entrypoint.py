# src/tools/simulator/entrypoint.py
"""
Add-only simulator adapter (SAFE: does not modify core system).
...
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional, Callable
import time

from ...betfair.book_builder import BookBuilder
from ...strategy.selector import MarketSelector
from ...strategy.arbiter import Arbiter
from ...strategy.sizing import Sizer
from ...execution.interfaces.intents import QuoteIntent

ProposalProvider = Callable[[BookBuilder, List[str], int], List[Any]]

def _now_ms() -> int:
    return int(time.time() * 1000)

class SimulatorApp:
    def __init__(self, profile: str, *, cfg_override: Optional[Dict[str, Any]] = None) -> None:
        self.profile = profile
        self.book = BookBuilder()
        self.selector = MarketSelector(profile)
        self.arbiter = Arbiter(profile, cfg_override=cfg_override or {})
        self.sizer = Sizer(profile, cfg_override=cfg_override or {})
        self._proposal_provider: Optional[ProposalProvider] = None

    def set_proposal_provider(self, provider: ProposalProvider) -> None:
        self._proposal_provider = provider

    def process_update(self, update: Dict[str, Any], ts_ms: Optional[int] = None) -> Dict[str, Any]:
        ts = int(ts_ms or _now_ms())
        affected: List[str] = []

        if "books" in update and isinstance(update["books"], list):
            try:
                self.book.reseed_from_snapshot(update["books"])
                for b in update["books"]:
                    mid = str(b.get("marketId") or b.get("market_id") or "")
                    if mid:
                        affected.append(mid)
            except Exception:
                pass
        else:
            try:
                mid = str(update.get("market_id", ""))
                if mid:
                    self.book.apply_delta(update)
                    affected.append(mid)
            except Exception:
                pass

        if not self._proposal_provider or not affected:
            return {"ts_ms": ts, "affected": affected, "plans": [], "intents": []}

        proposals = []
        try:
            proposals = list(self._proposal_provider(self.book, affected, ts))
        except Exception:
            proposals = []

        plans = self.arbiter.decide(proposals, snapshots=self.book, now_ms=ts)

        intents: List[QuoteIntent] = []
        for plan in plans:
            bankroll = float(getattr(plan, "bankroll_override", 10.0) or 10.0)
            intent = self.sizer.size_plan(plan, bankroll=bankroll, book=self.book)
            if intent is not None:
                intents.append(intent)

        return {
            "ts_ms": ts,
            "affected": affected,
            "plans": [getattr(p, "__dict__", dict(p)) for p in plans],
            "intents": [i.to_wire() if hasattr(i, "to_wire") else getattr(i, "__dict__", dict(i)) for i in intents],
        }
