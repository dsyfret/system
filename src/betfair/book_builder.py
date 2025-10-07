# src/betfair/book_builder.py
"""
Location : src/betfair/book_builder.py
Purpose  : Maintain canonical in-memory market books and key derived metrics.
           - Supports DISPLAY (virtualised) and RAW ladders (best N)
           - Tracks per-market metadata like suspend_reason and bet_delay_models
           - Produces micro-mid/spreads and best-2 RAW depth per runner
           - (NEW) Tracks per-market MBR (commission): pct, source, updated_ts

Notes:
- BookBuilder.apply_delta(...) accepts a normalized delta dict from streaming or REST seed helpers.
- Market-level "market_def" deltas update MarketState (status/in_play/bet_delay/etc).
- Runner-level deltas expect {"display": {...}, "raw": {...}, "img": bool} like streaming.
"""

from __future__ import annotations
from typing import Dict, List, Optional, Tuple
import time

from ..core.schemas import (
    Level,
    RunnerBook,
    MarketState,
    OrderBookSnapshot,
)
from ..betfair.ticks import distance_in_ticks


class BookBuilder:
    def __init__(self) -> None:
        self._markets: Dict[str, OrderBookSnapshot] = {}
        self._last_reseed_ms: Dict[str, int] = {}
        self._fresh_levels: Dict[str, int] = {}

        # NEW: per-market MBR storage (pct is decimal fraction, e.g. 0.05 for 5%)
        self._mbr_pct: Dict[str, float] = {}
        self._mbr_source: Dict[str, str] = {}
        self._mbr_updated_ts: Dict[str, int] = {}

    # --------- Public API --------- #
    def reseed_from_snapshot(self, books: List[Dict]) -> int:
        """Merge REST listMarketBook results into current books (idempotent); returns markets reseeded.
        Expects each item to include 'marketId' and 'runners' with availableToBack/Lay, and optionally 'marketDefinition'.
        Sets freshness flags for levels up to 3.
        """
        if not books:
            return 0
        count = 0
        now_ms = int(time.time() * 1000)
        for b in books:
            try:
                mid = str(b.get("marketId") or b.get("market_id"))
                if not mid:
                    continue
                # Market-level def
                md = b.get("marketDefinition") or {}
                try:
                    # MBR capture from REST MarketBook.marketDefinition.marketBaseRate
                    mbr_raw = md.get("marketBaseRate")
                    if mbr_raw is not None:
                        mbr_pct = float(mbr_raw)
                        # Betfair often returns percent (e.g., 5.0) -> convert to fraction
                        if mbr_pct > 1.0:
                            mbr_pct = mbr_pct / 100.0
                        self._update_mbr(mid, mbr_pct, source="book")

                    mdef = {
                        "in_play": bool(md.get("inPlay", md.get("in_play", False))),
                        "status": md.get("status") or ("OPEN" if not md.get("suspendTime") else None),
                        "bet_delay": int(md.get("betDelay", md.get("bet_delay", 0) or 0)),
                        "off_dt": int(md.get("marketTime", md.get("off_dt", 0) or 0)),
                        "suspend_reason": md.get("suspendReason") or None,
                    }
                    self.apply_delta({"market_id": mid, "market_def": mdef})
                except Exception:
                    pass
                # Runner ladders (use REST EX_BEST_OFFERS as both display & raw)
                for r in b.get("runners", []):
                    sel = int(r.get("selectionId") or r.get("selection_id") or 0)
                    if not sel:
                        continue
                    ex = r.get("ex", {}) or {}
                    backs = ex.get("availableToBack", []) or []
                    lays = ex.get("availableToLay", []) or []
                    disp = {
                        "back": [{"price": float(x.get("price")), "size": float(x.get("size"))} for x in backs[:3]],
                        "lay":  [{"price": float(x.get("price")), "size": float(x.get("size"))} for x in lays[:3]],
                    }
                    raw = disp  # lacking separate RAW from REST; acceptable for reseed
                    self.apply_delta({
                        "market_id": mid,
                        "selection_id": sel,
                        "img": True,
                        "display": disp,
                        "raw": raw,
                    })
                self._last_reseed_ms[mid] = now_ms
                self._fresh_levels[mid] = 3
                count += 1
            except Exception:
                continue
        return count

    def set_market_metadata(
        self,
        market_id: str,
        suspend_reason=None,
        bet_delay_models=None,
        bet_delay=None,
        bsp_available=None,
        *,
        market_base_rate: Optional[float] = None,
        mbr_source: Optional[str] = None,
    ) -> None:
        """
        Update MarketState metadata fields for a market. Safe if snapshot doesn't exist.
        Only non-None values are applied.
        (NEW) Optionally update per-market MBR via market_base_rate (fraction) + mbr_source.
        """
        snap = self._markets.get(market_id)
        if not snap:
            snap = OrderBookSnapshot(market=MarketState(market_id=market_id))
            self._markets[market_id] = snap

        if bet_delay is not None:
            try:
                snap.market.bet_delay = int(bet_delay)
            except Exception:
                pass
        if suspend_reason is not None:
            snap.market.suspend_reason = str(suspend_reason)
        if bet_delay_models is not None:
            try:
                snap.market.bet_delay_models = bet_delay_models
            except Exception:
                pass
        if bsp_available is not None:
            try:
                snap.market.bsp_available = bool(bsp_available)
            except Exception:
                pass

        # NEW: set MBR if provided
        if market_base_rate is not None:
            try:
                mbr_pct = float(market_base_rate)
                if mbr_pct > 1.0:
                    mbr_pct = mbr_pct / 100.0
                self._update_mbr(market_id, mbr_pct, source=str(mbr_source or "catalogue"))
            except Exception:
                pass

    def apply_delta(self, delta: Dict) -> None:
        mid = delta["market_id"]
        snap = self._markets.get(mid)
        if not snap:
            snap = OrderBookSnapshot(market=MarketState(market_id=mid))
            self._markets[mid] = snap

        # Market definition updates (in_play/status/bet_delay/off_dt/rfs/extra)
        if "market_def" in delta:
            md = delta["market_def"] or {}
            # NEW: MBR capture via stream normalization (if provided)
            mbr_raw = md.get("market_base_rate") or md.get("marketBaseRate")
            if mbr_raw is not None:
                try:
                    mbr_pct = float(mbr_raw)
                    if mbr_pct > 1.0:
                        mbr_pct = mbr_pct / 100.0
                    self._update_mbr(mid, mbr_pct, source="stream")
                except Exception:
                    pass

            snap.market.in_play = bool(md.get("in_play", snap.market.in_play))
            status = md.get("status")
            if status:
                snap.market.status = status
            if md.get("off_dt") is not None:
                snap.market.off_dt = int(md["off_dt"])
            if md.get("bet_delay") is not None:
                snap.market.bet_delay = int(md["bet_delay"])
            if md.get("suspend_reason") is not None:
                snap.market.suspend_reason = md["suspend_reason"]
            # optional aux fields: bet_delay_models, bsp_available, etc. are set via set_market_metadata

        # Runner price/trade updates
        sel = int(delta["selection_id"])
        rb = snap.runners.get(sel)
        if not rb:
            rb = RunnerBook(selection_id=sel)
            snap.runners[sel] = rb

        if delta.get("img"):
            # reset ladders on img (streaming-style)
            rb.best_back = []
            rb.best_lay = []
            rb.best_back_disp = []
            rb.best_lay_disp = []
            rb.best_back_raw = []
            rb.best_lay_raw = []
            rb.traded_by_price = {}

        # DISPLAY best
        disp_back = _levels(delta.get("display", {}).get("back"))
        disp_lay  = _levels(delta.get("display", {}).get("lay"))
        if disp_back is not None:
            rb.best_back_disp = disp_back
            rb.best_back = disp_back
        if disp_lay is not None:
            rb.best_lay_disp = disp_lay
            rb.best_lay = disp_lay

        # RAW best
        raw_back = _levels(delta.get("raw", {}).get("back"))
        raw_lay  = _levels(delta.get("raw", {}).get("lay"))
        if raw_back is not None:
            rb.best_back_raw = raw_back
        if raw_lay is not None:
            rb.best_lay_raw = raw_lay

        # Traded by price (optional)
        traded = _levels(delta.get("traded"))
        if traded is not None:
            rb.traded_by_price = {lv.price: lv.size for lv in traded}

        # Derived per-runner
        rb.raw_depth_best2 = _depth_best2(rb.best_back_raw, rb.best_lay_raw)

        # Snapshot-wide spreads/micro-mid (DISPLAY + RAW) — any runner’s best is ok for these coarse metrics
        snap.micro_mid = _micro_mid(rb.best_back_disp, rb.best_lay_disp)
        snap.micro_mid_raw = _micro_mid(rb.best_back_raw, rb.best_lay_raw)
        snap.spread_ticks = _spread_ticks(rb.best_back_disp, rb.best_lay_disp)
        snap.spread_ticks_raw = _spread_ticks(rb.best_back_raw, rb.best_lay_raw)

    def snapshot(self, market_id: str) -> Optional[OrderBookSnapshot]:
        return self._markets.get(market_id)

    def all_snapshots(self) -> Dict[str, OrderBookSnapshot]:
        return self._markets

    # --------- NEW: per-market MBR API --------- #

    def _update_mbr(self, market_id: str, pct: float, *, source: str) -> None:
        self._mbr_pct[market_id] = float(pct)
        self._mbr_source[market_id] = str(source)
        self._mbr_updated_ts[market_id] = int(time.time() * 1000)

    def get_mbr(self, market_id: str) -> Tuple[Optional[float], Optional[str]]:
        """
        Return (pct, source) if known; pct is decimal fraction (0.05 = 5%), source is one of: 'stream','book','catalogue'.
        Unknown -> (None, None).
        """
        return self._mbr_pct.get(market_id), self._mbr_source.get(market_id)


# --------- helpers --------- #

def _levels(levels_like) -> Optional[List[Level]]:
    if levels_like is None:
        return None
    out: List[Level] = []
    for d in levels_like:
        try:
            p = float(d.get("price", 0.0))
            s = float(d.get("size", 0.0))
            if p > 0 and s >= 0:
                out.append(Level(price=p, size=s))
        except Exception:
            continue
    return out

def _depth_best2(back: List[Level], lay: List[Level]) -> Optional[float]:
    if not back or not lay:
        return None
    b = sum(lv.size for lv in back[:2])
    l = sum(lv.size for lv in lay[:2])
    return float(b + l)

def _micro_mid(back: List[Level], lay: List[Level]) -> Optional[float]:
    if not back or not lay:
        return None
    return 0.5 * (back[0].price + lay[0].price)

def _spread_ticks(back: List[Level], lay: List[Level]) -> Optional[int]:
    if not back or not lay:
        return None
    return distance_in_ticks(back[0].price, lay[0].price)
