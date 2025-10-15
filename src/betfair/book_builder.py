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
    MarketState,
    OrderBookSnapshot,
    RunnerBook,
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

        # --- Queue-aware (read-only) signals (EMA-based), per (market, selection) ---
        self._qa_last_traded_sum: Dict[Tuple[str,int], Tuple[float,int]] = {}
        self._qa_match_rate_ema: Dict[Tuple[str,int], float] = {}
        self._qa_last_traded_ts: Dict[Tuple[str,int], int] = {}
        # Tunables (safe defaults; not required in config)
        self._qa_window_ms: int = 5000
        self._qa_eps_size: float = 0.01
        self._qa_max_ttf_ms: int = 15000

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
                mid = str(b.get("marketId") or b.get("market_id") or "")
                if not mid:
                    continue
                snap = self._markets.get(mid)
                if not snap:
                    snap = OrderBookSnapshot(market=MarketState(market_id=mid))
                    self._markets[mid] = snap

                # Optional marketDefinition
                md = b.get("marketDefinition") or b.get("market_def") or {}
                st = snap.market
                st.status = md.get("status", st.status)
                st.in_play = bool(md.get("inPlay", st.in_play))
                st.bet_delay = md.get("betDelay", st.bet_delay)
                st.off_dt = md.get("marketTime", st.off_dt)
                st.suspend_reason = md.get("suspendReason", st.suspend_reason)
                st.bet_delay_models = md.get("betDelayModels", st.bet_delay_models)

                # MBR if present
                mbr_raw = md.get("marketBaseRate") or md.get("market_base_rate")
                if mbr_raw is not None:
                    try:
                        mbr_pct = float(mbr_raw)
                        if mbr_pct > 1.0:
                            mbr_pct = mbr_pct / 100.0
                        self._update_mbr(mid, mbr_pct, source="book")
                    except Exception:
                        pass

                # Runners
                for r in b.get("runners", []) or []:
                    sel = int(r.get("selectionId") or r.get("selection_id") or 0)
                    if not sel:
                        continue
                    rb = snap.runners.get(sel)
                    if not rb:
                        rb = RunnerBook(selection_id=sel)
                        snap.runners[sel] = rb

                    # DISPLAY virt best
                    vb = _levels(r.get("ex", {}).get("availableToBack"))
                    vl = _levels(r.get("ex", {}).get("availableToLay"))
                    if vb is not None:
                        rb.best_back_disp = vb
                        rb.best_back = vb
                    if vl is not None:
                        rb.best_lay_disp = vl
                        rb.best_lay = vl

                    # RAW best (if present in REST seed)
                    raw_b = _levels(r.get("raw", {}).get("availableToBack"))
                    raw_l = _levels(r.get("raw", {}).get("availableToLay"))
                    if raw_b is not None:
                        rb.best_back_raw = raw_b
                    if raw_l is not None:
                        rb.best_lay_raw = raw_l

                    # Traded by price
                    t = _levels((r.get("ex") or {}).get("tradedVolume"))
                    if t is not None:
                        rb.traded_by_price = {lv.price: lv.size for lv in t}

                    rb.raw_depth_best2 = _depth_best2(rb.best_back_raw, rb.best_lay_raw)

                # Freshness markers
                self._last_reseed_ms[mid] = now_ms
                self._fresh_levels[mid] = 3
                count += 1
            except Exception:
                continue
        return count

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

            st = snap.market
            st.status = md.get("status", st.status)
            st.in_play = bool(md.get("in_play", st.in_play))
            st.bet_delay = md.get("bet_delay", st.bet_delay)
            st.off_dt = md.get("off_dt", st.off_dt)
            st.suspend_reason = md.get("suspend_reason", st.suspend_reason)
            st.bet_delay_models = md.get("bet_delay_models", st.bet_delay_models)

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
            # Queue-aware: update traded sum EMA + last traded ts
            try:
                key = (mid, sel)
            except NameError:
                key = (mid, int(delta.get("selection_id")))
            try:
                now_ms = int(time.time() * 1000)
                total_traded = float(sum(rb.traded_by_price.values()))
                prev_sum, prev_ts = self._qa_last_traded_sum.get(key, (total_traded, now_ms))
                delta_sz = max(0.0, total_traded - prev_sum)
                dt_ms = max(1, now_ms - prev_ts)
                inst_rate = (delta_sz / (dt_ms / 1000.0)) if dt_ms > 0 else 0.0
                # EMA with adaptive alpha based on dt and window
                alpha = 1.0 - (2.718281828459045 ** (-(dt_ms / max(1.0, float(self._qa_window_ms)))))
                ema_prev = self._qa_match_rate_ema.get(key, 0.0)
                ema = (alpha * inst_rate) + ((1.0 - alpha) * ema_prev)
                self._qa_match_rate_ema[key] = float(max(0.0, ema))
                self._qa_last_traded_sum[key] = (total_traded, now_ms)
                if delta_sz > 0.0:
                    self._qa_last_traded_ts[key] = now_ms
            except Exception:
                pass

        # Derived per-runner
        rb.raw_depth_best2 = _depth_best2(rb.best_back_raw, rb.best_lay_raw)

        # Snapshot-wide spreads/micro-mid (DISPLAY + RAW) — any runner’s best is ok for these coarse metrics
        snap.micro_mid = _micro_mid(rb.best_back_disp, rb.best_lay_disp)
        snap.micro_mid_raw = _micro_mid(rb.best_back_raw, rb.best_lay_raw)
        snap.spread_ticks = _spread_ticks(rb.best_back_disp, rb.best_lay_disp)
        snap.spread_ticks_raw = _spread_ticks(rb.best_back_raw, rb.best_lay_raw)

    def get_snapshot(self, market_id: str) -> Optional[OrderBookSnapshot]:
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

    # --------- NEW: Queue-aware read-only helpers --------- #

    def size_ahead_at(self, market_id: str, selection_id: int, side: str, price: float, *, use_raw: bool = True) -> float:
        """
        Return the size currently queued ahead of us at `price` on `side`.
        - If quoting inside the spread (no level at that price), returns 0.0
        - If price matches the current best price on that side, returns size at that level
        - Else (deeper or off-level), returns 0.0 conservatively
        """
        snap = self._markets.get(str(market_id))
        if not snap:
            return 0.0
        rb = snap.runners.get(int(selection_id))
        if not rb:
            return 0.0
        side = str(side).upper()
        ladder = (rb.best_back_raw if use_raw else rb.best_back) if side == "BACK" else (rb.best_lay_raw if use_raw else rb.best_lay)
        if not ladder:
            return 0.0
        # exact level match?
        for lv in ladder:
            if float(lv.price) == float(price):
                return float(max(0.0, lv.size))
        # If we're inside spread (better than current best), there is no queue ahead yet
        try:
            best_price = ladder[0].price
            # BACK inside-spread: price > best_back; LAY inside-spread: price < best_lay
            if (side == "BACK" and float(price) > float(best_price)) or (side == "LAY" and float(price) < float(best_price)):
                return 0.0
        except Exception:
            pass
        # Otherwise (off-level), be conservative: 0.0 until we have precise depth at that tick
        return 0.0

    def match_rate_back_ema(self, market_id: str, selection_id: int) -> float:
        """EMA of matched size/sec for the runner (used for BACK estimates; symmetric here)."""
        return float(self._qa_match_rate_ema.get((str(market_id), int(selection_id)), 0.0))

    def match_rate_lay_ema(self, market_id: str, selection_id: int) -> float:
        """EMA of matched size/sec for the runner (used for LAY estimates; symmetric here)."""
        return float(self._qa_match_rate_ema.get((str(market_id), int(selection_id)), 0.0))

    def p_fill_at(self, market_id: str, selection_id: int, side: str, price: float, size: float) -> float:
        """
        Heuristic probability of fill over the default window based on queue and EMA match rate.
        Uses a simple exponential decay over expected time-to-fill.
        """
        ttf = self.ttf_ms_at(market_id, selection_id, side, price, size)
        horizon_ms = float(self._qa_window_ms)
        if ttf <= 0:
            return 1.0
        # p ≈ exp(- ttf / horizon)
        import math as _math
        p = _math.exp(- float(ttf) / max(1.0, horizon_ms))
        return float(min(1.0, max(0.0, p)))

    def ttf_ms_at(self, market_id: str, selection_id: int, side: str, price: float, size: float) -> int:
        """
        Expected time-to-fill (ms) ≈ (size_ahead + size) / match_rate_ema.
        Capped to max_ttf_ms; returns max_ttf_ms when insufficient data.
        """
        ahead = self.size_ahead_at(market_id, selection_id, side, price)
        rate = self._qa_match_rate_ema.get((str(market_id), int(selection_id)), 0.0)
        eps = float(self._qa_eps_size)
        if rate <= 0.0:
            return int(self._qa_max_ttf_ms)
        ttf_s = max(0.0, (ahead + max(0.0, float(size))) / max(eps, float(rate)))
        ttf_ms = int(min(self._qa_max_ttf_ms, ttf_s * 1000.0))
        return ttf_ms

    def last_traded_ts_ms(self, market_id: str, selection_id: int) -> Optional[int]:
        """Timestamp (ms) of the last observed trade for the runner, if any."""
        return self._qa_last_traded_ts.get((str(market_id), int(selection_id)))


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
