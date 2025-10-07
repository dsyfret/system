# src/strategy/helpers/cross_market.py
"""
Location : src/strategy/helpers/cross_market.py
Purpose  : WIN↔PLACE parity helpers. Production-ready for observer/live use.

API
- map_win_place(markets_index) -> dict[(win_mid, place_mid, runner_id)] = {"runner_name": str}
- observe_parity(win_book, place_book, runner_id) -> dict with keys:
    fresh: bool            # both books recent enough (heuristic)
    liq_ok: bool           # basic liquidity heuristic (best-2)
    delta_bps: float       # implied WIN from PLACE vs quoted WIN (basis points)
    details: dict          # raw components

Notes
- Tolerant to partial shapes (DISPLAY/RAW). No hard deps on your BookBuilder.
- Freshness heuristic defaults to lenient if timestamps missing.
- The PLACE→WIN conversion is a coarse proxy (Phase-1: replace with explicit mapping).
"""

from __future__ import annotations
from typing import Any, Dict, Tuple, Optional
import time
import logging

log = logging.getLogger(__name__)


def _now_ms() -> int:
    return int(time.time() * 1000)


def map_win_place(markets_index: Any) -> Dict[Tuple[str, str, int], Dict[str, Any]]:
    mapping: Dict[Tuple[str, str, int], Dict[str, Any]] = {}
    if not markets_index:
        return mapping
    try:
        wins = dict(getattr(markets_index, "WIN", {}) or markets_index.get("WIN", {}) or {})
        places = dict(getattr(markets_index, "PLACE", {}) or markets_index.get("PLACE", {}) or {})
    except Exception:
        return mapping

    for win_mid, w in wins.items():
        w_runners = (getattr(w, "runners", None) or w.get("runners", {}) or {})
        for place_mid, p in places.items():
            p_runners = (getattr(p, "runners", None) or p.get("runners", {}) or {})
            w_ids = set(w_runners.keys())
            p_ids = set(p_runners.keys())
            for rid in (w_ids & p_ids):
                try:
                    rname = (w_runners[rid].get("name")
                             or getattr(w_runners[rid], "name", None)
                             or p_runners[rid].get("name")
                             or getattr(p_runners[rid], "name", None)
                             or str(rid))
                except Exception:
                    rname = str(rid)
                mapping[(str(win_mid), str(place_mid), int(rid))] = {"runner_name": rname}
    return mapping


def _best_prices(book: Any) -> Optional[tuple[float, float]]:
    try:
        if getattr(book, "best_back_price", None) and getattr(book, "best_lay_price", None):
            return float(book.best_back_price), float(book.best_lay_price)
        best_back = None
        best_lay = None
        for rb in getattr(book, "runners", {}).values():
            bb = getattr(rb, "best_back", None)
            bl = getattr(rb, "best_lay", None)
            if bb and len(bb) > 0:
                p = float(getattr(bb[0], "price", 0.0) or 0.0)
                best_back = p if best_back is None else max(best_back, p)
            if bl and len(bl) > 0:
                p = float(getattr(bl[0], "price", 0.0) or 0.0)
                best_lay = p if best_lay is None else min(best_lay, p)
        if best_back and best_lay:
            return best_back, best_lay
    except Exception:
        pass
    return None


def observe_parity(win_book: Any, place_book: Any, runner_id: int) -> Dict[str, Any]:
    obs = {"fresh": False, "liq_ok": False, "delta_bps": 0.0, "details": {}}

    # Freshness (1s window if timestamps present)
    try:
        now = _now_ms()
        w_ts = int(getattr(win_book, "asof_ms", now))
        p_ts = int(getattr(place_book, "asof_ms", now))
        obs["fresh"] = (now - w_ts) <= 1000 and (now - p_ts) <= 1000
    except Exception:
        obs["fresh"] = True

    wp = _best_prices(win_book)
    pp = _best_prices(place_book)
    if not wp or not pp:
        obs["details"]["reason"] = "no_best_prices"
        return obs

    w_back, w_lay = wp
    p_back, p_lay = pp

    # Simple best-2 liquidity heuristic across both markets
    def _best2_sz(book: Any, side: str) -> float:
        total = 0.0
        for rb in getattr(book, "runners", {}).values():
            lvls = getattr(rb, f"best_{side}_raw", None) or getattr(rb, f"best_{side}", [])
            for lvl in lvls[:2]:
                total += float(getattr(lvl, "size", 0.0) or 0.0)
        return total

    try:
        liq_back = _best2_sz(win_book, "back") + _best2_sz(place_book, "back")
        liq_lay  = _best2_sz(win_book, "lay") + _best2_sz(place_book, "lay")
        obs["liq_ok"] = min(liq_back, liq_lay) >= 500.0
    except Exception:
        obs["liq_ok"] = False

    # Coarse probabilities via mid of odds
    def _prob(back: float, lay: float) -> float:
        q = (back + lay) / 2.0
        return 1.0 / q if q > 1e-9 else 0.0

    mid_win = _prob(w_back, w_lay)
    place_q = _prob(p_back, p_lay)
    implied_win_from_place = max(1e-9, place_q * 0.33)  # proxy; replace with explicit mapping in Phase-1

    delta = (implied_win_from_place - mid_win) * 10000.0  # basis points

    obs["delta_bps"] = float(delta)
    obs["details"] = {
        "runner_id": int(runner_id),
        "win_back": float(w_back),
        "win_lay": float(w_lay),
        "place_back": float(p_back),
        "place_lay": float(p_lay),
        "mid_win_prob": float(mid_win),
        "implied_win_from_place": float(implied_win_from_place),
    }
    return obs