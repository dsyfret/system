# src/strategy/edges/maker.py
"""
Location : src/strategy/edges/maker.py
Purpose  : Propose inside-spread maker quotes with strict hygiene.
Plain    : Uses DISPLAY (virtualised) top-of-book for price correctness and RAW depth
           for queue sanity. TTL/min-lifetime and profile gates are enforced upstream
           by selector/router; this edge no longer performs its own in-play gating.
Config   : configs/edges.yaml -> edges.maker.*
Inputs   : snapshot (OrderBookSnapshot), selection_id (int or None to iterate all), cfg (edges.maker dict), now_ms (optional)
Outputs  : list[EdgeProposal-like dict]  (arbiter/trader will convert to QuoteIntent)
Notes    : - Returns 0–2 proposals per runner (one BACK, one LAY) in the base form.
           - Optional overlay "stacking" can add extra quotes deeper inside the spread.
           - Virtual–raw divergence boosts confidence slightly (weight/ev hint).
"""

from __future__ import annotations
from ...core.schemas import EdgeProposal
from typing import List, Dict, Any, Optional
import time

from ...betfair.ticks import one_tick_down, one_tick_up, distance_in_ticks


def propose(snapshot, selection_id: Optional[int], cfg: Dict[str, Any], now_ms: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Return proposed maker quotes. If selection_id is None, iterate over all runners.
    Overlay behaviour (toggleable via cfg['overlays']):
      - stacking.enabled: bool (default False). When True, emit up to N additional quotes per side,
        each further inside the spread by 'tick_spacing' ticks and sized by a decaying 'size_mult'^level.
        Config path:
            edges:
              maker:
                overlays:
                  stacking:
                    enabled: false
                    levels: 0           # number of extra levels per side (0 = off)
                    tick_spacing: 1     # ticks between stacked levels
                    size_mult: 0.6      # multiply size per deeper level
                    min_raw_best2_sum: 300.0  # require sufficient thickness
    """
    out: List[Dict[str, Any]] = []
    now_ms = now_ms or int(time.time() * 1000)

    if not snapshot or not snapshot.market or not snapshot.runners:
        return out

    # Overlay config (tolerant to older YAMLs)
    overlays = (cfg.get("overlays") or {}) if isinstance(cfg, dict) else {}
    stacking_cfg = overlays.get("stacking") or {}
    stacking_enabled = bool(stacking_cfg.get("enabled", False))
    stack_levels = int(stacking_cfg.get("levels", 0))
    stack_tick_spacing = int(stacking_cfg.get("tick_spacing", 1))
    stack_size_mult = float(stacking_cfg.get("size_mult", 0.6))
    stack_min_raw_best2 = float(stacking_cfg.get("min_raw_best2_sum", 300.0))

    sel_ids = [selection_id] if selection_id is not None else list(snapshot.runners.keys())

    for sel in sel_ids:
        rb = snapshot.runners.get(sel)
        if not rb:
            continue

        # Must have DISPLAY (virtualised) top-of-book on both sides
        if not rb.best_back or not rb.best_lay:
            continue

        # --- spread & depth checks ---
        v_back = float(rb.best_back[0].price)
        v_lay  = float(rb.best_lay[0].price)
        sp_ticks = distance_in_ticks(v_back, v_lay)
        if sp_ticks is None:
            continue

        min_spread_ticks = int(cfg.get("min_spread_ticks", 2))
        if sp_ticks < min_spread_ticks:
            continue

        # RAW depth best-2 (queue sanity). Fallback to DISPLAY if RAW missing.
        raw_depth_best2 = getattr(rb, "raw_depth_best2", None)
        if raw_depth_best2 is None:
            raw_depth_best2 = _visible_depth_best2_display(rb)
        min_depth_best2 = float(cfg.get("min_depth_best2", 200.0))
        if float(raw_depth_best2) < min_depth_best2:
            continue

        # --- time window awareness (optional gentle TTL tighten into off) ---
        ttl_ms = int(cfg.get("min_lifetime_ms", 2000))
        off_ms = getattr(snapshot.market, "off_dt", None)
        if off_ms:
            mins_to_off = (int(off_ms) - now_ms) / 60000.0
            if -5.0 <= mins_to_off <= 0:
                ttl_ms = max(1000, int(ttl_ms * 0.75))  # slightly shorter near the off

        # --- virtual–raw divergence heuristic (fill-probability booster) ---
        raw_back_depth1 = _depth_at_level(rb, side="BACK", levels=1, prefer_raw=True)
        raw_lay_depth1  = _depth_at_level(rb, side="LAY",  levels=1, prefer_raw=True)
        thin_raw_back = raw_back_depth1 < max(1.0, min_depth_best2 * 0.25)
        thin_raw_lay  = raw_lay_depth1  < max(1.0, min_depth_best2 * 0.25)

        base_ev_ticks = float(cfg.get("ev_assume_ticks", 0.8))  # conservative net ticks you aim to earn
        ev_boost = float(cfg.get("ev_divergence_boost", 0.2))   # small bump if divergence present
        weight_base = 1.0
        default_size = float(cfg.get("default_size", 2.0))

        # -------------------- Primary inside quotes -------------------- #
        primary_prices = {"BACK": None, "LAY": None}

        # BACK proposal (one tick inside best lay)
        try:
            target_back = one_tick_down(v_lay)
        except Exception:
            target_back = None
        if target_back and target_back < v_lay:
            ev_b = base_ev_ticks + (ev_boost if thin_raw_lay else 0.0)
            out.append(EdgeProposal(
                edge_id="maker_back_inside",
                market_id=snapshot.market.market_id,
                selection_id=sel,
                side="BACK",
                price=float(target_back),
                size_hint=default_size,
                ttl_ms=ttl_ms,
                ev_net_ticks=ev_b,
                weight=weight_base + (0.1 if thin_raw_lay else 0.0),
                rationale=_why_maker("BACK", sp_ticks, raw_depth_best2, thin_raw_lay),
            ))
            primary_prices["BACK"] = float(target_back)

        # LAY proposal (one tick inside best back)
        try:
            target_lay = one_tick_up(v_back)
        except Exception:
            target_lay = None
        if target_lay and target_lay > v_back:
            ev_l = base_ev_ticks + (ev_boost if thin_raw_back else 0.0)
            out.append(EdgeProposal(
                edge_id="maker_lay_inside",
                market_id=snapshot.market.market_id,
                selection_id=sel,
                side="LAY",
                price=float(target_lay),
                size_hint=default_size,
                ttl_ms=ttl_ms,
                ev_net_ticks=ev_l,
                weight=weight_base + (0.1 if thin_raw_back else 0.0),
                rationale=_why_maker("LAY", sp_ticks, raw_depth_best2, thin_raw_back),
            ))
            primary_prices["LAY"] = float(target_lay)

        # -------------------- Overlay: stacking (optional) -------------------- #
        if stacking_enabled and stack_levels > 0:
            # require a bit more thickness for stacking
            if raw_depth_best2 is None or float(raw_depth_best2) < float(stack_min_raw_best2):
                pass  # too thin; skip stacking
            else:
                # Generate deeper levels for each side where we emitted a primary quote
                for side in ("BACK", "LAY"):
                    base_px = primary_prices.get(side)
                    if base_px is None:
                        continue
                    for lvl in range(1, stack_levels + 1):
                        steps = lvl * max(1, stack_tick_spacing)
                        px = _shift_ticks(base_px, steps, side=side)
                        if px is None:
                            break
                        size = max(0.0, default_size * (stack_size_mult ** lvl))
                        if size <= 0.0:
                            break
                        out.append(EdgeProposal(
                            edge_id=f"maker_{side.lower()}_inside_stack{lvl}",
                            market_id=snapshot.market.market_id,
                            selection_id=sel,
                            side=side,
                            price=float(px),
                            size_hint=float(size),
                            ttl_ms=ttl_ms,
                            ev_net_ticks=base_ev_ticks,  # keep conservative
                            weight=weight_base * (0.95 ** lvl),
                            rationale=_why_maker(f\"{side}_stack{lvl}\", sp_ticks, raw_depth_best2, thin_raw_lay if side==\"BACK\" else thin_raw_back),
                        ))

    return out


# ------------------------- helpers -------------------------

def _visible_depth_best2_display(rb) -> float:
    b = sum((lv.size for lv in (rb.best_back[:2] if rb.best_back else [])), 0.0)
    l = sum((lv.size for lv in (rb.best_lay[:2] if rb.best_lay else [])), 0.0)
    return float(b + l)

def _depth_at_level(rb, *, side: str, levels: int = 1, prefer_raw: bool = True) -> float:
    side = side.upper()
    if side == "BACK":
        lvls = (getattr(rb, "best_back_raw", None) if prefer_raw else None) or getattr(rb, "best_back", [])  # type: ignore
    else:
        lvls = (getattr(rb, "best_lay_raw", None) if prefer_raw else None) or getattr(rb, "best_lay", [])   # type: ignore
    return float(sum((lv.size for lv in (lvls[:levels] if lvls else [])), 0.0))

def _why_maker(side: str, sp_ticks: int, raw_depth2: float, thin_raw_side: bool) -> str:
    parts = [
        f"inside_spread_{side}",
        f"spread_ticks={sp_ticks}",
        f"raw_best2={int(raw_depth2) if raw_depth2 is not None else -1}"
    ]
    if thin_raw_side:
        parts.append("virt_raw_divergence")
    return ";".join(parts)

def _shift_ticks(px: float, ticks: int, *, side: str) -> Optional[float]:
    """Shift price by N ticks deeper inside the spread for a given side."""
    try:
        if side.upper() == "BACK":
            out = px
            for _ in range(max(0, int(ticks))):
                nxt = one_tick_down(out)
                if nxt is None:
                    return None
                out = float(nxt)
            return out
        else:
            out = px
            for _ in range(max(0, int(ticks))):
                nxt = one_tick_up(out)
                if nxt is None:
                    return None
                out = float(nxt)
            return out
    except Exception:
        return None
