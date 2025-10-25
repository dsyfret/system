"""
Location : src/strategy/edges/maker.py
Purpose  : Propose inside-spread maker quotes with strict hygiene.
Config   : configs/edges.yaml -> edges.maker.*
Inputs   : StrategyContext (ctx) with snapshot, cfg, book, now_ms, etc.
Outputs  : list[EdgeProposal]
"""

from __future__ import annotations
from typing import List, Dict, Any, Optional

from ...core.schemas import EdgeProposal
from ...betfair.ticks import one_tick_down, one_tick_up, distance_in_ticks
from ..context import StrategyContext


def run(ctx: StrategyContext) -> List[EdgeProposal]:
    out: List[EdgeProposal] = []

    snapshot = ctx.snapshot
    cfg: Dict[str, Any] = dict(ctx.cfg or {})
    now_ms = int(ctx.now_ms)

    if not snapshot or not snapshot.market or not snapshot.runners:
        return out

    overlays = (cfg.get("overlays") or {}) if isinstance(cfg, dict) else {}

    # --- stacking overlay knobs ---
    stacking_cfg = overlays.get("stacking") or {}
    stacking_enabled = bool(stacking_cfg.get("enabled", False))
    stack_levels = int(stacking_cfg.get("levels", 0))
    stack_tick_spacing = int(stacking_cfg.get("tick_spacing", 1))
    stack_size_mult = float(stacking_cfg.get("size_mult", 0.6))
    stack_min_raw_best2 = float(stacking_cfg.get("min_raw_best2_sum", 300.0))
    stack_max_spread = stacking_cfg.get("max_spread_ticks", None)
    stack_max_spread = int(stack_max_spread) if stack_max_spread is not None else None
    stack_ttl_bump_ms = int(stacking_cfg.get("ttl_bump_ms", 0))

    # --- queue-aware overlay knobs (optional) ---
    qa_cfg = overlays.get("queue_aware") or {}
    qa_enabled = bool(qa_cfg.get("enabled", False))
    qa_min_p_fill = float(qa_cfg.get("min_p_fill", 0.0))
    qa_max_ttf_ms = int(qa_cfg.get("max_ttf_ms", 999999))

    sel_ids = [ctx.selection_id] if ctx.selection_id is not None else list(snapshot.runners.keys())

    for sel in sel_ids:
        rb = snapshot.runners.get(sel)
        if not rb:
            continue
        if not rb.best_back or not rb.best_lay:
            continue

        v_back = float(rb.best_back[0].price)
        v_lay  = float(rb.best_lay[0].price)
        sp_ticks = distance_in_ticks(v_back, v_lay)
        if sp_ticks is None:
            continue

        min_spread_ticks = int(cfg.get("min_spread_ticks", 2))
        if sp_ticks < min_spread_ticks:
            continue

        raw_depth_best2 = getattr(rb, "raw_depth_best2", None)
        if raw_depth_best2 is None:
            raw_depth_best2 = _visible_depth_best2_display(rb)
        min_depth_best2 = float(cfg.get("min_depth_best2", 200.0))
        if float(raw_depth_best2) < min_depth_best2:
            continue

        ttl_ms = int(cfg.get("min_lifetime_ms", 2000))
        off_ms = getattr(snapshot.market, "off_dt", None)
        if off_ms:
            mins_to_off = (int(off_ms) - now_ms) / 60000.0
            if -5.0 <= mins_to_off <= 0:
                ttl_ms = max(1000, int(ttl_ms * 0.75))

        raw_back_depth1 = _depth_at_level(rb, side="BACK", levels=1, prefer_raw=True)
        raw_lay_depth1  = _depth_at_level(rb, side="LAY",  levels=1, prefer_raw=True)
        thin_raw_back = raw_back_depth1 < max(1.0, min_depth_best2 * 0.25)
        thin_raw_lay  = raw_lay_depth1  < max(1.0, min_depth_best2 * 0.25)

        base_ev_ticks = float(cfg.get("ev_assume_ticks", 0.8))
        ev_boost = float(cfg.get("ev_divergence_boost", 0.2))
        weight_base = 1.0
        default_size = float(cfg.get("default_size", 2.0))

        primary_prices = {"BACK": None, "LAY": None}

        # BACK (one tick inside best lay)
        try:
            target_back = one_tick_down(v_lay)
        except Exception:
            target_back = None
        if target_back and target_back < v_lay:
            ev_b = base_ev_ticks + (ev_boost if thin_raw_lay else 0.0)
            ep_b = EdgeProposal(
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
            )
            if _queue_keep(ep_b, qa_enabled, ctx.book, qa_min_p_fill, qa_max_ttf_ms):
                out.append(ep_b)
                primary_prices["BACK"] = float(target_back)

        # LAY (one tick inside best back)
        try:
            target_lay = one_tick_up(v_back)
        except Exception:
            target_lay = None
        if target_lay and target_lay > v_back:
            ev_l = base_ev_ticks + (ev_boost if thin_raw_back else 0.0)
            ep_l = EdgeProposal(
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
            )
            if _queue_keep(ep_l, qa_enabled, ctx.book, qa_min_p_fill, qa_max_ttf_ms):
                out.append(ep_l)
                primary_prices["LAY"] = float(target_lay)

        # Overlay: stacking
        if stacking_enabled and stack_levels > 0:
            if stack_max_spread is not None and sp_ticks > stack_max_spread:
                pass
            elif raw_depth_best2 is None or float(raw_depth_best2) < float(stack_min_raw_best2):
                pass
            else:
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
                        ttl_each = ttl_ms + (lvl * max(0, stack_ttl_bump_ms))
                        ep_s = EdgeProposal(
                            edge_id=f"maker_{side.lower()}_inside_stack{lvl}",
                            market_id=snapshot.market.market_id,
                            selection_id=sel,
                            side=side,
                            price=float(px),
                            size_hint=float(size),
                            ttl_ms=int(ttl_each),
                            ev_net_ticks=base_ev_ticks,
                            weight=weight_base * (0.95 ** lvl),
                            rationale=_why_maker(
                                f"{side}_stack{lvl}",
                                sp_ticks,
                                raw_depth_best2,
                                thin_raw_lay if side == "BACK" else thin_raw_back,
                            ),
                        )
                        if _queue_keep(ep_s, qa_enabled, ctx.book, qa_min_p_fill, qa_max_ttf_ms):
                            out.append(ep_s)

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


def _queue_keep(ep: Any, qa_enabled: bool, book: Any, min_p_fill: float, max_ttf_ms: int) -> bool:
    if not qa_enabled or book is None:
        return True
    try:
        mid = getattr(ep, "market_id")
        sel = int(getattr(ep, "selection_id"))
        side = str(getattr(ep, "side")).upper()
        price = float(getattr(ep, "price"))
        size = float(getattr(ep, "size_hint", 0.0) or 0.0)

        p_fill = float(book.p_fill_at(mid, sel, side, price, size))
        if p_fill < float(min_p_fill):
            return False
        ttf_ms = int(book.ttf_ms_at(mid, sel, side, price, size))
        if ttf_ms > int(max_ttf_ms):
            return False
        return True
    except Exception:
        return True
