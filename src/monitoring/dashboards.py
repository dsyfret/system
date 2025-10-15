# src/monitoring/dashboards.py
"""
Location : src/monitoring/dashboards.py
Purpose  : Minimal dashboard hooks—emit periodic summary logs, chartable by Prometheus/Grafana later.
Plain    : trader calls emit_summary(book, obs_counts, acks, order_manager, intents_last_minute)
"""

from __future__ import annotations
from typing import Dict, Any, Deque, Optional, Tuple, Iterable
from collections import deque

from ..core.logging import log_event
from ..core.metrics import (
    get_stream_reconnects_count,
    get_order_stream_reconnects_count,
    get_order_ws_age_s,
    get_clock_drift_s,
    # added: pacing and funds telemetry getters
    get_pacing_ttl_prevent_total,
    get_pacing_budget_trip_total,
    get_funds_mismatch_rel,
    get_funds_mismatch_total,
)


def _runner_depth_score(rb) -> float:
    """
    Conservative depth score for sampling:
    Prefer explicit raw_depth_best2 if present; else sum best-2 on whichever ladder we have.
    """
    try:
        if getattr(rb, "raw_depth_best2", None) is not None:
            return float(rb.raw_depth_best2 or 0.0)
    except Exception:
        pass
    score = 0.0
    try:
        backs = getattr(rb, "best_back_raw", None) or getattr(rb, "best_back", []) or []
        lays  = getattr(rb, "best_lay_raw",  None) or getattr(rb, "best_lay",  []) or []
        score = float(sum(getattr(x, "size", 0.0) or 0.0 for x in backs[:2]) +
                      sum(getattr(x, "size", 0.0) or 0.0 for x in lays[:2]))
    except Exception:
        score = 0.0
    return score


def _best_prices(rb) -> Tuple[Optional[float], Optional[float]]:
    """
    Return (best_back_price, best_lay_price) from RAW ladders if present, else DISPLAY.
    """
    try:
        backs = getattr(rb, "best_back_raw", None) or getattr(rb, "best_back", []) or []
        lays  = getattr(rb, "best_lay_raw",  None) or getattr(rb, "best_lay",  []) or []
        bb = float(backs[0].price) if backs else None
        bl = float(lays[0].price)  if lays  else None
        return bb, bl
    except Exception:
        return None, None


def _emit_queue_sample(book, *, top_n_per_market: int = 3) -> None:
    """
    Sample a few runners per market and emit queue-aware snapshots:
      - queue_match_rate_ema (per runner)
      - queue_ttf_ms at best BACK/LAY (size=1.0 reference)
      - queue_size_ahead at the best BACK/LAY price
    Cardinality kept in check by sampling top-N by depth.
    """
    try:
        all_snaps = getattr(book, "all_snapshots", None)
        if not callable(all_snaps):
            return
        snapshots = all_snaps() or {}
    except Exception:
        return

    for mid, snap in list(snapshots.items()):
        try:
            runners = getattr(snap, "runners", {}) or {}
            if not runners:
                continue
            # Sample top-N runners by depth score
            items = list(runners.items())
            items.sort(key=lambda kv: _runner_depth_score(kv[1]), reverse=True)
            for sel, rb in items[: max(0, int(top_n_per_market))]:
                try:
                    # Best prices
                    bb, bl = _best_prices(rb)

                    # Match-rate EMA (shared for both sides in current implementation)
                    try:
                        mr = float(book.match_rate_back_ema(str(mid), int(sel)))
                    except Exception:
                        mr = 0.0

                    # BACK side metrics at best back price
                    if bb is not None:
                        try:
                            sa_b = float(book.size_ahead_at(str(mid), int(sel), "BACK", float(bb)))
                        except Exception:
                            sa_b = 0.0
                        try:
                            ttf_b = int(book.ttf_ms_at(str(mid), int(sel), "BACK", float(bb), 1.0))
                        except Exception:
                            ttf_b = None
                        log_event(
                            "dash", "queue", "ok",
                            market_id=str(mid), selection_id=int(sel), side="BACK",
                            best_price=float(bb), size_ahead=sa_b, match_rate_ema=mr, ttf_ms=ttf_b
                        )

                    # LAY side metrics at best lay price
                    if bl is not None:
                        try:
                            sa_l = float(book.size_ahead_at(str(mid), int(sel), "LAY", float(bl)))
                        except Exception:
                            sa_l = 0.0
                        try:
                            ttf_l = int(book.ttf_ms_at(str(mid), int(sel), "LAY", float(bl), 1.0))
                        except Exception:
                            ttf_l = None
                        log_event(
                            "dash", "queue", "ok",
                            market_id=str(mid), selection_id=int(sel), side="LAY",
                            best_price=float(bl), size_ahead=sa_l, match_rate_ema=mr, ttf_ms=ttf_l
                        )
                except Exception:
                    # Never let a single runner break the loop
                    continue
        except Exception:
            continue


def emit_summary(book, obs_counts: Dict[str, int], acks: Any, order_manager: Any, intents_last_minute: Deque[int]) -> None:
    """
    Log a JSON summary snapshot once per second; scrape with Prometheus/Grafana.
    We keep it schema-light: new fields can be added safely.
    """
    try:
        active_markets = 0
        try:
            active_markets = len(getattr(book, "all_snapshots")() or {})
        except Exception:
            pass

        intents_per_min = 0
        try:
            intents_per_min = len(list(intents_last_minute))
        except Exception:
            pass

        fills_per_min = getattr(acks, "fills_last_minute", lambda: 0)()
        replace_rate = getattr(order_manager, "replace_rate_last_minute", lambda: 0.0)()
        fee_share = getattr(order_manager, "fee_share_last_minute", lambda: None)()

        payload = {
            "active_markets": active_markets,
            "intents_per_min": intents_per_min,
            "fills_per_min": fills_per_min,
            "replace_rate": replace_rate,
            "fee_share": fee_share,
            "obs_counts": dict(obs_counts or {}),
            # new observability
            "stream_reconnects_total": get_stream_reconnects_count(),
            "order_stream_reconnects_total": get_order_stream_reconnects_count(),
            "order_ws_last_activity_age_s": get_order_ws_age_s(),
            "clock_drift_s": get_clock_drift_s(),
            # pacing/TTL friction
            "pacing_ttl_prevent_total": get_pacing_ttl_prevent_total(),
            "pacing_budget_trip_total": get_pacing_budget_trip_total(),
            # funds mismatch
            "funds_mismatch_rel": get_funds_mismatch_rel(),
            "funds_mismatch_total": get_funds_mismatch_total(),
        }
        log_event("dash", "summary", "ok", **payload)

        # NEW: emit small queue-aware samples (best prices; top-3 runners per market)
        try:
            _emit_queue_sample(book, top_n_per_market=3)
        except Exception:
            # Don't let queue sampling affect the summary emission cadence
            pass

    except Exception:
        log_event("dash", "summary", "error")
