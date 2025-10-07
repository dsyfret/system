# src/monitoring/dashboards.py
"""
Location : src/monitoring/dashboards.py
Purpose  : Minimal dashboard hooks—emit periodic summary logs, chartable by Prometheus/Grafana later.
Plain    : trader calls emit_summary(book, obs_counts, acks, order_manager, intents_last_minute)
"""

from __future__ import annotations
from typing import Dict, Any, Deque, Optional
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
    except Exception:
        log_event("dash", "summary", "error")
