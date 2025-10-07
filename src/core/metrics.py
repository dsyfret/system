# src/core/metrics.py
"""
Location : src/core/metrics.py
Purpose  : Prometheus metrics helpers; start HTTP endpoint when app boots.
Inputs   : --
Outputs  : module-level helpers: observe_latency, inc_counter, set_gauge
Notes    : Keep it tiny. Can add StatsD later if preferred.
"""
from __future__ import annotations
from typing import Optional, Tuple
from prometheus_client import Counter, Gauge, Histogram, start_http_server
import time

# -------------------- Generic helpers -------------------- #

_metric_lat = Histogram("bb_decision_ack_latency_ms", "Decision→ACK latency (ms)")
_metric_cnt = Counter("bb_events_total", "Generic event counter", ["event"])
_metric_gau = Gauge("bb_gauges", "Generic gauges", ["name"])

def start_metrics_server(port: int = 8008) -> None:
    try:
        start_http_server(port)
    except OSError:
        # already started
        pass

def observe_latency(ms: float) -> None:
    _metric_lat.observe(ms)

def inc(event: str, n: int = 1) -> None:
    _metric_cnt.labels(event=event).inc(n)

def set_gauge(name: str, val: float) -> None:
    _metric_gau.labels(name=name).set(val)

# -------------------- Focused metrics -------------------- #

_reseed_hist = Histogram("bb_reseed_latency_ms", "Reseed duration (ms)")
_stream_reconnects = Counter("bb_stream_reconnects_total", "Market stream reconnects (count)")
_order_stream_reconnects = Counter("bb_order_stream_reconnects_total", "Order stream reconnects (count)")
_reseed_runs = Counter("bb_reseed_runs_total", "Book reseed runs (count)")
_order_ws_age = Gauge("bb_order_ws_last_activity_age_s", "Order WS last-activity age (seconds)")
_clock_drift = Gauge("bb_clock_drift_s", "Clock drift estimate vs Betfair publish time (s)")

# In-process snapshots for Alerts/dashboards (cheap and cheerful)
_ts0 = time.time()
_stream_reconnects_val = 0
_order_stream_reconnects_val = 0
_order_ws_age_val: float = 0.0
_clock_drift_val: float = 0.0

def observe_reseed_latency(ms: float) -> None:
    _reseed_hist.observe(ms)

def inc_stream_reconnects(n: int = 1) -> None:
    global _stream_reconnects_val
    _stream_reconnects.inc(n)
    _stream_reconnects_val += int(n)

def inc_order_stream_reconnects(n: int = 1) -> None:
    global _order_stream_reconnects_val
    _order_stream_reconnects.inc(n)
    _order_stream_reconnects_val += int(n)

def inc_reseed_runs(n: int = 1) -> None:
    _reseed_runs.inc(n)

def set_order_ws_age_s(val: float) -> None:
    global _order_ws_age_val
    _order_ws_age.set(val)
    _order_ws_age_val = float(val)

def set_clock_drift_s(val: float) -> None:
    global _clock_drift_val
    _clock_drift.set(val)
    _clock_drift_val = float(val)

# -------------------- Snapshots for Alerts -------------------- #

def get_uptime_s() -> float:
    return max(0.0, time.time() - _ts0)

def get_stream_reconnects_count() -> int:
    return int(_stream_reconnects_val)

def get_order_stream_reconnects_count() -> int:
    return int(_order_stream_reconnects_val)

def get_order_ws_age_s() -> float:
    return float(_order_ws_age_val)

def get_clock_drift_s() -> float:
    return float(_clock_drift_val)

# -------------------- Funds mismatch telemetry -------------------- #

_funds_mismatch_rel_val: float = 0.0
_funds_mismatch_total_val: int = 0

def set_funds_mismatch_rel(rel: float) -> None:
    """Set current relative funds mismatch (e.g., percent difference)."""
    global _funds_mismatch_rel_val
    try:
        _funds_mismatch_rel_val = float(rel)
    except Exception:
        return
    try:
        set_gauge("funds_mismatch_rel", _funds_mismatch_rel_val)
    except Exception:
        pass

def get_funds_mismatch_rel() -> float:
    return float(_funds_mismatch_rel_val)

def inc_funds_mismatch_total(n: int = 1) -> None:
    """Increment total sustained mismatch events."""
    global _funds_mismatch_total_val
    _funds_mismatch_total_val += int(n)
    try:
        inc("funds_mismatch_total", int(n))
    except Exception:
        pass

def get_funds_mismatch_total() -> int:
    return int(_funds_mismatch_total_val)
