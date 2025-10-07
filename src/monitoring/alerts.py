# src/monitoring/alerts.py
"""
Location : src/monitoring/alerts.py
Purpose  : Threshold alerts for health & efficiency:
           - stream reconnect rate (per hour)
           - clock drift sustained above threshold
           - low free balance / funds mismatch (hooks)
Plain    : Alerts.from_profile(cfg) → Alerts; call alerts.check(...) periodically.
"""

from __future__ import annotations
from typing import Optional, Any
import time

from ..core.logging import log_event
from ..core.metrics import (
    get_uptime_s,
    get_stream_reconnects_total,
    get_clock_drift_s,
    # pacing SLO counters are accessed lazily inside helpers
)

class Alerts:
    def __init__(
        self,
        *,
        reconnect_warn_per_hour: int = 10,
        clock_drift_warn_s: float = 2.0,
        clock_drift_min_duration_s: float = 10.0,
        debounce_s: float = 60.0,
    ) -> None:
        self.reconnect_warn_per_hour = int(reconnect_warn_per_hour)
        self.clock_drift_warn_s = float(clock_drift_warn_s)
        self.clock_drift_min_duration_s = float(clock_drift_min_duration_s)
        self.debounce_s = float(debounce_s)

        # state
        self._last_reconn_check_ts: float = 0.0
        self._last_reconn_count: int = 0
        self._last_reconn_alert_ts: float = 0.0

        self._drift_high_since: Optional[float] = None
        self._drift_alerted: bool = False

    @staticmethod
    def from_profile(cfg: Any) -> "Alerts":
        # Read optional thresholds from config
        metrics_cfg = getattr(cfg, "metrics", {}) or {}
        reconnect_warn_per_hour = int(metrics_cfg.get("reconnect_warn_per_hour", 10))
        drift = metrics_cfg.get("drift", {}) or {}
        clock_drift_warn_s = float(drift.get("warn_s", 2.0))
        clock_drift_min_duration_s = float(drift.get("min_duration_s", 10.0))
        return Alerts(
            reconnect_warn_per_hour=reconnect_warn_per_hour,
            clock_drift_warn_s=clock_drift_warn_s,
            clock_drift_min_duration_s=clock_drift_min_duration_s,
        )

    # -------- funds hooks (optional) -------- #

    def low_free_balance(self, available: float, min_required: float) -> None:
        if available < min_required:
            log_event("alert", "funds.low", "pause", available=available, min_required=min_required)

    def funds_mismatch(self, internal_liability: float, api_exposure: float, rel_diff: float, threshold: float, duration_s: float) -> None:
        log_event(
            "alert",
            "funds.mismatch",
            "warn",
            internal_liability=internal_liability,
            api_exposure=api_exposure,
            mismatch_rel=rel_diff,
            threshold=threshold,
            sustained_s=duration_s,
        )

    # -------- periodic check -------- #

    def check(self, health: Any, acks: Any, order_manager: Any) -> None:
        """Call every ~1s to evaluate alert conditions."""
        self._check_reconnect_rate()
        self._check_clock_drift()

    # -------- implementations -------- #

    def _check_reconnect_rate(self) -> None:
        now = time.time()
        uptime = get_uptime_s()
        count_now = int(get_stream_reconnects_total())

        # First data point baseline
        if self._last_reconn_check_ts == 0.0:
            self._last_reconn_check_ts = now
            self._last_reconn_count = count_now
            return

        dt = max(1.0, now - self._last_reconn_check_ts)
        dc = max(0, count_now - self._last_reconn_count)
        per_hour = (dc / dt) * 3600.0

        if per_hour >= self.reconnect_warn_per_hour:
            if now - self._last_reconn_alert_ts > self.debounce_s:
                log_event("alert", "stream.reconnect_rate", "warn",
                          per_hour=per_hour, threshold=self.reconnect_warn_per_hour,
                          count_delta=dc, window_s=int(dt))
                self._last_reconn_alert_ts = now

        self._last_reconn_check_ts = now
        self._last_reconn_count = count_now

    def _check_clock_drift(self) -> None:
        drift = float(get_clock_drift_s())
        now = time.time()
        if drift >= self.clock_drift_warn_s:
            if self._drift_high_since is None:
                self._drift_high_since = now
            elif (now - self._drift_high_since) >= self.clock_drift_min_duration_s and not self._drift_alerted:
                log_event("alert", "clock.drift", "warn",
                          drift_s=drift, min_duration_s=self.clock_drift_min_duration_s)
                self._drift_alerted = True
        else:
            # reset
            self._drift_high_since = None
            self._drift_alerted = False


# ---- Pacing SLO (pacing/TTL) wiring (non-invasive; optional) ----

def _alerts__pacing_init(self):
    if not hasattr(self, "_pacing_init_done"):
        self._pacing_init_done = True
        cfg = getattr(self, "metrics_cfg", {}) if hasattr(self, "metrics_cfg") else {}
        pacing = cfg.get("pacing", {}) if isinstance(cfg, dict) else {}
        warn = pacing.get("warn_per_min", {}) if isinstance(pacing, dict) else {}
        self._pacing_warn_ttl = float(warn.get("ttl_prevent", 60.0))
        self._pacing_warn_budget = float(warn.get("budget_trip", 10.0))
        self._pacing_last_ts = 0.0
        self._pacing_last_prevent = 0
        self._pacing_last_budget = 0

def _alerts__check_pacing_slo(self):
    from ..core.metrics import get_pacing_ttl_prevent_total, get_pacing_budget_trip_total
    _alerts__pacing_init(self)
    now = time.time()
    if self._pacing_last_ts == 0.0:
        self._pacing_last_ts = now
        self._pacing_last_prevent = int(get_pacing_ttl_prevent_total())
        self._pacing_last_budget = int(get_pacing_budget_trip_total())
        return
    prev_ttl = int(self._pacing_last_prevent)
    prev_bgt = int(self._pacing_last_budget)
    cur_ttl = int(get_pacing_ttl_prevent_total())
    cur_bgt = int(get_pacing_budget_trip_total())
    dt = max(1.0, now - float(self._pacing_last_ts))
    rate_ttl = (cur_ttl - prev_ttl) * (60.0 / dt)
    rate_bgt = (cur_bgt - prev_bgt) * (60.0 / dt)

    self._pacing_last_ts = now
    self._pacing_last_prevent = cur_ttl
    self._pacing_last_budget = cur_bgt

    # Debounce baseline
    if not hasattr(self, "debounce_s"):
        self.debounce_s = 60.0
    if not hasattr(self, "_debounce_next"):
        self._debounce_next = 0.0

    # Emit only if each per-minute rate breaches its warn threshold and debounce window elapsed
    if (rate_ttl >= float(self._pacing_warn_ttl) or rate_bgt >= float(self._pacing_warn_budget)) and now >= float(self._debounce_next):
        log_event("alert", "pacing", "warn",
                  ttl_prevent_per_min=rate_ttl, budget_trip_per_min=rate_bgt,
                  warn_ttl=self._pacing_warn_ttl, warn_budget=self._pacing_warn_budget)
        self._debounce_next = now + float(self.debounce_s)


# ---- Funds mismatch staleness (non-invasive; optional) ----

def _alerts__funds_init(self):
    if not hasattr(self, "_funds_breach_since"):
        self._funds_breach_since = 0.0
    if not hasattr(self, "_funds_warn_pct"):
        # Look under metrics.funds.* first; fall back to risk.funds.*
        self._funds_warn_pct = 5.0
    if not hasattr(self, "_funds_warn_secs"):
        self._funds_warn_secs = 30.0

def _alerts__check_funds_mismatch(self):
    from ..core.metrics import get_funds_mismatch_rel, inc_funds_mismatch_total
    _alerts__funds_init(self)
    now = time.time()
    rel = float(get_funds_mismatch_rel())
    # Interpret thresholds: if configured >=1.0, treat as percent scale
    warn_pct = float(self._funds_warn_pct)
    warn_fraction = (warn_pct/100.0) if warn_pct >= 1.0 else warn_pct
    if rel >= warn_fraction:
        if self._funds_breach_since == 0.0:
            self._funds_breach_since = now
        sustained = now - self._funds_breach_since
        if sustained >= self._funds_warn_secs and now >= float(getattr(self, "_debounce_next", 0.0)):
            log_event("alert", "funds.mismatch", "warn",
                      mismatch_rel=rel, warn_pct=warn_pct, sustained_s=sustained)
            inc_funds_mismatch_total(1)
            self._debounce_next = now + float(self.debounce_s)
    else:
        self._funds_breach_since = 0.0


# ---- Discovery staleness (optional) ----
def _alerts__discovery_init(self):
    if not hasattr(self, "_discovery_last_ts"):
        self._discovery_last_ts = 0.0
    if not hasattr(self, "_discovery_last_alert_ts"):
        self._discovery_last_alert_ts = 0.0
    if not hasattr(self, "_discovery_max_staleness_s"):
        # Default; can be overridden via Alerts.from_profile wrapper below
        self._discovery_max_staleness_s = 900.0

def _alerts__discovery_heartbeat(self, last_ts: float) -> None:
    try:
        _alerts__discovery_init(self)
        self._discovery_last_ts = float(last_ts or 0.0)
    except Exception:
        pass

def _alerts__check_discovery_staleness(self):
    _alerts__discovery_init(self)
    now = time.time()
    last = float(getattr(self, "_discovery_last_ts", 0.0))
    if last <= 0.0:
        return  # no data yet
    stale_s = max(0.0, now - last)
    max_s = float(getattr(self, "_discovery_max_staleness_s", 900.0))
    if stale_s >= max_s:
        last_alert = float(getattr(self, "_discovery_last_alert_ts", 0.0))
        if now - last_alert >= float(getattr(self, "debounce_s", 60.0)):
            log_event("alert", "discovery.refresh", "stale", stale_s=stale_s, max_staleness_s=max_s)
            self._discovery_last_alert_ts = now

# Expose heartbeat as a method on Alerts
try:
    Alerts.discovery_heartbeat = _alerts__discovery_heartbeat  # type: ignore[attr-defined]
except Exception:
    pass

# Wrap Alerts.from_profile to capture discovery.max_staleness_s into the instance
try:
    _Alerts_from_profile_orig = Alerts.from_profile  # type: ignore[attr-defined]
except Exception:
    _Alerts_from_profile_orig = None

def _alerts__from_profile_with_discovery(cfg):
    a = _Alerts_from_profile_orig(cfg) if _Alerts_from_profile_orig else Alerts()
    try:
        disc = getattr(cfg, "discovery", {}) or {}
        a._discovery_max_staleness_s = float(disc.get("max_staleness_s", 900.0))
    except Exception:
        a._discovery_max_staleness_s = 900.0
    # init state
    a._discovery_last_ts = 0.0
    a._discovery_last_alert_ts = 0.0
    return a

try:
    Alerts.from_profile = staticmethod(_alerts__from_profile_with_discovery)  # type: ignore[attr-defined]
except Exception:
    pass


# ---- Single wrapper that calls original Alerts.check, then our SLO checks ----
try:
    _Alerts_check_orig_consolidated = Alerts.check  # type: ignore[attr-defined]
except Exception:
    _Alerts_check_orig_consolidated = None

def _alerts__check_with_extras(self):
    if _Alerts_check_orig_consolidated:
        try:
            _Alerts_check_orig_consolidated(self)
        except Exception:
            pass
    try:
        _alerts__check_discovery_staleness(self)
    except Exception:
        pass
    try:
        _alerts__check_pacing_slo(self)
    except Exception:
        pass
    try:
        _alerts__check_funds_mismatch(self)
    except Exception:
        pass

try:
    Alerts.check = _alerts__check_with_extras  # type: ignore[attr-defined]
except Exception:
    pass
