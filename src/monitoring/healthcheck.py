"""
Location : src/monitoring/healthcheck.py
Purpose  : Simple health snapshot: stream freshness, order ack freshness, DB available.
Plain    : You feed timestamps into it; call .snapshot() to get a dict of statuses.
"""

from __future__ import annotations
import os
import time
from typing import Dict, Any

from ..core.config import load_config
from ..core.logging import log_event
from ..data.store import DataStore


class Healthcheck:
    def __init__(self, profile_name: str) -> None:
        self.profile_name = profile_name
        snap = load_config(profile_name)
        ks = (getattr(snap, "risk", {}) or {}).get("kill_switches", {})
        self._stream_thresh = int(ks.get("stream_stale_ms", 10_000))
        self._ack_thresh = int(ks.get("ack_p95_ms", 10_000))

        self._last_stream_ms: int = 0
        self._last_ack_ms: int = 0
        self._db_ok: bool = False
        # warm DB test
        try:
            DataStore(profile_name).close()
            self._db_ok = True
        except Exception:
            self._db_ok = False

    def update_stream_ts(self, ts_ms: int) -> None:
        self._last_stream_ms = int(ts_ms)

    def update_ack_ts(self, ts_ms: int) -> None:
        self._last_ack_ms = int(ts_ms)

    def snapshot(self, now_ms: int | None = None) -> Dict[str, Any]:
        now = int(now_ms or time.time() * 1000)
        stream_age_ms = now - self._last_stream_ms if self._last_stream_ms else None
        ack_age_ms = now - self._last_ack_ms if self._last_ack_ms else None

        status = {
            "stream_ok": (stream_age_ms is not None and stream_age_ms < self._stream_thresh),
            "acks_ok": (ack_age_ms is not None and ack_age_ms < self._ack_thresh),
            "db_ok": self._db_ok,
            "stream_age_ms": stream_age_ms,
            "ack_age_ms": ack_age_ms,
            "stream_thresh_ms": self._stream_thresh,
            "ack_thresh_ms": self._ack_thresh,
        }
        log_event("health", "health.snapshot", "ok" if all(status[k] for k in ("stream_ok", "acks_ok", "db_ok")) else "warn", **status)
        return status