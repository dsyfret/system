"""
Location : src/strategy/risk.py
Purpose  : Centralised risk hard-stops (kill-switches) for runtime:
           - Stream stale (no price deltas for too long)
           - Slow ACKs (decision→ACK p95 too high)
           - Intraday P&L shock (large loss)
           - Drawdown stop (equity down too far)

Plain    : Evaluate current telemetry and return an action:
           "OK" | "PAUSE" | "FLATTEN" | "FREEZE" plus a reason.
Config   : configs/risk.yaml -> kill_switches.*, actions.*
Inputs   : now_ms, last_stream_ts_ms, ack_p95_ms (optional), pnl_today (optional),
           dd_equity_pct (optional). Missing metrics are ignored.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Tuple

from ..core.config import load_config


@dataclass
class RiskDecision:
    action: str   # "OK" | "PAUSE" | "FLATTEN" | "FREEZE"
    reason: str


class RiskManager:
    def __init__(self, profile_name: str) -> None:
        snap = load_config(profile_name)
        self.profile = profile_name
        cfg = getattr(snap, "risk", {}) or {}
        ks = (cfg.get("kill_switches") or {})
        self.stream_stale_ms: int = int(ks.get("stream_stale_ms", 4000))
        self.ack_p95_ms: int = int(ks.get("ack_p95_ms", 150))
        self.pnl_shock_abs: float = float(ks.get("pnl_shock_abs", 0.0))  # 0 disables
        self.dd_stop_pct: float = float(ks.get("dd_stop_pct", 0.0))      # 0 disables

        acts = (cfg.get("actions") or {})
        self.on_stream_stale: str = str(acts.get("on_stream_stale", "PAUSE"))
        self.on_ack_slow: str = str(acts.get("on_ack_slow", "PAUSE"))
        self.on_pnl_shock: str = str(acts.get("on_pnl_shock", "FLATTEN"))
        self.on_dd_stop: str = str(acts.get("on_dd_stop", "FREEZE"))

    def evaluate(self,
                 *,
                 now_ms: int,
                 last_stream_ts_ms: Optional[int],
                 ack_p95_ms: Optional[float] = None,
                 pnl_today: Optional[float] = None,
                 dd_equity_pct: Optional[float] = None) -> RiskDecision:
        # 1) Stream stale?
        if last_stream_ts_ms:
            age = now_ms - int(last_stream_ts_ms)
            if age >= self.stream_stale_ms:
                return RiskDecision(self.on_stream_stale, f"stream_stale_{age}ms")

        # 2) ACK p95 slow?
        if ack_p95_ms is not None and self.ack_p95_ms > 0:
            if ack_p95_ms >= self.ack_p95_ms:
                return RiskDecision(self.on_ack_slow, f"ack_p95_{int(ack_p95_ms)}ms")

        # 3) Intraday P&L shock?
        if self.pnl_shock_abs > 0.0 and pnl_today is not None:
            if pnl_today <= -abs(self.pnl_shock_abs):
                return RiskDecision(self.on_pnl_shock, f"pnl_shock_{pnl_today:.2f}")

        # 4) Drawdown stop?
        if self.dd_stop_pct > 0.0 and dd_equity_pct is not None:
            if dd_equity_pct >= self.dd_stop_pct:
                return RiskDecision(self.on_dd_stop, f"dd_stop_{dd_equity_pct:.3f}")

        return RiskDecision("OK", "ok")