"""
Location : src/execution/interfaces/reports.py
Purpose  : Canonical ExecutionReport (executor -> strategy) with stable wire format.
Notes    : Supports partial fills and VWAP accounting (ack_tracker aggregates).
"""

from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Literal, Optional, Dict, Any


Action = Literal["PLACED", "REPLACED", "CANCELED", "PARTIAL", "FILLED", "REJECT"]


@dataclass(frozen=True)
class ExecutionReport:
    # Wire schema version (freeze seam; increment on breaking changes)
    v: int = 1

    # Identity
    intent_id: str
    order_id: str
    market_id: str
    selection_id: int
    # Optional bundle context (multi-leg / paired orders)
    bundle_id: Optional[str] = None
    
    # Lifecycle
    action: Action
    ack_ts_ms: int

    # Fill info (for PARTIAL/FILLED)
    fill_ts_ms: Optional[int] = None
    fill_price: Optional[float] = None
    fill_size: Optional[float] = None
    # Optional rich fill history (executor may send multiple partials)
    fill_events: Optional[list[dict]] = None  # [{ts_ms:int, size:float, price:float}, ...]

    # Costing / telemetry
    fees: Optional[float] = None
    latency_ms: Optional[int] = None
    reject_code: Optional[str] = None
    executor_stats: Dict[str, Any] = None   # e.g., {"msgs_per_sec": 4, "replace_count": 2}

    # ---------- Wire helpers ---------- #

    def to_wire(self) -> Dict[str, Any]:
        d = asdict(self)
        d["v"] = int(getattr(self, "v", 1))
        # Remove Nones for a tidier wire format
        for k in ["bundle_id", "fill_ts_ms", "fill_price", "fill_size", "fill_events",
                  "fees", "latency_ms", "reject_code", "executor_stats"]:
            if d.get(k) is None:
                d.pop(k, None)
        return d

    @staticmethod
    def from_wire(d: Dict[str, Any]) -> "ExecutionReport":
        return ExecutionReport(
            v=int(d.get("v", 1)),            
            intent_id=str(d["intent_id"]),
            order_id=str(d["order_id"]),
            market_id=str(d["market_id"]),
            selection_id=int(d["selection_id"]),
            bundle_id=str(d["bundle_id"]) if d.get("bundle_id") is not None else None,            
            action=str(d["action"]).upper(),  # type: ignore[arg-type]
            ack_ts_ms=int(d["ack_ts_ms"]),
            fill_ts_ms=int(d["fill_ts_ms"]) if d.get("fill_ts_ms") is not None else None,
            fill_price=float(d["fill_price"]) if d.get("fill_price") is not None else None,
            fill_size=float(d["fill_size"]) if d.get("fill_size") is not None else None,
            fill_events=list(d["fill_events"]) if d.get("fill_events") is not None else None,            
            fees=float(d["fees"]) if d.get("fees") is not None else None,
            latency_ms=int(d["latency_ms"]) if d.get("latency_ms") is not None else None,
            reject_code=str(d["reject_code"]) if d.get("reject_code") is not None else None,
            executor_stats=dict(d["executor_stats"]) if d.get("executor_stats") is not None else None,

        )
