# src/execution/executor_py/ack_tracker.py
"""
Location : src/execution/executor_py/ack_tracker.py
Purpose  : Track order lifecycle acks, accumulate partial fills into VWAP,
           expose p95 decision→ACK latency and per-minute fill rate for dashboards.

(ENH 2025-09) Bundles foundation:
  - Track mapping of intents to bundle_id
  - Provide bundle_summary(bundle_id) for router/executor coordination
"""

from __future__ import annotations

from collections import deque, defaultdict
from typing import Dict, Tuple, Optional, List, Set
import contextlib
import time

from ...core.logging import log_event
from ...core import metrics as metr
from ..interfaces.queue import IPC
from ..interfaces.intents import QuoteIntent
from ..interfaces.reports import ExecutionReport


def _p95(samples: List[float]) -> float:
    """Return the 95th percentile of a list of numbers (0.0 for empty)."""
    if not samples:
        return 0.0
    xs = sorted(float(x) for x in samples if x is not None)
    if not xs:
        return 0.0
    # nearest-rank on 0..n-1 indexing
    k = int(0.95 * (len(xs) - 1))
    return float(xs[k])


class AckTracker:
    """
    Tracks order acknowledgements & fills.
    - on_placed / on_replaced / on_canceled: execution acks (with latency telemetry)
    - on_fill / on_fill_by_order: partial fills aggregation
    Exposes:
      * fills_per_min()     → int
      * p95_latency_ms()    → float
      * vwap(intent_id)     → float
      * filled_size(intent_id) → float
      * (NEW) bundle_summary(bundle_id) → dict
    """

    def __init__(self, ipc: IPC) -> None:
        self.ipc = ipc
        # intent_id -> (order_id, cum_size, cum_vwap_num, market_id, selection_id)
        self._fills: Dict[str, Tuple[str, float, float, str, int]] = {}
        # reverse map: order_id -> intent_id
        self._by_order: Dict[str, str] = {}

        # decision→ACK latencies (ms), rolling window
        self._lat_ms: deque[float] = deque(maxlen=512)

        # last minute fill timestamps (ms since epoch)
        self._fill_ts_1m: deque[int] = deque()

        # -------- Bundles (NEW) --------
        # bundle_id -> set(intent_id)
        self._bundle_members: Dict[str, Set[str]] = defaultdict(set)
        # intent_id -> bundle_id
        self._intent_bundle: Dict[str, str] = {}
        # bundle-level counters (best-effort)
        # bundle_id -> {"placed": int, "canceled": int}
        self._bundle_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: {"placed": 0, "canceled": 0})

    # ---------- helpers ----------
    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    # ---------- recovery helpers ----------
    def register_order(self, *, order_id: str, market_id: str, selection_id: int, intent_id: Optional[str] = None) -> None:
        """Register an order id so later fills/cancels can map back to an intent."""
        iid = intent_id if intent_id else f"unknown:{order_id}"
        self._by_order[str(order_id)] = iid
        self._fills.setdefault(iid, (str(order_id), 0.0, 0.0, str(market_id), int(selection_id)))

    # ---------- acks ----------
    async def on_placed(self, qi: QuoteIntent, order_id: str, ack_ts_ms: int) -> None:
        """Place ack."""
        self._by_order[str(order_id)] = qi.intent_id
        self._fills.setdefault(qi.intent_id, (str(order_id), 0.0, 0.0, qi.market_id, int(qi.selection_id)))

        # Bundles: record membership and placed count
        if qi.bundle_id:
            bid = str(qi.bundle_id)
            self._bundle_members[bid].add(qi.intent_id)
            self._intent_bundle[qi.intent_id] = bid
            self._bundle_counts[bid]["placed"] += 1

        lat = max(0.0, float(int(ack_ts_ms) - int(qi.client_ts_ms)))
        self._lat_ms.append(lat)
        with contextlib.suppress(Exception):
            # Optional metrics glue
            metr.set_gauge("bf_ack_latency_ms", float(lat))

        er = ExecutionReport(
            intent_id=qi.intent_id,
            order_id=str(order_id),
            market_id=qi.market_id,
            selection_id=int(qi.selection_id),
            action="PLACED",
            ack_ts_ms=int(ack_ts_ms),
            latency_ms=int(lat),
        )
        await self.ipc.publish_report(er)

    async def on_replaced(self, qi: QuoteIntent, order_id: str, ack_ts_ms: int) -> None:
        """Replace ack (order id may change)."""
        old = self._fills.get(qi.intent_id, ("", 0.0, 0.0, qi.market_id, int(qi.selection_id)))
        _, cum_sz, cum_num, mk, sel = old
        self._fills[qi.intent_id] = (str(order_id), float(cum_sz), float(cum_num), mk, sel)
        self._by_order[str(order_id)] = qi.intent_id

        lat = max(0.0, float(int(ack_ts_ms) - int(qi.client_ts_ms)))
        self._lat_ms.append(lat)
        with contextlib.suppress(Exception):
            metr.set_gauge("bf_ack_latency_ms", float(lat))

        er = ExecutionReport(
            intent_id=qi.intent_id,
            order_id=str(order_id),
            market_id=mk,
            selection_id=sel,
            action="REPLACED",
            ack_ts_ms=int(ack_ts_ms),
            latency_ms=int(lat),
        )
        await self.ipc.publish_report(er)

    async def on_canceled(self, qi: QuoteIntent, order_id: str, ack_ts_ms: int) -> None:
        """Cancel ack (with intent)."""
        # Bundles: bump canceled if member
        bid = self._intent_bundle.get(qi.intent_id)
        if bid:
            self._bundle_counts[bid]["canceled"] += 1

        er = ExecutionReport(
            intent_id=qi.intent_id,
            order_id=str(order_id),
            market_id=qi.market_id,
            selection_id=int(qi.selection_id),
            action="CANCELED",
            ack_ts_ms=int(ack_ts_ms),
        )
        await self.ipc.publish_report(er)
        self._by_order.pop(str(order_id), None)
        self._fills.pop(qi.intent_id, None)

    async def on_canceled_by_order(self, *, order_id: str, market_id: str, selection_id: int, ack_ts_ms: int) -> None:
        """Order-stream cancel (no QuoteIntent available)."""
        intent_id = self._by_order.get(str(order_id), f"unknown:{order_id}")
        # Bundles: bump canceled if member
        bid = self._intent_bundle.get(intent_id)
        if bid:
            self._bundle_counts[bid]["canceled"] += 1

        er = ExecutionReport(
            intent_id=intent_id,
            order_id=str(order_id),
            market_id=str(market_id),
            selection_id=int(selection_id),
            action="CANCELED",
            ack_ts_ms=int(ack_ts_ms),
        )
        await self.ipc.publish_report(er)
        self._by_order.pop(str(order_id), None)
        self._fills.pop(intent_id, None)

    # ---------- fills ----------
    async def on_fill(self, qi: QuoteIntent, order_id: str, fill_price: float, fill_size: float, fill_ts_ms: int) -> None:
        """Partial fill (when we still have the QuoteIntent)."""
        prev = self._fills.get(qi.intent_id, (str(order_id), 0.0, 0.0, qi.market_id, int(qi.selection_id)))
        _, cum_sz, cum_num, mk, sel = prev
        cum_sz_new = float(cum_sz) + float(fill_size)
        cum_num_new = float(cum_num) + float(fill_price) * float(fill_size)
        self._fills[qi.intent_id] = (str(order_id), cum_sz_new, cum_num_new, mk, sel)

        with contextlib.suppress(Exception):
            self._fill_ts_1m.append(int(fill_ts_ms))

        er = ExecutionReport(
            intent_id=qi.intent_id,
            order_id=str(order_id),
            market_id=mk,
            selection_id=sel,
            action="PARTIAL",
            ack_ts_ms=int(fill_ts_ms),
            fill_ts_ms=int(fill_ts_ms),
            fill_price=float(fill_price),
            fill_size=float(fill_size),
        )
        await self.ipc.publish_report(er)

    async def on_fill_by_order(self, *, order_id: str, market_id: str, selection_id: int,
                               fill_price: float, fill_size: float, fill_ts_ms: int) -> None:
        """Partial fill (order-stream only, map by order id)."""
        intent_id = self._by_order.get(str(order_id), f"unknown:{order_id}")
        prev = self._fills.get(intent_id, (str(order_id), 0.0, 0.0, str(market_id), int(selection_id)))
        _, cum_sz, cum_num, mk, sel = prev
        cum_sz_new = float(cum_sz) + float(fill_size)
        cum_num_new = float(cum_num) + float(fill_price) * float(fill_size)
        self._fills[intent_id] = (str(order_id), cum_sz_new, cum_num_new, mk, sel)

        with contextlib.suppress(Exception):
            self._fill_ts_1m.append(int(fill_ts_ms))

        er = ExecutionReport(
            intent_id=intent_id,
            order_id=str(order_id),
            market_id=mk,
            selection_id=sel,
            action="PARTIAL",
            ack_ts_ms=int(fill_ts_ms),
            fill_ts_ms=int(fill_ts_ms),
            fill_price=float(fill_price),
            fill_size=float(fill_size),
        )
        await self.ipc.publish_report(er)

    # ---------- metrics ----------
    def _prune_fill_window(self, now_ms: int) -> None:
        cutoff = int(now_ms) - 60_000
        dq = self._fill_ts_1m
        while dq and dq[0] < cutoff:
            dq.popleft()

    def fills_per_min(self) -> int:
        self._prune_fill_window(self._now_ms())
        return int(len(self._fill_ts_1m))

    def p95_latency_ms(self) -> float:
        return _p95(list(self._lat_ms))

    # ---------- inspection ----------
    def vwap(self, intent_id: str) -> float:
        """Return volume-weighted average fill price for an intent (0.0 if no fills)."""
        _, cum_sz, cum_num, _, _ = self._fills.get(str(intent_id), ("", 0.0, 0.0, "", 0))
        return (float(cum_num) / float(cum_sz)) if float(cum_sz) > 0.0 else 0.0

    def filled_size(self, intent_id: str) -> float:
        """Return cumulated filled size for an intent (0.0 if none)."""
        _, cum_sz, _, _, _ = self._fills.get(str(intent_id), ("", 0.0, 0.0, "", 0))
        return float(cum_sz)

    # ---------- bundles (NEW) ----------
    def bundle_summary(self, bundle_id: str) -> Dict[str, Any]:
        """
        Return a best-effort snapshot for router logic:
          {
            "members": [intent_id, ...],
            "placed": int,
            "canceled": int,
            "filled_size": {intent_id: float},
          }
        """
        bid = str(bundle_id)
        members = list(self._bundle_members.get(bid, set()))
        placed = int(self._bundle_counts.get(bid, {}).get("placed", 0))
        canceled = int(self._bundle_counts.get(bid, {}).get("canceled", 0))
        fs = {}
        for iid in members:
            fs[iid] = self.filled_size(iid)
        return {
            "members": members,
            "placed": placed,
            "canceled": canceled,
            "filled_size": fs,
        }
