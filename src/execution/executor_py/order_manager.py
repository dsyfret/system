# src/execution/executor_py/order_manager.py
"""
Location : src/execution/executor_py/order_manager.py
Purpose  : Place/replace/cancel pacing + LIVE ORDER CACHE (+ mass-cancel helper).
           Adds:
             - Interface sanity with OrdersClient variants:
                * place_limit(qi) -> (order_id, ack_ts_ms, remaining)
                * replace_price(order_id, new_price) -> (new_order_id, ack_ts_ms)
                * cancel_order(order_id) -> ack_ts_ms
             - Dry-run mode (execution.dry_run=true) simulates lifecycle but still emits acks/reports.
             - Per-minute counters (places, replaces) + replace:place ratio for dashboards/alerts.
             - Optional per-market minute new-risk budget gate from throttles.new_risk_budget_abs_per_min.
             - Informational message-cost accounting (rolling 1h window) with configurable rates.

(ENH 2025-09) Funds-mismatch tripwire support:
             - liability_snapshot() → float  (current internal unmatched liability across live orders)
             - liability_snapshot_by_market() → Dict[str, float]

(ENH 2025-10) Exposure accessors (for Arbiter liability cap integration):
             - exposure_for(market_id, selection_id, side) → float
             - exposure_for_runner(market_id, selection_id) → float  (sum of both sides)
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List
import contextlib
import time
import uuid

from ...core.logging import log_event
from ...core import metrics as metr
from ...core.config import load_config

from ..interfaces.queue import IPC
from ..interfaces.intents import QuoteIntent
from ..interfaces.reports import ExecutionReport

from ...betfair.orders import OrdersClient
from .ack_tracker import AckTracker
from ..policies import evaluate_persistence


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class LiveOrder:
    order_id: str
    market_id: str
    selection_id: int
    side: str
    price: float
    remaining: float
    ts_ms: int           # last action time (place/replace)
    source: str          # "local" | "recovered"

    def as_dict(self) -> Dict[str, Any]:
        return {
            "order_id": self.order_id,
            "market_id": self.market_id,
            "selection_id": int(self.selection_id),
            "side": self.side,
            "price": float(self.price),
            "remaining": float(self.remaining),
            "ts_ms": int(self.ts_ms),
            "source": self.source,
        }


class OrderManager:
    def __init__(self, profile_name: str, ipc: IPC, orders: OrdersClient, acks: AckTracker) -> None:
        self.profile = profile_name
        self.cfg = load_config(profile_name)
        self.ipc = ipc
        self.orders = orders
        self.acks = acks

        # ---- counters (per-minute op rate) ----
        self._place_ts_1m: deque[int] = deque()
        self._replace_ts_1m: deque[int] = deque()

        # ---- dry-run toggle ----
        exec_cfg = getattr(self.cfg, "execution", {}) or {}
        self.dry_run: bool = bool((exec_cfg or {}).get("dry_run", False))

        # ---- per-market minute new-risk budget ----
        thr = getattr(self.cfg, "throttles", {}) or {}
        self._per_market_budget_per_min: float = float((thr.get("new_risk_budget_abs_per_min") or 0.0) or 0.0)
        self._market_budget_events_1m: Dict[str, deque[Tuple[int, float]]] = {}

        # ---- message-cost accounting (informational) ----
        msg_cfg = (thr.get("messages") or {})
        self._msg_rate_default: float = float(msg_cfg.get("rate_default", 0.0) or 0.0)
        self._msg_rate_discounted: float = float(msg_cfg.get("rate_discounted", 0.0) or 0.0)
        self._msg_discount_threshold_hr: int = int(msg_cfg.get("discount_threshold_per_hour", 1000) or 1000)
        self._msg_total: int = 0
        self._msg_cost_total: float = 0.0
        self._msg_events_1h: deque[int] = deque()

        # ---- live order cache ----
        # market_id -> ((selection_id, side) -> LiveOrder)
        self._live: Dict[str, Dict[Tuple[int, str], LiveOrder]] = {}

        # replace min-lifetime tracking
        # (market_id, selection_id, side) -> last_place_ts_ms
        self._last_place_ms: Dict[Tuple[str, int, str], int] = {}

    # =================== helpers =================== #
    def _tick(self) -> int:
        return _now_ms()

    # ---- message-cost accounting ---- #
    def _inc_msg_cost(self) -> None:
        now = self._tick()
        self._msg_events_1h.append(now)
        cutoff = now - 3_600_000
        while self._msg_events_1h and self._msg_events_1h[0] < cutoff:
            self._msg_events_1h.popleft()
        hourly = len(self._msg_events_1h)

        rate = self._msg_rate_discounted if hourly > self._msg_discount_threshold_hr else self._msg_rate_default
        self._msg_total += 1
        self._msg_cost_total += float(rate)
        with contextlib.suppress(Exception):
            metr.inc("bf_messages_total", 1)
            metr.set_gauge("bf_messages_hour", float(hourly))
            metr.set_gauge("bf_message_cost_total", float(self._msg_cost_total))

    # ---- per-market budget ---- #
    def _budget_gate(self, market_id: str) -> None:
        if self._per_market_budget_per_min <= 0.0:
            return
        now = self._tick()
        dq = self._market_budget_events_1m.setdefault(str(market_id), deque())
        cutoff = now - 60_000
        while dq and dq[0][0] < cutoff:
            dq.popleft()
        used = sum(val for (_, val) in dq)
        if used > (self._per_market_budget_per_min + 1e-9):
            raise RuntimeError("per-market new-risk budget exceeded")

    def _budget_add(self, market_id: str, liability: float) -> None:
        if self._per_market_budget_per_min <= 1e-9:
            return
        dq = self._market_budget_events_1m.setdefault(str(market_id), deque())
        dq.append((self._tick(), float(liability)))

    # ---- live cache ---- #
    def _register_local(self, qi: QuoteIntent, order_id: str, *, price: float, remaining: float) -> None:
        now_ms = self._tick()
        self._last_place_ms[(qi.market_id, qi.selection_id, qi.side)] = now_ms
        lo = LiveOrder(order_id, qi.market_id, int(qi.selection_id), str(qi.side), float(price), float(remaining), now_ms, "local")
        self._live.setdefault(qi.market_id, {})[(int(qi.selection_id), str(qi.side))] = lo

    def get_live(self, market_id: str) -> Dict[Tuple[int, str], LiveOrder]:
        return self._live.setdefault(str(market_id), {})

    def find_live_order_id(self, market_id: str, selection_id: int, side: str) -> Optional[str]:
        lo = self.get_live(market_id).get((int(selection_id), str(side)))
        return lo.order_id if lo else None

    def _replace_lifetime_ok(self, market_id: str, selection_id: int, side: str) -> bool:
        cfg = getattr(self.cfg, "throttles", {}) or {}
        min_life_ms = int((cfg.get("min_quote_lifetime_ms") or 0) or 0)
        last = int(self._last_place_ms.get((market_id, selection_id, side), 0))
        return (self._tick() - last) >= max(0, min_life_ms)

    # ---- rebuild live cache from listCurrentOrders ---- #
    def rebuild_from_list_current_orders(self, current_orders_payload: Dict[str, Any]) -> None:
        now_ms = self._tick()
        self._live.clear()
        cur = []
        if isinstance(current_orders_payload, dict):
            cur = current_orders_payload.get("currentOrders", []) or []
        for row in cur:
            try:
                order_id = str(row.get("betId", "") or row.get("bet_id", "") or "")
                market_id = str(row.get("marketId", "") or "")
                selection_id = int(row.get("selectionId", 0) or 0)
                side = str(row.get("side", "BACK") or "BACK").upper()
                price = float(row.get("price", 0.0) or (row.get("priceSize") or {}).get("price", 0.0) or 0.0)
                size_matched = float(row.get("sizeMatched", 0.0) or 0.0)
                base_size = float(row.get("size", (row.get("priceSize") or {}).get("size", 0.0)) or 0.0)
                remaining = float(row.get("sizeRemaining", base_size - size_matched) or 0.0)
                status = str(row.get("status", "EXECUTABLE") or "EXECUTABLE").upper()
                if not order_id or not market_id or selection_id <= 0:
                    continue
                if status not in ("EXECUTABLE", "PENDING") and remaining <= 1e-9:
                    continue
                lo = LiveOrder(order_id, market_id, selection_id, side, price, remaining, now_ms, "recovered")
                self._live.setdefault(market_id, {})[(selection_id, side)] = lo
                with contextlib.suppress(Exception):
                    self.acks.register_order(order_id=order_id, market_id=market_id, selection_id=selection_id)
            except Exception:
                continue

    # =================== internal liability snapshots =================== #
    def liability_snapshot_by_market(self) -> Dict[str, float]:
        """
        Compute current internal *unmatched* liability per market from the live-order cache.

        BACK  : liability ≈ remaining stake
        LAY   : liability ≈ (price - 1) * remaining stake

        Returns:
            dict market_id -> liability (non-negative float)
        """
        out: Dict[str, float] = {}
        for market_id, mp in self._live.items():
            liab = 0.0
            for (sel_id, side), lo in mp.items():
                rem = float(max(0.0, lo.remaining))
                if rem <= 1e-12:
                    continue
                if str(lo.side).upper() == "LAY":
                    liab += max(0.0, (float(lo.price) - 1.0) * rem)
                else:  # BACK
                    liab += rem
            out[str(market_id)] = float(liab)
        return out

    def liability_snapshot(self) -> float:
        """
        Compute current internal *total* unmatched liability across all markets.

        This is intentionally read-only and O(#live orders). No network calls.
        """
        total = 0.0
        for liab in self.liability_snapshot_by_market().values():
            total += float(liab)
        # Optional: expose a metric if available
        with contextlib.suppress(Exception):
            metr.set_gauge("internal_unmatched_liability", float(total))
        return float(total)

    # =================== exposure accessors (NEW) =================== #
    def exposure_for(self, market_id: str, selection_id: int, side: str) -> float:
        """
        Return current *unmatched* liability for a specific (market_id, selection_id, side).

        BACK: stake remaining
        LAY : (price - 1) * stake remaining
        """
        try:
            lo = self.get_live(str(market_id)).get((int(selection_id), str(side).upper()))
            if not lo:
                return 0.0
            rem = max(0.0, float(lo.remaining))
            if rem <= 1e-12:
                return 0.0
            if str(lo.side).upper() == "LAY":
                return max(0.0, (float(lo.price) - 1.0) * rem)
            return rem
        except Exception:
            return 0.0

    def exposure_for_runner(self, market_id: str, selection_id: int) -> float:
        """
        Return current *unmatched* liability for a runner (summing both sides, if any).
        """
        try:
            mp = self.get_live(str(market_id))
            total = 0.0
            for side in ("BACK", "LAY"):
                lo = mp.get((int(selection_id), side))
                if not lo:
                    continue
                rem = max(0.0, float(lo.remaining))
                if rem <= 1e-12:
                    continue
                if side == "LAY":
                    total += max(0.0, (float(lo.price) - 1.0) * rem)
                else:
                    total += rem
            return float(total)
        except Exception:
            return 0.0

    # =================== client IO =================== #
    async def _place_via_client(self, qi: QuoteIntent) -> Tuple[str, int, float]:
        self._inc_msg_cost()
        if hasattr(self.orders, "place_limit"):
            order_id, ack_ts_ms, remaining = await self.orders.place_limit(qi)
            return str(order_id), int(ack_ts_ms), float(remaining)
        # Fallback: some adapters may call place_limit_order(qi) and return raw dict
        if hasattr(self.orders, "place_limit_order"):
            data = await self.orders.place_limit_order(qi)
            order_id = str((data or {}).get("betId", "") or (data or {}).get("bet_id", "") or "")
            ack_ts_ms = int((data or {}).get("placedDateMs", 0) or 0)
            remaining = float((data or {}).get("sizeRemaining", float(qi.size) or 0.0) or 0.0)
            return order_id, ack_ts_ms, remaining
        raise AttributeError("OrdersClient has no supported place method")

    async def _replace_via_client(self, qi: QuoteIntent, new_price: float) -> Tuple[str, int]:
        self._inc_msg_cost()
        if hasattr(self.orders, "replace_price"):
            order_id = self.find_live_order_id(qi.market_id, qi.selection_id, qi.side)
            if not order_id:
                raise RuntimeError("replace_price: no existing order id")
            new_id, ack_ts_ms = await self.orders.replace_price(order_id, float(new_price))
            return str(new_id), int(ack_ts_ms)
        if hasattr(self.orders, "replace_order"):
            data = await self.orders.replace_order(qi, float(new_price))
            new_id = str((data or {}).get("betId", "") or (data or {}).get("bet_id", "") or "")
            ack_ts_ms = int((data or {}).get("placedDateMs", 0) or 0)
            return new_id, ack_ts_ms
        raise AttributeError("OrdersClient has no supported replace method")

    async def _cancel_via_client(self, qi: QuoteIntent) -> int:
        self._inc_msg_cost()
        if hasattr(self.orders, "cancel_order"):
            order_id = self.find_live_order_id(qi.market_id, qi.selection_id, qi.side)
            if not order_id:
                # nothing to cancel
                return self._tick()
            ack_ts_ms = await self.orders.cancel_order(order_id)
            return int(ack_ts_ms)
        if hasattr(self.orders, "cancel"):
            order_id = self.find_live_order_id(qi.market_id, qi.selection_id, qi.side)
            if not order_id:
                return self._tick()
            ack_ts_ms = await self.orders.cancel(order_id)
            return int(ack_ts_ms)
        if hasattr(self.orders, "cancel_orders"):
            # bulk shape; use single
            order_id = self.find_live_order_id(qi.market_id, qi.selection_id, qi.side)
            data = await self.orders.cancel_orders([{"betId": order_id}])
            # best-effort timestamp
            return self._tick()
        raise AttributeError("OrdersClient has no supported cancel method")

    # =================== per-minute op counters =================== #
    def _prune_ops_window(self, now_ms: int) -> None:
        cutoff = int(now_ms) - 60_000
        while self._place_ts_1m and self._place_ts_1m[0] < cutoff:
            self._place_ts_1m.popleft()
        while self._replace_ts_1m and self._replace_ts_1m[0] < cutoff:
            self._replace_ts_1m.popleft()

    def places_per_min(self) -> int:
        self._prune_ops_window(self._tick())
        return int(len(self._place_ts_1m))

    def replaces_per_min(self) -> int:
        self._prune_ops_window(self._tick())
        return int(len(self._replace_ts_1m))

    def replace_place_ratio(self) -> float:
        p = float(self.places_per_min())
        r = float(self.replaces_per_min())
        return float(r / (p if p > 0.0 else 1.0))

    # =================== actions =================== #
    async def place(self, qi: QuoteIntent) -> None:
        # budget check
        try:
            self._budget_gate(qi.market_id)
        except Exception:
            metr.inc_pacing_budget_trip(1)
# persistence enforcement via policies (ROBUST: no broad suppress)
        try:
            p_use, reason = evaluate_persistence(
                self.profile, str(qi.side), float(qi.size), float(qi.price), str(qi.persistence)
            )
        except Exception as e:
            log_event("exec", "policy.persistence", "error", error=type(e).__name__)
        else:
            if p_use and p_use != qi.persistence:
                swapped = False
                # try normal assignment first
                try:
                    qi.persistence = p_use  # may fail if QuoteIntent is frozen
                    swapped = True
                except Exception as e1:
                    # fallback for frozen dataclass
                    try:
                        object.__setattr__(qi, "persistence", p_use)
                        swapped = True
                    except Exception as e2:
                        log_event(
                            "exec", "policy.persistence", "apply_fail",
                            market_id=qi.market_id, selection_id=qi.selection_id,
                            old=str(qi.persistence), new=str(p_use),
                            err1=type(e1).__name__, err2=type(e2).__name__,
                            reason=reason
                        )
                if swapped:
                    log_event(
                        "exec", "policy.persistence", "swap",
                        market_id=qi.market_id, selection_id=qi.selection_id,
                        reason=reason, old=str("TAKE_SP" if str(qi.persistence) != str(p_use) else p_use), new=str(p_use)
                    )

        if self.dry_run:
            order_id = f"DRY-{uuid.uuid4().hex[:12]}"
            ack_ts_ms = self._tick()
            remaining = float(qi.size)
            self._register_local(qi, order_id, price=qi.price, remaining=remaining)
            # counters
            with contextlib.suppress(Exception):
                self._place_ts_1m.append(int(ack_ts_ms))
            await self.acks.on_placed(qi, order_id, ack_ts_ms)
            log_event("exec", "order.place", "dry_ok",
                      market_id=qi.market_id, selection_id=qi.selection_id,
                      side=qi.side, price=qi.price, size=qi.size)
            return

        try:
            order_id, ack_ts_ms, remaining = await self._place_via_client(qi)
            with contextlib.suppress(Exception):
                self._place_ts_1m.append(int(ack_ts_ms))
        except Exception as e:
            log_event("exec", "order.place", "fail",
                      market_id=qi.market_id, selection_id=qi.selection_id,
                      side=qi.side, error=type(e).__name__)
            return

        self._register_local(qi, order_id, price=qi.price, remaining=remaining)
        await self.acks.on_placed(qi, order_id, ack_ts_ms)

        # book-keeping: budget add (liability ~ price * size)
        with contextlib.suppress(Exception):
            self._budget_add(qi.market_id, liability=float(qi.price) * float(qi.size))

        log_event("exec", "order.place", "ok",
                  market_id=qi.market_id, selection_id=qi.selection_id,
                  side=qi.side, price=qi.price, size=qi.size)

    async def replace(self, qi: QuoteIntent, new_price: float) -> None:
        if not self._replace_lifetime_ok(qi.market_id, qi.selection_id, qi.side):
            log_event("exec", "order.replace", "skip_min_life",
                      market_id=qi.market_id, selection_id=qi.selection_id, side=qi.side)
            metr.inc_pacing_ttl_prevent(1)
            return

        order_id = self.find_live_order_id(qi.market_id, qi.selection_id, qi.side)
        if not order_id:
            log_event("exec", "order.replace", "no_live",
                      market_id=qi.market_id, selection_id=qi.selection_id, side=qi.side)
            return

        if self.dry_run:
            with contextlib.suppress(Exception):
                self._replace_ts_1m.append(self._tick())
            lo = self.get_live(qi.market_id).get((qi.selection_id, qi.side))
            if not lo:
                log_event("exec", "order.replace", "no_local",
                          market_id=qi.market_id, selection_id=qi.selection_id, side=qi.side)
                return
            lo.price = float(new_price)
            lo.ts_ms = self._tick()
            await self.acks.on_replaced(qi, lo.order_id, lo.ts_ms)
            log_event("exec", "order.replace", "dry_ok",
                      market_id=qi.market_id, selection_id=qi.selection_id,
                      side=qi.side, price=new_price)
            return

        try:
            new_id, ack_ts_ms = await self._replace_via_client(qi, new_price)
            with contextlib.suppress(Exception):
                self._replace_ts_1m.append(int(ack_ts_ms))
        except Exception as e:
            log_event("exec", "order.replace", "fail",
                      market_id=qi.market_id, selection_id=qi.selection_id,
                      side=qi.side, error=type(e).__name__)
            return

        # update local cache with new id/price
        live = self.get_live(qi.market_id)
        lo = live.get((qi.selection_id, qi.side))
        if lo:
            lo.order_id = str(new_id) or lo.order_id
            lo.price = float(new_price)
            lo.ts_ms = int(ack_ts_ms)
        await self.acks.on_replaced(qi, new_id or "", ack_ts_ms)
        log_event("exec", "order.replace", "ok",
                  market_id=qi.market_id, selection_id=qi.selection_id,
                  side=qi.side, price=new_price)

    async def cancel(self, qi: QuoteIntent) -> None:
        order_id = self.find_live_order_id(qi.market_id, qi.selection_id, qi.side)
        if not order_id:
            log_event("exec", "order.cancel", "no_live",
                      market_id=qi.market_id, selection_id=qi.selection_id, side=qi.side)
            return

        if self.dry_run:
            lo = self.get_live(qi.market_id).pop((qi.selection_id, qi.side), None)
            if not lo:
                return
            ack_ts_ms = self._tick()
            await self.acks.on_canceled(qi, lo.order_id, ack_ts_ms)
            log_event("exec", "order.cancel", "dry_ok",
                      market_id=qi.market_id, selection_id=qi.selection_id, side=qi.side)
            return

        try:
            ack_ts_ms = await self._cancel_via_client(qi)
        except Exception as e:
            log_event("exec", "order.cancel", "fail",
                      market_id=qi.market_id, selection_id=qi.selection_id,
                      side=qi.side, error=type(e).__name__)
            return

        self.get_live(qi.market_id).pop((qi.selection_id, qi.side), None)
        await self.acks.on_canceled(qi, order_id or "", ack_ts_ms)
        log_event("exec", "order.cancel", "ok",
                  market_id=qi.market_id, selection_id=qi.selection_id, side=qi.side)

    # =================== mass-cancel per market =================== #
    async def cancel_all_in_market(self, market_id: str, reason: Optional[str] = None) -> None:
        live = list(self.get_live(market_id).values())
        for lo in live:
            qi = QuoteIntent(
                intent_id=f"cancel:{lo.order_id}",
                market_id=str(lo.market_id),
                selection_id=int(lo.selection_id),
                side=str(lo.side),
                price=float(lo.price),
                size=0.0,
                tif="GTC",
                persistence="CANCEL",
                min_lifetime_ms=0,
                max_replace_rate_per_min=60,
                edge_contribs=[],
                risk_tags={"reason": reason or "cancel_all"},
                client_ts_ms=self._tick(),
            )
            with contextlib.suppress(Exception):
                await self.cancel(qi)
