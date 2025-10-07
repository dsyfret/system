# src/execution/executor_py/router.py
"""
Location : src/execution/executor_py/router.py
Purpose  : Per-market state machine; routes intents; now cancels after long SUSPENDED.
           + NEW: serve() loop that consumes intents and performs place/replace/cancel.
           + NEW (2025-09): Bundle policy enforcement:
               - ALL_OR_CANCEL: if any leg fails, cancel sister legs already placed in the bundle and
                 mark the bundle failed so subsequent legs are ignored.
               - BEST_EFFORT: tolerate partials (no forced cancels).
"""
from __future__ import annotations
from typing import Dict, Any, Optional, Set, List, Tuple
import time
import asyncio

from ...core.logging import log_event
from ..interfaces.queue import IPC
from ..interfaces.intents import QuoteIntent
from ..interfaces.reports import ExecutionReport
from .order_manager import OrderManager
from .ack_tracker import AckTracker
from ...betfair.orders import OrdersClient
from ..policies import should_cancel_after_suspended


class ExecutorRouter:
    def __init__(self, profile_name: str, ipc: IPC, order_manager: OrderManager | None = None) -> None:
        self.profile = profile_name
        self.ipc = ipc
        self.acks = order_manager.acks if order_manager else AckTracker(ipc)
        self.orders = order_manager.orders if order_manager else OrdersClient(profile_name)
        self.order_manager = order_manager or OrderManager(profile_name, ipc, self.orders, self.acks)

        # market_id -> {status,in_play,bet_delay,suspend_started_ms,canceled_due_to_suspend}
        self._state: Dict[str, Dict[str, Any]] = {}

        # ---------- Bundles (NEW) ----------
        # Track intent membership we *see* come through the router (even if placement fails)
        # bundle_id -> set(intent_id)
        self._bundle_members_seen: Dict[str, Set[str]] = {}
        # bundle_id -> policy ("ALL_OR_CANCEL" | "BEST_EFFORT")
        self._bundle_policy: Dict[str, str] = {}
        # bundles marked failed (we will ignore subsequent legs safely)
        self._bundle_failed: Set[str] = set()

    async def serve(self) -> None:
        """
        Executor main loop: consume QuoteIntents and perform place/replace/cancel.
        Idempotent: if nothing changed, we do nothing.
        """
        async for qi in self.ipc.iter_intents():
            try:
                # ---------- Bundle book-keeping (always record) ----------
                bid = (qi.bundle_id or "").strip() or None
                if bid:
                    self._bundle_members_seen.setdefault(bid, set()).add(qi.intent_id)
                    if qi.bundle_policy:
                        self._bundle_policy[str(bid)] = str(qi.bundle_policy)

                # If bundle previously failed and policy is ALL_OR_CANCEL: ignore new legs
                if bid and (bid in self._bundle_failed) and (self._bundle_policy.get(bid) == "ALL_OR_CANCEL"):
                    log_event(
                        "exec", "router.bundle.ignore_after_fail", "skip",
                        bundle_id=bid, intent_id=qi.intent_id
                    )
                    # Publish a REJECT so upstream knows this leg won’t run
                    try:
                        await self._publish_reject(qi, reason="BUNDLE_FAILED")
                    except Exception:
                        pass
                    continue

                # ---------- Normal route logic ----------
                live_id = self.order_manager.find_live_order_id(qi.market_id, qi.selection_id, qi.side)
                if not live_id:
                    await self.order_manager.place(qi)
                else:
                    # If price differs (beyond fp tolerance), replace; else no-op
                    live = self.order_manager.get_live(qi.market_id).get((qi.selection_id, qi.side))
                    if (not live) or (abs(float(live.price) - float(qi.price)) > 1e-9):
                        await self.order_manager.replace(qi, new_price=qi.price)
                    else:
                        log_event(
                            "exec", "router.noop", "unchanged",
                            market_id=qi.market_id, selection_id=qi.selection_id, side=qi.side, price=qi.price
                        )

            except asyncio.CancelledError:
                raise
            except Exception as e:
                # Publish REJECT so the strategy is aware of the failure
                try:
                    await self._publish_reject(qi, reason=type(e).__name__)
                except Exception:
                    pass
                log_event(
                    "exec", "router.error", "intent_failed",
                    market_id=qi.market_id, selection_id=qi.selection_id, side=qi.side, error=type(e).__name__
                )

                # ---------- Bundle enforcement on failure ----------
                # If this leg belongs to an ALL_OR_CANCEL bundle, mark failed and cancel sisters already placed.
                bid = (qi.bundle_id or "").strip() or None
                if bid and self._bundle_policy.get(bid) == "ALL_OR_CANCEL":
                    if bid not in self._bundle_failed:
                        self._bundle_failed.add(bid)
                        try:
                            await self._cancel_bundle_sisters(bid, exclude_intent_id=qi.intent_id)
                        except Exception:
                            # best-effort; do not re-raise
                            pass

    # ---------------- Bundle helpers ---------------- #

    async def _cancel_bundle_sisters(self, bundle_id: str, exclude_intent_id: Optional[str] = None) -> None:
        """
        Cancel already-placed sister legs for a bundle using AckTracker knowledge.
        We do *not* invent structure: we only cancel orders we can resolve via live-cache.

        Strategy:
          - Ask AckTracker for bundle_summary (members + placed/canceled counts).
          - For each member != exclude:
              - Resolve (order_id, market_id, selection_id) from AckTracker’s internal fill map.
              - Resolve side by scanning OrderManager live-cache in that market for the order_id.
              - Build a minimal QuoteIntent and call order_manager.cancel(qi).
        """
        bid = str(bundle_id)
        try:
            summary = self.acks.bundle_summary(bid)
        except Exception:
            summary = {"members": [], "placed": 0, "canceled": 0, "filled_size": {}}

        members: List[str] = list(summary.get("members", []) or [])
        # Router-side seen intents (may include ones that never placed)
        seen_extra = list(self._bundle_members_seen.get(bid, set()))
        # Prefer max coverage
        if seen_extra:
            for iid in seen_extra:
                if iid not in members:
                    members.append(iid)

        # Cancel each resolvable live order belonging to the bundle
        cancel_tasks: List[asyncio.Task] = []
        for iid in members:
            if exclude_intent_id and iid == exclude_intent_id:
                continue

            # Resolve (order_id, market_id, selection_id) from AckTracker._fills
            # We avoid changing AckTracker’s API; best-effort access.
            try:
                order_id, _, _, market_id, selection_id = self.acks._fills.get(iid, ("", 0.0, 0.0, "", 0))  # type: ignore[attr-defined]
            except Exception:
                order_id, market_id, selection_id = "", "", 0

            if not order_id or not market_id or not selection_id:
                # Not placed (or not known) → nothing to cancel
                continue

            # Resolve side by scanning live-cache for this (market_id, order_id)
            side = self._resolve_side_by_order(market_id, order_id)
            if not side:
                # Could not map to a live order → skip
                continue

            # Build a minimal QuoteIntent to drive cancel()
            qi = self._build_minimal_qi_for_cancel(
                intent_id=f"bundle-cancel:{iid}",
                market_id=str(market_id),
                selection_id=int(selection_id),
                side=side,
            )
            log_event(
                "exec", "router.bundle.cancel", "sister",
                bundle_id=bid, intent_id=iid, market_id=market_id, selection_id=selection_id, side=side
            )
            try:
                cancel_tasks.append(asyncio.create_task(self.order_manager.cancel(qi)))
            except Exception:
                # best-effort
                pass

        if cancel_tasks:
            await asyncio.gather(*cancel_tasks, return_exceptions=True)

    def _resolve_side_by_order(self, market_id: str, order_id: str) -> Optional[str]:
        """
        Search the live-cache for a (selection, side) whose LiveOrder.order_id matches.
        Returns "BACK"/"LAY" or None if not found.
        """
        try:
            mp = self.order_manager.get_live(market_id)
            for (sel, side), lo in mp.items():
                if str(lo.order_id) == str(order_id):
                    return str(side)
        except Exception:
            pass
        return None

    def _build_minimal_qi_for_cancel(self, *, intent_id: str, market_id: str, selection_id: int, side: str) -> QuoteIntent:
        """
        Create a minimal QuoteIntent sufficient for OrderManager.cancel().
        Only market_id, selection_id, side are used by cancel(); other fields are placeholders.
        """
        return QuoteIntent(
            intent_id=str(intent_id),
            market_id=str(market_id),
            selection_id=int(selection_id),
            side=str(side).upper(),       # type: ignore[arg-type]
            price=1.01,
            size=0.0,
            tif="GTC",                    # type: ignore[arg-type]
            persistence="CANCEL",         # type: ignore[arg-type]
            min_lifetime_ms=0,
            max_replace_rate_per_min=60,
            edge_contribs=[],
            risk_tags={"reason": "bundle_enforcement"},
            client_ts_ms=int(time.time() * 1000),
        )

    async def _publish_reject(self, qi: QuoteIntent, *, reason: str) -> None:
        er = ExecutionReport(
            intent_id=qi.intent_id,
            order_id="",
            market_id=qi.market_id,
            selection_id=qi.selection_id,
            action="REJECT",
            ack_ts_ms=int(time.time() * 1000),
            reject_code=str(reason),
        )
        await self.ipc.publish_report(er)

    # ---------------- Market-definition path (unchanged) ---------------- #

    def apply_market_def(self, market_id: Optional[str], *, status: Optional[str], in_play: Optional[bool], bet_delay: Optional[int]) -> None:
        if not market_id:
            return
        st = self._state.setdefault(market_id, {
            "status": "OPEN", "in_play": False, "bet_delay": 0,
            "suspend_started_ms": None, "canceled_due_to_suspend": False
        })
        now_ms = int(time.time() * 1000)
        if status is not None:
            prev = st["status"]
            st["status"] = status
            # Track suspend window
            if status == "SUSPENDED":
                if st["suspend_started_ms"] is None:
                    st["suspend_started_ms"] = now_ms
                    st["canceled_due_to_suspend"] = False
                else:
                    # Check threshold on repeated defs while suspended
                    if (not st["canceled_due_to_suspend"]) and should_cancel_after_suspended(self.profile, st["suspend_started_ms"], now_ms):
                        # Fire-and-forget async cancel all
                        try:
                            asyncio.create_task(self.order_manager.cancel_all_in_market(market_id, reason="long_suspended"))
                            st["canceled_due_to_suspend"] = True
                            log_event("exec", "router.suspend_cancel", "triggered", market_id=market_id)
                        except Exception:
                            pass
            else:
                # Any non-suspended state clears timer/flag
                st["suspend_started_ms"] = None
                st["canceled_due_to_suspend"] = False

        if in_play is not None:
            st["in_play"] = bool(in_play)
        if bet_delay is not None:
            st["bet_delay"] = int(bet_delay)

    def rebuild_live_cache(self, current_orders_payload: Dict[str, Any]) -> None:
        try:
            self.order_manager.rebuild_from_list_current_orders(current_orders_payload)
            log_event("exec", "router.rebuild", "ok")
        except Exception as e:
            log_event("exec", "router.rebuild", "error", error=type(e).__name__)

    # --------------- Direct action proxies --------------- #

    async def place(self, qi: QuoteIntent) -> None:
        await self.order_manager.place(qi)

    async def replace(self, qi: QuoteIntent, new_price: float) -> None:
        await self.order_manager.replace(qi, new_price)

    async def cancel(self, qi: QuoteIntent) -> None:
        await self.order_manager.cancel(qi)
