# src/betfair/order_stream.py
"""
Location : src/betfair/order_stream.py
Purpose  : Real-time fills via Betfair Order Stream (WebSocket).
           Also contains the poll-based OrderWatcher fallback.

Plain    : OrderStreamClient connects to Betfair WS, authenticates, subscribes
           to ORDER stream, and turns matched deltas into AckTracker events.
           If WS is disabled/unavailable, OrderWatcher (poller) does the job.

Notes    : Field names in Betfair order messages can appear abbreviated.
           This client handles the common shapes:
             - 'oc' list with entries containing:
                 'id'   -> betId
                 'sm'   -> sizeMatched (cumulative)
                 'avp'  -> averagePriceMatched
           If your stream uses full names ('sizeMatched', 'averagePriceMatched'),
           it still works.
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple, Callable
import asyncio
import json
import time
import aiohttp

from ..core.config import load_config
from ..core.logging import log_event
from ..core.metrics import set_order_ws_age_s, set_clock_drift_s, inc_order_stream_reconnects
from ..betfair.auth import AuthManager
from ..execution.ack_tracker import AckTracker
from .rest import RestClient


# ----------------------------- WebSocket order stream ----------------------------- #

class OrderStreamClient:
    def __init__(self, profile_name: str, acks: AckTracker) -> None:
        snap = load_config(profile_name)
        bf = getattr(snap, "betfair", {}) or {}
        os_cfg = (bf.get("orders_stream") or {})
        self.ws_url = str(os_cfg.get("ws_url", "wss://stream-api.betfair.com:443/ws"))
        self.heartbeat_ms = int(os_cfg.get("heartbeat_ms", 5000))
        self.profile_name = profile_name
        self.acks = acks

        self.auth = AuthManager()
        # order_id -> (sizeMatched, avgPriceMatched)
        self._known: Dict[str, Tuple[float, float]] = {}
        self._running = False
        self._on_activity: Optional[Callable[[int], None]] = None
        self._last_activity_ms = int(time.time() * 1000)

    async def run(self, *, on_activity: Optional[Callable[[int], None]] = None) -> None:
        """Long-running: connects, auths, subscribes, handles messages, auto-reconnects."""
        self._running = True
        self._on_activity = on_activity
        backoff = 1.0
        while self._running:
            try:
                await self._session_loop()
                backoff = 1.0  # clean exit/reset
            except asyncio.CancelledError:
                break
            except Exception as e:
                log_event("orders", "orders.ws.error", "loop", error=type(e).__name__)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 10.0)

    def stop(self) -> None:
        self._running = False

    async def _session_loop(self) -> None:
        await self.auth.ensure_session_async()
        headers = {
            "X-Application": self.auth.app_key,
            "X-Authentication": getattr(self.auth, "session_token", "") or "",
        }
        async with aiohttp.ClientSession(headers=headers) as sess:
            async with sess.ws_connect(self.ws_url, heartbeat=self.heartbeat_ms / 1000.0) as ws:
                # Authenticate on the WS (some stacks require this op, others rely on headers only)
                await ws.send_str(json.dumps({
                    "op": "authentication",
                    "appKey": self.auth.app_key,
                    "session": getattr(self.auth, "session_token", "") or ""
                }))

                # Subscribe to ORDER stream
                await ws.send_str(json.dumps({
                    "op": "orderSubscription",
                    "orderFilter": {},  # all orders for this account
                    "initialClk": None,
                    "clk": None,
                    "conflateMs": 0
                }))
                inc_order_stream_reconnects(1)
                log_event("orders", "orders.ws", "subscribed")

                async for msg in ws:
                    now_ms = int(time.time() * 1000)
                    # bump last activity on any message
                    self._last_activity_ms = now_ms
                    if self._on_activity:
                        try:
                            self._on_activity(self._last_activity_ms)
                        except Exception:
                            pass

                    if msg.type == aiohttp.WSMsgType.CLOSED:
                        break
                    if msg.type == aiohttp.WSMsgType.ERROR:
                        raise RuntimeError("order ws transport error")
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)

                        # clock drift sample if 'pt' present
                        try:
                            pt = data.get("pt")
                            if isinstance(pt, (int, float)):
                                drift_s = max(0.0, (now_ms - int(pt)) / 1000.0)
                                set_clock_drift_s(drift_s)
                        except Exception:
                            pass

                        # 'oc' (orderChanges) containing cumulative values; handle deltas
                        if "oc" in data:
                            await self._handle_ocm(data["oc"])
                    # (binary ignored)

    async def _handle_ocm(self, oc_list: List[Dict[str, Any]]) -> None:
        """Process a list of order changes; emit incremental fills."""
        now_ms = int(time.time() * 1000)
        latest: Dict[str, Tuple[float, float, Dict[str, Any]]] = {}

        for o in oc_list:
            # Extract orderId, cumulative sizeMatched and avgPrice
            oid = str(o.get("id") or o.get("betId") or "")
            if not oid:
                continue
            size = float(o.get("sm") if o.get("sm") is not None else o.get("sizeMatched") or 0.0)
            avg = float(o.get("avp") if o.get("avp") is not None else o.get("averagePriceMatched") or 0.0)
            latest[oid] = (size, avg, o)

        for oid, (size_new, avg_new, row) in latest.items():
            size_old, avg_old = self._known.get(oid, (0.0, 0.0))
            if size_new > size_old + 1e-9:
                inc_sz = size_new - size_old
                inc_px_num = avg_new * size_new - avg_old * size_old
                inc_px = inc_px_num / inc_sz if inc_sz > 0 else (avg_new or avg_old)

                # Try to extract market/selection for completeness if present
                market_id = str(row.get("marketId") or row.get("mid") or "")
                selection_id = int(row.get("selectionId") or row.get("sid") or 0)

                await self.acks.on_fill_by_order(
                    order_id=oid,
                    size=inc_sz,
                    price=inc_px,
                    market_id=market_id or None,
                    selection_id=selection_id or None,
                    ts_ms=now_ms,
                )
            self._known[oid] = (size_new, avg_new)

    # Health helpers
    def last_activity_age_s(self) -> float:
        """Seconds since last order WS activity."""
        return max(0.0, (int(time.time() * 1000) - int(getattr(self, "_last_activity_ms", 0))) / 1000.0)

    def is_unhealthy(self) -> bool:
        """True if inactivity exceeds 2× heartbeat_ms."""
        try:
            return self.last_activity_age_s() > (self.heartbeat_ms / 1000.0) * 2.0
        except Exception:
            return False


# ----------------------------- Poll fallback ----------------------------- #

class OrderWatcher:
    def __init__(self, profile_name: str, rest: RestClient, acks: AckTracker, poll_interval_s: Optional[float] = None) -> None:
        snap = load_config(profile_name)
        bf = getattr(snap, "betfair", {}) or {}
        orders_poll = (bf.get("orders_poll") or {})
        self.poll_interval_s = float(orders_poll.get("interval_s", 1.0 if poll_interval_s is None else poll_interval_s))

        self.profile_name = profile_name
        self.rest = rest
        self.acks = acks
        self._known: Dict[str, Tuple[float, float]] = {}
        self._running = False
        self._on_activity: Optional[Callable[[int], None]] = None

    async def run(self, market_ids: Optional[List[str]] = None, *, on_activity: Optional[Callable[[int], None]] = None) -> None:
        self._running = True
        self._on_activity = on_activity
        backoff = 0.5
        while self._running:
            try:
                await self._poll_once(market_ids)
                backoff = 0.5
                await asyncio.sleep(self.poll_interval_s)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log_event("orders", "orders.watch.error", "poll", error=type(e).__name__)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10.0)

    def stop(self) -> None:
        self._running = False

    async def _poll_once(self, market_ids: Optional[List[str]]) -> None:
        try:
            data = await self.rest.list_current_orders(market_ids=market_ids)  # type: ignore[arg-type]
        except TypeError:
            data = await self.rest.list_current_orders(market_ids=list(market_ids or []))  # compatibility
        except Exception:
            return
        now_ms = int(time.time() * 1000)
        # Parse REST shape: { currentOrders: [ { betId, sizeMatched, averagePriceMatched, marketId, selectionId }, ... ] }
        orders = (data or {}).get("currentOrders", []) if isinstance(data, dict) else []
        latest: Dict[str, Tuple[float, float, Dict[str, Any]]] = {}
        for row in orders:
            oid = str(row.get("betId") or "")
            if not oid:
                continue
            size = float(row.get("sizeMatched") or 0.0)
            avg = float(row.get("averagePriceMatched") or 0.0)
            latest[oid] = (size, avg, row)
        # Emit fills
        for oid, (size_new, avg_new, row) in latest.items():
            size_old, avg_old = self._known.get(oid, (0.0, 0.0))
            if size_new > size_old + 1e-9:
                inc_sz = size_new - size_old
                inc_px_num = avg_new * size_new - avg_old * size_old
                inc_px = inc_px_num / inc_sz if inc_sz > 0 else (avg_new or avg_old)
                await self.acks.on_fill_by_order(
                    order_id=oid,
                    size=inc_sz,
                    price=inc_px,
                    market_id=str(row.get("marketId") or ""),
                    selection_id=int(row.get("selectionId") or 0),
                    ts_ms=now_ms,
                )
            self._known[oid] = (size_new, avg_new)
        # Activity callback for health
        if self._on_activity:
            try:
                self._on_activity(now_ms)
            except Exception:
                pass
