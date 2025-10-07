# src/betfair/orders.py
"""
Location : src/betfair/orders.py
Purpose  : Minimal JSON-RPC client for placeOrders / replaceOrders / cancelOrders.
Plain    : Used by the Python executor to actually hit the Betfair API.

Change   : Provide high-level adapter methods the executor expects:
           - place_limit(qi) -> (order_id, ack_ts_ms, remaining)
           - replace_price(order_id, new_price) -> (new_order_id, ack_ts_ms)
           - cancel_order(order_id) -> ack_ts_ms

Hardening v1 (2025-09):
           - Idempotency: set customerRef and customerOrderRef using qi.intent_id
           - Strategy tagging: set customerStrategyRef = "<profile or 'pyexec'>:<edge or 'edge'>"
           - Duplicate/timeout recovery: resolve by listCurrentOrders on customer refs
           - (NEW) Centralized error ladder integration: refresh-session / backoff / pause flags
"""

from __future__ import annotations
from typing import Any, Dict, Tuple, Optional, List
import aiohttp
import asyncio
import time

from ..core.config import load_config
from ..core.logging import log_event
from ..execution.interfaces.intents import QuoteIntent
from .auth import AuthManager
from .error_ladder import classify_error, backoff_seq_ms


_PERSISTENCE_MAP = {
    "CANCEL": "LAPSE",
    "KEEP": "PERSIST",
    "TAKE_SP": "MARKET_ON_CLOSE",
}


class OrdersClient:
    def __init__(self, profile_name: str, auth: Optional[AuthManager] = None) -> None:
        snap = load_config(profile_name)
        bf = getattr(snap, "betfair", {})
        betting = bf.get("betting", {}) if isinstance(bf, dict) else {}
        self.jsonrpc_url = betting.get("jsonrpc_url", "https://api.betfair.com/exchange/betting/json-rpc/v1")
        self.timeout = aiohttp.ClientTimeout(total=float(betting.get("timeout_s", 8)))
        self.auth = auth or AuthManager()
        self.profile_name = profile_name

    # ------------- High-level adapters used by executor ------------- #

    async def place_limit(self, qi: QuoteIntent) -> Tuple[str, int, float]:
        """
        Adapter: place a limit order using a QuoteIntent and return
        (order_id, ack_ts_ms, remaining_size).
        Raises RuntimeError on failure.

        Idempotency:
          - customerRef & customerOrderRef are set to qi.intent_id (if available)
          - customerStrategyRef tags "<profile>:<edge>"
          - On duplicate/timeout, we look up the order by customer refs.
        """
        customer_ref = str(getattr(qi, "intent_id", "") or f"{qi.market_id}:{qi.selection_id}:{int(time.time()*1000)}")
        strategy_ref = self._strategy_ref_for(qi)

        attempt = 0
        max_attempts = max(3, len(backoff_seq_ms()))
        while True:
            ok, res, st = await self._place_orders_raw(
                market_id=qi.market_id,
                selection_id=qi.selection_id,
                side=qi.side,
                price=qi.price,
                size=qi.size,
                persistence=qi.persistence,
                customer_ref=customer_ref,
                customer_order_ref=customer_ref,
                customer_strategy_ref=strategy_ref,
            )
            now_ms = int(time.time() * 1000)

            if ok:
                order_id = str(res.get("betId") or res.get("orderId") or "")
                if not order_id:
                    order_id = await self._find_by_customer_ref(customer_ref)
                    if not order_id:
                        raise RuntimeError("place_failed:no_bet_id")
                remaining = float(qi.size)
                return order_id, now_ms, remaining

            # Try idempotent resolution on any failure first
            order_id = await self._find_by_customer_ref(customer_ref)
            if order_id:
                log_event("orders", "orders.place.idempotent", "resolved_duplicate", customer_ref=customer_ref, order_id=order_id)
                return order_id, now_ms, float(qi.size)

            # Ladder decision
            dec = classify_error("orders.place", payload=res, http_status=st, attempt=attempt)
            if dec.refresh_session:
                log_event("orders", "orders.place.ladder", "refresh_session")
                await self.auth.refresh_session_async()
            if dec.pause_new_risk:
                log_event("orders", "orders.place.ladder", "pause_new_risk", reason=dec.reason)
            if dec.fatal and not dec.retry_backoff_ms:
                raise RuntimeError(f"place_failed:{dec.reason}")
            if attempt >= max_attempts - 1:
                raise RuntimeError(f"place_failed:{dec.reason or 'unknown'}:{res}")
            if dec.retry_backoff_ms:
                await asyncio.sleep(dec.retry_backoff_ms / 1000.0)
            attempt += 1

    async def replace_price(self, order_id: str, new_price: float) -> Tuple[str, int]:
        """
        Adapter: replace a live order's price, returning (new_order_id, ack_ts_ms).
        On some stacks the betId is preserved; in that case return the same id.
        Raises RuntimeError on failure.
        """
        attempt = 0
        max_attempts = max(3, len(backoff_seq_ms()))
        while True:
            ok, res, st = await self._replace_orders_raw(order_id, new_price)
            now_ms = int(time.time() * 1000)
            if ok:
                # Try to find the new bet id if provided; otherwise fall back to old id
                new_id = None
                try:
                    new_id = (
                        res.get("placeInstructionReport", {})
                        or (
                            res.get("instructionReports", [{}])[0].get("placeInstructionReport", {})
                            if isinstance(res.get("instructionReports"), list) and res["instructionReports"]
                            else {}
                        )
                    ).get("betId")
                except Exception:
                    new_id = None
                return str(new_id or order_id), now_ms

            dec = classify_error("orders.replace", payload=res, http_status=st, attempt=attempt)
            if dec.refresh_session:
                log_event("orders", "orders.replace.ladder", "refresh_session")
                await self.auth.refresh_session_async()
            if dec.fatal and not dec.retry_backoff_ms:
                raise RuntimeError(f"replace_failed:{dec.reason}")
            if attempt >= max_attempts - 1:
                raise RuntimeError(f"replace_failed:{dec.reason or 'unknown'}:{res}")
            if dec.retry_backoff_ms:
                await asyncio.sleep(dec.retry_backoff_ms / 1000.0)
            attempt += 1

    async def cancel_order(self, order_id: str) -> int:
        """
        Adapter: cancel a live order by id and return ack_ts_ms.
        Raises RuntimeError on failure.
        """
        attempt = 0
        max_attempts = max(3, len(backoff_seq_ms()))
        while True:
            ok, res, st = await self._cancel_orders_raw(order_id)
            now_ms = int(time.time() * 1000)
            if ok:
                return now_ms

            dec = classify_error("orders.cancel", payload=res, http_status=st, attempt=attempt)
            if dec.refresh_session:
                log_event("orders", "orders.cancel.ladder", "refresh_session")
                await self.auth.refresh_session_async()
            if dec.fatal and not dec.retry_backoff_ms:
                raise RuntimeError(f"cancel_failed:{dec.reason}")
            if attempt >= max_attempts - 1:
                raise RuntimeError(f"cancel_failed:{dec.reason or 'unknown'}:{res}")
            if dec.retry_backoff_ms:
                await asyncio.sleep(dec.retry_backoff_ms / 1000.0)
            attempt += 1

    # ------------- Raw JSON-RPC calls (kept private) ------------- #

    async def _place_orders_raw(
        self,
        *,
        market_id: str,
        selection_id: int,
        side: str,
        price: float,
        size: float,
        persistence: str,
        customer_ref: Optional[str] = None,
        customer_order_ref: Optional[str] = None,
        customer_strategy_ref: Optional[str] = None,
    ) -> Tuple[bool, Dict[str, Any], int]:
        """
        Raw: SportsAPING/v1.0/placeOrders. Returns (ok, report_or_error_dict, http_status).
        """
        await self.auth.ensure_session_async()
        headers = {
            "X-Application": self.auth.app_key,
            "X-Authentication": self.auth.session_token or "",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        order = {
            "selectionId": int(selection_id),
            "side": side.upper(),
            "orderType": "LIMIT",
            "customerOrderRef": customer_order_ref or None,
            "limitOrder": {
                "size": float(size),
                "price": float(price),
                "persistenceType": _PERSISTENCE_MAP.get(str(persistence).upper(), "LAPSE"),
            }
        }

        # Strip None fields to keep payload tidy
        order = {k: v for k, v in order.items() if v is not None}

        payload = {
            "jsonrpc": "2.0",
            "method": "SportsAPING/v1.0/placeOrders",
            "params": {
                "marketId": market_id,
                "instructions": [order],
            },
            "id": 1
        }
        # Top-level idempotency & strategy tags
        if customer_ref:
            payload["params"]["customerRef"] = customer_ref
        if customer_strategy_ref:
            payload["params"]["customerStrategyRef"] = customer_strategy_ref

        try:
            async with aiohttp.ClientSession(headers=headers, timeout=self.timeout) as s:
                async with s.post(self.jsonrpc_url, json=payload) as resp:
                    st = resp.status
                    data = await resp.json(content_type=None)
                    if st != 200:
                        log_event("orders", "orders.place.fail", "http", status=st, info=_short(data))
                        return False, {"error": data}, st
                    try:
                        rep = data["result"]["instructionReports"][0]
                        if rep["status"] == "SUCCESS":
                            return True, {"betId": rep.get("betId") or rep.get("orderId")}, st
                        return False, rep, st
                    except Exception:
                        log_event("orders", "orders.place.fail", "bad_response", info=_short(data))
                        return False, {"error": data}, st
        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            log_event("orders", "orders.place.fail", "net", error=type(e).__name__)
            return False, {"error": str(e)}, 599

    async def _replace_orders_raw(self, bet_id: str, new_price: float) -> Tuple[bool, Dict[str, Any], int]:
        await self.auth.ensure_session_async()
        headers = {
            "X-Application": self.auth.app_key,
            "X-Authentication": self.auth.session_token or "",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        payload = {
            "jsonrpc": "2.0",
            "method": "SportsAPING/v1.0/replaceOrders",
            "params": {
                "marketId": None,  # optional if bet id unique
                "instructions": [{"betId": bet_id, "newPrice": float(new_price)}],
            },
            "id": 1
        }
        try:
            async with aiohttp.ClientSession(headers=headers, timeout=self.timeout) as s:
                async with s.post(self.jsonrpc_url, json=payload) as resp:
                    st = resp.status
                    data = await resp.json(content_type=None)
                    if st != 200:
                        log_event("orders", "orders.replace.fail", "http", status=st, info=_short(data))
                        return False, {"error": data}, st
                    try:
                        rep = data["result"]["instructionReports"][0]
                        if rep["status"] == "SUCCESS":
                            return True, rep, st
                        return False, rep, st
                    except Exception:
                        log_event("orders", "orders.replace.fail", "bad_response", info=_short(data))
                        return False, {"error": data}, st
        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            log_event("orders", "orders.replace.fail", "net", error=type(e).__name__)
            return False, {"error": str(e)}, 599

    async def _cancel_orders_raw(self, bet_id: str) -> Tuple[bool, Dict[str, Any], int]:
        await self.auth.ensure_session_async()
        headers = {
            "X-Application": self.auth.app_key,
            "X-Authentication": self.auth.session_token or "",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        payload = {
            "jsonrpc": "2.0",
            "method": "SportsAPING/v1.0/cancelOrders",
            "params": {
                "instructions": [{"betId": bet_id}],
            },
            "id": 1
        }
        try:
            async with aiohttp.ClientSession(headers=headers, timeout=self.timeout) as s:
                async with s.post(self.jsonrpc_url, json=payload) as resp:
                    st = resp.status
                    data = await resp.json(content_type=None)
                    if st != 200:
                        log_event("orders", "orders.cancel.fail", "http", status=st, info=_short(data))
                        return False, {"error": data}, st
                    try:
                        rep = data["result"]["instructionReports"][0]
                        if rep["status"] == "SUCCESS":
                            return True, rep, st
                        return False, rep, st
                    except Exception:
                        log_event("orders", "orders.cancel.fail", "bad_response", info=_short(data))
                        return False, {"error": data}, st
        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            log_event("orders", "orders.cancel.fail", "net", error=type(e).__name__)
            return False, {"error": str(e)}, 599

    # ------------- Idempotent resolution helpers ------------- #

    async def _find_by_customer_ref(self, customer_ref: str) -> str:
        """
        Query listCurrentOrders and find a betId whose customerOrderRef or customerRef matches.
        Returns betId or "" if not found.
        """
        try:
            await self.auth.ensure_session_async()
            headers = {
                "X-Application": self.auth.app_key,
                "X-Authentication": self.auth.session_token or "",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
            payload = {
                "jsonrpc": "2.0",
                "method": "SportsAPING/v1.0/listCurrentOrders",
                "params": {
                    # Some stacks allow filtering by customer refs; to be robust we fetch a window and scan.
                    "fromRecord": 0,
                    "recordCount": 200,  # bounded scan
                },
                "id": 1,
            }
            async with aiohttp.ClientSession(headers=headers, timeout=self.timeout) as s:
                async with s.post(self.jsonrpc_url, json=payload) as resp:
                    if resp.status != 200:
                        return ""
                    data = await resp.json(content_type=None)
                    cur: List[Dict[str, Any]] = (data.get("result", {}) or {}).get("currentOrders", []) if isinstance(data, dict) else []
                    for o in cur:
                        if str(o.get("customerOrderRef") or "") == customer_ref:
                            return str(o.get("betId") or "")
                        if str(o.get("customerRef") or "") == customer_ref:
                            return str(o.get("betId") or "")
                    return ""
        except Exception:
            return ""

    def _strategy_ref_for(self, qi: QuoteIntent) -> str:
        # Compose a small, stable tag without assuming optional attributes exist
        edge = getattr(qi, "edge_id", None) or getattr(qi, "edge", None) or "edge"
        profile = self.profile_name or "pyexec"
        return f"{profile}:{edge}"


def _short(data: Any) -> Any:
    try:
        if isinstance(data, dict):
            keys = ("error", "faultCode", "faultString", "message", "data", "result", "status", "instructionReports")
            return {k: data.get(k) for k in keys if k in data}
        return str(data)[:400]
    except Exception:
        return str(data)[:400]
