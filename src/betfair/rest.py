# src/betfair/rest.py
"""
Location : src/betfair/rest.py
Purpose  : Thin REST client for discovery and resync/seed calls:
           - listMarketCatalogue (discovery)
           - listMarketBook (seed books after reconnect)
           - listCurrentOrders (seed open orders/fills after reconnect)

Notes:
- Reads app key + session token directly from AuthManager.
- Provides virtualise/roll-up knobs so REST view matches your streaming lens.
- (NEW) normalize_market_book exposes marketBaseRate (commission) when present.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
import asyncio
import aiohttp

from ..core.config import load_config
from ..core.logging import log_event
from .auth import AuthManager
from pathlib import Path
import os
import time
import json


class RestClient:
    def __init__(self, profile_name: str, auth: Optional[AuthManager] = None, timeout_s: Optional[float] = None) -> None:
        snap = load_config(profile_name)
        bf = getattr(snap, "betfair", {})
        rest = bf.get("rest", {}) if isinstance(bf, dict) else {}
        self.base_url = (rest.get("base_url") or "https://api.betfair.com/exchange/betting/rest/v1.0/").rstrip("/")
        self.timeout_s = float(rest.get("timeout_s", 10 if timeout_s is None else timeout_s))
        self.auth = auth or AuthManager()
        self.timeout = aiohttp.ClientTimeout(total=self.timeout_s)
        # dry-run flag (used by build_orders_client)
        exec_cfg = getattr(snap, "execution", {}) or {}
        self._dry_run = bool(exec_cfg.get("dry_run", False))

    # --------------- Public calls --------------- #

    async def list_market_catalogue(
        self,
        market_filter: Dict[str, Any],
        market_projection: Optional[List[str]] = None,
        sort: str = "FIRST_TO_START",
        max_results: int = 200,
    ) -> List[Dict[str, Any]]:
        mp = market_projection or ["EVENT", "MARKET_START_TIME", "RUNNER_METADATA"]
        payload = {
            "filter": market_filter,
            "marketProjection": mp,
            "sort": sort,
            "maxResults": max_results,
        }
        st, data = await self._post("listMarketCatalogue", payload)
        if st == 200 and isinstance(data, list):
            return data
        return []

    async def list_market_book(
        self,
        market_ids: List[str],
        *,
        virtualise: bool = True,
        depth: int = 3,
        rollup_model: str = "STAKE",
        rollup_limit: int = 0,
        price_data: Optional[List[str]] = None,
        match_projection: str = "NO_ROLLUP",
        order_projection: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Seed/refresh book(s) — handy after stream reconnect.
        - virtualise=True -> see virtualised best (matches website)
        - depth: best N levels (0..2 -> top 3)
        """
        pd = price_data or ["EX_BEST_OFFERS", "EX_TRADED"]
        price_projection = {
            "priceData": pd,
            "virtualise": bool(virtualise),
            "exBestOffersOverrides": {
                "bestPricesDepth": int(max(1, min(3, depth))),
                "rollupModel": rollup_model,
                "rollupLimit": int(rollup_limit),
            },
        }
        payload: Dict[str, Any] = {
            "marketIds": market_ids,
            "priceProjection": price_projection,
            "matchProjection": match_projection,
        }
        if order_projection:
            payload["orderProjection"] = order_projection

        st, data = await self._post("listMarketBook", payload)
        if st == 200 and isinstance(data, list):
            return data
        return []

    async def list_current_orders(
        self,
        *,
        market_ids: Optional[List[str]] = None,
        order_projection: Optional[str] = None,
        date_range: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Seed your live order state (open/exec) after reconnect.
        """
        payload: Dict[str, Any] = {
            "orderProjection": order_projection,
        }
        if market_ids:
            payload["marketIds"] = market_ids
        if date_range:
            payload["dateRange"] = date_range

        st, data = await self._post("listCurrentOrders", payload)
        if st == 200 and isinstance(data, dict):
            return data
        return {"currentOrders": [], "moreAvailable": False}

    # ---------- Optional: normalizers (for seeding BookBuilder) ---------- #

    @staticmethod
    def normalize_market_book(mb: Dict[str, Any]) -> Dict[str, Any]:
        """Return a light-normalized dict with pass-through of suspendReason/betDelayModels and MBR.

        This does **not** reshape runner ladders; it exposes market-level fields
        in a shape compatible with BookBuilder's market-def delta:
          { "market_id": ..., "market_def": { ... } }
        """
        if not isinstance(mb, dict):
            return {}
        md = mb.get("marketDefinition") or {}
        # Fallbacks from top-level when marketDefinition is absent (REST often flattens fields)
        in_play = bool(md.get("inPlay")) if "inPlay" in md else bool(mb.get("inPlay") or mb.get("inplay") or mb.get("isInplay", False))
        status = str(md.get("status") or mb.get("status") or "OPEN").upper()
        bet_delay = int(md.get("betDelay") or mb.get("betDelay") or 0)
        bsp_market = bool(md.get("bspMarket") or mb.get("bspMarket") or mb.get("isBspReconciled", False))
        off_dt = md.get("marketTime") or mb.get("marketTime")
        off_dt = int(off_dt) if off_dt else None

        # (Pass-through) suspend reason & bet delay models
        suspend_reason = md.get("suspendReason") or mb.get("suspendReason") or md.get("suspend_reason") or mb.get("suspend_reason")
        models = md.get("betDelayModels") or mb.get("betDelayModels") or md.get("bet_delay_models") or mb.get("bet_delay_models") or None
        if isinstance(models, str):
            models = [models]
        elif models is not None and not isinstance(models, list):
            models = None

        # Market base rate (commission) — often present in marketDefinition
        mbr_raw = md.get("marketBaseRate") or mb.get("marketBaseRate")
        mbr_pct = None
        if mbr_raw is not None:
            try:
                mbr_pct = float(mbr_raw)
                if mbr_pct > 1.0:
                    mbr_pct = mbr_pct / 100.0
            except Exception:
                mbr_pct = None

        # Market id
        market_id = mb.get("marketId") or md.get("marketId")

        # Reduction factors (best-effort)
        rfs = {}
        try:
            rlist = md.get("runners") or mb.get("runners") or []
            for r in rlist:
                sid = r.get("id") or r.get("selectionId")
                if sid is None:
                    continue
                sid = int(sid)
                af = float(r.get("adjustmentFactor", 0.0))
                rfs[sid] = af
        except Exception:
            rfs = {}

        md_out = {
            "in_play": in_play,
            "status": status,
            "bet_delay": bet_delay,
            "bsp_market": bsp_market,
            "off_dt": off_dt,
            "suspend_reason": str(suspend_reason) if suspend_reason else None,
            "bet_delay_models": list(models) if isinstance(models, list) else None,
            "rfs": rfs,
        }
        if mbr_pct is not None:
            md_out["market_base_rate"] = mbr_pct

        return {
            "market_id": str(market_id) if market_id else None,
            "market_def": md_out,
        }

    @classmethod
    def normalize_market_books(cls, books: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [cls.normalize_market_book(b) for b in books if isinstance(b, dict)]

    # --------------- Seed helper for trader ---------------- #

    async def get_market_catalogue_state(self, market_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Return {market_id: {suspend_reason, bet_delay_models, bet_delay, status, in_play, market_base_rate, mbr_source}}
        for the given markets. Uses listMarketBook and the same normalization as streaming.
        """
        out: Dict[str, Dict[str, Any]] = {}
        if not market_ids:
            return out
        try:
            books = await self.list_market_book(
                market_ids=market_ids,
                virtualise=True,
                depth=1,
                match_projection="NO_ROLLUP",
            )
        except Exception:
            log_event("rest", "rest.seed", "listMarketBook error")
            return out

        for b in (books or []):
            nb = self.normalize_market_book(b)
            mid = nb.get("market_id")
            md = nb.get("market_def") or {}
            if not mid or not isinstance(md, dict):
                continue
            out[str(mid)] = {
                "suspend_reason": md.get("suspend_reason"),
                "bet_delay_models": md.get("bet_delay_models"),
                "bet_delay": md.get("bet_delay"),
                "status": md.get("status"),
                "in_play": md.get("in_play"),
                "market_base_rate": md.get("market_base_rate"),
                "mbr_source": "book" if md.get("market_base_rate") is not None else None,
            }
        return out

    # --------------- Orders client factory ---------------- #

    def build_orders_client(self) -> "OrdersClientBase":
        """
        Return an object with place/replace/cancel methods the executor expects.
        If execution.dry_run is enabled, return a dummy client that logs only.
        """
        if self._dry_run:
            log_event("rest", "orders", "dry_run_client")
            return DummyOrdersClient(self)
        return OrdersClient(self)

    # --------------- Internal HTTP --------------- #

    async def _post(self, method: str, body: Dict[str, Any]) -> Tuple[int, Any]:
        """Generic POST with centralized error ladder: refresh-once, jittered backoff on 429/5xx,
        and graceful return of last status/data on exhaustion.
        """
        from .error_ladder import classify_error, backoff_seq_ms  # local import to avoid import cycles
        await self.auth.ensure_session_async()
        url = f"{self.base_url}/{method}/"
        # Always build headers using latest token
        def _headers() -> Dict[str, str]:
            return {
                "X-Application": self.auth.app_key,
                "X-Authentication": self.auth.session_token or "",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }

        attempts = max(2, len(backoff_seq_ms()))
        last_status: int = 0
        last_data: Any = {}
        for attempt in range(attempts):
            try:
                async with aiohttp.ClientSession(timeout=self.timeout) as s:
                    async with s.post(url, headers=_headers(), json=body) as resp:
                        st = resp.status
                        try:
                            data = await resp.json(content_type=None)
                        except Exception:
                            data = {"raw": await resp.text()}
                        last_status, last_data = st, data

                        if st == 200:
                            return st, data

                        # Classify and act
                        dec = classify_error(context=f"rest.{method}", payload=data, http_status=st, attempt=attempt)
                        if dec.refresh_session:
                            try:
                                await self.auth.refresh_session_async()
                                log_event("rest", "rest.retry", "auth_refresh", method=method)
                            except Exception:
                                pass
                        if dec.retry_backoff_ms and attempt < attempts - 1:
                            await asyncio.sleep(max(0.0, dec.retry_backoff_ms / 1000.0))
                            continue  # retry
                        # fallthrough: return error as-is
                        log_event("rest", "rest.error", "call failed", method=method, status=st, info=_safe_info(data))
                        return st, data
            except asyncio.TimeoutError:
                last_status, last_data = 0, {}
                log_event("rest", "rest.error", "timeout", method=method)
            except aiohttp.ClientError as e:
                last_status, last_data = 0, {}
                log_event("rest", "rest.error", "http", method=method, error=type(e).__name__)

            # brief pause between transport-level retries
            if attempt < attempts - 1:
                await asyncio.sleep(0.25)
        return last_status, last_data

def _safe_info(data: Any) -> Any:
    if isinstance(data, dict):
        keys = ("faultCode", "faultString", "error", "message", "detail", "data")
        return {k: data.get(k) for k in keys if k in data}
    return str(data)[:500]


# ---------------- OrdersClient types ---------------- #


    # --- Simulator snapshot recorder (opt-in) ---
    def start_snapshot_recorder(self, market_ids_provider=None) -> None:
        """
        Begin periodic listMarketBook snapshots to JSONL while simulator.record.enabled is true.
        market_ids_provider: optional callable returning a List[str] of active marketIds.
        If not provided, recorder remains idle (no-op).
        Safe to call multiple times.
        """
        try:
            cfg = load_config()
            rec = (((cfg or {}).get("simulator") or {}).get("record") or {})
            enabled = bool(rec.get("enabled", False))
            if not enabled:
                return
            interval = float(rec.get("snap_interval_s", 10.0))
            out_dir = rec.get("dir", "logs/sim")
            max_files = int(rec.get("max_files", 10))
            os.makedirs(out_dir, exist_ok=True)
        except Exception:
            return

        if getattr(self, "_sim_snap_task", None):
            return  # already running

        async def _snap_loop():
            def _gc():
                try:
                    files = sorted(Path(out_dir).glob("snap-*.jsonl"))
                    if len(files) > max_files:
                        for f in files[: len(files) - max_files]:
                            try:
                                f.unlink()
                            except Exception:
                                pass
                except Exception:
                    pass

            fpath = None
            f = None
            try:
                ts = time.strftime("%Y%m%d-%H%M%S")
                fpath = Path(out_dir) / f"snap-{ts}.jsonl"
                f = open(fpath, "a", encoding="utf-8")
                _gc()
                while True:
                    try:
                        mids = []
                        if callable(market_ids_provider):
                            mids = list(market_ids_provider()) or []
                        if mids:
                            books = await self.list_market_book(market_ids=mids, virtualise=True, depth=3)  # type: ignore
                            rec = {"ts_ms": int(time.time()*1000), "type": "reseed", "books": books}
                            f.write(json.dumps(rec, ensure_ascii=False) + "\\n")
                            f.flush()
                    except Exception:
                        pass
                    await asyncio.sleep(interval)
            finally:
                try:
                    if f:
                        f.close()
                except Exception:
                    pass

        self._sim_snap_task = asyncio.create_task(_snap_loop())

class OrdersClientBase:
    async def place(self, market_id: str, instructions: List[Dict[str, Any]], customer_ref: Optional[str] = None) -> Dict[str, Any]:
        raise NotImplementedError

    async def replace(self, market_id: str, instructions: List[Dict[str, Any]], customer_ref: Optional[str] = None) -> Dict[str, Any]:
        raise NotImplementedError

    async def cancel(self, market_id: str, instructions: List[Dict[str, Any]], customer_ref: Optional[str] = None) -> Dict[str, Any]:
        raise NotImplementedError

    # aliases
    async def place_orders(self, *args, **kwargs) -> Dict[str, Any]:
        return await self.place(*args, **kwargs)

    async def replace_orders(self, *args, **kwargs) -> Dict[str, Any]:
        return await self.replace(*args, **kwargs)

    async def cancel_orders(self, *args, **kwargs) -> Dict[str, Any]:
        return await self.cancel(*args, **kwargs)


class OrdersClient(OrdersClientBase):
    """Thin wrapper over RestClient._post for live mode."""

    def __init__(self, rest: RestClient) -> None:
        self._rest = rest

    async def place(self, market_id: str, instructions: List[Dict[str, Any]], customer_ref: Optional[str] = None) -> Dict[str, Any]:
        payload = {"marketId": market_id, "instructions": instructions}
        if customer_ref:
            payload["customerRef"] = customer_ref
        _st, data = await self._rest._post("placeOrders", payload)
        return data if isinstance(data, dict) else {}

    async def replace(self, market_id: str, instructions: List[Dict[str, Any]], customer_ref: Optional[str] = None) -> Dict[str, Any]:
        payload = {"marketId": market_id, "instructions": instructions}
        if customer_ref:
            payload["customerRef"] = customer_ref
        _st, data = await self._rest._post("replaceOrders", payload)
        return data if isinstance(data, dict) else {}

    async def cancel(self, market_id: str, instructions: List[Dict[str, Any]], customer_ref: Optional[str] = None) -> Dict[str, Any]:
        payload = {"marketId": market_id, "instructions": instructions}
        if customer_ref:
            payload["customerRef"] = customer_ref
        _st, data = await self._rest._post("cancelOrders", payload)
        return data if isinstance(data, dict) else {}

    # utility
    async def list_current_orders(self, **kwargs) -> Dict[str, Any]:
        return await self._rest.list_current_orders(**kwargs)


class DummyOrdersClient(OrdersClientBase):
    """No-op client for dry-run; logs the intent and returns success-shaped payloads."""

    def __init__(self, rest: RestClient) -> None:
        self._rest = rest

    async def place(self, market_id: str, instructions: List[Dict[str, Any]], customer_ref: Optional[str] = None) -> Dict[str, Any]:
        log_event("orders.dry", "place", "ok", market_id=market_id, n=len(instructions))
        return {"instructionReports": [{"status": "SUCCESS"} for _ in instructions]}

    async def replace(self, market_id: str, instructions: List[Dict[str, Any]], customer_ref: Optional[str] = None) -> Dict[str, Any]:
        log_event("orders.dry", "replace", "ok", market_id=market_id, n=len(instructions))
        return {"instructionReports": [{"status": "SUCCESS"} for _ in instructions]}

    async def cancel(self, market_id: str, instructions: List[Dict[str, Any]], customer_ref: Optional[str] = None) -> Dict[str, Any]:
        log_event("orders.dry", "cancel", "ok", market_id=market_id, n=len(instructions))
        return {"instructionReports": [{"status": "SUCCESS"} for _ in instructions]}
