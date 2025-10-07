# src/betfair/accounts.py
"""
Location : src/betfair/accounts.py
Purpose  : Fetch live account funds from Betfair (available-to-bet balance).
Notes    : Uses Betfair Account API (JSON-RPC). Retries once on auth error.
Changes  : (P1) Always use AuthManager.session_token in headers; rebuild headers
           after a forced refresh so the retry uses the new token.

Hardening v1 (2025-09):
           - Add AccountFundsPoller: periodic getAccountFunds with callbacks.
           - Optional gating: caller can pause/resume new risk based on free funds.
"""

from __future__ import annotations
import os
import asyncio
from typing import Any, Dict, Optional, Tuple, Callable, Awaitable

import aiohttp

from ..core.logging import log_event
from .auth import AuthManager

DEFAULT_ACCOUNT_BASE = os.getenv(
    "BETFAIR_ACCOUNT_HOST",
    "https://api.betfair.com/exchange/account/json-rpc/v1",
)

JSONRPC_METHOD = "AccountAPING/v1.0/getAccountFunds"  # Betfair method name


class AccountsClient:
    def __init__(
        self,
        auth: Optional[AuthManager] = None,
        base_url: Optional[str] = None,
        wallet: Optional[str] = None,  # "UK" or "AUS"
        timeout_s: float = 10.0,
    ) -> None:
        self.auth = auth or AuthManager()
        self.base_url = (base_url or DEFAULT_ACCOUNT_BASE).rstrip("/")
        self.wallet = (wallet or "AUS").upper()
        self.timeout = aiohttp.ClientTimeout(total=timeout_s)

    async def get_account_funds(self) -> Dict[str, Any]:
        """
        Returns Betfair's getAccountFunds result dict, or {} on failure.
        Keys typically include: availableToBetBalance, exposure, discountRate, pointsBalance...
        """
        status, data = await self._call(JSONRPC_METHOD, {"wallet": self.wallet})
        if status == 200 and isinstance(data, dict) and "result" in data:
            res = data["result"] or {}
            log_event(
                "accounts", "accounts.ok", "account funds ok",
                wallet=self.wallet, available=res.get("availableToBetBalance")
            )
            return res
        return {}

    async def get_available_bankroll(self, haircut_pct: float = 1.0) -> float:
        """
        Returns available-to-bet balance * haircut_pct (e.g., 0.8 = use 80% as bankroll).
        """
        funds = await self.get_account_funds()
        avail = float(funds.get("availableToBetBalance", 0.0) or 0.0)
        bankroll = max(0.0, avail * float(haircut_pct))
        log_event(
            "accounts", "accounts.bankroll", "computed bankroll",
            wallet=self.wallet, available=avail, haircut_pct=haircut_pct, bankroll=bankroll
        )
        return bankroll

    # --------------- internal --------------- #

    async def _call(self, method: str, params: Dict[str, Any]) -> Tuple[int, Any]:
        """JSON-RPC call with centralized ladder (auth refresh + backoff)."""
        from .error_ladder import classify_error, backoff_seq_ms  # local import
        await self.auth.ensure_session_async()

        def _headers() -> Dict[str, str]:
            return {
                "X-Application": self.auth.app_key,
                "X-Authentication": self.auth.session_token or "",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }

        payload = {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}

        attempts = max(2, len(backoff_seq_ms()))
        last_st: int = 0
        last_data: Any = {}
        for attempt in range(attempts):
            st, data = await self._once(_headers(), payload)
            last_st, last_data = st, data
            if st in (200, 204):
                return st, data

            dec = classify_error(context="accounts.call", payload=data, http_status=st, attempt=attempt)
            if dec.refresh_session:
                log_event("accounts", "accounts.retry", "auth_refresh")
                try:
                    await self.auth.refresh_session_async()
                except Exception:
                    pass

            if dec.retry_backoff_ms and attempt < attempts - 1:
                await asyncio.sleep(max(0.0, dec.retry_backoff_ms / 1000.0))
                continue

            # brief retry for generic transport codes if ladder didn't provide backoff
            if st in (429, 500, 502, 503, 504) and attempt < attempts - 1:
                await asyncio.sleep(0.5)
                continue

            return st, data

        return last_st, last_data

    async def _once(self, headers: Dict[str, str], payload: Dict[str, Any]) -> Tuple[int, Any]:
        try:
            async with aiohttp.ClientSession(headers=headers, timeout=self.timeout) as s:
                async with s.post(self.base_url, json=payload) as resp:
                    st = resp.status
                    try:
                        data = await resp.json(content_type=None)
                    except Exception:
                        data = await resp.text()
                    if st != 200:
                        log_event(
                            "accounts", "accounts.error", "account call failed",
                            status=st, info=_safe_info(data)
                        )
                    return st, data
        except asyncio.TimeoutError:
            log_event("accounts", "accounts.error", "timeout")
            return 0, {}
        except aiohttp.ClientError as e:
            log_event("accounts", "accounts.error", "http error", error=type(e).__name__)
            return 0, {}

def _safe_info(data: Any) -> Any:
    if isinstance(data, dict):
        return {k: data.get(k) for k in ("faultcode", "faultstring", "error", "message", "loginStatus", "status") if k in data}
    return str(data)[:500]

def _looks_like_auth_error(data: Any) -> bool:
    try:
        if isinstance(data, dict):
            if str(data.get("error", "")).upper().find("AUTH") >= 0:
                return True
            if str(data.get("loginStatus", "")).upper() in ("INVALID_SESSION_INFORMATION", "NOT_AUTHORIZED"):
                return True
    except Exception:
        pass
    return False


# --------------------- Hardening: Poller --------------------- #

class AccountFundsPoller:
    """
    Periodically polls getAccountFunds, surfaces updates via callback, and can
    drive "pause new risk" gating at the caller.

    Usage:
      poller = AccountFundsPoller(
          client=AccountsClient(...),
          interval_s=20.0,
          buffer_abs=25.0,       # pause if available < 25
          buffer_pct=0.0,        # or pause if available < pct of (available at boot)
          on_update=async_fn,     # gets dict with funds and booleans
      )
      asyncio.create_task(poller.run())
    """
    def __init__(
        self,
        client: AccountsClient,
        *,
        interval_s: float = 20.0,
        buffer_abs: float = 0.0,
        buffer_pct: float = 0.0,
        on_update: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
    ) -> None:
        self.client = client
        self.interval_s = float(interval_s)
        self.buffer_abs = max(0.0, float(buffer_abs))
        self.buffer_pct = max(0.0, float(buffer_pct))
        self.on_update = on_update

        self._running = False
        self._boot_available: Optional[float] = None

    async def run(self) -> None:
        self._running = True
        while self._running:
            try:
                funds = await self.client.get_account_funds()
                available = float(funds.get("availableToBetBalance", 0.0) or 0.0)
                exposure = float(funds.get("exposure", 0.0) or 0.0)
                exposure_limit = float(funds.get("exposureLimit", 0.0) or 0.0)

                if self._boot_available is None:
                    self._boot_available = available

                min_required = self.buffer_abs
                if self.buffer_pct > 0.0 and self._boot_available is not None:
                    min_required = max(min_required, self._boot_available * self.buffer_pct)

                low = available < min_required if min_required > 0 else False

                payload = {
                    "funds": funds,
                    "available": available,
                    "exposure": exposure,
                    "exposure_limit": exposure_limit,
                    "min_required": float(min_required),
                    "low": bool(low),
                }
                if self.on_update:
                    try:
                        await self.on_update(payload)
                    except Exception:
                        pass

            except Exception:
                # log is already performed in AccountsClient; keep poller resilient
                pass

            await asyncio.sleep(max(1.0, self.interval_s))

    def stop(self) -> None:
        self._running = False
