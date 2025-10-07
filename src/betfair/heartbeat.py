"""
Location : src/betfair/heartbeat.py
Purpose  : Server-side Heartbeat pinger for Betfair Exchange.
           If enabled, periodically posts a Heartbeat JSON-RPC request to the betting API.
           If heartbeats stop, Betfair attempts to cancel all unmatched LIMIT orders.

Config   : base.yaml
  betfair.heartbeat.enabled: true|false
  betfair.heartbeat.preferred_timeout_s: 30   # 10..300

Usage    : pinger = HeartbeatPinger(profile, auth_manager)
           asyncio.create_task(pinger.run())
"""

from __future__ import annotations
import asyncio
import json
from typing import Optional

import aiohttp

from ..core.config import load_config
from ..core.logging import log_event
from .auth import AuthManager


class HeartbeatPinger:
    def __init__(self, profile_name: str, auth: AuthManager) -> None:
        self.profile = profile_name
        self.auth = auth
        snap = load_config(profile_name)
        bf = getattr(snap, "betfair", {}) or {}
        hb = bf.get("heartbeat", {}) or {}
        self.enabled: bool = bool(hb.get("enabled", False))
        self.preferred_timeout_s: int = int(hb.get("preferred_timeout_s", 30))
        self.url: str = str((bf.get("betting") or {}).get("jsonrpc_url", "https://api.betfair.com/exchange/betting/json-rpc/v1"))
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def run(self) -> None:
        if not self.enabled:
            return
        self._running = True
        period = max(5, int(self.preferred_timeout_s * 0.5))
        while self._running:
            try:
                await self._tick()
            except asyncio.CancelledError:
                break
            except Exception:
                # swallow; retry next period
                pass
            await asyncio.sleep(period)

    async def _tick(self) -> None:
        await self.auth.ensure_session_async()
        headers = {
            "X-Application": getattr(self.auth, "app_key", ""),
            "X-Authentication": getattr(self.auth, "session_token", ""),
            "Content-Type": "application/json",
        }
        payload = {
            "jsonrpc": "2.0",
            "method": "Heartbeat",
            "params": {"preferredTimeoutSeconds": int(self.preferred_timeout_s)},
            "id": 1,
        }
        async with aiohttp.ClientSession() as sess:
            async with sess.post(self.url, headers=headers, data=json.dumps(payload)) as resp:
                _ = await resp.text()
                if resp.status != 200:
                    log_event("heartbeat", "post", "non200", status=resp.status)
                else:
                    log_event("heartbeat", "post", "ok", timeout=self.preferred_timeout_s)

    def stop(self) -> None:
        self._running = False