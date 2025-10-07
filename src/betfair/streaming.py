# src/betfair/streaming.py
"""
Location : src/betfair/streaming.py
Purpose  : WebSocket streaming client: market books (and control ops).
           - Authenticates with AuthManager (refresh on demand).
           - Supports subscribe/add/remove of market IDs.
           - Yields raw stream messages; Trader/BookBuilder do downstream normalization.

Notes:
- Reconnect with jittered backoff list from config and automatic re-auth + re-subscribe (in chunks).
- Heartbeat-aware read timeout; logs staleness before reconnect.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, AsyncIterator, Set, Callable, Awaitable
import asyncio
import json
import random
import time
import websockets
from pathlib import Path
import os

from ..core.logging import log_event
from ..core.config import load_config
from ..core.metrics import inc_stream_reconnects, set_clock_drift_s
from .auth import AuthManager


class StreamingClient:
    def __init__(self, profile_name: str) -> None:
        self.profile = profile_name
        self.cfg = load_config(profile_name)
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._auth = AuthManager()
        self._subscribed: Set[str] = set()
        self._next_msg_id = 1

        # Config knobs
        bf = getattr(self.cfg, "betfair", {}) or {}
        st_cfg = bf.get("streaming", {}) if isinstance(bf, dict) else {}
        self._hb_ms: int = int(st_cfg.get("heartbeat_ms", 2500))
        self._backoff_list_ms: List[int] = list(st_cfg.get("backoff_ms", [500, 1000, 2000, 5000, 10000]))
        if not self._backoff_list_ms:
            self._backoff_list_ms = [500, 1000, 2000, 5000, 10000]
        self._backoff_i: int = 0
        self._resub_chunk: int = int(st_cfg.get("resubscribe_chunk", 50))

        # Activity tracking
        self._last_activity_ms: int = int(time.time() * 1000)

        # Endpoint
        host = (bf.get("stream_host") or "stream-api.betfair.com")
        self._url = f"wss://{host}:443/ws"

        # Reseed callback (set by Trader)
        self._reseed_cb: Optional[Callable[[], Awaitable[None]]] = None

    def set_reseed_handler(self, cb: Optional[Callable[[], Awaitable[None]]]) -> None:
        """Register a coroutine to call after (re)connect & resubscribe completes."""
        self._reseed_cb = cb

    # ------------- lifecycle ------------- #

    async def connect(self) -> None:
        try:
            self._ws = await websockets.connect(self._url)
            self._last_activity_ms = int(time.time() * 1000)
            log_event("stream", "connect", "ok", url=self._url)
        except Exception as e:
            self._ws = None
            log_event("stream", "connect", "error", error=type(e).__name__)
            raise

    async def close(self) -> None:
        try:
            if self._ws:
                await self._ws.close()
        except Exception:
            pass
        finally:
            self._ws = None

    async def authenticate(self) -> None:
        await self._auth.ensure_session_async()
        msg = {
            "op": "authentication",
            "id": self._next_id(),
            "appKey": self._auth.app_key,
            "session": getattr(self._auth, "session_token", "") or "",
        }
        await self._send(msg)

    async def subscribe_markets(self, market_ids: List[str]) -> None:
        self._subscribed = set(str(x) for x in market_ids)
        await self._send(self._sub_msg_full(list(self._subscribed)))

    async def add_markets(self, market_ids: List[str]) -> None:
        need = [str(x) for x in market_ids if str(x) not in self._subscribed]
        if not need:
            return
        self._subscribed |= set(need)
        await self._send(self._sub_msg_add(need))

    async def remove_markets(self, market_ids: List[str]) -> None:
        drop = [str(x) for x in market_ids if str(x) in self._subscribed]
        if not drop:
            return
        self._subscribed -= set(drop)
        await self._send(self._sub_msg_remove(drop))

    async def read(self) -> AsyncIterator[Dict[str, Any]]:
        """Yield raw JSON-decoded messages; auto-reconnect on transport errors; heartbeat-aware timeout."""
        while True:
            try:
                if not self._ws:
                    await self._reconnect_and_resubscribe()
                timeout_s = max(5.0, (self._hb_ms * 2) / 1000.0)
                raw = await asyncio.wait_for(self._ws.recv(), timeout=timeout_s)  # type: ignore[union-attr]
                msg = json.loads(raw)
                try:
                    _get_simrec().write_ws(msg)
                except Exception:
                    pass
                self._last_activity_ms = int(time.time() * 1000)

                # Clock drift sampling (prefer 'pt' if present — publish time ms)
                try:
                    pt = msg.get("pt")
                    if isinstance(pt, (int, float)):
                        drift_s = max(0.0, (int(time.time() * 1000) - int(pt)) / 1000.0)
                        set_clock_drift_s(drift_s)
                except Exception:
                    pass

                yield msg
            except asyncio.TimeoutError:
                log_event("stream", "stale", "timeout", hb_ms=self._hb_ms)
                await self._reconnect_and_resubscribe()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log_event("stream", "read", "error", error=type(e).__name__)
                await self._reconnect_and_resubscribe()

    async def _reconnect_and_resubscribe(self) -> None:
        """Close, reconnect, re-authenticate, re-subscribe in chunks with jittered backoff."""
        await self.close()
        # Choose backoff from configured list with ±15% jitter
        base = int(self._backoff_list_ms[min(self._backoff_i, len(self._backoff_list_ms)-1)])
        jitter = max(0, int(base * 0.15))
        delay_ms = base + random.randint(-jitter, jitter)
        self._backoff_i = min(self._backoff_i + 1, len(self._backoff_list_ms)-1)
        await asyncio.sleep(max(0.0, delay_ms / 1000.0))
        try:
            await self.connect()
            await self.authenticate()
            # Reset backoff ladder on success
            self._backoff_i = 0
            # Re-subscribe (chunked)
            if self._subscribed:
                ids = list(self._subscribed)
                for i in range(0, len(ids), max(1, self._resub_chunk)):
                    await self._send(self._sub_msg_add(ids[i:i+self._resub_chunk]))
                    await asyncio.sleep(0)  # yield
            inc_stream_reconnects(1)
            log_event("stream", "reconnect", "ok", subs=len(self._subscribed))
            # Notify reseed-required
            try:
                if self._reseed_cb:
                    await self._reseed_cb()
            except Exception:
                log_event("stream", "reseed", "cb_error")
        except Exception as e:
            log_event("stream", "reconnect", "fail", error=type(e).__name__)
            # allow loop to try again with next backoff step
            await asyncio.sleep(1.0)

    # ------------- helpers ------------- #

    def _next_id(self) -> int:
        self._next_msg_id += 1
        return self._next_msg_id

    async def _send(self, msg: Dict[str, Any]) -> None:
        if not self._ws:
            raise RuntimeError("ws not connected")
        await self._ws.send(json.dumps(msg))

    def _sub_msg_full(self, market_ids: List[str]) -> Dict[str, Any]:
        msg = {
            "op": "marketSubscription",
            "id": self._next_id(),
            "marketFilter": {"marketIds": market_ids},
            "marketDataFilter": {
                "fields": ["EX_BEST_OFFERS", "EX_ALL_OFFERS", "EX_TRADED", "MARKET_DEF"],
                "ladderLevels": int(getattr(self.cfg, "stream_levels", 3)),
            },
        }
        return msg

    def _sub_msg_add(self, market_ids: List[str]) -> Dict[str, Any]:
        return {
            "op": "marketSubscription",
            "id": self._next_id(),
            "addMarketIds": market_ids,
        }

    def _sub_msg_remove(self, market_ids: List[str]) -> Dict[str, Any]:
        return {
            "op": "marketSubscription",
            "id": self._next_id(),
            "removeMarketIds": market_ids,
        }


# --- Simulator Recorder (add-only, feature-flagged) ---
class _SimRecorder:
    def __init__(self):
        try:
            cfg = load_config()
            rec = (((cfg or {}).get("simulator") or {}).get("record") or {})
            self.enabled = bool(rec.get("enabled", False))
            self.dir = rec.get("dir", "logs/sim")
            self.max_mb = float(rec.get("max_file_mb", 200.0))
            self.max_files = int(rec.get("max_files", 10))
            # NEW knobs
            self.normalize_ws = bool(rec.get("normalize_ws", True))    # emit per-market deltas
            self.write_raw_ws = bool(rec.get("write_raw_ws", True))    # keep legacy raw ws lines
            self.delta_prefix = str(rec.get("delta_file_prefix", "wsdelta"))
        except Exception:
            self.enabled = False
            self.dir = "logs/sim"
            self.max_mb = 200.0
            self.max_files = 10
            self.normalize_ws = True
            self.write_raw_ws = True
            self.delta_prefix = "wsdelta"

        # State
        os.makedirs(self.dir, exist_ok=True)
        self._fp_raw = None
        self._bytes_raw = 0
        self._fp_delta = None
        self._bytes_delta = 0

        self._open_new_raw()
        self._open_new_delta()

    # --- file mgmt ---
    def _open_new_raw(self):
        if not self.enabled:
            return
        ts = time.strftime("%Y%m%d-%H%M%S")
        path = Path(self.dir) / f"stream-{ts}.jsonl"
        try:
            self._fp_raw = open(path, "a", encoding="utf-8")
            self._bytes_raw = 0
            self._gc_old_files(prefix="stream-")
        except Exception:
            self._fp_raw = None
            self._bytes_raw = 0

    def _open_new_delta(self):
        if not self.enabled:
            return
        ts = time.strftime("%Y%m%d-%H%M%S")
        path = Path(self.dir) / f"{self.delta_prefix}-{ts}.jsonl"
        try:
            self._fp_delta = open(path, "a", encoding="utf-8")
            self._bytes_delta = 0
            self._gc_old_files(prefix=f"{self.delta_prefix}-")
        except Exception:
            self._fp_delta = None
            self._bytes_delta = 0

    def _gc_old_files(self, *, prefix: str):
        try:
            files = sorted(Path(self.dir).glob(f"{prefix}*.jsonl"))
            if len(files) > self.max_files:
                for f in files[: len(files) - self.max_files]:
                    try:
                        f.unlink()
                    except Exception:
                        pass
        except Exception:
            pass

    def _rotate_if_needed(self):
        # raw
        try:
            if self._fp_raw and (self._bytes_raw / (1024*1024) >= self.max_mb):
                self._fp_raw.close()
                self._open_new_raw()
        except Exception:
            pass
        # delta
        try:
            if self._fp_delta and (self._bytes_delta / (1024*1024) >= self.max_mb):
                self._fp_delta.close()
                self._open_new_delta()
        except Exception:
            pass

    # --- writers ---
    def write_ws(self, msg: dict):
        """Legacy entrypoint called by StreamingClient. Writes raw (optional) and normalized per-market delta lines."""
        if not self.enabled:
            return

        now_ms = int(time.time() * 1000)
        # Raw WS (optional; preserves current behavior)
        if self.write_raw_ws and self._fp_raw:
            rec = {"ts_ms": now_ms, "type": "ws", "msg": msg}
            line = json.dumps(rec, ensure_ascii=False)
            try:
                self._fp_raw.write(line + "\n")
                self._fp_raw.flush()
                self._bytes_raw += len(line) + 1
            except Exception:
                pass

        # Per-market deltas (from mcm.mc[])
        if self.normalize_ws and self._fp_delta:
            try:
                if msg.get("op") == "mcm":
                    ts_ms = int(msg.get("pt") or now_ms)
                    for m in (msg.get("mc") or []):
                        # Emit the market-chunk exactly as seen live; simulator will pass this to process_update()
                        mid = m.get("id") if isinstance(m, dict) else None
                        rec = {"ts_ms": ts_ms, "type": "delta", "market_id": mid, "delta": m}
                        line = json.dumps(rec, ensure_ascii=False)
                        try:
                            self._fp_delta.write(line + "\n")
                            self._fp_delta.flush()
                            self._bytes_delta += len(line) + 1
                        except Exception:
                            continue
            except Exception:
                pass

        # Rotation check
        self._rotate_if_needed()


_simrec = None
def _get_simrec():
    global _simrec
    if _simrec is None:
        _simrec = _SimRecorder()
    return _simrec
