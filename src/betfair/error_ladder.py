# src/betfair/error_ladder.py
"""
Purpose : Centralized mapping from Betfair faults / HTTP statuses to safe actions.
Usage   :
    from .error_ladder import classify_error, LadderDecision, backoff_seq_ms

    dec = classify_error(context="orders.place", payload=resp_json, http_status=st)
    if dec.refresh_session:
        await auth.refresh_session_async()
    if dec.retry_backoff_ms:
        await asyncio.sleep(dec.retry_backoff_ms / 1000)

Notes:
- Keeps UI-neutral; callers decide how to alert/pause.
- Designed for REST/JSON-RPC and streaming control responses.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List
import os
import random

# Default backoff ladder (ms)
_DEFAULT_BACKOFF_MS = [1000, 2000, 3000, 5000, 8000]

def backoff_seq_ms() -> List[int]:
    raw = os.getenv("BETFAIR_BACKOFF_MS", "")
    if raw:
        try:
            arr = [int(x.strip()) for x in raw.split(",") if x.strip()]
            if arr:
                return arr
        except Exception:
            pass
    return list(_DEFAULT_BACKOFF_MS)

def _jitter(ms: int, pct: float = 0.15) -> int:
    if ms <= 0:
        return 0
    span = int(ms * pct)
    return max(0, ms + random.randint(-span, span))

@dataclass(frozen=True)
class LadderDecision:
    retry_backoff_ms: int = 0     # sleep then retry request
    refresh_session: bool = False # perform login refresh once
    narrow_subs: bool = False     # advise caller to reduce subscriptions/batch
    pause_new_risk: bool = False  # advise caller to pause intent gating
    fatal: bool = False           # abort the operation (not the process)
    reason: str = ""              # short machine string
    info: str = ""                # short human string

def _extract_code(payload: Any) -> str:
    # Betfair variants:
    #   {"error": {"data": {"APINGException": {"errorCode": "INSUFFICIENT_FUNDS", ...}}}}
    #   {"faultCode": "...", "faultString": "..."}
    #   {"errorCode": "TOO_MUCH_DATA"}
    try:
        if isinstance(payload, dict):
            if "error" in payload and isinstance(payload["error"], dict):
                data = payload["error"].get("data") or {}
                apx = data.get("APINGException") or {}
                code = apx.get("errorCode") or data.get("errorCode") or payload["error"].get("code")
                if code:
                    return str(code)
            if "errorCode" in payload:
                return str(payload.get("errorCode"))
            if "faultCode" in payload:
                return str(payload.get("faultCode"))
            if "loginStatus" in payload and str(payload.get("loginStatus","")).upper() not in ("SUCCESS","SUCCESSFUL"," OK "):
                return "LOGIN_FAILED"
    except Exception:
        pass
    return ""

def classify_error(context: str, payload: Any = None, http_status: Optional[int] = None, attempt: int = 0) -> LadderDecision:
    code = _extract_code(payload)
    # HTTP layer first
    if http_status is not None:
        if http_status == 429:
            seq = backoff_seq_ms()
            step = seq[min(attempt, len(seq)-1)]
            return LadderDecision(retry_backoff_ms=_jitter(step), reason="http_429", info="Too Many Requests")
        if 500 <= http_status <= 599:
            seq = backoff_seq_ms()
            step = seq[min(attempt, len(seq)-1)]
            return LadderDecision(retry_backoff_ms=_jitter(step), reason=f"http_{http_status}", info="Server error")
        if http_status in (401, 403):
            # Unauthorized/Forbidden → refresh session once
            return LadderDecision(refresh_session=True, reason=f"http_{http_status}", info="Auth required")

    # JSON faults
    code_u = code.upper()
    if code_u == "BETTING_RESTRICTED_LOCATION":
        return LadderDecision(fatal=True, pause_new_risk=True, reason=code_u, info="Restricted location")
    if code_u in ("INVALID_SESSION_INFORMATION", "NO_SESSION", "AUTHORIZATION_ERROR"):
        return LadderDecision(refresh_session=True, reason=code_u, info="Invalid session")
    if code_u in ("TOO_MUCH_DATA", "SUBSCRIPTION_LIMIT_EXCEEDED", "INPUT_LIMIT_REACHED"):
        seq = backoff_seq_ms()
        step = seq[min(attempt, len(seq)-1)]
        return LadderDecision(retry_backoff_ms=_jitter(step), narrow_subs=True, reason=code_u, info="Too much data/subscriptions")
    if code_u in ("INSUFFICIENT_FUNDS", "INSUFFICIENT_BALANCE"):
        return LadderDecision(pause_new_risk=True, fatal=True, reason=code_u, info="Insufficient funds")
    if code_u in ("SERVICE_BUSY", "INTERNAL_ERROR", "UNEXPECTED_ERROR"):
        seq = backoff_seq_ms()
        step = seq[min(attempt, len(seq)-1)]
        return LadderDecision(retry_backoff_ms=_jitter(step), reason=code_u, info="Transient error")
    # default: no special action
    return LadderDecision(reason=code_u or "unknown", info="no_action")
