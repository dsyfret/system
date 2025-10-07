# src/betfair/auth.py
"""
Location : src/betfair/auth.py
Purpose  : Handle Betfair authentication headers and session refresh.
Modes    : "cert" (client certificate) or "password" (username/password SSO).
Inputs   : Environment variables (recommended), or pass values explicitly.
Outputs  : HTTP headers with X-Application (app key) and X-Authentication (session token).
Notes    : Never logs secrets. Certificate login is the preferred, stable mode.

(ENH 2025-09) Ladder-aware diagnostics:
  - On login/keepAlive failures, classify via error ladder and include
    ladder_reason / ladder_info in logs. No behavior changes.
"""

from __future__ import annotations
import asyncio
import os
import ssl
import logging
from typing import Dict, Optional, Any, Tuple

import aiohttp

# Default SSO host (Betfair Identity)
DEFAULT_SSO_HOST = os.getenv("BETFAIR_SSO_HOST", "https://identitysso.betfair.com/api/")

# Optional keepAlive cadence (minutes). If unset, defaults to ~17 minutes.
DEFAULT_KEEPALIVE_MINUTES = int(os.getenv("BETFAIR_KEEPALIVE_MINUTES", "17"))

# Centralized error ladder (used only for diagnostics here; no control-flow change)
try:
    from .error_ladder import classify_error
except Exception:  # pragma: no cover - keep auth robust if ladder not present
    classify_error = None  # type: ignore[assignment]

log = logging.getLogger("betfair.auth")


class AuthManager:
    """
    Simple manager for Betfair auth. Reads configuration from environment by default.

    ENV variables (recommended):
      BETFAIR_APP_KEY      : Betfair application key
      BETFAIR_AUTH_MODE    : "cert" | "password"        (default: "cert")
      BETFAIR_CERT_FILE    : path to client certificate (.crt/.pem)  [cert mode]
      BETFAIR_KEY_FILE     : path to private key (.key/.pem)         [cert mode]
      BETFAIR_USERNAME     : account username                         [password mode]
      BETFAIR_PASSWORD     : account password                         [password mode]
      BETFAIR_SSO_HOST     : SSO base URL (default provided)
      BETFAIR_SESSION      : optional existing session token
      BETFAIR_KEEPALIVE_MINUTES : keepAlive interval in minutes (default: 17)
    """

    def __init__(
        self,
        app_key: Optional[str] = None,
        mode: Optional[str] = None,
        sso_host: Optional[str] = None,
        cert_file: Optional[str] = None,
        key_file: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        session_token: Optional[str] = None,
    ) -> None:
        self.app_key = app_key or os.getenv("BETFAIR_APP_KEY", "")
        self.mode = (mode or os.getenv("BETFAIR_AUTH_MODE", "cert")).lower()
        self.sso_host = sso_host or DEFAULT_SSO_HOST
        self.cert_file = cert_file or os.getenv("BETFAIR_CERT_FILE", "")
        self.key_file = key_file or os.getenv("BETFAIR_KEY_FILE", "")
        self.username = username or os.getenv("BETFAIR_USERNAME", "")
        self.password = password or os.getenv("BETFAIR_PASSWORD", "")
        self._session_token = session_token or os.getenv("BETFAIR_SESSION", "")

        if not self.app_key:
            log.warning("Betfair app key missing (BETFAIR_APP_KEY). Only shadow runs will work.")

        if self.mode not in ("cert", "password"):
            log.warning("Unknown BETFAIR_AUTH_MODE=%s (expected 'cert' or 'password'). Falling back to 'cert'.", self.mode)
            self.mode = "cert"

        # --- Hardening additions ---
        self._lock = asyncio.Lock()
        self._keepalive_task: Optional[asyncio.Task] = None
        self._keepalive_minutes = int(DEFAULT_KEEPALIVE_MINUTES)

    # ---------- Public API ----------

    @property
    def session_token(self) -> str:
        """Expose the current session token (read-only)."""
        return self._session_token

    def get_headers(self) -> Dict[str, str]:
        """
        Return request headers. If no session token is present, attempt to login once.
        This is a synchronous convenience wrapper; for robust flows prefer ensure_session_async().
        """
        if not self._session_token:
            try:
                self._session_token = asyncio.run(self._login_once_async())
                if self._session_token:
                    os.environ["BETFAIR_SESSION"] = self._session_token  # make available to child tasks
            except RuntimeError:
                # Already inside an event loop; skip auto-login here.
                log.warning("No session token and cannot run login in current context; headers may be invalid.")
        return {
            "X-Application": self.app_key,
            "X-Authentication": self._session_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    async def ensure_session_async(self) -> str:
        """
        Ensure we have a valid session token; if missing, login now. Returns the token.
        (Single-flight guarded)
        """
        async with self._lock:
            if not self._session_token:
                self._session_token = await self._login_once_async()
                if self._session_token:
                    os.environ["BETFAIR_SESSION"] = self._session_token
            return self._session_token

    async def refresh_session_async(self) -> str:
        """
        Always perform a fresh login and replace the current token. Returns the new token.
        Use this after a 401/403 from Betfair.
        (Single-flight guarded)
        """
        async with self._lock:
            self._session_token = await self._login_once_async(force=True)
            if self._session_token:
                os.environ["BETFAIR_SESSION"] = self._session_token
            return self._session_token

    # ---------- Background keepAlive ----------

    def start_keepalive(self, minutes: Optional[int] = None) -> None:
        """
        Idempotently start the Betfair keepAlive loop.
        - minutes: override cadence; if None, use BETFAIR_KEEPALIVE_MINUTES or default.
        """
        if minutes is not None:
            try:
                self._keepalive_minutes = max(1, int(minutes))
            except Exception:
                pass
        if self._keepalive_task is None or self._keepalive_task.done():
            self._keepalive_task = asyncio.create_task(self._keepalive_loop())

    async def _keepalive_loop(self) -> None:
        """
        Periodically posts to <SSO>/keepAlive with current session to extend validity.
        If no session present we skip; if call fails we just try again next tick.
        (ENH) Logs ladder_reason/info on non-200 responses for faster diagnosis.
        """
        period_s = max(60, self._keepalive_minutes * 60)
        url = f"{self.sso_host.rstrip('/')}/keepAlive"
        headers_base = {"X-Application": self.app_key, "Accept": "application/json"}
        timeout = aiohttp.ClientTimeout(total=10)
        while True:
            try:
                await asyncio.sleep(period_s)
                tok = self._session_token
                if not tok:
                    continue
                headers = dict(headers_base)
                headers["X-Authentication"] = tok
                async with aiohttp.ClientSession(headers=headers, timeout=timeout) as s:
                    async with s.post(url) as resp:
                        data = await _safe_json(resp)
                        if resp.status != 200 and classify_error is not None:
                            dec = classify_error("auth.keepalive", payload=data, http_status=resp.status, attempt=0)
                            _log_login_failure(data, mode="keepalive", ladder_reason=dec.reason, ladder_info=dec.info)
            except asyncio.CancelledError:
                break
            except Exception:
                # swallow & continue
                continue

    # ---------- Internal helpers ----------

    async def _login_once_async(self, force: bool = False) -> str:
        if self.mode == "cert":
            return await self._login_cert_async()
        else:
            return await self._login_password_async()

    async def _login_cert_async(self) -> str:
        """
        Certificate-based login against Betfair SSO.
        - Uses client certificate + key to authenticate.
        """
        if not (self.cert_file and self.key_file):
            log.error("Cert login selected but BETFAIR_CERT_FILE / BETFAIR_KEY_FILE not set.")
            return ""

        ssl_ctx = ssl.create_default_context()
        try:
            ssl_ctx.load_cert_chain(certfile=self.cert_file, keyfile=self.key_file)
        except Exception as e:
            log.error("Failed to load client certificate/key: %s", type(e).__name__)
            return ""

        headers = {"X-Application": self.app_key, "Accept": "application/json"}
        url = f"{self.sso_host.rstrip('/')}/certlogin"
        timeout = aiohttp.ClientTimeout(total=10)

        async with aiohttp.ClientSession(headers=headers, timeout=timeout) as s:
            try:
                async with s.post(url, ssl=ssl_ctx) as resp:
                    data = await _safe_json(resp)
                    token = _extract_session_token(data)
                    if token:
                        log.info(
                            "Betfair cert login success",
                            extra={"component": "betfair.auth", "event": "auth.login_success", "fields": {"mode": "cert"}},
                        )
                        return token
                    # ladder-aware log (diagnostic only)
                    ladder_reason, ladder_info = _ladder_diag(data, resp.status)
                    _log_login_failure(data, mode="cert", ladder_reason=ladder_reason, ladder_info=ladder_info)
                    return ""
            except Exception as e:
                log.error("Betfair cert login error: %s", type(e).__name__)
                return ""

    async def _login_password_async(self) -> str:
        """
        Username/password SSO login (less preferred).
        """
        if not (self.username and self.password):
            log.error("Password login selected but BETFAIR_USERNAME / BETFAIR_PASSWORD not set.")
            return ""

        headers = {"X-Application": self.app_key, "Accept": "application/json"}
        url = f"{self.sso_host.rstrip('/')}/login"
        data = {"username": self.username, "password": self.password}
        timeout = aiohttp.ClientTimeout(total=10)

        async with aiohttp.ClientSession(headers=headers, timeout=timeout) as s:
            try:
                async with s.post(url, data=data) as resp:
                    j = await _safe_json(resp)
                    token = _extract_session_token(j)
                    if token:
                        log.info(
                            "Betfair password login success",
                            extra={"component": "betfair.auth", "event": "auth.login_success", "fields": {"mode": "password"}},
                        )
                        return token
                    # ladder-aware log (diagnostic only)
                    ladder_reason, ladder_info = _ladder_diag(j, resp.status)
                    _log_login_failure(j, mode="password", ladder_reason=ladder_reason, ladder_info=ladder_info)
                    return ""
            except Exception as e:
                log.error("Betfair password login error: %s", type(e).__name__)
                return ""

# ---------- Top-level convenience (keeps existing imports working) ----------

# A singleton-ish manager using environment config.
_manager: Optional[AuthManager] = None

def _mgr() -> AuthManager:
    global _manager
    if _manager is None:
        _manager = AuthManager()
    return _manager

def get_headers() -> Dict[str, str]:
    """Return headers, attempting a one-time login if no session is present."""
    return _mgr().get_headers()

def refresh_session() -> None:
    """
    Force a fresh login now (sync wrapper).
    Use if you just got a 401/403. Safe to call; ignores errors.
    """
    try:
        asyncio.run(_mgr().refresh_session_async())
    except RuntimeError:
        # If already in an event loop, do nothing here; caller can await refresh_session_async instead.
        pass


# ---------- Small utilities (no secrets logged) ----------

async def _safe_json(resp: aiohttp.ClientResponse) -> dict:
    """Attempt to parse JSON; return {} on failure."""
    try:
        return await resp.json(content_type=None)
    except Exception:
        try:
            text = await resp.text()
            return {"_raw": text, "_status": resp.status}
        except Exception:
            return {"_status": resp.status}

def _extract_session_token(payload: dict) -> str:
    """
    Betfair SSO typically returns 'sessionToken' and 'loginStatus' fields on success.
    We avoid strict schema assumptions and just look for a plausible token.
    """
    if not isinstance(payload, dict):
        return ""
    token = payload.get("sessionToken") or payload.get("token")
    status = str(payload.get("loginStatus", "")).upper()
    if token and (not status or status in ("SUCCESS", "SUCCESSFUL")):
        return str(token)
    return ""

def _ladder_diag(payload: dict, http_status: int) -> Tuple[str, str]:
    """
    Ask the centralized ladder for a diagnostic label for this auth response.
    Returns (reason, info). Purely for logging; does not change behavior.
    """
    if classify_error is None:
        return "", ""
    try:
        dec = classify_error("auth.login", payload=payload, http_status=http_status, attempt=0)
        return str(dec.reason or ""), str(dec.info or "")
    except Exception:
        return "", ""

def _log_login_failure(payload: dict, *, mode: str = "", ladder_reason: str = "", ladder_info: str = "") -> None:
    # Log a non-sensitive summary for diagnostics
    info = {k: payload.get(k) for k in ("loginStatus", "error", "status", "_status") if isinstance(payload, dict) and k in payload}
    fields = {"info": info}
    if mode:
        fields["mode"] = mode
    if ladder_reason:
        fields["ladder_reason"] = ladder_reason
    if ladder_info:
        fields["ladder_info"] = ladder_info
    extra = {"component": "betfair.auth", "event": "auth.login_failure", "fields": fields}
    log.warning("Betfair login failed", extra=extra)
