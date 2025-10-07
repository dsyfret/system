"""
Location : src/strategy/bankroll.py
Purpose  : Provide the current bankroll number for sizing:
           - source: 'live' -> fetch from Betfair Accounts (with haircut)
           - source: 'fixed' -> use fixed amount from config
"""

from __future__ import annotations
import asyncio
from typing import Optional

from ..core.config import load_config
from ..core.logging import log_event
from ..betfair.accounts import AccountsClient
from ..betfair.auth import AuthManager


async def get_bankroll(profile_name: str, auth: Optional[AuthManager] = None) -> float:
    """
    Returns the bankroll to use right now (float).
    Reads risk.bankroll.{source, fixed_amount, haircut_pct} and betfair.account.wallet/base_url.
    """
    snap = load_config(profile_name)
    rb = getattr(snap, "risk", {}).get("bankroll", {})  # type: ignore[attr-defined]
    source = (rb.get("source") or "live").lower()
    haircut = float(rb.get("haircut_pct", 0.8))
    fixed_amount = float(rb.get("fixed_amount", 10000.0))

    acct_cfg = getattr(snap.betfair, "account", {}) if hasattr(snap, "betfair") else {}
    base_url = (acct_cfg.get("base_url") or "").strip() or None
    wallet = (acct_cfg.get("wallet") or "AUS").strip().upper()

    if source == "fixed":
        log_event("bankroll", "bankroll.fixed", "using fixed bankroll", amount=fixed_amount)
        return fixed_amount

    # live
    client = AccountsClient(auth=auth or AuthManager(), base_url=base_url, wallet=wallet)
    bankroll = await client.get_available_bankroll(haircut_pct=haircut)
    return bankroll


def get_bankroll_sync(profile_name: str, auth: Optional[AuthManager] = None) -> float:
    """
    Synchronous convenience wrapper. Safe to call at startup.
    If already inside an event loop, returns 0.0 (prefer the async version in that case).
    """
    try:
        return asyncio.run(get_bankroll(profile_name, auth=auth))
    except RuntimeError:
        # In an existing event loop — caller should await get_bankroll(...)
        log_event("bankroll", "bankroll.warn", "event loop detected; returning 0.0 placeholder")
        return 0.0