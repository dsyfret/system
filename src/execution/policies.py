"""
Location : src/execution/policies.py
Purpose  : Execution-time safety policies:
           - Persistence choice guards (e.g., TAKE_SP min liability).
           - In-play maker allow/deny.
           - Long SUSPENDED auto-cancel threshold.
           - (NEW) Bet-delay model gating.

Plain:
- If a quote asks for TAKE_SP but implied SP liability is below the configured
  minimum, switch to CANCEL (don’t rely on SP to convert).
- If a market stays SUSPENDED “too long”, cancel unmatched to avoid reopening/in-play risk.
- If market’s bet-delay “model” is disallowed by profile/risk config, exclude.
"""
from __future__ import annotations
from typing import Optional, Tuple, Dict, Any, Iterable
import time

from ..core.config import load_config
from ..core.logging import log_event

# ---------- helpers for config paths ----------

def _get_sp_policy_cfg(snap: Any) -> Dict[str, Any]:
    """
    Resolve the SP persistence policy from config with this precedence:
      1) execution.policies.persistence.sp
      2) execution.policy.persistence.sp   (alias)
      3) profile.sp                        (legacy)
    Returns a dict (may be empty).
    """
    exec_cfg = getattr(snap, "execution", {}) or {}
    # canonical
    pol = (exec_cfg.get("policies") or {}) or {}
    sp = ((pol.get("persistence") or {}).get("sp") or {})
    if sp:
        return sp

    # alias: execution.policy.persistence.sp
    pol_alias = (exec_cfg.get("policy") or {}) or {}
    sp_alias = ((pol_alias.get("persistence") or {}).get("sp") or {})
    if sp_alias:
        return sp_alias

    # legacy: profile.sp
    prof = getattr(snap, "profile", {}) or {}
    legacy = (prof.get("sp") or {})
    return legacy or {}

def _float_or(*vals: Any, default: float) -> float:
    for v in vals:
        try:
            if v is None:
                continue
            return float(v)
        except Exception:
            continue
    return float(default)

# ---------- Persistence (SP) ----------

def evaluate_persistence(
    profile_name: str,
    side: str,
    stake: float,
    price: float,
    requested: str,
) -> Tuple[str, str]:
    """
    Returns (persistence_to_use, reason).
      requested: "KEEP" | "CANCEL" | "TAKE_SP"

    Policy keys supported (preferred path):
      execution:
        policies:
          persistence:
            sp:
              enabled: true
              min_liability_abs: 5.0      # or 'min_liability'
              side_rules:
                BACK: CANCEL_BELOW_MIN    # or 'ALLOW'
                LAY:  CANCEL_BELOW_MIN

    Fallbacks:
      - alias path: execution.policy.persistence.sp
      - legacy path: profile.sp { take_sp_enabled, min_liability_abs }
    """
    snap = load_config(profile_name)
    sp_cfg = _get_sp_policy_cfg(snap)

    # canonical keys
    enabled = bool(sp_cfg.get("enabled", True))
    min_liab = _float_or(sp_cfg.get("min_liability_abs"), sp_cfg.get("min_liability"), default=10.0)
    side_rules = sp_cfg.get("side_rules") or {}

    # legacy compatibility
    if not sp_cfg:
        prof = getattr(snap, "profile", {}) or {}
        legacy_sp = (prof.get("sp") or {})
        enabled = bool(legacy_sp.get("take_sp_enabled", True))
        min_liab = _float_or(legacy_sp.get("min_liability_abs"), default=min_liab)
        side_rules = {}  # legacy had no side_rules; LAY-only rule below will apply

    side_u = str(side).upper()
    req = str(requested).upper()

    # If not asking for TAKE_SP, return as-is.
    if req != "TAKE_SP":
        return req, "as_requested"

    # Global enable switch
    if not enabled:
        log_event("policy", "persistence.swap", "take_sp_disabled")
        return "CANCEL", "take_sp_disabled"

    # Compute a conservative liability proxy for SP:
    # - BACK liability ~ stake
    # - LAY liability  ~ (max(price,1.01)-1) * stake
    if side_u == "BACK":
        liability = max(0.0, float(stake))
    else:  # LAY or unknown -> treat as LAY for safety
        liability = max(0.0, (max(1.01, float(price)) - 1.0) * float(stake))

    # Side-specific rule, if provided
    rule = str(side_rules.get(side_u, "")).upper() if isinstance(side_rules, dict) else ""

    if rule == "CANCEL_BELOW_MIN":
        if liability < float(min_liab):
            log_event("policy", "persistence.swap", "sp_min_liability_side_rule",
                      side=side_u, liab=liability, min_liab=min_liab)
            return "CANCEL", "sp_min_liability"
        # else allow TAKE_SP
        return "TAKE_SP", "ok"

    if rule == "ALLOW":
        return "TAKE_SP", "ok"

    # Default behavior (legacy): only LAY enforced with min-liability
    if side_u == "LAY" and liability < float(min_liab):
        log_event("policy", "persistence.swap", "sp_min_liability_legacy",
                  side=side_u, liab=liability, min_liab=min_liab)
        return "CANCEL", "sp_min_liability"

    return "TAKE_SP", "ok"

# ---------- In-play maker policy ----------

def allow_maker_in_play(profile_name: str, bet_delay_s: int) -> bool:
    """
    Returns True if maker quotes are allowed in-play under current profile policy.
    """
    snap = load_config(profile_name)
    prof = getattr(snap, "profile", {}) or {}
    ip = (prof.get("in_play") or {})
    allow = bool(ip.get("allow_maker", False))
    max_delay = int(ip.get("bet_delay_max_s", 2))
    return bool(allow and bet_delay_s <= max_delay)

# ---------- Bet-delay model gating (NEW) ----------

def bet_delay_models_allowed(profile_name: str, models: Optional[Iterable[str]]) -> bool:
    """
    Returns True if the market's bet-delay "model(s)" are allowed.
    Precedence for disallowed lists:
      1) profile.in_play.bet_delay_overrides.disallowed_models   (new)
      2) profile.in_play.bet_delay_overrides.long_delay_models   (treat as disallowed)
      3) risk.bet_delay.disallowed_models                        (global fallback)
    Missing/empty lists -> allowed.
    """
    try:
        snap = load_config(profile_name)
        prof = getattr(snap, "profile", {}) or {}
        ip = (prof.get("in_play") or {})
        ov = (ip.get("bet_delay_overrides") or {})

        # Profile-level lists
        dis1 = ov.get("disallowed_models") or []
        dis2 = ov.get("long_delay_models") or []  # treat as disallowed if present

        # Risk-level fallback
        risk_cfg = getattr(snap, "risk", {}) or {}
        dis3 = ((risk_cfg.get("bet_delay") or {}).get("disallowed_models") or [])

        disallowed = {str(x).upper() for x in list(dis1) + list(dis2) + list(dis3)}
        if not disallowed:
            return True

        present = {str(x).upper() for x in (models or [])}
        return len(present & disallowed) == 0
    except Exception:
        # Fail-open to avoid over-blocking if config is malformed
        return True

# ---------- Long SUSPENDED cancel policy ----------

def suspend_cancel_threshold_s(profile_name: str) -> int:
    """
    Read optional threshold from config, fallback to 120s.
    Config path (optional):
      execution:
        suspend:
          cancel_after_s: 120
    """
    snap = load_config(profile_name)
    exec_cfg = getattr(snap, "execution", {}) or {}
    sus = (exec_cfg.get("suspend") or {})
    return int(sus.get("cancel_after_s", 120))

def should_cancel_after_suspended(profile_name: str, suspend_started_ms: int, now_ms: Optional[int] = None) -> bool:
    """
    Return True if we should cancel unmatched because the market has been
    SUSPENDED longer than the configured threshold.
    """
    if not suspend_started_ms:
        return False
    now = now_ms if now_ms is not None else int(time.time() * 1000)
    elapsed_s = (now - int(suspend_started_ms)) / 1000.0
    return elapsed_s >= suspend_cancel_threshold_s(profile_name)
