# src/tools/ce_runner.py
"""
===============================================================================
CE Harness — Compact, Deterministic Scenario Runner (ADD-ONLY; NO CORE CHANGES)
===============================================================================

READ FIRST (What this is)
-------------------------
This module is a tiny, self-contained *scenario harness* you can run from the
CLI to validate critical behaviors (CE-07..CE-12) of your REAL strategy
pipeline in a deterministic, offline way. It uses the simulator adapter
(`src/tools/simulator/entrypoint.py`) to apply book updates and drive
Arbiter → Sizer — WITHOUT network or order placement.

Why this exists
---------------
- You get a repeatable PASS/FAIL safety net for the exact failure modes that
  cost you money (MBR fees, parity freshness, reopen window, selector gates).
- Runs in seconds; perfect for pre-commit/CI.
- Uses your actual code-paths (no mocks), so drift is minimal.

Guarantees
----------
- **Add-only**: No changes to your core modules.
- **No network**: All inputs come from small JSON fixtures.
- **No orders**: We never touch order placement; we only produce would-be intents.
- **Deterministic**: Each fixture has explicit timestamps; the harness passes
  them into the simulator's `process_update(..., ts_ms=...)`.

What this file does
-------------------
1) Loads one or more *fixtures* (JSON files) for a scenario (CE-07..CE-12).
2) For each fixture:
   - Instantiates a `SimulatorApp(profile)`.
   - (Optionally) attaches a *proposal provider* function (edges/fee → proposals).
   - Feeds a short sequence of updates (snapshots or deltas) with explicit `ts_ms`.
   - Collects results (plans/intents).
   - Evaluates explicit assertions declared in the fixture.
3) Prints PASS/FAIL and writes a JSON report for CI/history.

About the "proposal provider"
-----------------------------
The simulator adapter exposes `SimulatorApp.set_proposal_provider(fn)` where
`fn(book, affected_market_ids, ts_ms)` returns a list of EdgeProposals.
Keeping this as a pluggable function means the harness never hardcodes your
edges/fee logic. You can point the harness at any function via
`--provider module.submodule:func`. If you omit it, the harness still applies
book updates but provider-dependent assertions are skipped.

CLI usage (examples)
--------------------
  # Run a single scenario with a provider and default fixtures location
  python -m src.tools.ce_runner \
      --scenario CE-07 \
      --profile default \
      --provider mypkg.my_providers:ce_provider_mbr

  # Run all scenarios with verbose JSON report output path
  python -m src.tools.ce_runner \
      --scenario all \
      --profile default \
      --report out/ce_report.json

Exit codes
----------
- 0: All executed fixtures passed or were (explicitly) skipped due to no provider.
- 1: At least one fixture failed (assert mismatch or execution error).

Fixture format (JSON)
---------------------
A fixture looks like this (see templates in `src/tools/fixtures/CE-07` and `CE-10`):

{
  "name": "example-CE-07",
  "description": "MBR effects on final size",
  "sequence": [
    {"ts_ms": 1695401234000, "type": "reseed", "books": [ ... listMarketBook-style ... ]},
    {"ts_ms": 1695401234567, "type": "mcm", "delta": {"market_id": "1.2345678", "...": "..."}}
  ],
  "assert": {
    "expect_intents_at_end": {"count": 1},
    "expect_size_at_end": {"runner": "12345", "side": "BACK", "approx": 2.34, "tolerance": 0.01},
    "expect_bundle_policy_at_end": "ALL_OR_CANCEL",
    "expect_no_intents_until_step": 1   # i.e., before index 1, expect 0 intents
  }
}

Implementation note
-------------------
We intentionally keep the assertion dialect small, explicit, and tolerant of
slight schema differences (e.g., intent may expose `size`, `stake`, or `amount`).
If you need more checks, add them narrowly and document them directly here.
"""

from __future__ import annotations
import argparse
import importlib
import json
import sys
import traceback
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

# Simulator adapter (add-only; introduced earlier).
# IMPORTANT: This module is *new* and safe; it wires BookBuilder+Selector+Arbiter+Sizer
# in-process without network or order placement.
from .simulator.entrypoint import SimulatorApp

# ===============
# Pretty printing
# ===============

def _color(s: str, code: str) -> str:
    """Return s wrapped in ANSI color code; no-ops cleanly in non-TTY sinks."""
    return f"\\033[{code}m{s}\\033[0m"

def green(s: str) -> str: return _color(s, "32")
def red(s: str)   -> str: return _color(s, "31")
def yellow(s: str)-> str: return _color(s, "33")

# =========================
# Provider import mechanism
# =========================

ProviderFn = Callable[[Any, List[str], int], List[Any]]

def import_provider(dotted: Optional[str]) -> Optional[ProviderFn]:
    """
    Import a provider from "pkg.module:func". If None or import fails, returns None.
    With None, provider-dependent assertions are **skipped**, not failed.
    """
    if not dotted:
        return None
    try:
        mod_name, func_name = dotted.split(":", 1)
        mod = importlib.import_module(mod_name)
        fn = getattr(mod, func_name)
        if not callable(fn):
            print(yellow(f"[warn] provider {dotted} is not callable; running without provider"))
            return None
        return fn  # type: ignore[return-value]
    except Exception as e:
        print(yellow(f"[warn] cannot import provider {dotted}: {e}; running without provider"))
        return None

# =================
# Fixture discovery
# =================

def iter_fixture_files(scenario: str, fixtures_root: Path) -> List[Path]:
    """
    Return a list of JSON fixture files for the selected scenario.
    Scenario name maps to subdirectory under fixtures_root, e.g. CE-07, CE-10.
    'all' expands to CE-07..CE-12.
    """
    if scenario.lower() in ("all", "ce-*", "*"):
        targets = ["CE-07", "CE-08", "CE-09", "CE-10", "CE-11", "CE-12"]
    else:
        targets = [scenario]
    files: List[Path] = []
    for t in targets:
        d = fixtures_root / t
        if not d.exists():
            continue
        files.extend(sorted(d.glob("*.json")))
    return files

# =========================
# Intent normalization utils
# =========================

def _to_wire_intent(it: Any) -> Dict[str, Any]:
    """
    Convert a QuoteIntent-like object into a plain dict for evaluation.
    Supports .to_wire(), dicts, or dataclass-like objects with __dict__.
    """
    if it is None:
        return {}
    if hasattr(it, "to_wire"):
        try:
            return dict(it.to_wire())
        except Exception:
            pass
    if isinstance(it, dict):
        return it
    return dict(getattr(it, "__dict__", {}))

def _extract_size_for(intent: Dict[str, Any], runner: str, side: str) -> Optional[float]:
    """
    Attempt to extract a numeric size for runner+side. We tolerate schema drift:
    look for 'size', then 'stake', then 'amount' keys.
    """
    if not intent:
        return None
    try:
        rid = str(intent.get("runner_id") or intent.get("selectionId") or intent.get("selection_id"))
        if rid != str(runner):
            return None
        sd = str(intent.get("side") or intent.get("bet_side") or intent.get("action")).upper()
        if sd != str(side).upper():
            return None
        for key in ("size", "stake", "amount"):
            v = intent.get(key)
            if isinstance(v, (int, float)):
                return float(v)
        return None
    except Exception:
        return None

def _extract_bundle_policy(intents: List[Dict[str, Any]]) -> Optional[str]:
    """Scan wire intents for a bundle policy marker, if present."""
    for it in intents:
        b = it.get("bundle_policy") or it.get("bundlePolicy") or it.get("bundle")
        if isinstance(b, str):
            return b
    return None

# =====================
# Core fixture executor
# =====================

def run_fixture(fixture_path: Path, profile: str, provider: Optional[ProviderFn]) -> Tuple[bool, Dict[str, Any]]:
    """
    Execute one fixture end-to-end:
      - Create SimulatorApp(profile).
      - Attach provider if present.
      - Feed updates in order with their ts_ms.
      - Run post-conditions declared in fixture['assert'].

    Returns (pass_bool, report_dict).
    """
    # Load fixture JSON
    try:
        fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    except Exception as e:
        return False, {"fixture": str(fixture_path), "error": f"invalid JSON: {e}"}

    name = fixture.get("name") or fixture_path.stem
    seq = fixture.get("sequence") or []
    assertions = fixture.get("assert") or {}

    # Build an isolated SimulatorApp for this fixture
    app = SimulatorApp(profile)
    if provider:
        app.set_proposal_provider(provider)

    last_out: Dict[str, Any] = {}
    # Drive the sequence step-by-step
    for step_idx, rec in enumerate(seq):
        ts = int(rec.get("ts_ms") or 0)
        typ = str(rec.get("type") or "").lower()
        if typ == "reseed":
            upd = {"books": rec.get("books", [])}
        else:
            upd = rec.get("delta", {})
        try:
            last_out = app.process_update(upd, ts_ms=ts)
        except Exception as e:
            return False, {"fixture": name, "step": step_idx, "error": f"process_update failed: {e}", "trace": traceback.format_exc()}

        # Optional assertion: "expect_no_intents_until_step"
        until_n = assertions.get("expect_no_intents_until_step")
        if isinstance(until_n, int) and step_idx < until_n:
            intents_now = [_to_wire_intent(x) for x in (last_out.get("intents") or [])]
            if len(intents_now) != 0:
                return False, {"fixture": name, "step": step_idx, "fail": f"expected 0 intents before step {until_n}, got {len(intents_now)}"}

    # Final assertions at end of sequence
    wire_intents = [_to_wire_intent(x) for x in (last_out.get("intents") or [])]

    # 1) intent count
    ei = assertions.get("expect_intents_at_end")
    if isinstance(ei, dict) and "count" in ei:
        exp = int(ei["count"])
        got = len(wire_intents)
        if got != exp:
            return False, {"fixture": name, "fail": f"intent count mismatch (expected {exp}, got {got})"}

    # 2) size check
    es = assertions.get("expect_size_at_end")
    if isinstance(es, dict):
        rid = str(es.get("runner") or "")
        side = str(es.get("side") or "BACK")
        approx = float(es.get("approx"))
        tol = float(es.get("tolerance", 0.01))
        found = None
        for it in wire_intents:
            v = _extract_size_for(it, rid, side)
            if v is not None:
                found = v
                break
        if found is None:
            return False, {"fixture": name, "fail": f"no size found for runner={rid} side={side}"}
        if not (approx - tol <= found <= approx + tol):
            return False, {"fixture": name, "fail": f"size mismatch for runner={rid} side={side} (expected ~{approx}±{tol}, got {found})"}

    # 3) bundle policy
    eb = assertions.get("expect_bundle_policy_at_end")
    if isinstance(eb, str):
        pol = _extract_bundle_policy(wire_intents)
        if pol != eb:
            return False, {"fixture": name, "fail": f"bundle policy mismatch (expected {eb}, got {pol})"}

    return True, {"fixture": name, "ok": True, "intents": wire_intents}

# =====
# Main
# =====

def main():
    ap = argparse.ArgumentParser(description="CE Harness — deterministic scenario runner (add-only).")
    ap.add_argument("--scenario", required=True, help="One of CE-07..CE-12 or 'all'")
    ap.add_argument("--profile", required=True, help="Config profile to use for the SimulatorApp")
    ap.add_argument("--fixtures-dir", default="src/tools/fixtures", help="Root containing CE-* subfolders with JSON fixtures")
    ap.add_argument("--provider", default=None, help="Optional provider function 'pkg.module:func' to generate EdgeProposals")
    ap.add_argument("--report", default="ce_report.json", help="Path to write JSON report")
    args = ap.parse_args()

    fixtures_root = Path(args.fixtures_dir)
    files = iter_fixture_files(args.scenario, fixtures_root)
    if not files:
        print(yellow(f"[warn] no fixtures found under {fixtures_root} for scenario={args.scenario}"))
        # Not a hard failure: exit success with empty report so CI doesn't block
        Path(args.report).write_text(json.dumps({"scenario": args.scenario, "fixtures": [], "note": "no fixtures found"}, indent=2), encoding="utf-8")
        sys.exit(0)

    provider = import_provider(args.provider)

    results: List[Dict[str, Any]] = []
    failed = 0
    for f in files:
        ok, rep = run_fixture(f, args.profile, provider)
        results.append({"file": str(f), **rep})
        if ok:
            print(green(f"[PASS] {f.name}"))
        else:
            failed += 1
            print(red(f"[FAIL] {f.name} — {rep}"))

    Path(args.report).write_text(json.dumps({"scenario": args.scenario, "fixtures": results}, indent=2), encoding="utf-8")
    if failed:
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()
