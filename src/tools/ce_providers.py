# src/tools/ce_providers.py
"""
================================================================================
CE Providers — Add-only provider functions for the CE harness (rich docs edition)
================================================================================

What this module is
-------------------
A set of *pluggable* **provider functions** for the CE harness (`src/tools/ce_runner.py`).
A **provider** is a small callable that, given:
    (book, affected_market_ids, ts_ms)
returns a list of **EdgeProposals** that the simulator adapter will feed through:
    Arbiter → Sizer → QuoteIntent
in a purely **offline** context (no network, no orders).

Why it exists
-------------
- Keeps the CE runner **decoupled** from your edge implementations.
- Lets you **swap** different combinations of edges without changing the harness.
- Central place to apply **FeeGate** (per-market MBR-aware) to any raw proposals the
  edges produce, before they hit the Arbiter/Sizer.

Safety / Design
---------------
- **Add-only**: Does not modify core modules or edges.
- **Stable surface**: All providers expose the same signature:
      provider(book, affected_market_ids, ts_ms) -> list[EdgeProposal]
- **Tolerant**: When calling your edge functions we accept multiple signatures
  to avoid guessing (see `_call_edge_with_fallbacks`).
- **Fail-open by default**: If FeeGate errors, we return raw proposals so the
  CE runner can still proceed (you can switch to strict later if desired).
- **Stubs available**: Until your edges are finalized, use the stub providers
  to validate the CE harness itself (no intents will be produced).

Key terms
---------
- **book**: Your in-memory `BookBuilder` instance from the simulator adapter.
  If it exposes per-market MBR access (as your system does), `FeeGate` can
  consult it via `snapshot=book`.
- **affected_market_ids**: Small list of `marketId` strings impacted by the
  current update (the simulator supplies this).
- **ts_ms**: Event timestamp in milliseconds; passed through to edges that
  optionally use a clock for TTL/time-budget logic.

Quick start
-----------
1) **Stub run (no intents; harness smoke-test):**
   ```bash
   python -m src.tools.ce_runner --scenario CE-07 --profile default \
     --provider src.tools.ce_providers:ce_provider_stub
   ```

2) **Fee-gated multiplex (wire real edges later via env):**
   ```bash
   # Windows (cmd)
   set CE_PROFILE=default
   set CE_EDGES=pkg.edges.maker:propose,pkg.edges.parity:propose
   # Linux/macOS
   export CE_PROFILE=default
   export CE_EDGES=pkg.edges.maker:propose,pkg.edges.parity:propose

   python -m src.tools.ce_runner --scenario CE-07 --profile default \
     --provider src.tools.ce_providers:ce_provider_fee_gated_multiplex
   ```

3) **Programmatic wiring (no environment):**
   ```python
   from src.tools.ce_providers import make_fee_gated_provider
   from src.strategy.edges import maker, parity
   provider = make_fee_gated_provider("default", [maker.propose, parity.propose])
   ```

Notes
-----
- The **order** of edges in `CE_EDGES` (or the provided list) determines the
  concatenation order of raw proposals before FeeGate.
- If one edge callable raises, we **logically skip** it and continue, so one
  broken edge won’t sink a full scenario run.
- You can replace the fail-open FeeGate behavior with strict failure once your
  edges are stable (search for `fail-open` comments).

"""

from __future__ import annotations
from typing import Any, Callable, Iterable, List, Optional, Sequence
import importlib
import os

# Use your real FeeGate (per-market MBR support, etc.).
# We do NOT catch import errors here deliberately: if FeeGate cannot be imported in
# your environment, you'd want the CE run to fail loudly rather than silently skip.
from src.strategy.fee_gate import FeeGate  # type: ignore


# =============================================================================
# Utility: dynamic import of "module:function" entries
# =============================================================================

def _import_callable(dotted: str) -> Callable[..., Any]:
    """
    Import a callable specified as "pkg.module:func".

    Example:
        _import_callable("src.strategy.edges.maker:propose")

    Raises:
        ImportError / AttributeError if module/attr missing,
        TypeError if the attribute is not callable.
    """
    mod_name, func_name = dotted.split(":", 1)
    mod = importlib.import_module(mod_name)
    fn = getattr(mod, func_name)
    if not callable(fn):
        raise TypeError(f"{dotted} is not callable")
    return fn


def _call_edge_with_fallbacks(fn: Callable[..., Any], book: Any, mids: Sequence[str], ts_ms: int) -> Iterable[Any]:
    """
    Call an edge function using a small cascade of permissive signatures:

        Preferred: fn(book, mids, ts_ms)
        Fallbacks:  fn(book, mids)   OR   fn(book, ts_ms)   OR   fn(book)

    Returns:
        Any iterable (list/tuple/generator) of proposals. If all signatures fail
        with TypeError (wrong arity), returns empty list.

    Why?
        We avoid making assumptions about your eventual edge API while still
        supporting the most likely shapes.
    """
    try:
        return fn(book, mids, ts_ms)  # type: ignore[misc]
    except TypeError:
        pass
    try:
        return fn(book, mids)  # type: ignore[misc]
    except TypeError:
        pass
    try:
        return fn(book, ts_ms)  # type: ignore[misc]
    except TypeError:
        pass
    try:
        return fn(book)  # type: ignore[misc]
    except TypeError:
        return []


# =============================================================================
# Public: simple stubs (safe while edges are in flux)
# =============================================================================

def ce_provider_stub(book: Any, affected_market_ids: Sequence[str], ts_ms: int) -> List[Any]:
    """
    Minimal provider that returns no proposals.

    Use cases:
      - Validate the CE harness wiring and fixtures without producing intents.
      - Baseline run to ensure SimulatorApp + Arbiter + Sizer import paths work.

    Returns:
      [] (empty list), ensuring downstream produces zero intents.
    """
    return []


def ce_provider_maker_stub(book: Any, affected_market_ids: Sequence[str], ts_ms: int) -> List[Any]:
    """
    Placeholder for a maker edge provider. Currently returns nothing.
    Swap this for your real maker proposal function later.
    """
    return []


def ce_provider_parity_stub(book: Any, affected_market_ids: Sequence[str], ts_ms: int) -> List[Any]:
    """
    Placeholder for a parity (WIN↔PLACE) edge provider. Currently returns nothing.
    Swap this for your real parity proposal function later.
    """
    return []


def ce_provider_all_stub(book: Any, affected_market_ids: Sequence[str], ts_ms: int) -> List[Any]:
    """
    Placeholder for a multiplexed provider (maker + parity + MR + SR).
    Currently returns nothing; replace with calls to your real edges.
    """
    return []


# =============================================================================
# Public: fee-gated multiplex provider (reads CE_EDGES)
# =============================================================================

def ce_provider_fee_gated_multiplex(book: Any, affected_market_ids: Sequence[str], ts_ms: int) -> List[Any]:
    """
    Fee-gated multiplex provider.

    Discovers edge callables from env var CE_EDGES (comma-separated list like
    'pkg.edges.maker:propose,pkg.edges.parity:propose'), calls them permissively,
    concatenates their proposals, then *filters* via FeeGate using the active profile.

    Environment:
      CE_PROFILE : str  → FeeGate(profile_name=CE_PROFILE)  (default 'default')
      CE_EDGES   : str  → Comma-separated 'pkg.module:func' entries

    Behavior:
      - Each edge is called using `_call_edge_with_fallbacks` to avoid arity mismatches.
      - If an edge callable raises (not TypeError), we *skip* and continue.
      - FeeGate is applied once at the end; on Gate error we **fail-open** (return raw).

    Returns:
      List of proposals that *passed* FeeGate (or raw proposals on gate error).
    """
    profile = os.getenv("CE_PROFILE", "default")
    gate = FeeGate(profile_name=profile)

    edges_spec = os.getenv("CE_EDGES", "").strip()
    if not edges_spec:
        # Nothing configured; behave like the stub to avoid surprises.
        return []

    raw: List[Any] = []
    for item in edges_spec.split(","):
        dotted = item.strip()
        if not dotted:
            continue
        try:
            fn = _import_callable(dotted)
            out = list(_call_edge_with_fallbacks(fn, book, affected_market_ids, ts_ms)) or []
            raw.extend(out)
        except Exception:
            # Keep going; one broken edge shouldn't stop the whole run.
            # Switch to strict by re-raising here once your edges are stable.
            continue

    # Fee-filter using per-market MBR via book (if available)
    try:
        passed = gate.filter(raw, snapshot=book)
    except Exception:
        # Fail-open so the CE harness can still surface edge behavior.
        passed = raw

    return list(passed)


# =============================================================================
# Public: programmatic constructor (no environment dependency)
# =============================================================================

def make_fee_gated_provider(profile: str, edge_callables: Sequence[Callable[..., Any]]):
    """
    Build a provider that calls the given `edge_callables` and Fee-gates the union.

    Example:
        from src.tools.ce_providers import make_fee_gated_provider
        from src.strategy.edges import maker, parity
        provider = make_fee_gated_provider("default", [maker.propose, parity.propose])

    Notes:
      - Order of `edge_callables` determines raw concatenation order.
      - Like the env-based variant, this uses fail-open semantics if FeeGate raises.

    Returns:
      A provider function with signature `(book, affected_market_ids, ts_ms) -> list`.
    """
    gate = FeeGate(profile_name=profile)
    calls = list(edge_callables or [])

    def _provider(book: Any, affected_market_ids: Sequence[str], ts_ms: int) -> List[Any]:
        raw: List[Any] = []
        for fn in calls:
            try:
                out = list(_call_edge_with_fallbacks(fn, book, affected_market_ids, ts_ms)) or []
                raw.extend(out)
            except Exception:
                continue
        try:
            return list(gate.filter(raw, snapshot=book))
        except Exception:
            return raw  # fail-open; switch to strict by raising once stable

    return _provider
