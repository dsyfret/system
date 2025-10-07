"""
Location : src/betfair/ticks.py
Purpose  : Betfair odds ladder helpers (snap, step, distance, move by N ticks).
Notes    : Uses official bands:
  [1.01,2.00]:0.01, (2.00,3.00]:0.02, (3.00,4.00]:0.05, (4.00,6.00]:0.1,
  (6.00,10.0]:0.2, (10.0,20.0]:0.5, (20.0,30.0]:1, (30.0,50.0]:2,
  (50.0,100.0]:5, (100.0,1000.0]:10
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import List, Tuple

MIN_ODDS = 1.01
MAX_ODDS = 1000.0

# (low_inclusive, high_inclusive, step)
BRACKETS: List[Tuple[float, float, float]] = [
    (1.01,   2.00,   0.01),
    (2.00,   3.00,   0.02),
    (3.00,   4.00,   0.05),
    (4.00,   6.00,   0.10),
    (6.00,  10.00,   0.20),
    (10.00, 20.00,   0.50),
    (20.00, 30.00,   1.00),
    (30.00, 50.00,   2.00),
    (50.00,100.00,   5.00),
    (100.00,1000.00,10.00),
]

# Precompute starting tick index of each bracket (dedup shared boundaries).
START_IDX: List[int] = []
TOTAL_TICKS = 0
for i, (lo, hi, step) in enumerate(BRACKETS):
    if i == 0:
        START_IDX.append(0)
        count = int(round((hi - lo) / step)) + 1  # inclusive
    else:
        START_IDX.append(TOTAL_TICKS - 1)         # reuse boundary tick
        count = int(round((hi - lo) / step)) + 1  # inclusive (we share the low)
    TOTAL_TICKS += count

def _clamp(x: float) -> float:
    return max(MIN_ODDS, min(MAX_ODDS, float(x)))

def _round2(x: float) -> float:
    # Odds are displayed to 2dp; this is sufficient for all Betfair steps.
    return round(x + 1e-12, 2)

def _find_bracket_idx(p: float) -> int:
    p = _clamp(p)
    for i, (lo, hi, _s) in enumerate(BRACKETS):
        if lo - 1e-12 <= p <= hi + 1e-12:
            return i
    return len(BRACKETS) - 1

def tick_size(price: float) -> float:
    """Default size for arithmetic; at exact boundaries, this returns the *current* band size."""
    i = _find_bracket_idx(price)
    return BRACKETS[i][2]

def _step_up(price: float) -> float:
    i = _find_bracket_idx(price)
    lo, hi, s = BRACKETS[i]
    # At an exact high boundary, step uses the next band's size (if any).
    if abs(price - hi) < 1e-12 and i < len(BRACKETS) - 1:
        s = BRACKETS[i + 1][2]
    return _clamp(_round2(price + s))

def _step_down(price: float) -> float:
    i = _find_bracket_idx(price)
    lo, hi, s = BRACKETS[i]
    # At an exact low boundary (except very first), step uses previous band's size.
    if abs(price - lo) < 1e-12 and i > 0:
        s = BRACKETS[i - 1][2]
    return _clamp(_round2(price - s))

def snap(price: float, mode: str = "nearest") -> float:
    """
    Snap to legal odds. mode: 'nearest' | 'down' | 'up'.
    """
    p = _clamp(price)
    i = _find_bracket_idx(p)
    lo, hi, s = BRACKETS[i]
    # raw candidates within this band
    k = round((p - lo) / s)
    cand = _round2(lo + k * s)
    # ensure inside band
    cand = min(max(cand, lo), hi)
    if mode == "down":
        if cand > p:  # overshot
            return _step_down(cand)
        return cand
    if mode == "up":
        if cand < p:
            return _step_up(cand)
        return cand
    # nearest: compare down vs up
    down = cand if cand <= p else _step_down(cand)
    up   = cand if cand >= p else _step_up(cand)
    return up if (up - p) < (p - down) else down

def price_to_index(price: float) -> int:
    """Map a legal price to its global tick index (0 = 1.01)."""
    p = snap(price, "nearest")
    i = _find_bracket_idx(p)
    lo, hi, s = BRACKETS[i]
    k = int(round((p - lo) / s))
    return START_IDX[i] + k

def index_to_price(idx: int) -> float:
    """Inverse of price_to_index, clamped to ladder."""
    idx = max(0, min(idx, TOTAL_TICKS - 1))
    # locate bracket
    for i in range(len(BRACKETS)):
        lo, hi, s = BRACKETS[i]
        start = START_IDX[i]
        # size of this bracket inclusive
        count = int(round((hi - lo) / s)) + 1
        end = start + count - 1
        if start <= idx <= end:
            k = idx - start
            return _round2(lo + k * s)
    return MAX_ODDS

def tick_up(price: float) -> float:
    """Next legal tick above (handles band crossings)."""
    return _step_up(snap(price, "up"))

def tick_down(price: float) -> float:
    """Next legal tick below (handles band crossings)."""
    return _step_down(snap(price, "down"))

def n_ticks_from(price: float, n: int) -> float:
    """Move n ticks from price (n can be negative)."""
    base_idx = price_to_index(price)
    return index_to_price(base_idx + n)

def distance_in_ticks(p1: float, p2: float) -> int:
    """Absolute distance in ticks between two prices."""
    return abs(price_to_index(p1) - price_to_index(p2))

def spread_in_ticks(best_back: float, best_lay: float) -> int:
    """Convenience: spread measured in ticks."""
    return distance_in_ticks(best_back, best_lay)

# ---- Back-compat aliases (modules import these names elsewhere) ----
def one_tick_up(price: float) -> float:
    return tick_up(price)

def one_tick_down(price: float) -> float:
    return tick_down(price)