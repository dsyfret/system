# src/core/schemas.py
"""
Location : src/core/schemas.py
Purpose  : Typed models used across the app: market snapshots, runner books,
           edge proposals, quote plans/intents, execution reports, fills, etc.
Notes    : RunnerBook carries both DISPLAY (virtual) and RAW best levels.
           MarketState now includes bet_delay, suspend_reason, bet_delay_models.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Literal


# ------- Basic price level ------- #
@dataclass
class Level:
    price: float
    size: float


# ------- Market state / meta ------- #
@dataclass
class MarketState:
    market_id: str
    status: str = "OPEN"             # OPEN | SUSPENDED | CLOSED ...
    in_play: bool = False
    off_dt: Optional[int] = None     # ms epoch
    bsp_available: bool = False
    reduction_factors: Dict[int, float] = field(default_factory=dict)

    # New structured fields
    bet_delay: int = 0                              # seconds
    suspend_reason: Optional[str] = None           # e.g. "Goal", "RedCard" (if available)
    bet_delay_models: Optional[List[str]] = None   # e.g. ["VARIABLE","FOOTBALL_V2"]


# ------- Runner book (dual-view) ------- #
@dataclass
class RunnerBook:
    selection_id: int

    # DISPLAY (virtual) best ladder (top N)
    best_back: List[Level] = field(default_factory=list)      # alias for display
    best_lay: List[Level]  = field(default_factory=list)      # alias for display
    best_back_disp: List[Level] = field(default_factory=list) # explicit display
    best_lay_disp: List[Level]  = field(default_factory=list)

    # RAW best ladder (top N)
    best_back_raw: List[Level] = field(default_factory=list)
    best_lay_raw: List[Level]  = field(default_factory=list)

    # Traded & LTP
    traded_by_price: Dict[float, float] = field(default_factory=dict)
    ltp: Optional[float] = None

    # Convenience computed metric: top-2 total depth from RAW
    raw_depth_best2: Optional[float] = None


# ------- Snapshot for a whole market ------- #
@dataclass
class OrderBookSnapshot:
    market: MarketState
    runners: Dict[int, RunnerBook] = field(default_factory=dict)

    # Spreads/micro-mids from DISPLAY (virtual) and RAW
    micro_mid: Optional[float] = None
    micro_mid_raw: Optional[float] = None
    spread_ticks: Optional[int] = None         # display (kept for back-compat)
    spread_ticks_raw: Optional[int] = None


# ------- Proposals (from edges) ------- #
@dataclass
class EdgeProposal:
    market_id: str
    selection_id: int
    edge_id: str
    side: Literal["BACK", "LAY"]
    price: float
    size_hint: float = 0.0
    ttl_ms: int = 2000
    weight: float = 1.0
    ev_net_ticks: float = 0.0
    rationale: str = ""


# (Other models like QuoteIntent/ExecutionReport live in execution/interfaces/*)
    bundle_id: Optional[str] = None
    bundle_policy: Optional[str] = None

@dataclass
class SizeDecisionRecord:
    intent_id: str
    market_id: str
    selection_id: int
    side: str
    proposed_price: float
    snapped_price: float
    bankroll_source: str           # "live" | "fixed"
    bankroll_amount: float
    raw_kelly_stake: float
    clamps_applied: list           # e.g., ["STAKE_BELOW_MIN","PRICE_OFF_TICK"]
    mbr_pct: float
    final_size: float
    ts_ms: int
