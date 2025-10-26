from __future__ import annotations
from typing import Any, Dict, Optional, Tuple

# This wrapper is the "seam". Today it delegates to BookBuilder; later you can
# replace internals with a PyO3-backed Rust core without touching callers.

class Hotloop:
    """
    Signals seam (read-only) for edges/guards/sizer/arbiter.

    Contract (frozen API):
      - Lifecycle: update_config, upsert_market, drop_market
      - Ingest: apply_snapshot(...), apply_delta(...)
      - Signals: best(...), best2_sum(...), spread_ticks(...),
                 size_ahead(...), match_rate_ema(...), pfill_ttf(...),
                 get_mbr(...), last_traded_ts_ms(...)

      - Event detectors (stubs for now): sweep_sniffer(...), big_move_trigger(...), rf_seen_recently(...)
      - Trend lens (stub): trend_lens(...)

    Today this proxies to BookBuilder. Keep this surface stable.
    """

    def __init__(self, book_builder: Any, config: Dict[str, Any] | None = None) -> None:
        self._book = book_builder
        self._cfg = dict(config or {})

    # -------- lifecycle -------- #
    def update_config(self, config: Dict[str, Any]) -> None:
        self._cfg.update(config or {})

    def upsert_market(self, market_id: str) -> None:
        # BookBuilder upserts on first delta; no-op here.
        return None

    def drop_market(self, market_id: str) -> None:
        # No explicit drop in BookBuilder; harmless no-op.
        return None

    # -------- ingest (delegates to BookBuilder for now) -------- #
    def apply_snapshot(self, market_id: str, books: Dict[str, Any], ts_ms: int) -> int:
        # BookBuilder expects list of books; callers already use self.rest helpers elsewhere.
        try:
            return int(self._book.reseed_from_snapshot(books))  # type: ignore[attr-defined]
        except Exception:
            return 0

    def apply_delta(self, market_id: str, delta: Dict[str, Any] | str, ts_ms: int) -> None:
        # We accept dict or raw JSON string for future Rust decoder. For now expect dict.
        if isinstance(delta, dict):
            self._book.apply_delta(delta)  # type: ignore[attr-defined]
        else:
            # If a raw JSON string ever arrives pre-Rust, caller should parse to dict first.
            raise TypeError("apply_delta currently expects a dict; raw JSON supported when Rust hotloop is added.")

    # -------- book summaries / thickness -------- #
    def best(self, market_id: str, runner_id: int) -> Dict[str, Dict[str, float] | None]:
        snap = self._book.get_snapshot(str(market_id))  # type: ignore[attr-defined]
        rb = snap.runners.get(int(runner_id)) if snap else None
        if not rb:
            return {"best_back": None, "best_lay": None}
        bb = rb.best_back[0] if rb.best_back else None
        bl = rb.best_lay[0] if rb.best_lay else None
        return {
            "best_back": {"price": float(bb.price), "size": float(bb.size)} if bb else None,
            "best_lay":  {"price": float(bl.price), "size": float(bl.size)} if bl else None,
        }

    def best2_sum(self, market_id: str, runner_id: int, side: str) -> float:
        snap = self._book.get_snapshot(str(market_id))  # type: ignore[attr-defined]
        rb = snap.runners.get(int(runner_id)) if snap else None
        if not rb:
            return 0.0
        side = str(side).upper()
        lvls = (rb.best_back_raw if side == "BACK" else rb.best_lay_raw) or (rb.best_back if side == "BACK" else rb.best_lay)
        if not lvls:
            return 0.0
        return float(sum(lv.size for lv in lvls[:2]))

    def spread_ticks(self, market_id: str, runner_id: int) -> Optional[int]:
        snap = self._book.get_snapshot(str(market_id))  # type: ignore[attr-defined]
        rb = snap.runners.get(int(runner_id)) if snap else None
        if not rb or not rb.best_back or not rb.best_lay:
            return None
        # BookBuilder already computes spread_ticks on snapshot; use per-runner for safety
        try:
            from ...betfair.ticks import distance_in_ticks  # local import to avoid cycle at module load
            return distance_in_ticks(rb.best_back[0].price, rb.best_lay[0].price)  # type: ignore[arg-type]
        except Exception:
            return snap.spread_ticks if snap else None

    # -------- queue-aware metrics -------- #
    def size_ahead(self, market_id: str, runner_id: int, side: str, price: float) -> float:
        return float(self._book.size_ahead_at(str(market_id), int(runner_id), str(side).upper(), float(price)))  # type: ignore[attr-defined]

    def match_rate_ema(self, market_id: str, runner_id: int, side: str) -> float:
        side = str(side).upper()
        if side == "BACK":
            return float(self._book.match_rate_back_ema(str(market_id), int(runner_id)))  # type: ignore[attr-defined]
        return float(self._book.match_rate_lay_ema(str(market_id), int(runner_id)))       # type: ignore[attr-defined]

    def pfill_ttf(self, market_id: str, runner_id: int, side: str, price: float, ttl_ms: int) -> Dict[str, float]:
        ttf = int(self._book.ttf_ms_at(str(market_id), int(runner_id), str(side).upper(), float(price), 0.0))  # type: ignore[attr-defined]
        # ttl_ms is not used by BookBuilder today; included for future Rust use. Keep the param for frozen API.
        p = float(self._book.p_fill_at(str(market_id), int(runner_id), str(side).upper(), float(price), 0.0))  # type: ignore[attr-defined]
        return {"p_fill": p, "ttf_ms": float(ttf)}

    # -------- extras used elsewhere -------- #
    def get_mbr(self, market_id: str) -> Tuple[Optional[float], Optional[str]]:
        return self._book.get_mbr(str(market_id))  # type: ignore[attr-defined]

    def last_traded_ts_ms(self, market_id: str, runner_id: int) -> Optional[int]:
        return self._book.last_traded_ts_ms(str(market_id), int(runner_id))  # type: ignore[attr-defined]

    # -------- event detectors (stubs; keep API frozen) -------- #
    def sweep_sniffer(self, market_id: str, runner_id: int) -> bool:
        # Placeholder: false until you add a signal. Keep API for Rust swap.
        return False

    def big_move_trigger(self, market_id: str, runner_id: int) -> bool:
        return False

    def rf_seen_recently(self, market_id: str, runner_id: int) -> bool:
        return False

    # -------- trend lens (stub) -------- #
    def trend_lens(self, market_id: str, runner_id: int) -> Dict[str, float | str]:
        return {"bias": 0.0, "confidence": 0.0, "regime": "chop"}

    # -------- discoverability -------- #
    def capabilities(self) -> Dict[str, bool]:
        return {
            "best": True,
            "best2_sum": True,
            "spread_ticks": True,
            "size_ahead": True,
            "match_rate_ema": True,
            "pfill_ttf": True,
            "get_mbr": True,
            "last_traded_ts_ms": True,
            "sweep_sniffer": False,
            "big_move_trigger": False,
            "rf_seen_recently": False,
            "trend_lens": False,
        }
