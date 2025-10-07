# src/data/ledger.py
"""
Location : src/data/ledger.py
Purpose  : Consume ExecutionReports, persist them, and expose simple KPIs (VWAP, fee %, volume).
Plain    : Listens on IPC reports; writes orders/replaces/fills to SQLite via DataStore.
(ENH)    : Optional MBR stash per intent (for future persistence).
(ENH 2025-09): If QuoteIntent carries a clamp reason in qi.risk_tags['clamp_reason'],
               persist it when the intent is recorded (best-effort, optional).
Notes    : Constructor is backward-compatible:
           - Ledger(cfg)                     ← simple usage (no IPC loop)
           - Ledger(profile_name:str)        ← also accepted
           - Ledger(cfg, ipc=IPC(...))       ← enables async run() loop
"""

from __future__ import annotations
import asyncio
from typing import Optional, Dict, Tuple, Any

from ..core.logging import log_event
from ..execution.interfaces.queue import IPC
from ..execution.interfaces.reports import ExecutionReport
from ..execution.interfaces.intents import QuoteIntent  # type hints only
from .store import DataStore


class Ledger:
    def __init__(self, cfg_or_profile: Any, ipc: Optional[IPC] = None, store: Optional[DataStore] = None) -> None:
        # Accept both config snapshot and profile name; DataStore accepts same as before.
        self.cfg_or_profile = cfg_or_profile
        self.ipc = ipc  # optional
        try:
            self.store = store or DataStore(cfg_or_profile)
        except Exception:
            # last resort: try with a string, else raise later on use
            self.store = store or DataStore(str(cfg_or_profile))
        # Optional mapping of intent_id -> (mbr_pct, source)
        self._intent_mbr: Dict[str, Tuple[float, str]] = {}

    async def run(self) -> None:
        """
        Blocking loop: reads reports from IPC and persists + logs KPIs.
        If no IPC is wired, it idles.
        """
        if self.ipc is None:
            log_event("ledger", "ledger.run", "no_ipc")
            while True:
                await asyncio.sleep(60.0)
        async for er in self.ipc.iter_reports():  # type: ignore[union-attr]
            await self._handle_report(er)

    # Allow strategy to persist requested QuoteIntent before sending to executor
    def record_intent(self, qi: QuoteIntent) -> None:
        try:
            self.store.record_intent(
                intent_id=qi.intent_id,
                market_id=qi.market_id,
                selection_id=qi.selection_id,
                side=str(qi.side),
                price=float(qi.price),
                size=float(qi.size),
                tif=str(qi.tif),
                persistence=str(qi.persistence),
                client_ts_ms=int(qi.client_ts_ms),
            )
            # If we have an MBR note for this intent, try to persist it (best-effort)
            mbr = self._intent_mbr.get(qi.intent_id)
            if mbr is not None:
                mbr_pct, mbr_src = mbr
                try:
                    if hasattr(self.store, "record_intent_mbr"):
                        self.store.record_intent_mbr(qi.intent_id, mbr_pct, mbr_src)  # type: ignore[attr-defined]
                except Exception:
                    pass

            # (NEW) Persist clamp reason if present in risk_tags, best-effort
            try:
                clamp_reason = None
                if hasattr(qi, "risk_tags") and isinstance(qi.risk_tags, dict):
                    clamp_reason = qi.risk_tags.get("clamp_reason")
                if clamp_reason and hasattr(self.store, "record_intent_clamp"):
                    self.store.record_intent_clamp(qi.intent_id, str(clamp_reason))  # type: ignore[attr-defined]
            except Exception:
                pass
        except Exception as e:
            log_event("ledger", "ledger.intent", "record_fail", error=type(e).__name__)

    # Side-channel to attach MBR used for a given intent id (to persist later on report/intent)
    def set_intent_mbr(self, intent_id: str, mbr_pct: float, source: str) -> None:
        try:
            self._intent_mbr[str(intent_id)] = (float(mbr_pct), str(source))
        except Exception:
            pass

    async def _handle_report(self, er: ExecutionReport) -> None:
        # Save raw for audit
        try:
            self.store.write_report_raw(er.to_wire(), er.ack_ts_ms)
        except Exception:
            pass

        action = er.action
        if action == "PLACED":
            try:
                self.store.record_placed(
                    intent_id=er.intent_id,
                    order_id=er.order_id,
                    market_id=er.market_id,
                    selection_id=er.selection_id,
                    side="",
                    requested_price=er.requested_price if hasattr(er, "requested_price") else 0.0,
                    requested_size=er.requested_size if hasattr(er, "requested_size") else 0.0,
                    tif=getattr(er, "tif", "GTC"),
                    persistence=getattr(er, "persistence", "CANCEL"),
                    placed_ts_ms=er.ack_ts_ms,
                )
            except Exception:
                pass
            # Best-effort: attach MBR to the order if DataStore has a slot
            mbr = self._intent_mbr.get(er.intent_id)
            if mbr is not None:
                try:
                    if hasattr(self.store, "record_order_mbr"):
                        self.store.record_order_mbr(er.order_id, mbr[0], mbr[1])  # type: ignore[attr-defined]
                except Exception:
                    pass
        elif action == "REPLACED":
            try:
                self.store.record_replaced(order_id=er.order_id, new_price=getattr(er, "fill_price", 0.0), ts_ms=er.ack_ts_ms)
            except Exception:
                pass
        elif action in ("PARTIAL", "FILLED"):
            try:
                self.store.record_fill(
                    intent_id=er.intent_id, order_id=er.order_id,
                    market_id=er.market_id, selection_id=er.selection_id,
                    side="", price=getattr(er, "fill_price", 0.0), size=getattr(er, "fill_size", 0.0),
                    ts_ms=getattr(er, "fill_ts_ms", er.ack_ts_ms), fees=getattr(er, "fees", None)
                )
            except Exception:
                pass
            # Optionally persist MBR at fill time
            mbr = self._intent_mbr.get(er.intent_id)
            if mbr is not None:
                try:
                    if hasattr(self.store, "record_fill_mbr"):
                        self.store.record_fill_mbr(er.intent_id, er.order_id, mbr[0], mbr[1])  # type: ignore[attr-defined]
                except Exception:
                    pass


# P2-8: SizeDecisionRecord persistence (JSONL sink to avoid DB migrations)
def record_size_decision(self, rec):
    """Persist a SizeDecisionRecord. If a DB table exists in this project,
    you can replace this JSONL sink with a proper INSERT matching your schema.
    """
    try:
        import json, os, time
        path = getattr(self, "base_path", None) or getattr(self, "log_dir", None) or "."
        fn = os.path.join(path, "size_decisions.jsonl")
        with open(fn, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec.__dict__, ensure_ascii=False) + "\n")
    except Exception:
        # Non-fatal
        pass
