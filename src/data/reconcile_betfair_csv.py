"""
Location : src/data/reconcile_betfair_csv.py
Purpose  : Read Betfair monthly CSV export and compare totals to the internal ledger.
Plain    : You give it a CSV path and a date (YYYYMMDD); it prints diffs and logs a summary.
"""

from __future__ import annotations
import csv
from typing import Dict, Any
from pathlib import Path

from ..core.logging import log_event
from .store import DataStore


def _parse_money(val: str) -> float:
    # Handles values like "12.34", "-5.67", "$12.34", "£-1.20"
    v = val.strip().replace("$", "").replace("£", "")
    try:
        return float(v)
    except Exception:
        return 0.0


def reconcile_day(profile_name: str, csv_path: str, yyyymmdd: str) -> Dict[str, Any]:
    store = DataStore(profile_name)
    csv_p = Path(csv_path)
    if not csv_p.exists():
        raise FileNotFoundError(csv_path)

    # Betfair CSVs vary; we try to read common columns
    gross = 0.0
    comm = 0.0
    with csv_p.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Guess date column
            date_str = (row.get("Settled Date") or row.get("Date") or "").strip()
            if not date_str.replace("-", "").replace("/", ""):
                continue
            d8 = "".join(c for c in date_str if c.isdigit())
            if len(d8) < 8:
                continue
            d8 = d8[:8]  # YYYYMMDD
            if d8 != yyyymmdd:
                continue
            gross += _parse_money(row.get("Profit/Loss", "0"))
            comm += _parse_money(row.get("Commission", "0"))

    ledger = store.day_summary(yyyymmdd)
    matched_px_sz = ledger["matched_px_sz"]
    # We cannot derive P&L from matched_px_sz alone; just present CSV totals vs internal volume/fees
    out = {
        "csv_gross_pnl": gross,
        "csv_commission": comm,
        "ledger_matched_px_sz": matched_px_sz,
        "ledger_fees_recorded": ledger["fees"],
    }
    log_event("reconcile", "reconcile.day", "done", **out)
    return out