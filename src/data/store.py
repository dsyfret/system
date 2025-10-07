# src/data/store.py
"""
Location : src/data/store.py
Purpose  : Tiny SQLite layer for orders/fills/events. Creates schema on first use.
Plain    : One DB file (default: data/ledger.sqlite3). Async via aiosqlite if available; else sync sqlite3.
"""

from __future__ import annotations
import os
import json
import sqlite3
from contextlib import contextmanager
from typing import Optional, Dict, Any, Iterable

from ..core.config import load_config
from ..core.logging import log_event


DEFAULT_DB_PATH = "data/ledger.sqlite3"


def _ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


class DataStore:
    def __init__(self, profile_name: str, db_path: Optional[str] = None) -> None:
        snap = load_config(profile_name)
        data_cfg = getattr(snap, "data", {}) or {}
        self.db_path = db_path or data_cfg.get("db_path", DEFAULT_DB_PATH)
        _ensure_dir(self.db_path)
        self._conn = sqlite3.connect(self.db_path, isolation_level=None)  # autocommit
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        cur = self._conn.cursor()
        # NEW: intents table (requested quotes)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS intents (
            intent_id TEXT PRIMARY KEY,
            market_id TEXT,
            selection_id INTEGER,
            side TEXT,
            price REAL,
            size REAL,
            tif TEXT,
            persistence TEXT,
            client_ts_ms INTEGER
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            intent_id TEXT,
            order_id TEXT,
            market_id TEXT,
            selection_id INTEGER,
            side TEXT,
            requested_price REAL,
            requested_size REAL,
            tif TEXT,
            persistence TEXT,
            placed_ts_ms INTEGER
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS replaces (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT,
            new_price REAL,
            ts_ms INTEGER
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            intent_id TEXT,
            order_id TEXT,
            market_id TEXT,
            selection_id INTEGER,
            side TEXT,
            price REAL,
            size REAL,
            ts_ms INTEGER,
            fees REAL
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS reports_raw (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload_json TEXT,
            ts_ms INTEGER
        );
        """)
        # (NEW) intent_meta for optional annotations like clamp_reason
        cur.execute("""
        CREATE TABLE IF NOT EXISTS intent_meta (
            intent_id TEXT PRIMARY KEY,
            clamp_reason TEXT
        );
        """)
        # helpful indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fills_intent ON fills(intent_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_intent ON orders(intent_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_reports_ts ON reports_raw(ts_ms);")
        cur.close()

    # ---------- writes ---------- #

    def write_report_raw(self, payload: Dict[str, Any], ts_ms: int) -> None:
        self._conn.execute(
            "INSERT INTO reports_raw (payload_json, ts_ms) VALUES (?, ?)",
            (json.dumps(payload, ensure_ascii=False), int(ts_ms)),
        )

    # NEW: persist requested QuoteIntent (for clean audit)
    def record_intent(self, *, intent_id: str, market_id: str, selection_id: int,
                      side: str, price: float, size: float, tif: str,
                      persistence: str, client_ts_ms: int) -> None:
        self._conn.execute("""
            INSERT OR REPLACE INTO intents(intent_id, market_id, selection_id, side, price, size, tif, persistence, client_ts_ms)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (intent_id, market_id, int(selection_id), side, float(price), float(size), tif, persistence, int(client_ts_ms)))

    def record_placed(self, *, intent_id: str, order_id: str, market_id: str, selection_id: int,
                      side: str, requested_price: float, requested_size: float,
                      tif: str, persistence: str, placed_ts_ms: int) -> None:
        self._conn.execute("""
            INSERT INTO orders(intent_id, order_id, market_id, selection_id, side, requested_price,
                               requested_size, tif, persistence, placed_ts_ms)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (intent_id, order_id, market_id, int(selection_id), side, float(requested_price),
              float(requested_size), tif, persistence, int(placed_ts_ms)))

    def record_replaced(self, *, order_id: str, new_price: float, ts_ms: int) -> None:
        self._conn.execute("""
            INSERT INTO replaces(order_id, new_price, ts_ms) VALUES (?,?,?)
        """, (order_id, float(new_price), int(ts_ms)))

    def record_fill(self, *, intent_id: str, order_id: str, market_id: str, selection_id: int,
                    side: str, price: float, size: float, ts_ms: int, fees: float | None) -> None:
        self._conn.execute("""
            INSERT INTO fills(intent_id, order_id, market_id, selection_id, side, price, size, ts_ms, fees)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (intent_id, order_id, market_id, int(selection_id), side, float(price), float(size),
              int(ts_ms), None if fees is None else float(fees)))

    # (NEW) optional annotations
    def record_intent_clamp(self, intent_id: str, clamp_reason: str) -> None:
        """
        Upsert a clamp reason for an intent. Non-fatal if called repeatedly.
        """
        self._conn.execute("""
            INSERT INTO intent_meta(intent_id, clamp_reason) VALUES(?,?)
            ON CONFLICT(intent_id) DO UPDATE SET clamp_reason=excluded.clamp_reason
        """, (str(intent_id), str(clamp_reason)))

    # ---------- reads / summaries ---------- #

    def vwap_for_intent(self, intent_id: str) -> float:
        cur = self._conn.cursor()
        cur.execute("SELECT SUM(price*size), SUM(size) FROM fills WHERE intent_id=?", (intent_id,))
        row = cur.fetchone()
        cur.close()
        if not row or row[1] in (None, 0):
            return 0.0
        return float(row[0]) / float(row[1])

    def day_summary(self, yyyymmdd: str) -> Dict[str, Any]:
        cur = self._conn.cursor()
        cur.execute("""
            SELECT COUNT(*), COALESCE(SUM(size),0), COALESCE(SUM(price*size),0), COALESCE(SUM(COALESCE(fees,0)),0)
            FROM fills
            WHERE strftime('%Y%m%d', datetime(ts_ms/1000, 'unixepoch')) = ?
        """, (yyyymmdd,))
        n, sum_size, sum_px_sz, sum_fees = cur.fetchone()
        cur.close()
        return {"fills": int(n), "matched_stake": float(sum_size), "matched_px_sz": float(sum_px_sz), "fees": float(sum_fees)}

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass
