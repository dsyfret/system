# src/tools/simulator/replay.py
"""
Offline replayer (SAFE: network-off).
...
"""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path
from typing import Iterator, Dict, Any

from .entrypoint import SimulatorApp

def _iter_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue

def _iter_inputs(input_dir: Path):
    for p in sorted(input_dir.glob("*.jsonl")):
        yield from _iter_jsonl(p)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Directory containing *.jsonl from recorder")
    ap.add_argument("--profile", required=True, help="Config profile name")
    ap.add_argument("--out", default="decisions_replay.jsonl", help="Output JSONL for decisions")
    args = ap.parse_args()

    in_dir = Path(args.input)
    if not in_dir.exists() or not in_dir.is_dir():
        print(f"[replay] input dir not found: {in_dir}", file=sys.stderr)
        sys.exit(2)

    app = SimulatorApp(args.profile)

    out_path = Path(args.out)
    with out_path.open("w", encoding="utf-8") as w:
        for rec in _iter_inputs(in_dir):
            ts = int(rec.get("ts_ms", 0) or 0)
            typ = str(rec.get("type", "")).lower()
            if typ == "ws":
                continue  # ignore raw ws; rely on normalized 'delta' or 'reseed' lines
            elif typ == "reseed":
                update = {"books": rec.get("books", [])}
            elif typ == "delta":
                update = rec.get("delta", {})
            else:
                # unknown record type; skip
                continue
            out = app.process_update(update, ts_ms=ts)
            w.write(json.dumps(out) + "\n")

    print(f"[replay] wrote {out_path}")

if __name__ == "__main__":
    main()
