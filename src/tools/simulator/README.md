# Simulator Scaffold (add-only)

This directory contains **safe**, add-only tools:
- `entrypoint.py`: an adapter that holds `BookBuilder`, `Selector`, `Arbiter`, `Sizer` and exposes `process_update(...)` for a single update.
- `replay.py`: an offline CLI that reads recorded JSONL events and pushes them into `entrypoint.SimulatorApp`.

**No network**, **no order placement**, **no edits** to core modules. Recorder hooks for live capture will be added separately and are feature-flagged OFF by default.
