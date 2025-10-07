"""
Location : src/core/logging.py
Purpose  : JSON logs to console AND (optionally) a rotating file for audits.
Notes    : Use log_event(...) to emit rich, uniform audit records.
"""
import json
import logging
import os
import sys
from datetime import datetime, timezone
from logging.handlers import TimedRotatingFileHandler


def _iso_utc(ts: float) -> str:
    """ISO-8601 with milliseconds in UTC, e.g. 2025-08-30T10:12:05.123Z"""
    return (
        datetime.fromtimestamp(ts, tz=timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = {
            "time": _iso_utc(record.created),
            "lvl": record.levelname,
            "component": getattr(record, "component", record.name),
            "event": getattr(record, "event", None),
            "msg": record.getMessage(),
        }
        # Optional structured fields payload
        fields = getattr(record, "fields", None)
        if isinstance(fields, dict):
            base.update(fields)
        return json.dumps(base, ensure_ascii=False)


def configure_logging(level: str = "INFO", file_path: str | None = None, retention_days: int = 14) -> None:
    """
    Configure root logger:
      - Always emit JSON logs to stdout.
      - If file_path is provided, also write a daily-rotating file kept for 'retention_days'.
    """
    logger = logging.getLogger()
    logger.setLevel(level)
    fmt = JsonFormatter()

    # Console (stdout)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.handlers[:] = [sh]

    # Optional rotating file
    if file_path:
        # Ensure directory exists
        log_dir = os.path.dirname(file_path)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        fh = TimedRotatingFileHandler(
            file_path, when="midnight", backupCount=int(retention_days), encoding="utf-8"
        )
        fh.setFormatter(fmt)
        logger.addHandler(fh)


def log_event(component: str, event: str, msg: str, **fields) -> None:
    """Emit a structured JSON log with rich fields (IDs, prices, reasons, etc.)."""
    logging.getLogger(component).info(
        msg, extra={"component": component, "event": event, "fields": fields}
    )