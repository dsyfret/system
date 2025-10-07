"""
Location : src/execution/executor_py/ack_hook.py
Purpose  : Non-invasive hook to mirror ACK/fill timestamps from AckTracker into a callback
           (e.g., Healthcheck.update_ack_ts) without modifying AckTracker internals.

Usage:
    from ..executor_py import ack_hook
    ack_hook.install(self.acks, self.health.update_ack_ts)

This wraps these methods if present on the tracker:
    - on_placed(qi, order_id, ack_ts_ms)
    - on_replaced(qi, old_order_id, new_order_id, ack_ts_ms)
    - on_canceled(qi, order_id, ack_ts_ms)
    - on_fill(intent_id, fill_ts_ms, price, size)
    - on_fill_by_order(order_id, fill_ts_ms, price, size)

If signatures differ slightly, we try our best to extract a timestamp arg.
"""

from __future__ import annotations
from typing import Any, Callable, Optional


def _extract_ts(args: tuple, kwargs: dict) -> Optional[int]:
    # Prefer named kwargs first
    for k in ("ack_ts_ms", "fill_ts_ms", "ts_ms"):
        if k in kwargs:
            try:
                return int(kwargs[k])
            except Exception:
                pass
    # Fallback: scan positional args from the end for something that looks like a timestamp
    for x in reversed(args):
        if isinstance(x, (int, float)):
            try:
                ts = int(x)
                # accept ms timestamps or reasonable ints
                if 0 < ts < 10**13:
                    return ts
            except Exception:
                continue
    return None


def _wrap(method: Callable[..., Any], hook: Callable[[int], None]) -> Callable[..., Any]:
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        # Call original first to preserve behavior and exceptions
        out = method(*args, **kwargs)
        try:
            ts = _extract_ts(args, kwargs)
            if ts is not None:
                hook(int(ts))
        except Exception:
            # Never let the hook break trading
            pass
        return out
    return wrapper


def install(acks: Any, hook: Callable[[int], None]) -> None:
    """
    Wraps known AckTracker methods if present. Safe to call multiple times
    (idempotent: we tag wrapped methods to avoid double-wrapping).
    """
    if not acks or not callable(hook):
        return

    def maybe_wrap(name: str) -> None:
        if not hasattr(acks, name):
            return
        fn = getattr(acks, name)
        # avoid double wrapping
        if getattr(fn, "_ack_hook_wrapped", False):
            return
        wrapped = _wrap(fn, hook)
        setattr(wrapped, "_ack_hook_wrapped", True)
        setattr(acks, name, wrapped)

    for nm in ("on_placed", "on_replaced", "on_canceled", "on_fill", "on_fill_by_order"):
        maybe_wrap(nm)