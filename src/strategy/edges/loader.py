# src/strategy/edges/loader.py
"""
EdgeLoader: config-driven edge registry/constructor.

- Reads edges.enabled (list[str]) and edges.registry[edge_name] = "module:Symbol".
- Supports both classes (instantiated with cfg) and callables (functions) as edge providers.
- Back-compat: if registry missing a key, attempts sensible defaults by edge name *only when called explicitly*.
  (Trader falls back to legacy feature flags when no edges.enabled/registry present.)
"""

from __future__ import annotations
from typing import Any, Dict
import importlib

class EdgeLoader:
    def __init__(self, cfg: Any) -> None:
        self.cfg = cfg

    def _resolve(self, target: str):
        """
        Import and return attribute referred to by 'module:attr'.
        """
        if ":" not in target:
            raise ValueError(f"Invalid edge target (expected 'module:attr'): {target}")
        mod_name, attr = target.split(":", 1)
        mod = importlib.import_module(mod_name)
        return getattr(mod, attr)

    def load(self) -> Dict[str, Any]:
        edges_cfg = getattr(self.cfg, "edges", {}) or {}
        enabled = list(edges_cfg.get("enabled") or [])
        registry = dict(edges_cfg.get("registry") or {})
        result: Dict[str, Any] = {}

        for name in enabled:
            target = registry.get(name)
            if not target:
                # No guessing here; Trader will fall back to legacy feature flags.
                continue
            obj = self._resolve(target)
            # If it's a class, instantiate with cfg; if it's a function/callable, keep as-is.
            try:
                if isinstance(obj, type):
                    try:
                        inst = obj(self.cfg)
                    except TypeError:
                        inst = obj()
                else:
                    inst = obj  # e.g., module-level propose(...)
            except Exception:
                # Skip edge if it fails to construct
                continue
            result[name] = inst
        return result
