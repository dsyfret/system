"""
EdgeLoader: config-driven edge registry/constructor.

- Reads edges.enabled (list[str]) and edges.registry[edge_name] = "module:Symbol".
- Supports both classes (instantiated with cfg) and callables (functions) as edge providers.
- Back-compat: if registry missing a key, attempts sensible defaults by edge name *only when called explicitly*.
  (Trader falls back to legacy feature flags when no edges.enabled/registry present.)
"""

from __future__ import annotations
from typing import Any, Dict, Callable
import importlib
import inspect

class EdgeLoader:
    def __init__(self, cfg: Any, services: Dict[str, Any] | None = None) -> None:
        self.cfg = cfg
        self.services = services or {}

    def _resolve(self, target: str):
        """
        Import and return attribute referred to by 'module:attr'.
        """
        if ":" not in target:
            raise ValueError(f"Invalid edge target (expected 'module:attr'): {target}")
        mod_name, attr = target.split(":", 1)
        mod = importlib.import_module(mod_name)
        return getattr(mod, attr)

    def _wrap_callable_with_services(self, fn: Callable[[Any, Any, Dict[str, Any], Any], Any]) -> Callable[..., Any]:
        """
        Wrap an edge callable so that its cfg receives injected services.
        Expected callable signature: (snapshot, selection_id, cfg, now_ms=None) or similar.
        """
        def _call_with_services(*args, **kwargs):
            # Find cfg arg by position (3rd) or name ('cfg')
            cfg = None
            new_args = list(args)
            sig = None
            try:
                sig = inspect.signature(fn)
            except Exception:
                sig = None

            if len(new_args) >= 3:
                cfg = new_args[2]
            else:
                cfg = kwargs.get("cfg")

            # Merge services into cfg under _services; also provide _queue_signals convenience if 'book' exists
            cfg_dict = dict(cfg or {})
            # Non-destructive merge of _services
            existing_services = dict(cfg_dict.get("_services") or {})
            merged_services = {**existing_services, **(self.services or {})}
            cfg_dict["_services"] = merged_services
            if "book" in merged_services and "_queue_signals" not in cfg_dict:
                cfg_dict["_queue_signals"] = merged_services["book"]

            # Write back into positional or kwarg
            if len(new_args) >= 3:
                new_args[2] = cfg_dict
            else:
                kwargs["cfg"] = cfg_dict

            return fn(*new_args, **kwargs)
        return _call_with_services

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
            # If it's a class, instantiate with cfg; if it's a function/callable, keep as-is (but wrap to inject services).
            try:
                if isinstance(obj, type):
                    try:
                        inst = obj(self.cfg)
                    except TypeError:
                        inst = obj()
                    result[name] = inst
                else:
                    # callable/function edge — wrap to inject services into cfg at call time
                    wrapped = self._wrap_callable_with_services(obj)
                    result[name] = wrapped
            except Exception:
                # Skip edge if it fails to construct
                continue
        return result
