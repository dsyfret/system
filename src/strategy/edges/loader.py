"""
EdgeLoader: config-driven edge registry/constructor (ctx-only).

- Reads edges.enabled (list[str]) and edges.registry[edge_name] = "module:Symbol".
- Returns a dict[str, Callable[[StrategyContext], Any]].
- No legacy cfg mutation or hidden injections.

Providers must expose one of:
  - def run(ctx: StrategyContext) -> Any
  - class with __call__(self, ctx: StrategyContext) -> Any
  - class with run(self, ctx: StrategyContext) -> Any
"""

from __future__ import annotations
from typing import Any, Dict, Callable
import importlib
import inspect

from ..context import StrategyContext

class EdgeLoader:
    def __init__(self, cfg: Any) -> None:
        self.cfg = cfg

    def _resolve(self, target: str) -> Any:
        if ":" not in target:
            raise ValueError(f"Invalid edge target (expected 'module:attr'): {target}")
        mod_name, attr = target.split(":", 1)
        mod = importlib.import_module(mod_name)
        return getattr(mod, attr)

    def _to_ctx_callable(self, obj: Any, name: str) -> Callable[[StrategyContext], Any]:
        # Plain callable (function or functor)
        if callable(obj) and not isinstance(obj, type):
            try:
                sig = inspect.signature(obj)
                if len(sig.parameters) == 1:
                    return obj  # type: ignore[return-value]
            except Exception:
                pass
            return obj  # assume __call__(ctx) compatible

        # Class → instantiate, then prefer __call__ or run
        if isinstance(obj, type):
            try:
                inst = obj()
            except TypeError:
                try:
                    inst = obj(self.cfg)
                except Exception as e:
                    raise TypeError(f"Edge '{name}' class could not be instantiated without legacy args: {e}") from e

            if callable(inst):
                return inst  # type: ignore[return-value]

            run = getattr(inst, "run", None)
            if callable(run):
                def _call(ctx: StrategyContext):
                    return run(ctx)
                return _call

        raise TypeError(
            f"Edge '{name}' must expose ctx-only callable: function run(ctx) or class with __call__(ctx)/run(ctx)"
        )

    def load(self) -> Dict[str, Callable[[StrategyContext], Any]]:
        edges_cfg = getattr(self.cfg, "edges", {}) or {}
        enabled = list(edges_cfg.get("enabled") or [])
        registry = dict(edges_cfg.get("registry") or {})
        result: Dict[str, Callable[[StrategyContext], Any]] = {}

        for name in enabled:
            target = registry.get(name)
            if not target:
                continue
            obj = self._resolve(target)
            try:
                result[name] = self._to_ctx_callable(obj, name)
            except Exception:
                continue
        return result
