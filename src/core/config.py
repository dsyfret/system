# src/core/config.py
"""
Location : src/core/config.py
Purpose  : Load & merge YAML configs → immutable ConfigSnapshot; env overrides supported.
Inputs   : ./configs/*.yaml + ./configs/profiles/<profile>.yaml
Outputs  : ConfigSnapshot (frozen pydantic model), hot-reload hook (optional)
Notes    : Precedence base → profile → edges/risk/throttles → env CFG__SECTION__KEY
(unchanged for 1C; included here for context of config fallback: betfair.commission_pct_default)
"""
from __future__ import annotations
import os, copy, yaml
from pydantic import BaseModel, Field
from typing import Any

CONFIG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "configs")

def _load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def _deep_merge(a: dict, b: dict) -> dict:
    res = copy.deepcopy(a)
    for k, v in (b or {}).items():
        if isinstance(v, dict) and isinstance(res.get(k), dict):
            res[k] = _deep_merge(res[k], v)
        else:
            res[k] = v
    return res

def _apply_env_overrides(cfg: dict) -> dict:
    # ENV format: CFG__section__sub__key=value  → cfg['section']['sub']['key']=parsed(value)
    for k, v in os.environ.items():
        if not k.startswith("CFG__"):
            continue
        path = k[5:].split("__")
        cur = cfg
        for p in path[:-1]:
            cur = cur.setdefault(p, {})
        # naive parse for numbers/bools
        val = v
        if v.lower() in ("true","false"):
            val = v.lower() == "true"
        else:
            try:
                if "." in v: val = float(v)
                else: val = int(v)
            except ValueError:
                pass
        cur[path[-1]] = val
    return cfg

class ConfigSnapshot(BaseModel):
    app: dict = Field(default_factory=dict)
    betfair: dict = Field(default_factory=dict)
    ipc: dict = Field(default_factory=dict)
    slo: dict = Field(default_factory=dict)
    logging: dict = Field(default_factory=dict)
    profile: dict = Field(default_factory=dict)
    edges: dict = Field(default_factory=dict)
    risk: dict = Field(default_factory=dict)
    throttles: dict = Field(default_factory=dict)
    feature_flags: dict = Field(default_factory=dict)
    metrics: dict = Field(default_factory=dict)
    # NEW: keep these blocks from YAML instead of dropping them
    discovery: dict = Field(default_factory=dict)
    execution: dict = Field(default_factory=dict)

    class Config:
        frozen = True

def load_config(profile: str) -> ConfigSnapshot:
    base = _load_yaml(os.path.join(CONFIG_DIR, "base.yaml"))
    prof = _load_yaml(os.path.join(CONFIG_DIR, "profiles", f"{profile}.yaml"))
    edges = _load_yaml(os.path.join(CONFIG_DIR, "edges.yaml"))
    risk  = _load_yaml(os.path.join(CONFIG_DIR, "risk.yaml"))
    thr   = _load_yaml(os.path.join(CONFIG_DIR, "throttles.yaml"))
    flags = _load_yaml(os.path.join(CONFIG_DIR, "feature_flags.yaml"))

    merged = _deep_merge(base, {})
    merged = _deep_merge(merged, {"profile": prof})
    merged = _deep_merge(merged, {"edges": edges.get("edges", {})})
    merged = _deep_merge(merged, {"risk": risk})
    merged = _deep_merge(merged, {"throttles": thr})
    merged = _deep_merge(merged, {"feature_flags": flags})

    merged = _apply_env_overrides(merged)
    return ConfigSnapshot(**merged)
