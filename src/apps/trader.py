# src/apps/trader.py
"""
Location : src/apps/trader.py
Purpose  : End-to-end orchestrator:
           streaming → book_builder → selector/allocator → edges → fee_gate + sizing → arbiter → intents
           → executor (router/order_manager/ack_tracker) → reports → ledger

Run:
  python -m src.apps.trader <profile>
"""

from __future__ import annotations
import asyncio
import sys
import time
from typing import Dict, Any, List, Optional, Tuple, Callable
from collections import deque

# Core / config / logging
from ..core.config import load_config
from ..core.logging import log_event, configure_logging
from ..core.metrics import start_metrics_server, inc, set_gauge

# Betfair I/O
from ..betfair.streaming import StreamingClient
from ..betfair.rest import RestClient
from ..betfair.book_builder import BookBuilder
from ..strategy.hotloop.api import Hotloop
from ..betfair.order_stream import OrderStreamClient, OrderWatcher
from ..betfair.heartbeat import HeartbeatPinger
from ..betfair.accounts import AccountsClient, AccountFundsPoller
from ..betfair.ticks import one_tick_up, one_tick_down

# Strategy: selector + allocator + risk + arbiter/fee/sizer
from ..strategy.selector import MarketSelector
from ..strategy.allocator import Allocator
from ..strategy.risk import RiskManager
from ..strategy.arbiter import Arbiter
from ..strategy.fee_gate import FeeGate
from ..strategy.sizing import Sizer

# Edges

from ..strategy.edges.loader import EdgeLoader  # ctx-only edge loader
from ..strategy.context import StrategyContext
# Trend overlay
from ..strategy.trend.engine import TrendEngine

# Execution seam and executor (Python)
from ..execution.ack_tracker import AckTracker
from ..execution.order_manager import OrderManager
from ..execution.executor_py.router import ExecutorRouter

# Interfaces
from ..execution.interfaces.intents import QuoteIntent
from ..execution.interfaces.queue import IPC

# Monitoring
from ..monitoring import dashboards
from ..monitoring.alerts import Alerts
from ..monitoring.healthcheck import Health

# Data
from ..data.store import DataStore
from ..data.ledger import Ledger
import src.core.metrics as metr


def _now_ms() -> int:
    return int(time.time() * 1000)


class TraderApp:
    def __init__(self, profile: str) -> None:
        self.profile = profile
        self.cfg = load_config(profile)
        configure_logging(self.cfg)

        # Metrics server
        try:
            port = int(getattr(self.cfg, "metrics", {}).get("port", 8008))
            if bool(getattr(self.cfg, "metrics", {}).get("enabled", True)):
                start_metrics_server(port)
        except Exception:
            pass

        # Telemetry windows
        self._intents_sent_1m: deque[int] = deque()

        # Dynamic edge observer counters (no hardcoded edge names)
        self._obs_counts: Dict[str, int] = {}

        # Health + Alerts
        self.health = Health(self.cfg)
        self.alerts = Alerts.from_profile(self.cfg)

        # Selector / allocator / arbiter and co
        self.selector = MarketSelector(self.cfg)
        self.allocator = Allocator(self.cfg)
        self.risk = RiskManager(self.cfg)
        self.arbiter = Arbiter(self.cfg)
        self.fee_gate = FeeGate(self.cfg)
        self.sizer = Sizer(self.cfg)

        # Data store & ledger
        self.store = DataStore(self.cfg)
        self.ledger = Ledger(self.cfg)

        # Background tasks
        self._dash_task: Optional[asyncio.Task] = None
        self._disc_task: Optional[asyncio.Task] = None
        self._funds_task: Optional[asyncio.Task] = None
        self._order_watcher_task: Optional[asyncio.Task] = None
        self._order_monitor_task: Optional[asyncio.Task] = None
        self._risk_task: Optional[asyncio.Task] = None
        self._hedge_task: Optional[asyncio.Task] = None  # (NEW) hedge-on-fill listener

        # Reconnect handling
        self._last_seed_ms: int = 0
        self._md_seen: Dict[str, Tuple[Optional[str], Tuple[str, ...]]] = {}

        # Known subscribed market ids (for discovery diffs)
        self._known_market_ids: set[str] = set()

        # (NEW) Discovery cache for expiry and staleness
        self._schedule: Dict[str, Tuple[float, float]] = {}  # market_id -> (start_ts, expires_ts)
        self._last_discovery_refresh_ts: float = 0.0

        # Execution actors
        self.ipc = IPC(self.profile)
        self.acks = AckTracker(self.ipc)
        self.orders = self._build_orders_client()
        self.order_manager = OrderManager(self.profile, self.ipc, self.orders, self.acks)
        # Router with edge-agnostic callback so we can register hedge candidates
        self.router = ExecutorRouter(
            self.profile,
            self.ipc,
            self.order_manager,
            on_intent_published=self._register_hedge_candidate,
        )

        # Wire exposure resolver so Arbiter can enforce per-runner liability caps
        try:
            if hasattr(self.arbiter, "set_exposure_resolver"):
                getter = getattr(self.order_manager, "exposure_for_runner", None) or getattr(self.order_manager, "exposure_for", None)
                if callable(getter):
                    self.arbiter.set_exposure_resolver(lambda mid, sel, side: getter(mid, sel, side))
                else:
                    self.arbiter.set_exposure_resolver(lambda *_: 0.0)
        except Exception:
            pass

        # Streams/REST
        self.stream = StreamingClient(self.profile)
        self.rest = RestClient(self.profile)
        # Book state + signals seam wrapper
        self.book = BookBuilder(queue_cfg=getattr(self.cfg, 'queue_signals', {}) or {})
        self.hotloop = Hotloop(self.book, config=getattr(self.cfg, 'hotloop', {}) or {})

        # (MBR to Sizer logs)
        try:
            self.sizer.set_mbr_resolver(self.hotloop.get_mbr)
        except Exception:
            pass

        # Orders: WS + poll fallback
        self.order_stream = OrderStreamClient(self.profile, self.acks)
        self.order_watcher = OrderWatcher(self.profile, self.rest, self.acks)
        self._last_order_ws_activity_ms: int = _now_ms()

        # Risk supervision
        self._last_risk_action: str = "OK"
        self._flatten_issued: bool = False

        # ---------- Funds mismatch tripwire state ----------
        rf = (getattr(self.cfg, "metrics", {}) or {}).get("funds", {}) or {}
        # accept fraction or percent; normalize to fraction internally
        try:
            mismatch_pct = rf.get("mismatch_pct", None)
            if mismatch_pct is None:
                # fallback to legacy config location under risk.funds if present
                mismatch_pct = ((getattr(self.cfg, "risk", {}) or {}).get("funds", {}) or {}).get("mismatch_pct", 0.05)
            self._funds_mismatch_pct: float = float(mismatch_pct if float(mismatch_pct) <= 1.0 else float(mismatch_pct) / 100.0)
        except Exception:
            self._funds_mismatch_pct = 0.05
        try:
            self._funds_mismatch_secs: float = float(rf.get("mismatch_secs", 15.0))
        except Exception:
            self._funds_mismatch_secs = 15.0
        self._funds_free_buffer_pct: float = float(((getattr(self.cfg, "accounts", {}) or {}).get("funds_buffer_pct", 0.0)) or 0.0)
        self._funds_mismatch_since_ms: Optional[int] = None

        # Optional internal-liability resolver
        internal_liab: Optional[Callable[[], float]] = None
        try:
            candidate = getattr(self.order_manager, "liability_snapshot", None)
            if callable(candidate):
                internal_liab = candidate  # type: ignore[assignment]
        except Exception:
            internal_liab = None
        self._internal_liability_fn: Optional[Callable[[], float]] = internal_liab

        # ---------- Hedge-on-fill (edge-agnostic) ----------
        # Build a per-edge hedge config map from edges.<edge>.hedge
        self._hedge_cfg_by_edge: Dict[str, Dict[str, Any]] = {}
        try:
            _edges_block = getattr(self.cfg, "edges", {}) or {}
            for _name, _cfg in _edges_block.items():
                if _name in ("enabled", "registry"):
                    continue
                if isinstance(_cfg, dict):
                    hed = (_cfg.get("hedge") or {})
                    if isinstance(hed, dict) and hed.get("enabled", False):
                        self._hedge_cfg_by_edge[str(_name)] = hed
        except Exception:
            self._hedge_cfg_by_edge = {}
        # intent_id -> (market_id, selection_id, side, edge_name)
        self._hedge_candidates: Dict[str, Tuple[str, int, str, str]] = {}

        # ---------- Event-driven loop state (NEW) ----------
        self._delta_event: asyncio.Event = asyncio.Event()
        self._dirty_mids: set[str] = set()

        # Loop micro-coalescing / watchdog (configurable)
        lp = (getattr(self.cfg, "profile", {}) or {}).get("loop", {}) or {}
        self._debounce_ms: int = int(lp.get("debounce_ms", 8))      # coalesce WS bursts (≈5–10ms)
        self._watchdog_ms: int = int(lp.get("watchdog_ms", 100))    # tick even when quiet
        self._max_batch_ms: int = int(lp.get("max_batch_ms", 20))   # hard cap on coalescing
           
    # -------------------- Feature flag adapter -------------------- #
    @staticmethod
    def _ff_bool(ff: Dict[str, Any], name: str, *, maker_strict: bool = False) -> bool:
        try:
            edges = ff.get("flags", {}).get("edges", {})
            val = edges.get(name)
            if val is not None:
                if name == "maker" and maker_strict:
                    return str(val).lower() == "live"
                return bool(val) if isinstance(val, bool) else str(val).lower() in ("true", "1", "yes", "on", "live", "observer")
        except Exception:
            pass
        try:
            val = ff.get(name)
            return bool(val)
        except Exception:
            return False

    # -------------------- Observer counters -------------------- #
    def _inc_obs(self, edge_name: str, n: int) -> None:
        try:
            self._obs_counts[edge_name] = self._obs_counts.get(edge_name, 0) + int(n)
        except Exception:
            pass

    # -------------------- Intents accounting -------------------- #
    def _prune_intents_window(self, now_ms: int) -> None:
        try:
            cutoff = now_ms - 60_000
            self._intents_sent_1m = deque([t for t in self._intents_sent_1m if t >= cutoff])
        except Exception:
            self._intents_sent_1m = deque()

    def _record_intent_emit(self, now_ms: int) -> None:
        try:
            self._intents_sent_1m.append(now_ms)
        except Exception:
            self._intents_sent_1m = deque([now_ms])

    # -------------------- Orders client -------------------- #
    def _build_orders_client(self):
        try:
            return self.rest.build_orders_client()
        except Exception:
            from ..betfair.orders import OrdersClient
            return OrdersClient(self.profile)

    # -------------------- Funds update callback -------------------- #
    async def _on_funds_update(self, upd: Dict[str, Any]) -> None:
        try:
            available = float(upd.get("available", 0.0))
            min_required = float(upd.get("min_required", 0.0))
            low = bool(upd.get("low", False))
            exposure_api = float(upd.get("exposure", 0.0) or 0.0)

            self.alerts.low_free_balance(available, min_required)
            if low:
                self.ipc.pause()
                log_event("gate", "gate.funds", "paused", available=available, min_required=min_required)
            else:
                if self._funds_mismatch_since_ms is None:
                    self.ipc.resume()

            liab_fn = self._internal_liability_fn
            if callable(liab_fn):
                try:
                    internal_liab = float(liab_fn())
                except Exception:
                    internal_liab = None
                if internal_liab is not None:
                    denom = max(1.0, abs(exposure_api), abs(internal_liab))
                    rel_diff = abs(internal_liab - exposure_api) / denom
                    try:
                        metr.set_funds_mismatch_rel(rel_diff)
                    except Exception:
                        pass

                    now_ms = _now_ms()
                    if rel_diff > self._funds_mismatch_pct:
                        if self._funds_mismatch_since_ms is None:
                            self._funds_mismatch_since_ms = now_ms
                        elapsed = (now_ms - self._funds_mismatch_since_ms) / 1000.0
                        if elapsed >= self._funds_mismatch_secs:
                            metr.inc_funds_mismatch_total(1)
                            self.ipc.pause()
                            try:
                                self.alerts.funds_mismatch(
                                    internal_liability=internal_liab,
                                    api_exposure=exposure_api,
                                    rel_diff=rel_diff,
                                    threshold=self._funds_mismatch_pct,
                                    duration_s=elapsed,
                                )
                            except Exception:
                                pass
                            log_event(
                                "gate", "gate.funds_mismatch", "paused",
                                internal_liability=internal_liab, api_exposure=exposure_api,
                                rel_diff=rel_diff, threshold=self._funds_mismatch_pct, duration_s=elapsed
                            )
                    else:
                        if self._funds_mismatch_since_ms is not None:
                            self._funds_mismatch_since_ms = None
                            if not low:
                                self.ipc.resume()
                                log_event("gate", "gate.funds_mismatch", "resumed")
        except Exception:
            pass

    # -------------------- Boot wiring -------------------- #
    async def boot(self) -> None:
        log_event("app", "boot", "start", profile=self.profile)
        self.ipc.pause()

        # Build edges via config-driven loader (single source of truth)
        used_loader = False
        try:
            edges_cfg = getattr(self.cfg, "edges", {}) or {}
            if isinstance(edges_cfg, dict) and (edges_cfg.get("enabled") or edges_cfg.get("registry")):
                try:
                    self.edges = EdgeLoader(self.cfg).load()
                    used_loader = True
                    log_event("edges", "loader", "loaded", count=len(self.edges))
                except Exception as e:
                    log_event("edges", "loader", "error", error=type(e).__name__)
                    self.edges = {}
        except Exception:
            self.edges = {}

        # No feature-flag instantiation fallback: loader is authoritative.

        trend_flag   = self._ff_bool(getattr(self.cfg, "feature_flags", {}) or {}, "maker_thick_trend")
        if trend_flag:
            self.trend = TrendEngine(self.cfg)

        # Market stream
        await self.stream.connect()
        await self.stream.authenticate()
        try:
            self.stream._auth.start_keepalive()
        except Exception:
            pass

        initial_ids = self.selector.initial_subscriptions()
        await self.stream.subscribe_markets(initial_ids)
        try:
            self._known_market_ids = set(str(x) for x in initial_ids)
        except Exception:
            self._known_market_ids = set()

        # Heartbeat
        try:
            asyncio.create_task(HeartbeatPinger(self.profile, self.stream._auth).run())
        except Exception:
            pass

        # Reseed + rebuild orders
        await self._seed_after_connect()
        try:
            cur = await self._list_current_orders_all(list(self._known_market_ids))
        except Exception:
            cur = {"currentOrders": [], "moreAvailable": False}
        try:
            self.order_manager.rebuild_from_list_current_orders(cur)
        except Exception:
            pass

        # Funds poller
        acct_cfg = (getattr(self.cfg, "accounts", {}) or {})
        poll = acct_cfg.get("poll") or {}
        interval = float(poll.get("interval_s", 20.0))
        buffer_abs = float(acct_cfg.get("funds_buffer_abs", 0.0))
        buffer_pct = float(acct_cfg.get("funds_buffer_pct", 0.0))
        try:
            # Ensure accounts client exists for funds polling
            from src.betfair.accounts import AccountsClient  # lazy import to avoid cycles
            self.accounts = getattr(self, 'accounts', None) or AccountsClient(self.stream._auth)
            poller = AccountFundsPoller(
                self.accounts,
                interval_s=interval,
                buffer_abs=buffer_abs,
                buffer_pct=buffer_pct,
                on_update=self._on_funds_update,
            )
            self._funds_task = asyncio.create_task(poller.run())
        except Exception:
            pass

        # Safe boot complete — allow intents to flow
        self.ipc.resume()

        # Reader tasks (market + orders)
        asyncio.create_task(self._read_market_stream())
        self._order_monitor_task = asyncio.create_task(self._monitor_order_ws())
        try:
            os_cfg = (getattr(self.cfg, "betfair", {}) or {}).get("orders_stream", {}) or {}
            if bool(os_cfg.get("enabled", True)):
                asyncio.create_task(self.order_stream.run(on_activity=self._on_order_activity))
            else:
                asyncio.create_task(self.order_watcher.run(market_ids=list(self._known_market_ids), on_activity=self._on_order_activity))
        except Exception:
            asyncio.create_task(self.order_watcher.run(market_ids=list(self._known_market_ids), on_activity=self._on_order_activity))

        # (CHANGED) Discovery: start periodic sliding-window refresh loop
        self._disc_task = asyncio.create_task(self._discover_and_subscribe())

        asyncio.create_task(self._emit_dashboard_loop())
        self._risk_task = asyncio.create_task(self._risk_supervisor_loop())

        # Start executor router main loop (consumes intents from IPC)
        try:
            asyncio.create_task(self.router.serve())
        except Exception:
            pass

        # (NEW) Hedge listener
        if self._hedge_cfg_by_edge and (self._hedge_task is None or self._hedge_task.done()):
            self._hedge_task = asyncio.create_task(self._hedge_on_fill_loop())

        log_event("app", "boot", "ok")

    async def _seed_after_connect(self) -> None:
        now = _now_ms()
        if now - getattr(self, "_last_seed_ms", 0) < 5000:
            return
        self._last_seed_ms = now
        try:
            self.ipc.pause()
        except Exception:
            pass
        t0 = time.time()
        try:
            ids = list(self._known_market_ids)
        except Exception:
            ids = []
        if ids:
            bf = getattr(self.cfg, "betfair", {}) or {}
            st = bf.get("streaming", {}) or {}
            chunk = int(st.get("resubscribe_chunk", 50))
            try:
                depth = int(getattr(self.cfg, "stream_levels", st.get("ladder_levels", 3)) or 3)
            except Exception:
                depth = 3
            for i in range(0, len(ids), max(1, chunk)):
                try:
                    books = await self.rest.list_market_book(ids[i:i+chunk], virtualise=True, depth=min(3, depth))
                except TypeError:
                    books = await self.rest.list_market_book(market_ids=ids[i:i+chunk], virtualise=True, depth=min(3, depth))
                try:
                    self.book.reseed_from_snapshot(books)
                except Exception:
                    pass
        try:
            md = await self.rest.get_market_catalogue_state(market_ids=list(self._known_market_ids))
            for mk, payload in (md or {}).items():
                try:
                    self.book.set_market_metadata(
                        mk,
                        payload.get("suspend_reason"),
                        payload.get("bet_delay_models"),
                        payload.get("bet_delay"),
                        payload.get("bsp_available"),
                        market_base_rate=payload.get("market_base_rate"),
                        mbr_source=payload.get("mbr_source"),
                    )
                except Exception:
                    continue
        except Exception:
            pass
        try:
            cur = await self._list_current_orders_all(list(self._known_market_ids))
        except Exception:
            cur = {"currentOrders": [], "moreAvailable": False}
        try:
            self.order_manager.rebuild_from_list_current_orders(cur)
        except Exception:
            pass
        dt_ms = (time.time() - t0) * 1000.0
        try:
            inc("reseed_runs_total", 1)
            set_gauge("reseed_last_ms", dt_ms)
        except Exception:
            pass
        try:
            self.ipc.resume()
        except Exception:
            pass

    def _on_order_activity(self, ts_ms: int) -> None:
        self._last_order_ws_activity_ms = int(ts_ms)
        try:
            self.health.bump_orders(ts_ms)
        except Exception:
            pass
        try:
            set_gauge("order_ws_last_activity_age_s", max(0.0, (_now_ms() - self._last_order_ws_activity_ms)/1000.0))
        except Exception:
            pass

    async def _monitor_order_ws(self) -> None:
        hb_ms = int(((getattr(self.cfg, "betfair", {}) or {}).get("orders_stream", {}) or {}).get("heartbeat_ms", 5000))
        while True:
            try:
                age_s = max(0.0, (_now_ms() - self._last_order_ws_activity_ms) / 1000.0)
                try:
                    set_gauge("order_ws_last_activity_age_s", age_s)
                except Exception:
                    pass
                if age_s > (hb_ms / 1000.0) * 2.0:
                    if self._order_watcher_task is None or self._order_watcher_task.done():
                        try:
                            log_event("orders", "ws_unhealthy", "start_fallback", age_s=age_s)
                        except Exception:
                            pass
                        try:
                            self._order_watcher_task = asyncio.create_task(
                                self.order_watcher.run(market_ids=list(self._known_market_ids), on_activity=self._on_order_activity)
                            )
                        except Exception:
                            pass
                else:
                    if self._order_watcher_task and not self._order_watcher_task.done():
                        try:
                            self.order_watcher.stop()
                        except Exception:
                            pass
                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(1.0)

    async def _read_market_stream(self) -> None:
        async for msg in self.stream.read():
            try:
                if msg.get("op") == "mcm":
                    for m in msg.get("mc", []):
                        self.book.apply_delta(m)
                        # NEW: mark changed market and wake event-driven loop
                        try:
                            mid = str(m.get("id") or m.get("marketId") or "")
                            if mid:
                                self._dirty_mids.add(mid)
                                self._delta_event.set()
                        except Exception:
                            pass
                elif msg.get("op") == "connection":
                    log_event("stream", "conn", msg.get("connectionId", ""))
                    await self._seed_after_connect()
            except Exception:
                log_event("stream", "mcm", "error")

    # -------------------- Trend helpers -------------------- #
    def _apply_trend_to_plan(self, plan: Any, feats: Dict[str, Any]) -> Any:
        """Adjust a QuotePlan-like obj with trend modifiers (size_mult, extra_min_life_ms)."""
        if not self.trend:
            return plan
        try:
            mods = self.trend.modifiers(feats)
            # pause flag is handled in the main loop (we skip producing intents)
            size_mult = float(mods.get("size_mult", 1.0) or 1.0)
            extra_ml = int(mods.get("extra_min_life_ms", 0) or 0)

            if size_mult != 1.0 or extra_ml > 0:
                # Dataclass-safe: build a shallow clone with tweaks
                from dataclasses import replace
                new_size = max(0.0, float(getattr(plan, "size", 0.0)) * size_mult)
                ml = int(getattr(plan, "min_lifetime_ms", 0)) + extra_ml
                plan = replace(plan, size=new_size, size_hint=new_size, min_lifetime_ms=ml)
        except Exception:
            pass
        return plan

    def _apply_trend_to_intent(self, intent: Any, feats: Dict[str, Any]) -> Any:
        """Annotate the QuoteIntent with trend metadata (no size/price changes here)."""
        try:
            if self.trend:
                intent = self.trend.annotate_intent(intent, feats)
        except Exception:
            pass
        return intent

    # -------------------- Strategy loop (EVENT-DRIVEN) -------------------- #
    async def run(self) -> None:
        """
        Event-driven hot loop:
          - Wake on streaming deltas (via _delta_event), with tiny debounce to coalesce bursts
          - Prefer markets that actually changed since the last tick
          - Flow: selector.pick_hot -> edges -> FeeGate -> Arbiter -> Sizer -> publish
        Retains:
          - Intent rate token-bucket (~per-minute)
          - Periodic bankroll refresh
          - Optional TrendEngine modifiers
        """
        await self.boot()

        # ---- Loop knobs (kept from your old loop; still configurable) ----
        prof = getattr(self.cfg, "profile", {}) or {}
        loop_sleep_s = float((prof.get("loop") or {}).get("sleep_s", 0.10))  # used only on rare backoff paths
        bankroll_refresh_s = float((prof.get("loop") or {}).get("bankroll_refresh_s", 5.0))
        intents_per_min_cap = int((prof.get("loop") or {}).get("intents_per_min_cap", 600))
        max_edges_per_market = int((prof.get("loop") or {}).get("max_edges_per_market", 8))
        max_plans_per_loop = int((prof.get("loop") or {}).get("max_plans_per_loop", 64))

        last_bankroll_fetch = 0.0
        bankroll = float(await self._cached_bankroll())
        try:
            setattr(self.sizer, "_bankroll_source", "live_or_fixed")
            setattr(self.sizer, "_bankroll_amount", float(bankroll))
        except Exception:
            pass

        while True:
            try:
                # Wait for a stream delta OR a watchdog tick
                try:
                    await asyncio.wait_for(self._delta_event.wait(), timeout=self._watchdog_ms / 1000.0)
                except asyncio.TimeoutError:
                    pass

                # Debounce micro-bursts but never beyond max_batch_ms
                if self._delta_event.is_set():
                    await asyncio.sleep(max(0.0, min(self._debounce_ms, self._max_batch_ms) / 1000.0))
                self._delta_event.clear()

                now_ms = _now_ms()

                # Periodic bankroll refresh
                if (time.time() - last_bankroll_fetch) >= bankroll_refresh_s:
                    bankroll = float(await self._cached_bankroll())
                    last_bankroll_fetch = time.time()
                    try:
                        setattr(self.sizer, "_bankroll_amount", float(bankroll))
                    except Exception:
                        pass

                # Rate-limit: cap publishes in last rolling minute
                self._prune_intents_window(now_ms)
                if len(self._intents_sent_1m) >= intents_per_min_cap:
                    # stay event-driven, but nudge scheduler a touch
                    await asyncio.sleep(loop_sleep_s)
                    continue

                # Compute hot set, bias to markets that actually changed
                try:
                    max_hot = int(getattr(self.allocator, "max_hot", 12))
                except Exception:
                    max_hot = 12
                hot_mids = self.selector.pick_hot(self.book, max_hot)  # uses your existing selector
                dirty = list(self._dirty_mids)
                if dirty:
                    dirty_set = set(dirty)
                    targets = [m for m in hot_mids if m in dirty_set] or hot_mids
                else:
                    targets = hot_mids
                self._dirty_mids.clear()
                if not targets:
                    continue

                # Snapshot map once
                snapshots = getattr(self.book, "all_snapshots", None)
                snaps_map = snapshots() if callable(snapshots) else (getattr(self.book, "snapshots", {}) or {})

                # Collect proposals per target market
                proposals: List[Any] = []
                tr = getattr(self, "trend", None)  # trend may not exist
                for mid in targets:
                    snap = snaps_map.get(mid)
                    if not snap:
                        continue

                    feats = tr.features(snap) if tr else {}

                    # Allow trend to pause maker-style flow defensively (as your old loop)
                    if tr:
                        try:
                            mods = tr.modifiers(feats)
                            if bool(mods.get("pause", False)):
                                # (you could allow non-maker lanes here if desired)
                                continue
                        except Exception:
                            pass

                    for edge_name, edge_callable in list(getattr(self, "edges", {}) or {}).items()[:max_edges_per_market]:
                        try:
                            ctx = StrategyContext(
                                market_id=mid,
                                selection_id=None,  # edge may iterate runners
                                snapshot=snap,
                                cfg=(getattr(self.cfg, "edges", {}) or {}).get(edge_name, {}) or {},
                                book=self.hotloop,
                                now_ms=now_ms,
                                edge_id=edge_name,
                            )
                            out = edge_callable(ctx) or []
                            if not out:
                                continue
                            for p in out:
                                if self.fee_gate.ok(p, snapshot=self.book):
                                    proposals.append(p)
                                else:
                                    self._inc_obs(edge_name, 1)
                        except Exception as e:
                            try:
                                log_event("edge", "edge.error", type(e).__name__, edge=edge_name)
                            except Exception:
                                pass
                            continue

                if not proposals:
                    continue

                # Arbiter: proposals -> plans
                plans = self.arbiter.decide(proposals, snapshots=self.book, now_ms=now_ms)
                if not plans:
                    continue

                if len(plans) > max_plans_per_loop:
                    plans = plans[:max_plans_per_loop]

                # Size & publish
                published = 0
                for plan in plans:
                    # Trend plan-level modifiers
                    if tr:
                        snap = snaps_map.get(plan.market_id)
                        feats = tr.features(snap) if snap else {}
                        plan = self._apply_trend_to_plan(plan, feats)

                    intent = self.sizer.size_plan(plan, bankroll=bankroll, book=self.book)
                    if not intent:
                        continue

                    # Optional per-intent risk gate if present
                    try:
                        admit = getattr(self.risk, "admit", None)
                        if callable(admit):
                            ok, _ = admit(intent, self.book)
                            if not ok:
                                continue
                    except Exception:
                        pass

                    intent = intent.snapped() if hasattr(intent, "snapped") else intent

                    if tr:
                        snap = snaps_map.get(intent.market_id)
                        feats = tr.features(snap) if snap else {}
                        intent = self._apply_trend_to_intent(intent, feats)

                    if not self.ipc.is_paused():
                        await self.ipc.publish_intent(intent)
                        self._record_intent_emit(now_ms)
                        published += 1

                    if len(self._intents_sent_1m) >= intents_per_min_cap:
                        break

                # If we published nothing (e.g., all blocked), give the loop a tiny breather
                if published == 0:
                    await asyncio.sleep(loop_sleep_s)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                try:
                    log_event("app", "run.error", type(e).__name__)
                except Exception:
                    pass
                await asyncio.sleep(0.010)
           
    # -------------------- Discovery helpers (adaptive split) -------------------- #

    async def _fetch_catalogue_window(
        self,
        market_filter: Dict[str, Any],
        market_projection: List[str],
        max_results: int,
    ) -> List[Dict[str, Any]]:
        """Single listMarketCatalogue call with compatibility for both arg styles."""
        try:
            cats = await self.rest.list_market_catalogue(
                market_filter=market_filter,
                market_projection=market_projection,
                sort="FIRST_TO_START",
                max_results=max_results,
            )
        except TypeError:
            cats = await self.rest.list_market_catalogue(market_filter, market_projection, "FIRST_TO_START", max_results)
        return cats or []

    async def _fetch_catalogue_adaptive(
        self,
        slice_from_s: int,
        slice_to_s: int,
        base_filter: Dict[str, Any],
        market_projection: List[str],
        max_results: int,
        min_slice_seconds: int = 300,
    ) -> Tuple[List[Dict[str, Any]], bool]:
        """
        Recursively split a time window until each call returns < max_results or the slice is small.
        Returns (catalogue_rows, hit_cap_boolean).
        """
        import time as _t

        window = max(0, int(slice_to_s - slice_from_s))
        if window <= 0:
            return [], False

        # Try whole window first
        mf = dict(base_filter)
        mf["marketStartTime"] = {
            "from": _t.strftime("%Y-%m-%dT%H:%M:%SZ", _t.gmtime(slice_from_s)),
            "to":   _t.strftime("%Y-%m-%dT%H:%M:%SZ", _t.gmtime(slice_to_s)),
        }
        rows = await self._fetch_catalogue_window(mf, market_projection, max_results)
        if len(rows) < max_results or window <= max(60, int(min_slice_seconds)):
            # Either we didn't hit the cap, or we hit minimum slice size—return what we got.
            return rows, len(rows) >= max_results

        # Cap hit with a large window → split
        mid = slice_from_s + window // 2
        left_rows, left_cap = await self._fetch_catalogue_adaptive(
            slice_from_s, mid, base_filter, market_projection, max_results, min_slice_seconds
        )
        right_rows, right_cap = await self._fetch_catalogue_adaptive(
            mid, slice_to_s, base_filter, market_projection, max_results, min_slice_seconds
        )
        return left_rows + right_rows, (left_cap or right_cap or True)

    async def _discover_and_subscribe(self) -> None:
        """Periodic sliding-window discovery with cache expiry and safe unsubscribe."""
        cfgd = getattr(self.cfg, "discovery", {}) or {}
        horizon_h = int(cfgd.get("horizon_hours", 24))
        event_type_ids = list(cfgd.get("event_type_ids", [7]))          # config-driven; fallback [7]
        market_types   = list(cfgd.get("market_types", ["WIN", "PLACE"]))
        country_codes  = list(cfgd.get("country_codes", []))
        max_results    = int(cfgd.get("max_results", 1000))
        slice_hours    = int(cfgd.get("slice_hours", 3))
        refresh_interval_s  = int(cfgd.get("refresh_interval_s", 180))
        grace_post_off_s    = int(cfgd.get("grace_post_off_s", 1200))
        unsubscribe_enabled = bool(cfgd.get("unsubscribe_enabled", True))
        min_slice_seconds   = int(cfgd.get("min_slice_seconds", 300))   # NEW (optional)

        market_projection = ["EVENT", "MARKET_START_TIME", "MARKET_DESCRIPTION", "RUNNER_DESCRIPTION"]
        chunk = int(((getattr(self.cfg, "betfair", {}) or {}).get("streaming", {}) or {}).get("resubscribe_chunk", 50))

        while True:
            try:
                now = time.time()
                t_end = now + horizon_h * 3600
                cursor = now
                discovered: Dict[str, float] = {}
                slice_truncated = False

                # Shared (time-agnostic) part of the filter
                base_filter: Dict[str, Any] = {
                    "bspOnly": False,
                    "turnInPlayEnabled": True,
                    "inPlayOnly": False,
                }
                if event_type_ids:   # [] => omit to scan all event types
                    base_filter["eventTypeIds"] = event_type_ids
                if market_types:     # [] => omit to scan all market types
                    base_filter["marketTypeCodes"] = market_types
                if country_codes:
                    base_filter["marketCountries"] = country_codes

                while cursor < t_end:
                    slice_from = int(cursor)
                    slice_to = int(min(t_end, cursor + slice_hours * 3600))

                    cats, hit_cap = await self._fetch_catalogue_adaptive(
                        slice_from, slice_to, base_filter, market_projection, max_results, min_slice_seconds
                    )
                    slice_truncated = slice_truncated or bool(hit_cap)

                    for c in (cats or []):
                        try:
                            mid = str(c.get("marketId") or "")
                            if not mid:
                                continue
                            # Parse start time robustly
                            st = c.get("marketStartTime") or c.get("openDate")
                            ts: Optional[float] = None
                            if isinstance(st, str):
                                try:
                                    from datetime import datetime, timezone
                                    ts = datetime.fromisoformat(st.replace("Z","+00:00")).timestamp()
                                except Exception:
                                    try:
                                        ts = time.mktime(time.strptime(st, "%Y-%m-%dT%H:%M:%SZ"))
                                    except Exception:
                                        ts = None
                            elif isinstance(st, (int, float)):
                                ts = float(st)
                            if ts is None:
                                continue
                            discovered[mid] = float(ts)  # dict deduplicates across splits
                        except Exception:
                            continue

                    cursor = slice_to
                    await asyncio.sleep(0)

                if slice_truncated:
                    try:
                        inc("discovery_slice_truncated_total", 1)
                    except Exception:
                        pass

                # Subscribe new markets
                new_ids = [m for m in discovered.keys() if m not in self._known_market_ids]
                if new_ids:
                    for i in range(0, len(new_ids), max(1, chunk)):
                        try:
                            await self.stream.add_markets(new_ids[i:i+chunk])
                        except Exception:
                            pass
                    try:
                        self._known_market_ids |= set(new_ids)
                    except Exception:
                        pass
                    try:
                        inc("discovery_new_markets_total", len(new_ids))
                    except Exception:
                        pass

                # Upsert schedule entries
                for mid, start_ts in discovered.items():
                    try:
                        self._schedule[mid] = (float(start_ts), float(start_ts) + float(grace_post_off_s))
                    except Exception:
                        continue

                # Expire old markets
                now_epoch = time.time()
                expired = [mid for (mid, (_st, exp)) in list(self._schedule.items()) if exp < now_epoch]
                if expired and unsubscribe_enabled:
                    for i in range(0, len(expired), max(1, chunk)):
                        try:
                            await self.stream.remove_markets(expired[i:i+chunk])
                        except Exception:
                            pass
                    for mid in expired:
                        try:
                            self._schedule.pop(mid, None)
                            self._known_market_ids.discard(mid)
                        except Exception:
                            pass
                    try:
                        inc("discovery_unsubscribed_total", len(expired))
                    except Exception:
                        pass

                # Metrics + alerts heartbeat
                self._last_discovery_refresh_ts = time.time()
                try:
                    set_gauge("discovery_last_refresh_ts", float(self._last_discovery_refresh_ts))
                    set_gauge("discovery_known_market_ids", float(len(self._known_market_ids)))
                    # staleness gauge will be updated in dashboard loop; reset here for completeness
                    set_gauge("discovery_staleness_s", 0.0)
                except Exception:
                    pass
                try:
                    # Notify Alerts for staleness monitoring
                    self.alerts.discovery_heartbeat(self._last_discovery_refresh_ts)
                except Exception:
                    pass

            except asyncio.CancelledError:
                break
            except Exception as e:
                try:
                    log_event("alert", "discovery.refresh", "error", err=str(e))
                except Exception:
                    pass
            await asyncio.sleep(max(10, refresh_interval_s))

    # -------------------- Orders pagination helper -------------------- #

    async def _list_current_orders_all(self, market_ids: List[str]) -> Dict[str, Any]:
        """
        Fetch all pages of listCurrentOrders. Compatible with both arg-style signatures.
        """
        from_record = 0
        # Allow config override; sensible default
        page_size = int(((getattr(self.cfg, "betfair", {}) or {}).get("orders", {}) or {}).get("page_size", 200))
        agg = {"currentOrders": [], "moreAvailable": False}
        while True:
            try:
                res = await self.rest.list_current_orders(
                    market_ids=market_ids,
                    from_record=from_record,
                    record_count=page_size,
                )
            except TypeError:
                try:
                    res = await self.rest.list_current_orders(market_ids, from_record, page_size)
                except Exception:
                    # Fallback to unpaged call if client doesn't support paging
                    res = await self.rest.list_current_orders(market_ids)
            except Exception:
                # Safety fallback
                res = {"currentOrders": [], "moreAvailable": False}

            agg["currentOrders"].extend(res.get("currentOrders", []))
            more = bool(res.get("moreAvailable"))
            if not more:
                break
            from_record += page_size
            await asyncio.sleep(0)  # yield
        return agg

    async def _risk_supervisor_loop(self) -> None:
        while True:
            try:
                dec = self.risk.evaluate(
                    now_ms=_now_ms(),
                    last_stream_ts_ms=getattr(self.stream, "_last_activity_ms", None),
                    ack_p95_ms=None,
                    pnl_today=None,
                    dd_equity_pct=None,
                )
                action = getattr(dec, "action", "OK")
                if action in ("PAUSE", "FREEZE"):
                    self.ipc.pause()
                elif action == "FLATTEN":
                    self.ipc.pause()
                    if not self._flatten_issued:
                        self._flatten_issued = True
                        ids = list(self._known_market_ids)
                        for mid in ids:
                            try:
                                asyncio.create_task(self.order_manager.cancel_all_in_market(mid, reason="risk_flatten"))
                            except Exception:
                                continue
                else:
                    if self.ipc.is_paused():
                        self.ipc.resume()
                    self._flatten_issued = False
                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(1.0)

    async def _emit_dashboard_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(1.0)
                now_ms = _now_ms()
                self._prune_intents_window(now_ms)
                dashboards.emit_summary(
                    self.book, self._obs_counts, self.acks, self.order_manager, self._intents_sent_1m
                )
                # Update discovery staleness gauge for visibility (alerts use heartbeat internally)
                try:
                    if self._last_discovery_refresh_ts > 0.0:
                        set_gauge("discovery_staleness_s", max(0.0, time.time() - float(self._last_discovery_refresh_ts)))
                except Exception:
                    pass
                self.alerts.check(self.health, self.acks, self.order_manager)
            except asyncio.CancelledError:
                raise
            except Exception:
                log_event("dash", "emit", "error")

    # -------------------- Hedge-on-fill (edge-agnostic) -------------------- #
    def _register_hedge_candidate(self, qi: QuoteIntent) -> None:
        """
        Register an emitted intent for hedge-on-fill IF the originating edge
        has edges.<edge>.hedge.enabled: true in config.
        Edge name inferred from the first edge_contribs[*].edge_id (prefix before first "_").
        """
        try:
            contribs = list(getattr(qi, "edge_contribs", []) or [])
            if not contribs:
                return
            raw_id = str(contribs[0].get("edge_id") or "").strip().lower()
            if not raw_id:
                return
            edge_name = raw_id.split("_", 1)[0]
            if edge_name not in self._hedge_cfg_by_edge:
                return
            self._hedge_candidates[str(qi.intent_id)] = (
                str(qi.market_id),
                int(qi.selection_id),
                str(qi.side).upper(),
                edge_name,
            )
        except Exception:
            return

    async def _hedge_on_fill_loop(self) -> None:
        """
        Listen to ExecutionReports; on PARTIAL/FILLED for eligible intents, craft a hedge proposal and route it through
        FeeGate → Arbiter → Sizer → Router. Prices are snapped before routing.
        """
        log_event("hedge", "hedge.loop", "start", enabled=bool(self._hedge_cfg_by_edge))
        async for er in self.ipc.iter_reports():
            try:
                if not self._hedge_cfg_by_edge:
                    continue
                if er.action not in ("PARTIAL", "FILLED"):
                    continue
                tup = self._hedge_candidates.get(str(er.intent_id))
                if not tup:
                    continue  # not registered for hedge

                mk, sel, orig_side, edge_name = tup
                # Per-edge hedge config
                hed_cfg = self._hedge_cfg_by_edge.get(edge_name) or {}
                cushion_ticks = int(hed_cfg.get("cushion_ticks", 1))
                hedge_ttl_ms = int(hed_cfg.get("ttl_ms", 3000))
                opp_side = "LAY" if str(orig_side).upper() == "BACK" else "BACK"
                fill_px = float(getattr(er, "fill_price", 0.0) or 0.0)
                fill_sz = float(getattr(er, "fill_size", 0.0) or 0.0)
                if fill_px <= 0.0 or fill_sz <= 0.0:
                    continue

                # Price nudge toward quicker completion
                px = fill_px
                try:
                    if opp_side == "LAY":
                        # Offer a slightly better lay for takers (nudge downwards)
                        for _ in range(max(0, cushion_ticks)):
                            nxt = one_tick_down(px)
                            if nxt is None:
                                break
                            px = float(nxt)
                    else:
                        # BACK hedge: nudge upwards
                        for _ in range(max(0, cushion_ticks)):
                            nxt = one_tick_up(px)
                            if nxt is None:
                                break
                            px = float(nxt)
                except Exception:
                    pass

                # Build a tiny proposal (goes through FeeGate & Arbiter for hygiene)
                from ..core.schemas import EdgeProposal  # local import to avoid cycles at module import
                hedge_ev_ticks = float(cushion_ticks or 1)  # conservative expected net ticks
                ttl_ms = int(hedge_ttl_ms)

                proposal = EdgeProposal(
                    edge_id=f"{edge_name}_hedge_on_fill",
                    market_id=str(mk),
                    selection_id=int(sel),
                    side=opp_side,
                    price=float(px),
                    size_hint=float(fill_sz),
                    ttl_ms=ttl_ms,
                    weight=1.0,
                    ev_net_ticks=hedge_ev_ticks,
                    rationale=f"hedge-on-fill {opp_side} (edge={edge_name}; cushion={cushion_ticks})"
                )

                ok, _rsn, _ctx = self.fee_gate.allow(proposal, snapshot=self.book)
                if not ok:
                    log_event("hedge", "hedge.skip", "fee_gate_block")
                    continue

                plans = self.arbiter.decide([proposal], snapshots=self.book)
                if not plans:
                    continue

                # Convert the single plan → QuoteIntent via sizer (size decision) and emit
                plan = plans[0]
                # Sizer expects an EdgeProposal shape; reuse proposal with Arbiter's target price & TTL
                proposal2 = EdgeProposal(
                    edge_id=f"{edge_name}_hedge_on_fill",
                    market_id=str(plan.market_id),
                    selection_id=int(plan.selection_id),
                    side=str(plan.side),
                    price=float(plan.price),
                    size_hint=float(fill_sz),
                    ttl_ms=int(plan.min_lifetime_ms),
                    weight=1.0,
                    ev_net_ticks=hedge_ev_ticks,
                    rationale="hedge-sized"
                )

                # Use explicit bankroll path to avoid any async in hot listener
                # We pass bankroll via helper to keep sizing consistent; details ignored here.
                try:
                    stake, reason, _details = self.sizer.size(proposal2, bankroll=await self._cached_bankroll())
                except TypeError:
                    stake, reason, _details = self.sizer.size(proposal2, bankroll=await self._cached_bankroll(), risk_multiplier=1.0)
                if stake <= 0.0:
                    log_event("hedge", "hedge.skip", reason)
                    continue

                qi = QuoteIntent(
                    intent_id=f"HEDGE:{edge_name}:{er.intent_id}:{_now_ms()}",
                    market_id=str(plan.market_id),
                    selection_id=int(plan.selection_id),
                    side=str(plan.side),
                    price=float(plan.price),
                    size=float(stake),
                    tif="GTC",
                    persistence=str(plan.persistence),
                    min_lifetime_ms=int(plan.min_lifetime_ms),
                    max_replace_rate_per_min=60,
                    edge_contribs=[{"edge_id": f"{edge_name}_hedge_on_fill", "weight": 1.0, "ev_net_ticks": hedge_ev_ticks, "price": plan.price, "size": stake}],
                    risk_tags={"reason": "hedge_on_fill", "parent_intent": er.intent_id, "edge": edge_name},
                    client_ts_ms=_now_ms(),
                )

                # snap & emit (publish to IPC; router.serve() will process + call our registrar)
                qi = qi.snapped() if hasattr(qi, "snapped") else qi
                if not self.ipc.is_paused():
                    await self.ipc.publish_intent(qi)
                    self._record_intent_emit(_now_ms())
                    log_event("hedge", "hedge.emit", "ok",
                              market_id=qi.market_id, selection_id=qi.selection_id,
                              side=qi.side, price=qi.price, size=qi.size, edge=edge_name)

            except asyncio.CancelledError:
                break
            except Exception as e:
                log_event("hedge", "hedge.error", type(e).__name__)

    async def _cached_bankroll(self) -> float:
        bankroll_source = (self.cfg.get('risk', {}).get('bankroll', {}).get('source') or 'fixed')
        if bankroll_source == 'live':
            try:
                # Locate a funds snapshot provider
                acct = getattr(self, 'accounts', None) or getattr(self, 'ctx', None)
                funds = None
                if acct is not None:
                    if hasattr(acct, 'latest_funds_snapshot'):
                        funds = acct.latest_funds_snapshot()
                    elif hasattr(acct, 'get_latest_funds'):
                        funds = acct.get_latest_funds()
                if funds and isinstance(funds, dict):
                    avail = float(funds.get('availableToBetBalance') or funds.get('available') or 0.0)
                    haircut = float(self.cfg.get('risk', {}).get('bankroll', {}).get('live_haircut_pct', 10))
                    live_bankroll = max(0.0, avail * (1.0 - haircut/100.0))
                    if live_bankroll > 0.0:
                        return float(live_bankroll)
            except Exception:
                pass
        # fixed fallback
        fixed_amt = float(self.cfg.get('risk', {}).get('bankroll', {}).get('fixed_amount', 0.0))
        return float(fixed_amt)


async def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python -m src.apps.trader <profile>")
        sys.exit(1)
    profile = sys.argv[1]
    app = TraderApp(profile)
    await app.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
