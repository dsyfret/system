"""
Location : src/execution/interfaces/queue.py
Purpose  : Simple, production-safe in-process IPC with pause/resume gate.
           - Uses asyncio queues for intents and reports (one topic each).
           - Reads topic names from configs/base.yaml (ipc.*).
           - Provides async publish/subscribe APIs.
           - Provides a pause gate for reconnect hygiene (no intents while paused).

Upgrade path:
  Replace this with Redis/Kafka/ZeroMQ later; keep the same IPC interface.
"""

from __future__ import annotations
import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator, Dict, Optional

from ...core.config import load_config
from ...core.logging import log_event
from .intents import QuoteIntent
from .reports import ExecutionReport


class IPC:
    """
    In-proc message bus:
      - .publish_intent(QuoteIntent)
      - .iter_intents(): async iterator for executor
      - .publish_report(ExecutionReport)
      - .iter_reports(): async iterator for strategy
      - .pause() / .resume() or 'async with ipc.paused(): ...'
    """

    def __init__(self, profile_name: str) -> None:
        snap = load_config(profile_name)
        ipc = getattr(snap, "ipc", {}) or {}
        self.intents_topic = str(ipc.get("intents_topic", "intents.v1"))
        self.reports_topic = str(ipc.get("reports_topic", "reports.v1"))

        # In-proc queues
        self._q_intents: asyncio.Queue[Dict] = asyncio.Queue(maxsize=10_000)
        self._q_reports: asyncio.Queue[Dict] = asyncio.Queue(maxsize=10_000)

        # Pause gate (when True, publishing intents waits)
        self._paused = asyncio.Event()
        self._paused.set()  # set = running; clear = paused

    # ---------------- Strategy-side API ---------------- #

    async def publish_intent(self, qi: QuoteIntent) -> None:
        """
        Strategy publishes an intent. If paused, this waits until resume().
        """
        await self._paused.wait()
        payload = qi.to_wire()
        try:
            self._q_intents.put_nowait(payload)
        except asyncio.QueueFull:
            # Backpressure: block until space is available (rare in steady state)
            await self._q_intents.put(payload)
        log_event("ipc", "ipc.intent", "publish", topic=self.intents_topic, market_id=qi.market_id, selection_id=qi.selection_id)

    async def iter_reports(self) -> AsyncIterator[ExecutionReport]:
        """
        Strategy subscribes to execution reports coming back from the executor.
        """
        while True:
            payload = await self._q_reports.get()
            try:
                yield ExecutionReport.from_wire(payload)
            finally:
                self._q_reports.task_done()

    # ---------------- Executor-side API ---------------- #

    async def iter_intents(self) -> AsyncIterator[QuoteIntent]:
        """
        Executor consumes intents to act on.
        """
        while True:
            payload = await self._q_intents.get()
            try:
                yield QuoteIntent.from_wire(payload)
            finally:
                self._q_intents.task_done()

    async def publish_report(self, er: ExecutionReport) -> None:
        """
        Executor publishes back execution reports.
        """
        payload = er.to_wire()
        try:
            self._q_reports.put_nowait(payload)
        except asyncio.QueueFull:
            await self._q_reports.put(payload)
        # (No pause on reports; they should flow even while paused.)
        # Minimal log to avoid noise
        # log_event("ipc", "ipc.report", "publish", topic=self.reports_topic, market_id=er.market_id, selection_id=er.selection_id)

    # ---------------- Pause / Resume ---------------- #

    def pause(self) -> None:
        """
        Stop new intents from leaving (used during reconnect hygiene).
        """
        self._paused.clear()
        log_event("ipc", "ipc.state", "paused")

    def resume(self) -> None:
        """
        Allow intents to flow again.
        """
        self._paused.set()
        log_event("ipc", "ipc.state", "resumed")

    @asynccontextmanager
    async def paused(self):
        """
        Async context manager: block intents during the critical section.
        """
        self.pause()
        try:
            yield
        finally:
            self.resume()