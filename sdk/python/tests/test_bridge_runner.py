"""BridgeRunner lifecycle tests.

These cover shutdown semantics that were previously only exercised indirectly
by concrete-bridge tests. They exist to catch regressions of the failure modes
listed in the P0 §1 remediation checklist:

* one misbehaving bridge must not block shutdown of the others;
* stop() must be bounded in time;
* CancelledError must propagate but not skip cleanup;
* stop() must be idempotent and safe after a partially-failed start().
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest

from openagentio.bridge.base import Bridge
from openagentio.bridge.config import BridgeConfig, BridgeDefinition
from openagentio.bridge.health import BridgeHealth, BridgeHealthSnapshot
from openagentio.bridge.runner import BridgeRunner
from openagentio.bridge.supervisor import EventSourceSupervisor, RestartPolicy


class _RecorderBridge(Bridge):
    """A trivial Bridge that records start/stop calls."""

    def __init__(
        self,
        *,
        start_raises: BaseException | None = None,
        stop_raises: BaseException | None = None,
        stop_hangs: bool = False,
    ) -> None:
        self.started = False
        self.stopped = 0
        self._start_raises = start_raises
        self._stop_raises = stop_raises
        self._stop_hangs = stop_hangs

    async def start(self) -> None:
        if self._start_raises is not None:
            raise self._start_raises
        self.started = True

    async def stop(self) -> None:
        self.stopped += 1
        if self._stop_hangs:
            # Simulate a bridge whose stop() never returns. The runner must
            # bound this via wait_for and move on.
            await asyncio.Event().wait()
        if self._stop_raises is not None:
            raise self._stop_raises


def _cfg(*names: str) -> BridgeConfig:
    return BridgeConfig(
        version="v1",
        bridges=tuple(BridgeDefinition(name=n, type=n) for n in names),
    )


def _factories(bridges: dict[str, _RecorderBridge]) -> dict[str, Any]:
    return {name: (lambda _bus, _defn, b=b: b) for name, b in bridges.items()}


async def test_shutdown_stops_all_bridges_when_one_raises(bus) -> None:
    """A failing stop() on one bridge must not skip the others."""
    a = _RecorderBridge(stop_raises=RuntimeError("boom"))
    b = _RecorderBridge()
    c = _RecorderBridge()
    runner = BridgeRunner(bus, _cfg("a", "b", "c"), _factories({"a": a, "b": b, "c": c}))

    await runner.start()
    await runner.stop()

    assert a.stopped == 1
    assert b.stopped == 1
    assert c.stopped == 1


async def test_shutdown_bounds_slow_bridge_and_continues(bus) -> None:
    """A bridge whose stop() hangs must not pin the runner.

    Regression: without the wait_for cap in BridgeRunner._shutdown, one such
    bridge would block the whole shutdown and manifest as pytest hanging on
    session teardown (see remediation checklist §1).
    """
    slow = _RecorderBridge(stop_hangs=True)
    fast = _RecorderBridge()

    # Use a short stop_timeout to keep the test fast. The production default
    # is 10s; we test the mechanism, not the number.
    runner = BridgeRunner(
        bus,
        _cfg("slow", "fast"),
        _factories({"slow": slow, "fast": fast}),
        stop_timeout=0.1,
    )

    await runner.start()
    loop = asyncio.get_event_loop()
    start = loop.time()
    await runner.stop()
    elapsed = loop.time() - start

    # LIFO order: fast is stopped first (returns instantly), slow times out
    # after ~0.1s. Total budget: comfortably under 1s.
    assert elapsed < 1.0, f"stop took {elapsed:.2f}s, expected < 1s"
    assert fast.stopped == 1
    assert slow.stopped == 1


async def test_stop_timeout_exposed(bus) -> None:
    """BridgeRunner exposes the configured stop timeout."""
    runner = BridgeRunner(bus, _cfg("a"), _factories({"a": _RecorderBridge()}))
    assert runner.stop_timeout == 10.0

    runner_fast = BridgeRunner(
        bus,
        _cfg("a"),
        _factories({"a": _RecorderBridge()}),
        stop_timeout=0.5,
    )
    assert runner_fast.stop_timeout == 0.5


async def test_stop_is_idempotent(bus) -> None:
    """stop() must be safe to call multiple times."""
    a = _RecorderBridge()
    runner = BridgeRunner(bus, _cfg("a"), _factories({"a": a}))
    await runner.start()
    await runner.stop()
    await runner.stop()  # second call is a no-op
    assert a.stopped == 1  # not stopped twice


async def test_stop_safe_after_partial_start_failure(bus) -> None:
    """If a bridge's start() raises, previously-started bridges get stop()'d."""
    a = _RecorderBridge()
    b = _RecorderBridge(start_raises=RuntimeError("no"))
    c = _RecorderBridge()
    runner = BridgeRunner(bus, _cfg("a", "b", "c"), _factories({"a": a, "b": b, "c": c}))

    with pytest.raises(RuntimeError, match="no"):
        await runner.start()

    # a was started and must be stopped. b was tracked (and start()-failed)
    # and is still stopped by the runner (contract: stop() is safe after
    # partial start). c was never reached.
    assert a.stopped == 1
    assert b.stopped == 1
    assert c.stopped == 0

    # Idempotent post-failure stop().
    await runner.stop()
    assert a.stopped == 1
    assert b.stopped == 1


async def test_shutdown_reraises_cancelled_after_cleanup(bus) -> None:
    """A bridge whose stop() raises CancelledError must not skip siblings."""
    a = _RecorderBridge(stop_raises=asyncio.CancelledError())
    b = _RecorderBridge()
    runner = BridgeRunner(bus, _cfg("a", "b"), _factories({"a": a, "b": b}))

    await runner.start()
    with pytest.raises(asyncio.CancelledError):
        await runner.stop()

    # Both must have been visited even though one raised CancelledError.
    assert a.stopped == 1
    assert b.stopped == 1


async def test_start_failure_preserved_despite_cleanup_cancelled(bus) -> None:
    """A CancelledError during rollback must not mask the original start exception."""
    a = _RecorderBridge()
    b = _RecorderBridge(
        start_raises=RuntimeError("boom"),
        stop_raises=asyncio.CancelledError(),
    )
    c = _RecorderBridge()
    runner = BridgeRunner(bus, _cfg("a", "b", "c"), _factories({"a": a, "b": b, "c": c}))

    with pytest.raises(RuntimeError, match="boom") as exc_info:
        await runner.start()

    # a was started and rolled back. b was tracked and stopped (its CancelledError
    # must be suppressed). c was never reached.
    assert a.stopped == 1
    assert b.stopped == 1
    assert c.stopped == 0

    # The traceback must end at the original start() failure, not at the
    # runner's re-raise point, confirming we used a bare raise.
    import traceback
    tb = traceback.extract_tb(exc_info.value.__traceback__)
    assert tb[-1].filename == __file__
    assert tb[-1].name == "start"


class _HealthBridge(Bridge):
    """A bridge that reports a fixed health snapshot."""

    def __init__(self, snapshot: BridgeHealthSnapshot) -> None:
        self._snapshot = snapshot

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    @property
    def health(self) -> BridgeHealthSnapshot:
        return self._snapshot


class _ActiveBridge(Bridge):
    """A fake active bridge using EventSourceSupervisor."""

    def __init__(self, work, policy: RestartPolicy) -> None:
        self._work = work
        self._policy = policy
        self._supervisor: EventSourceSupervisor | None = None

    async def start(self) -> None:
        self._supervisor = EventSourceSupervisor(
            "active", self._work, policy=self._policy
        )
        await self._supervisor.start()

    async def stop(self) -> None:
        if self._supervisor is not None:
            await self._supervisor.stop()

    @property
    def health(self) -> BridgeHealthSnapshot:
        if self._supervisor is not None:
            return self._supervisor.health
        return BridgeHealthSnapshot.unknown()


async def test_health_reflects_worst_bridge_state(bus) -> None:
    """Runner health aggregates per-bridge snapshots, worst wins."""
    healthy = _HealthBridge(BridgeHealthSnapshot.healthy())
    unhealthy = _HealthBridge(BridgeHealthSnapshot.unhealthy(message="bad"))
    factories = {
        "h": lambda _b, _d: healthy,
        "u": lambda _b, _d: unhealthy,
    }
    runner = BridgeRunner(bus, _cfg("h", "u"), factories)
    await runner.start()

    snapshot = runner.health
    assert snapshot.overall == BridgeHealth.UNHEALTHY
    assert snapshot.by_name("h").health == BridgeHealth.HEALTHY
    assert snapshot.by_name("u").health == BridgeHealth.UNHEALTHY

    await runner.stop()


async def test_health_detects_silent_stop(bus, caplog) -> None:
    """A bridge whose supervised task exits cleanly is visible in runner health."""
    caplog.set_level(logging.WARNING)
    calls = 0

    async def work() -> None:
        nonlocal calls
        calls += 1

    policy = RestartPolicy(
        max_restarts=1, base_delay_seconds=0.001, jitter_enabled=False
    )
    runner = BridgeRunner(
        bus, _cfg("active"), {"active": lambda _b, _d: _ActiveBridge(work, policy)}
    )
    await runner.start()
    await asyncio.sleep(0.05)

    snapshot = runner.health
    assert snapshot.overall in (BridgeHealth.DEGRADED, BridgeHealth.UNHEALTHY)
    assert snapshot.by_name("active").restarts_in_window >= 1
    assert any("bridge health degraded" in rec.message for rec in caplog.records)

    await runner.stop()


async def test_permanent_bridge_failure_does_not_affect_others(bus) -> None:
    """One unhealthy bridge must not stop sibling bridges or the runner."""

    async def work() -> None:
        raise RuntimeError("fail")

    policy = RestartPolicy(max_restarts=0, base_delay_seconds=0.001, jitter_enabled=False)
    a = _ActiveBridge(work, policy)
    b = _RecorderBridge()
    factories = {
        "active": lambda _b, _d: a,
        "rec": lambda _b, _d: b,
    }
    runner = BridgeRunner(bus, _cfg("active", "rec"), factories)
    await runner.start()
    await asyncio.sleep(0.05)

    assert runner.health.overall == BridgeHealth.UNHEALTHY
    assert b.started is True
    assert b.stopped == 0

    await runner.stop()
    assert b.stopped == 1
