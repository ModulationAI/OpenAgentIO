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
from typing import Any

import pytest

from openagentio.bridge.base import Bridge
from openagentio.bridge.config import BridgeConfig, BridgeDefinition
from openagentio.bridge.runner import BridgeRunner


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
    runner = BridgeRunner(
        bus, _cfg("slow", "fast"), _factories({"slow": slow, "fast": fast})
    )

    # Override the timeout to keep the test fast. The production default is
    # 10s; we test the mechanism, not the number.
    import openagentio.bridge.runner as runner_mod

    original = runner_mod._BRIDGE_STOP_TIMEOUT
    runner_mod._BRIDGE_STOP_TIMEOUT = 0.1
    try:
        await runner.start()
        loop = asyncio.get_event_loop()
        start = loop.time()
        await runner.stop()
        elapsed = loop.time() - start
    finally:
        runner_mod._BRIDGE_STOP_TIMEOUT = original

    # LIFO order: fast is stopped first (returns instantly), slow times out
    # after ~0.1s. Total budget: comfortably under 1s.
    assert elapsed < 1.0, f"stop took {elapsed:.2f}s, expected < 1s"
    assert fast.stopped == 1
    assert slow.stopped == 1


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
