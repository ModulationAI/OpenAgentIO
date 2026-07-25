"""Tests for openagentio.bridge.supervisor."""
from __future__ import annotations

import asyncio
import logging
from random import Random

import pytest

from openagentio.bridge.health import BridgeHealth
from openagentio.bridge.supervisor import (
    EventSourceSupervisor,
    PermanentBridgeError,
    RestartPolicy,
    SupervisorState,
)
from openagentio.bus.errors import AuthFailureError


async def test_task_recorded_and_cancelled_on_stop() -> None:
    """The supervisor records its task and cancels it on stop()."""
    started = False

    async def work() -> None:
        nonlocal started
        started = True
        await asyncio.Event().wait()

    sup = EventSourceSupervisor(
        "b", work, policy=RestartPolicy(max_restarts=0)
    )
    await sup.start()
    await asyncio.sleep(0)
    assert started
    assert sup._task is not None
    assert sup.state == SupervisorState.RUNNING

    await sup.stop()
    assert sup.state == SupervisorState.STOPPED
    assert sup._task is None or sup._task.done()


async def test_structured_log_on_exception(caplog) -> None:
    """Task failures produce structured log records."""
    caplog.set_level(logging.WARNING)

    async def work() -> None:
        raise RuntimeError("boom")

    sup = EventSourceSupervisor(
        "my-bridge",
        work,
        policy=RestartPolicy(max_restarts=0, base_delay_seconds=0.001),
    )
    await sup.start()
    await asyncio.sleep(0.05)

    assert any(
        "supervisor work failed" in rec.message
        and "my-bridge" in rec.message
        and "RuntimeError" in rec.message
        and "retryable=True" in rec.message
        for rec in caplog.records
    )
    assert sup.health.last_error is not None
    assert isinstance(sup.health.last_error, RuntimeError)


async def test_limited_auto_restart() -> None:
    """A failing task is restarted at most max_restarts times."""
    calls = 0

    async def work() -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("fail")

    sup = EventSourceSupervisor(
        "b",
        work,
        policy=RestartPolicy(
            max_restarts=2,
            base_delay_seconds=0.001,
            jitter_enabled=False,
        ),
    )
    await sup.start()
    await asyncio.sleep(0.1)

    assert calls == 3  # initial + 2 restarts
    assert sup.state == SupervisorState.FAILED
    assert sup.health.health == BridgeHealth.UNHEALTHY


async def test_backoff_bounds_with_jitter() -> None:
    """Jitter keeps the delay within [0, raw] for full jitter."""
    rng = Random(0)
    policy = RestartPolicy(
        base_delay_seconds=1.0,
        max_delay_seconds=10.0,
        jitter_enabled=True,
        max_jitter_ratio=1.0,
    )

    async def work() -> None:
        pass

    sup = EventSourceSupervisor(
        "b", work, policy=policy, random_source=rng
    )
    await sup.start()
    await asyncio.sleep(0)

    sup._consecutive_failures = 0
    raw0 = sup._compute_backoff(RuntimeError("x"))
    assert 0 <= raw0 <= policy.base_delay_seconds
    sup._consecutive_failures = 1
    raw1 = sup._compute_backoff(RuntimeError("x"))
    assert 0 <= raw1 <= policy.base_delay_seconds
    sup._consecutive_failures = 4
    raw4 = sup._compute_backoff(RuntimeError("x"))
    assert 0 <= raw4 <= policy.max_delay_seconds

    await sup.stop()


async def test_permanent_error_does_not_restart() -> None:
    """Permanent errors stop the supervisor immediately."""
    calls = 0

    async def work() -> None:
        nonlocal calls
        calls += 1
        raise AuthFailureError("bad token")

    sup = EventSourceSupervisor(
        "b", work, policy=RestartPolicy(max_restarts=10)
    )
    await sup.start()
    await asyncio.sleep(0.05)

    assert calls == 1
    assert sup.state == SupervisorState.FAILED
    assert sup.health.health == BridgeHealth.UNHEALTHY


async def test_clean_exit_is_restarted() -> None:
    """A work coroutine that returns is treated as a silent stop and restarted."""
    calls = 0

    async def work() -> None:
        nonlocal calls
        calls += 1

    sup = EventSourceSupervisor(
        "b",
        work,
        policy=RestartPolicy(
            max_restarts=2,
            base_delay_seconds=0.001,
            jitter_enabled=False,
        ),
    )
    await sup.start()
    await asyncio.sleep(0.1)

    assert calls == 3
    assert sup.state == SupervisorState.FAILED


async def test_stop_during_backoff() -> None:
    """Calling stop() during the backoff sleep cancels the pending restart."""
    sleep_started = asyncio.Event()

    async def sleeper(delay: float) -> None:
        sleep_started.set()
        await asyncio.Event().wait()

    async def work() -> None:
        raise RuntimeError("fail")

    sup = EventSourceSupervisor(
        "b",
        work,
        policy=RestartPolicy(base_delay_seconds=60.0, jitter_enabled=False),
        sleeper=sleeper,
    )
    await sup.start()
    await sleep_started.wait()

    await sup.stop()
    assert sup.state == SupervisorState.STOPPED


async def test_health_failure_threshold() -> None:
    """Health flips to UNHEALTHY once consecutive failures cross the threshold."""
    calls = 0

    async def work() -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("fail")

    sup = EventSourceSupervisor(
        "b",
        work,
        policy=RestartPolicy(
            max_restarts=10,
            base_delay_seconds=0.001,
            jitter_enabled=False,
            health_failure_threshold=2,
        ),
    )
    await sup.start()
    await asyncio.sleep(0.05)

    # After the first failure (consecutive=1) health is DEGRADED;
    # after the second failure (consecutive=2) it becomes UNHEALTHY.
    assert sup.health.consecutive_failures >= 2
    assert sup.health.health == BridgeHealth.UNHEALTHY
    assert sup.health.restarts_in_window >= 1

    await sup.stop()


async def test_start_during_backoff_is_noop() -> None:
    """Calling start() while a restart is already pending must not spawn a second work task."""
    sleep_started = asyncio.Event()

    async def sleeper(delay: float) -> None:
        sleep_started.set()
        await asyncio.Event().wait()

    calls = 0

    async def work() -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("fail")

    sup = EventSourceSupervisor(
        "b",
        work,
        policy=RestartPolicy(base_delay_seconds=60.0, jitter_enabled=False),
        sleeper=sleeper,
    )
    await sup.start()
    await sleep_started.wait()
    assert calls == 1

    await sup.start()  # must not create another work task while backoff is pending
    assert calls == 1

    await sup.stop()


async def test_retry_after_honored() -> None:
    """Exceptions carrying ``retry_after_ms`` use that value as the backoff base."""
    class RateLimited(Exception):
        retry_after_ms = 500

    sleeper_calls: list[float] = []

    async def sleeper(delay: float) -> None:
        sleeper_calls.append(delay)

    async def work() -> None:
        raise RateLimited("rate limited")

    sup = EventSourceSupervisor(
        "b",
        work,
        policy=RestartPolicy(
            max_restarts=1,
            base_delay_seconds=1.0,
            max_delay_seconds=300.0,
            jitter_enabled=False,
        ),
        sleeper=sleeper,
    )
    await sup.start()
    await asyncio.sleep(0.05)

    assert len(sleeper_calls) >= 1
    assert sleeper_calls[0] == 0.5
    await sup.stop()


async def test_stop_does_not_swallow_external_cancellation() -> None:
    """If the caller of stop() is cancelled, the CancelledError must propagate."""
    work_started = asyncio.Event()

    async def work() -> None:
        work_started.set()
        await asyncio.Event().wait()

    sup = EventSourceSupervisor("b", work, policy=RestartPolicy(max_restarts=0))
    await sup.start()
    await work_started.wait()

    async def stopper() -> None:
        await sup.stop()

    task = asyncio.create_task(stopper())
    await asyncio.sleep(0)  # let stopper enter await on the work task
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    # stop() should still have cleaned up before re-raising
    assert sup.state == SupervisorState.STOPPED
    assert sup._task is None or sup._task.done()
