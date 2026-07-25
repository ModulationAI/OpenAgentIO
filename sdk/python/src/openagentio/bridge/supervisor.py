"""Supervision for active Event Source bridges.

An :class:`EventSourceSupervisor` owns a single background coroutine (the
"work"), records the ``asyncio.Task``, applies a bounded restart/backoff
policy, classifies retryable vs permanent errors, and exposes a health
snapshot. It is designed to be embedded inside a :class:`openagentio.bridge.Bridge`
so that the :class:`openagentio.bridge.BridgeRunner` stays protocol-agnostic.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from random import Random
from typing import Awaitable, Callable

from openagentio.bridge.health import BridgeHealth, BridgeHealthSnapshot
from openagentio.bus.errors import AuthFailureError, InvalidRequestError
from openagentio.bridge.config import BridgeConfigError


class PermanentBridgeError(Exception):
    """Marker exception: a Bridge has hit a permanent error and should not retry."""


@dataclass(frozen=True)
class RestartPolicy:
    """Policy controlling how a supervisor restarts failed work."""

    max_restarts: int = 5
    restart_window_seconds: float = 300.0
    base_delay_seconds: float = 2.0
    max_delay_seconds: float = 300.0
    jitter_enabled: bool = True
    max_jitter_ratio: float = 1.0
    health_failure_threshold: int = 3


class SupervisorState:
    """Lifecycle states of a supervisor."""

    IDLE = "idle"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


Work = Callable[[], Awaitable[None]]
Clock = Callable[[], float]
Sleeper = Callable[[float], Awaitable[None]]
RetryPredicate = Callable[[BaseException], bool]


def default_is_retryable(exc: BaseException) -> bool:
    """Classify an exception as retryable or permanent.

    Permanent errors are those that will not resolve by waiting and retrying:
    cancellation, configuration mistakes, authentication/authorization failures,
    and explicitly-marked permanent errors.
    """
    if isinstance(
        exc,
        (
            asyncio.CancelledError,
            BridgeConfigError,
            AuthFailureError,
            InvalidRequestError,
            PermanentBridgeError,
        ),
    ):
        return False
    return True


class EventSourceSupervisor:
    """Supervise a single long-running async workload.

    The supervisor is intentionally low-level: it only knows about the work
coroutine, a restart policy, and a retry classifier. Bridge-specific semantics
(e.g., Matrix ``/sync`` rate limits) are communicated through the
``retry_after_ms`` attribute on exceptions or through a custom
``is_retryable`` predicate.
    """

    def __init__(
        self,
        name: str,
        work: Work,
        *,
        policy: RestartPolicy | None = None,
        is_retryable: RetryPredicate | None = None,
        clock: Clock | None = None,
        sleeper: Sleeper | None = None,
        random_source: Random | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._name = name
        self._work = work
        self._policy = policy or RestartPolicy()
        self._is_retryable = is_retryable or default_is_retryable
        self._clock = clock or time.monotonic
        self._sleeper = sleeper or asyncio.sleep
        self._random = random_source or Random()
        self._logger = logger or logging.getLogger("openagentio.bridge.supervisor")

        self._state: str = SupervisorState.IDLE
        self._stop_requested: bool = False
        self._task: asyncio.Task[None] | None = None
        self._restart_task: asyncio.Task[None] | None = None

        self._consecutive_failures: int = 0
        self._restarts: deque[float] = deque()
        self._last_success_at: float | None = None
        self._last_error: BaseException | None = None
        self._unhealthy_due_to_failures: bool = False

    @property
    def state(self) -> str:
        """Current supervisor lifecycle state."""
        return self._state

    @property
    def health(self) -> BridgeHealthSnapshot:
        """Snapshot of the supervisor's health."""
        if self._state == SupervisorState.IDLE:
            return BridgeHealthSnapshot.unknown(
                message="supervisor has not been started"
            )
        if self._state == SupervisorState.STOPPED:
            return BridgeHealthSnapshot.unknown(message="supervisor stopped")
        if self._state == SupervisorState.FAILED:
            return BridgeHealthSnapshot.unhealthy(
                message="supervisor failed permanently",
                last_error=self._last_error,
                consecutive_failures=self._consecutive_failures,
                restarts_in_window=self._restarts_in_window(),
            )

        restarts = self._restarts_in_window()
        if self._unhealthy_due_to_failures:
            return BridgeHealthSnapshot.unhealthy(
                message="too many consecutive failures",
                last_error=self._last_error,
                consecutive_failures=self._consecutive_failures,
                restarts_in_window=restarts,
            )
        if self._consecutive_failures > 0:
            return BridgeHealthSnapshot.degraded(
                message="work is failing/restarting",
                last_error=self._last_error,
                consecutive_failures=self._consecutive_failures,
                restarts_in_window=restarts,
            )
        return BridgeHealthSnapshot.healthy(
            message="work is running",
            last_success_at=self._last_success_at,
        )

    async def start(self) -> None:
        """Start the supervised work for the first time.

        Safe to call multiple times: if work is already running, or a restart
        is already scheduled during backoff, this is a no-op.
        """
        if self._task is not None and not self._task.done():
            return
        if self._restart_task is not None and not self._restart_task.done():
            return
        if self._state == SupervisorState.STOPPING:
            return
        self._stop_requested = False
        self._state = SupervisorState.RUNNING
        self._consecutive_failures = 0
        self._restarts.clear()
        self._last_error = None
        self._unhealthy_due_to_failures = False
        self._start_work(is_restart=False)

    def record_success(self) -> None:
        """Signal that the work just completed a successful iteration.

        Long-running loops can call this after each successful pass so that
        health can recover to ``HEALTHY`` once failures stop, even though the
        task itself does not exit.
        """
        self._consecutive_failures = 0
        self._last_error = None
        self._last_success_at = self._clock()
        self._unhealthy_due_to_failures = False

    async def stop(self, timeout: float | None = 10.0) -> None:
        """Stop the supervised work. Idempotent and safe to call multiple times.

        If the coroutine awaiting ``stop()`` is itself cancelled, the cancellation
        is propagated after best-effort cleanup. Cancellation that originates from
        our own cancellation of the work/restart tasks is swallowed.
        """
        self._stop_requested = True
        if self._state in (SupervisorState.STOPPED, SupervisorState.IDLE):
            return
        self._state = SupervisorState.STOPPING

        current_task = asyncio.current_task()

        def _cancelled_externally() -> bool:
            # Python 3.11+: cancelling() reports pending cancellation requests for
            # the current task. If >0, the cancellation came from outside stop().
            return current_task is not None and current_task.cancelling() > 0

        restart_task = self._restart_task
        self._restart_task = None
        if restart_task is not None and not restart_task.done():
            restart_task.cancel()
            try:
                await restart_task
            except asyncio.CancelledError:
                if _cancelled_externally():
                    self._state = SupervisorState.STOPPED
                    raise
            except Exception:  # noqa: BLE001 - best-effort cleanup
                self._logger.exception("supervisor restart task cleanup failed name=%s", self._name)

        task = self._task
        self._task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                if timeout is not None and timeout > 0:
                    await asyncio.wait_for(task, timeout=timeout)
                else:
                    await task
            except asyncio.TimeoutError:
                self._logger.warning(
                    "supervisor work task did not finish within timeout name=%s timeout=%.1f",
                    self._name,
                    timeout,
                )
            except asyncio.CancelledError:
                if _cancelled_externally():
                    self._state = SupervisorState.STOPPED
                    raise
            except Exception:  # noqa: BLE001 - best-effort cleanup
                self._logger.exception("supervisor work task cleanup failed name=%s", self._name)

        self._state = SupervisorState.STOPPED

    def _start_work(self, *, is_restart: bool) -> None:
        """Create and record a new work task."""
        if self._stop_requested:
            return

        if is_restart:
            restarts = self._restarts_in_window()
            if restarts >= self._policy.max_restarts:
                self._state = SupervisorState.FAILED
                self._last_error = self._last_error or PermanentBridgeError(
                    f"max restarts ({self._policy.max_restarts}) exceeded"
                )
                self._logger.error(
                    "supervisor max restarts exceeded name=%s restarts=%d window=%.1f",
                    self._name,
                    restarts,
                    self._policy.restart_window_seconds,
                )
                return
            self._restarts.append(self._clock())

        self._state = SupervisorState.RUNNING
        task = asyncio.create_task(self._work(), name=f"{self._name}-work")
        task.add_done_callback(self._on_task_done)
        self._task = task

    def _on_task_done(self, task: asyncio.Task[None]) -> None:
        """Handle work task completion."""
        if task is not self._task:
            return
        self._task = None

        if self._stop_requested:
            self._state = SupervisorState.STOPPED
            return

        if task.cancelled():
            exc: BaseException | None = asyncio.CancelledError()
        else:
            exc = task.exception()

        if exc is None:
            self._last_success_at = self._clock()
            self._consecutive_failures = 0
            self._last_error = None
            self._logger.warning(
                "supervisor work exited cleanly; treating as silent stop and restarting "
                "name=%s restarts=%d",
                self._name,
                self._restarts_in_window(),
            )
            self._schedule_restart(self._compute_backoff(None))
            return

        if isinstance(exc, asyncio.CancelledError):
            if self._stop_requested:
                self._state = SupervisorState.STOPPED
                return
            self._logger.warning(
                "supervisor work cancelled unexpectedly name=%s", self._name
            )
            self._schedule_restart(self._compute_backoff(exc))
            return

        retryable = self._is_retryable(exc)
        self._consecutive_failures += 1
        self._last_error = exc

        if self._consecutive_failures >= self._policy.health_failure_threshold:
            self._unhealthy_due_to_failures = True

        error_type = type(exc).__name__
        if retryable:
            self._logger.warning(
                "supervisor work failed name=%s error_type=%s error_message=%r "
                "retryable=%s consecutive_failures=%d restarts=%d",
                self._name,
                error_type,
                str(exc),
                True,
                self._consecutive_failures,
                self._restarts_in_window(),
            )
            self._schedule_restart(self._compute_backoff(exc))
        else:
            self._state = SupervisorState.FAILED
            self._logger.error(
                "supervisor work failed permanently name=%s error_type=%s "
                "error_message=%r retryable=%s consecutive_failures=%d",
                self._name,
                error_type,
                str(exc),
                False,
                self._consecutive_failures,
            )

    def _schedule_restart(self, delay: float) -> None:
        """Schedule a restart after ``delay`` seconds."""
        if self._stop_requested or self._state == SupervisorState.FAILED:
            return
        self._state = SupervisorState.RUNNING if self._state != SupervisorState.STOPPING else self._state
        task = asyncio.create_task(
            self._restart_after(delay), name=f"{self._name}-restart"
        )
        task.add_done_callback(self._on_restart_task_done)
        self._restart_task = task

    def _on_restart_task_done(self, task: asyncio.Task[None]) -> None:
        """Clear the restart task reference and log any unexpected exception."""
        if task is self._restart_task:
            self._restart_task = None
        if not task.cancelled():
            exc = task.exception()
            if exc is not None:
                self._logger.exception(
                    "supervisor restart task failed name=%s", self._name
                )

    async def _restart_after(self, delay: float) -> None:
        """Sleep then start a new work task."""
        try:
            await self._sleeper(delay)
        except asyncio.CancelledError:
            return
        if self._stop_requested:
            return
        self._start_work(is_restart=True)

    def _compute_backoff(self, exc: BaseException | None) -> float:
        """Compute the delay before the next restart attempt."""
        retry_after_ms = getattr(exc, "retry_after_ms", None)
        if isinstance(retry_after_ms, int) and retry_after_ms > 0:
            raw = min(retry_after_ms / 1000.0, self._policy.max_delay_seconds)
            return raw

        raw = min(
            self._policy.max_delay_seconds,
            self._policy.base_delay_seconds * (2 ** max(0, self._consecutive_failures - 1)),
        )
        if self._policy.jitter_enabled:
            jitter = self._random.random() * raw * self._policy.max_jitter_ratio
            return jitter
        return raw

    def _restarts_in_window(self) -> int:
        """Count restart attempts within the sliding window."""
        now = self._clock()
        window = self._policy.restart_window_seconds
        while self._restarts and self._restarts[0] < now - window:
            self._restarts.popleft()
        return len(self._restarts)


__all__ = [
    "EventSourceSupervisor",
    "PermanentBridgeError",
    "RestartPolicy",
    "SupervisorState",
    "default_is_retryable",
]
