"""Bridge runner: instantiates and orchestrates bridges declared in config.

The runner takes a *connected* :class:`openagentio.Bus` plus a parsed
:class:`BridgeConfig` and a mapping of ``type -> BridgeFactory``. It builds
each bridge, calls ``start()`` on it, and exposes a ``stop()`` that tears
them down in reverse order with best-effort cleanup.

The runner **does not own the Bus lifecycle**. Callers must connect the Bus
before ``start()`` and close it after ``stop()``. The runner never calls
``bus.close()``.

Concrete bridge implementations (HTTP/SSE, MCP, Matrix, ...) live in their
own submodules and are wired in by the caller — the runner has no built-in
registry in this phase per the dev roadmap (deferred to a later phase).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Mapping

from openagentio.bridge.base import Bridge, BridgeFactory
from openagentio.bridge.config import BridgeConfig, BridgeConfigError
from openagentio.bridge.health import BridgeHealth, BridgeHealthSnapshot

if TYPE_CHECKING:  # pragma: no cover
    from openagentio.bus import Bus


# Default cap on how long a single bridge's stop() is allowed to run before
# the runner moves on. Prevents one misbehaving bridge from pinning the whole
# process during shutdown (see remediation checklist §1: "test process does
# not exit"). This default is preserved as a module constant for backward
# compatibility; new code should prefer passing ``stop_timeout`` to
# BridgeRunner.
_DEFAULT_STOP_TIMEOUT = 10.0


@dataclass(frozen=True)
class RunnerHealthSnapshot:
    """Aggregate health view across all bridges managed by a runner."""

    overall: BridgeHealth
    bridges: dict[str, BridgeHealthSnapshot]

    def by_name(self, name: str) -> BridgeHealthSnapshot | None:
        """Look up the health snapshot for a single bridge by name."""
        return self.bridges.get(name)


class BridgeRunner:
    """Owns the lifecycle of a set of bridges attached to a single Bus.

    The supplied ``bus`` must already be connected; the runner does not
create, connect, or close it. Callers continue to manage Bus lifecycle
through the existing public API.

    Lifecycle ordering:

    1. Construct ``BridgeRunner(bus, config, factories)``.
    2. ``await runner.start()`` — bridges start in config order.
    3. If any bridge fails during ``start()``, already-started bridges are
       stopped in reverse order and the original exception is re-raised.
    4. ``await runner.stop()`` — stops all bridges in reverse order.
    5. ``await bus.close()`` — caller closes the Bus after the runner.

    Stop semantics:

    * Each bridge's ``stop()`` is bounded by ``stop_timeout`` seconds.
    * A timeout is logged and the runner continues with the next bridge.
    * Exceptions during ``stop()`` are logged and swallowed.
    * If shutdown is cancelled, the ``CancelledError`` is captured, the
      remaining bridges are still stopped, and the error is re-raised at the
      end so the caller's cancellation semantics are preserved.

    Health:

    * ``runner.health`` returns an aggregate snapshot with the worst per-bridge
      state. A single unhealthy bridge does not stop the runner or its siblings.
    """

    def __init__(
        self,
        bus: "Bus",
        config: BridgeConfig,
        factories: Mapping[str, BridgeFactory],
        *,
        logger: logging.Logger | None = None,
        stop_timeout: float = _DEFAULT_STOP_TIMEOUT,
    ) -> None:
        self._bus = bus
        self._config = config
        self._factories = dict(factories)
        self._stop_timeout = stop_timeout
        self._logger = logger or logging.getLogger("openagentio.bridge")
        self._bridges: list[tuple[str, Bridge]] = []
        self._started = False
        self._last_logged_health: dict[str, BridgeHealth] = {}

    @property
    def bridges(self) -> tuple[tuple[str, Bridge], ...]:
        """Started bridges in start order, as ``(name, bridge)`` tuples."""
        return tuple(self._bridges)

    @property
    def stop_timeout(self) -> float:
        """Per-bridge stop timeout in seconds."""
        return self._stop_timeout

    @property
    def health(self) -> RunnerHealthSnapshot:
        """Aggregate health snapshot across all bridges.

        The ``overall`` field is the worst state among tracked bridges:
        ``UNHEALTHY`` > ``DEGRADED`` > ``HEALTHY`` > ``UNKNOWN``. Transitions
        to ``DEGRADED`` or ``UNHEALTHY`` are logged once per bridge to make
        silent stops observable without affecting sibling bridges.
        """
        per_bridge: dict[str, BridgeHealthSnapshot] = {}
        worst = BridgeHealth.UNKNOWN
        severity = {
            BridgeHealth.UNKNOWN: 0,
            BridgeHealth.HEALTHY: 1,
            BridgeHealth.DEGRADED: 2,
            BridgeHealth.UNHEALTHY: 3,
        }
        for name, bridge in self._bridges:
            snapshot = bridge.health
            per_bridge[name] = snapshot
            if severity[snapshot.health] > severity[worst]:
                worst = snapshot.health

            last = self._last_logged_health.get(name)
            if snapshot.health != last:
                self._last_logged_health[name] = snapshot.health
                if snapshot.health in (BridgeHealth.DEGRADED, BridgeHealth.UNHEALTHY):
                    self._logger.warning(
                        "bridge health degraded name=%s health=%s message=%s "
                        "consecutive_failures=%d restarts=%d",
                        name,
                        snapshot.health.value,
                        snapshot.message,
                        snapshot.consecutive_failures,
                        snapshot.restarts_in_window,
                    )
                elif last in (BridgeHealth.DEGRADED, BridgeHealth.UNHEALTHY):
                    self._logger.info(
                        "bridge health recovered name=%s health=%s",
                        name,
                        snapshot.health.value,
                    )

        return RunnerHealthSnapshot(overall=worst, bridges=per_bridge)

    async def start(self) -> None:
        """Instantiate and start every bridge in the config.

        On failure of any bridge, already-started bridges are stopped in
        reverse order before re-raising the original exception. The
        currently-failing bridge is also included in the cleanup sweep:
        it is registered *before* ``start()`` is awaited so that any
        partial side effects (subprocess spawned, handler registered,
        socket opened) get a chance to be torn down via its ``stop()``
        — which the :class:`Bridge` contract requires to be safe even
        after a failed start.
        """
        if self._started:
            raise RuntimeError("BridgeRunner already started")
        try:
            for definition in self._config.bridges:
                factory = self._factories.get(definition.type)
                if factory is None:
                    raise BridgeConfigError(
                        f"bridge '{definition.name}': no factory registered "
                        f"for type {definition.type!r}"
                    )
                bridge = factory(self._bus, definition)
                self._logger.info(
                    "starting bridge name=%s type=%s",
                    definition.name,
                    definition.type,
                )
                # Track before start() so a partial start still gets stop()'d.
                self._bridges.append((definition.name, bridge))
                await bridge.start()
            self._started = True
        except BaseException as start_exc:
            # Best-effort rollback. A CancelledError raised by a bridge's
            # stop() during rollback must not mask the original start failure;
            # it is logged and suppressed so the caller receives the exception
            # that actually caused start() to fail.
            try:
                await self._shutdown()
            except asyncio.CancelledError:
                self._logger.warning(
                    "bridge stop raised CancelledError during start rollback; "
                    "suppressing it so the original %s is preserved",
                    type(start_exc).__name__,
                )
            raise

    async def stop(self) -> None:
        """Stop all started bridges in reverse order. Safe to call multiple
        times and safe to call even if :meth:`start` partially failed."""
        await self._shutdown()
        self._started = False

    async def _shutdown(self) -> None:
        # Best-effort teardown: every bridge gets a chance to stop, even if a
        # sibling raises. We catch broad Exception because a bridge's stop()
        # may bubble transport-specific errors we don't want to hard-fail on.
        # CancelledError from a bridge is treated as "that bridge is being
        # cancelled" — we still keep going through the rest, then re-raise at
        # the end so the caller's cancellation semantics are preserved.
        cancelled: BaseException | None = None
        while self._bridges:
            name, bridge = self._bridges.pop()
            try:
                await asyncio.wait_for(bridge.stop(), timeout=self._stop_timeout)
            except asyncio.TimeoutError:
                self._logger.error(
                    "bridge stop timed out after %.1fs name=%s",
                    self._stop_timeout,
                    name,
                )
            except asyncio.CancelledError as exc:
                cancelled = exc
                self._logger.warning(
                    "bridge stop cancelled name=%s (continuing shutdown)", name
                )
            except Exception:  # noqa: BLE001 - best-effort cleanup
                self._logger.exception("error stopping bridge name=%s", name)
        if cancelled is not None:
            raise cancelled


__all__ = ["BridgeRunner", "RunnerHealthSnapshot"]
