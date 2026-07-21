"""Bridge runner: instantiates and orchestrates bridges declared in config.

The runner takes a *connected* :class:`openagentio.Bus` plus a parsed
:class:`BridgeConfig` and a mapping of ``type -> BridgeFactory``. It builds
each bridge, calls ``start()`` on it, and exposes a ``stop()`` that tears
them down in reverse order with best-effort cleanup.

Concrete bridge implementations (HTTP/SSE, OpenAPI, ...) live in their own
submodules and are wired in by the caller — the runner has no built-in
registry in this phase per the dev roadmap (deferred to a later phase).
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Mapping

from openagentio.bridge.base import Bridge, BridgeFactory
from openagentio.bridge.config import BridgeConfig, BridgeConfigError

if TYPE_CHECKING:  # pragma: no cover
    from openagentio.bus import Bus


# Hard cap on how long a single bridge's stop() is allowed to run before the
# runner moves on. Prevents one misbehaving bridge from pinning the whole
# process during shutdown (see remediation checklist §1: "test process does
# not exit"). Not exposed as an option yet — revisit if a real deployment
# needs a longer teardown budget.
_BRIDGE_STOP_TIMEOUT = 10.0


class BridgeRunner:
    """Owns the lifecycle of a set of bridges attached to a single Bus.

    The supplied ``bus`` must already be connected; the runner does not
    create, connect, or close it. Callers continue to manage Bus lifecycle
    through the existing public API.
    """

    def __init__(
        self,
        bus: "Bus",
        config: BridgeConfig,
        factories: Mapping[str, BridgeFactory],
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._bus = bus
        self._config = config
        self._factories = dict(factories)
        self._logger = logger or logging.getLogger("openagentio.bridge")
        self._bridges: list[tuple[str, Bridge]] = []
        self._started = False

    @property
    def bridges(self) -> tuple[tuple[str, Bridge], ...]:
        """Started bridges in start order, as ``(name, bridge)`` tuples."""
        return tuple(self._bridges)

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
        except BaseException:
            await self._shutdown()
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
                await asyncio.wait_for(bridge.stop(), timeout=_BRIDGE_STOP_TIMEOUT)
            except asyncio.TimeoutError:
                self._logger.error(
                    "bridge stop timed out after %.1fs name=%s",
                    _BRIDGE_STOP_TIMEOUT,
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


__all__ = ["BridgeRunner"]
