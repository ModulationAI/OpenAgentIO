"""Bridge lifecycle interface.

A Bridge is a regular OpenAgentIO Bus client that translates between the
ACP/Envelope world and an external agent framework or protocol (e.g.
HTTP/SSE gateways, MCP servers, Matrix homeservers, custom WebSocket bots).

Concrete bridges are constructed by :class:`BridgeRunner` from configuration
and registered on the bus via the existing ``handle_stream`` /
``handle_invoke`` / ``subscribe`` APIs. This module only defines the
abstract surface — no Bus, Envelope or transport internals are touched.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:  # pragma: no cover - import only for typing
    from openagentio.bus import Bus
    from openagentio.bridge.config import BridgeDefinition


class Bridge(ABC):
    """Abstract bridge lifecycle.

    A bridge is a Bus client that owns exactly one slice of the runtime:
    translating between the ACP/Envelope bus and one external system.

    Contract:

    * ``start()`` registers one or more handlers on the supplied Bus (typically
      via ``bus.handle_stream(...)``, ``bus.handle_invoke(...)``, or
      ``bus.subscribe(...)``). It may raise if the external system cannot be
      reached or configured; the caller (usually :class:`BridgeRunner`) will
      then invoke ``stop()`` so partial side effects can be rolled back.

    * ``stop()`` releases any external resources owned by the bridge
      (subprocesses, sockets, background tasks, HTTP clients). It must be safe
      to call even when ``start()`` failed partway through, and it must be
      idempotent — calling it more than once must not raise. It must also
      revoke every Bus handler/subscription that the bridge registered;
      relying on ``Bus.close()`` is only a safety net.

    * A bridge does **not** own the :class:`Bus` instance it operates on.
      It must not call ``bus.close()`` inside ``stop()``.

    * A bridge should map external errors to :class:`openagentio.bus.errors.BusError`
      subclasses where practical, but it may propagate unexpected exceptions
      so that the runner or caller can log them.
    """

    @abstractmethod
    async def start(self) -> None:
        """Start the bridge: connect to the external system and register
        the bus handler(s). May raise; the caller will run :meth:`stop`
        for best-effort cleanup."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop the bridge and release any external resources.

        Must be safe after a failed or partial :meth:`start`, safe to call
        multiple times, and must unsubscribe every handler the bridge
        registered on the Bus.
        """


# A factory takes a connected Bus and a parsed BridgeDefinition, and returns
# a Bridge instance ready to be started. Factories are injected into the
# BridgeRunner so that the runner stays decoupled from concrete bridge
# implementations (HTTP/SSE, MCP, Matrix, ...). A formal global
# ``register_bridge`` registry is intentionally deferred to a later phase per
# the dev roadmap.
BridgeFactory = Callable[["Bus", "BridgeDefinition"], Bridge]


__all__ = ["Bridge", "BridgeFactory"]
