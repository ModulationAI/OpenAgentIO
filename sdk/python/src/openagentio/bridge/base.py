"""Bridge lifecycle interface.

A Bridge is a regular OpenAgentIO Bus client that translates between the
ACP/Envelope world and an external agent framework / protocol (e.g.
HTTP/SSE gateways, OpenAPI services, custom WebSocket bots).

Concrete bridges are constructed by :class:`BridgeRunner` from configuration
and registered on the bus via the existing ``handle_stream`` /
``handle_invoke`` APIs. This module only defines the abstract surface — no
Bus, Envelope or transport internals are touched.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:  # pragma: no cover - import only for typing
    from openagentio.bus import Bus
    from openagentio.bridge.config import BridgeDefinition


class Bridge(ABC):
    """Abstract bridge lifecycle.

    A bridge is expected to register one or more targets on the supplied Bus
    inside :meth:`start` (typically via ``bus.handle_stream(...)``) and to
    release any external resources (subprocesses, sockets, …) inside
    :meth:`stop`.

    Implementations should be safe to ``stop()`` even when ``start()`` failed
    partway through, so the runner can perform best-effort cleanup.
    """

    @abstractmethod
    async def start(self) -> None:
        """Start the bridge: connect to the external system and register
        the bus handler(s)."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop the bridge and release any external resources."""


# A factory takes a connected Bus and a parsed BridgeDefinition, and returns
# a Bridge instance ready to be started. Factories are injected into the
# BridgeRunner so that the runner stays decoupled from concrete bridge
# implementations (HTTP/SSE, OpenAPI, ...). A formal `register_bridge`
# registry is intentionally deferred to a later phase per the dev roadmap.
BridgeFactory = Callable[["Bus", "BridgeDefinition"], Bridge]


__all__ = ["Bridge", "BridgeFactory"]
