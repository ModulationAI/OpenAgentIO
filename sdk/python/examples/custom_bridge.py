"""Developer quickstart: implement a custom OpenAgentIO bridge.

Run:

    env PYTHONPATH=src .venv/bin/python examples/custom_bridge.py "hello bridge"

This example does not call a real external service. Replace
``_call_external_agent`` with your HTTP, SSE, WebSocket, or SDK client.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
import sys
from typing import TYPE_CHECKING, Any

from openagentio import Bus, InMemoryDriver, InvalidRequestError
from openagentio.bridge import (
    Bridge,
    BridgeDefinition,
    BridgeMappings,
)

if TYPE_CHECKING:  # pragma: no cover
    from openagentio import Envelope, StreamWriter, Subscription


class ExampleEchoBridge(Bridge):
    """Small bridge template for developers.

    A bridge owns one Bus target. It receives OpenAgentIO stream requests,
    calls an external agent, and writes streamed frames back to the Bus.
    """

    def __init__(self, bus: Bus, definition: BridgeDefinition) -> None:
        self._bus = bus
        self._definition = definition
        self.target = definition.name
        self._sub: Subscription | None = None

        cfg = dict(definition.config)
        self._prefix = str(cfg.get("prefix", "echo"))
        self._delay = float(cfg.get("delay", 0.02))
        self._text_field = definition.mappings.text_field or "text"

    @classmethod
    def from_options(
        cls,
        bus: Bus,
        *,
        target: str = "example.echo",
        prefix: str = "custom-agent",
        delay: float = 0.02,
        text_field: str = "text",
    ) -> "ExampleEchoBridge":
        """Create the bridge without exposing low-level config objects."""
        return cls(
            bus,
            BridgeDefinition(
                name=target,
                type="example_echo",
                config={"prefix": prefix, "delay": delay},
                mappings=BridgeMappings(text_field=text_field),
            ),
        )

    async def start(self) -> None:
        self._sub = await self._bus.handle_stream(
            self._definition.name,
            self._on_stream,
        )

    async def stop(self) -> None:
        sub = self._sub
        self._sub = None
        if sub is not None:
            await sub.unsubscribe()

    async def _on_stream(self, env: "Envelope", writer: "StreamWriter") -> None:
        payload = env.payload_json() or {}
        if not isinstance(payload, dict):
            raise InvalidRequestError("custom bridge payload must be a JSON object")

        text = payload.get(self._text_field)
        if text is None:
            raise InvalidRequestError(
                f"custom bridge payload missing {self._text_field!r}"
            )

        full = ""
        async for delta in self._call_external_agent(str(text), env):
            full += delta
            await writer.delta({"delta": delta})

        await writer.final({"text": full})

    async def _call_external_agent(
        self, text: str, env: "Envelope"
    ) -> AsyncIterator[str]:
        """Replace this method with the real external agent call."""
        session = f" session={env.session_id}" if env.session_id else ""
        response = f"{self._prefix}{session}: {text}"
        for token in response.split(" "):
            await asyncio.sleep(self._delay)
            yield token + " "


def example_echo_bridge_factory(
    bus: Bus, definition: BridgeDefinition
) -> ExampleEchoBridge:
    """Factory for config-driven BridgeRunner users."""
    return ExampleEchoBridge(bus, definition)


async def main() -> None:
    message = sys.argv[1] if len(sys.argv) > 1 else "hello bridge"

    bus = Bus(agent_id="custom-bridge-demo", transport=InMemoryDriver())
    await bus.connect()

    bridge = ExampleEchoBridge.from_options(bus, target="example.echo")
    await bridge.start()
    try:
        stream = await bus.stream_invoke(bridge.target, {"text": message})
        async for env in stream:
            payload: dict[str, Any] = env.payload_json() or {}
            if env.event_type == "agent.response.delta":
                print(payload.get("delta", ""), end="", flush=True)
            elif env.event_type == "agent.response.final":
                print()
            elif env.event_type == "agent.response.error":
                print(f"\nERROR: {payload.get('code')}: {payload.get('message')}")
    finally:
        await bridge.stop()
        await bus.close()


if __name__ == "__main__":
    asyncio.run(main())
