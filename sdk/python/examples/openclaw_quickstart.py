"""Minimal OpenClaw Gateway quickstart.

Prerequisites:

    export OPENCLAW_GATEWAY_BASE_URL=http://127.0.0.1:18789/v1
    export OPENCLAW_GATEWAY_TOKEN=your-token

Run:

    python examples/openclaw_quickstart.py "你好"
"""

from __future__ import annotations

import asyncio
import os
import sys

from openagentio import Bus, InMemoryDriver
from openagentio.bridge import OpenClawChatBridge


TARGET = "openclaw.chat"


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


async def main() -> None:
    message = sys.argv[1] if len(sys.argv) > 1 else "你好"

    bus = Bus(agent_id="quickstart-user", transport=InMemoryDriver())
    await bus.connect()

    _required_env("OPENCLAW_GATEWAY_TOKEN")
    bridge = OpenClawChatBridge.from_env(bus, target=TARGET)

    await bridge.start()
    try:
        stream = await bus.stream_invoke(TARGET, {"text": message})

        saw_delta = False
        async for env in stream:
            payload = env.payload_json()
            if env.event_type == "agent.response.delta":
                print(payload.get("delta", ""), end="", flush=True)
                saw_delta = True
            elif env.event_type == "agent.response.final":
                text = payload.get("text", "") if isinstance(payload, dict) else ""
                if not saw_delta and text:
                    print(text, end="")
                print()
            elif env.event_type == "agent.response.error":
                print(f"\nERROR: {payload.get('code')}: {payload.get('message')}")
    finally:
        await bridge.stop()
        await bus.close()


if __name__ == "__main__":
    asyncio.run(main())
