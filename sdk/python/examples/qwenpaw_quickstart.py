"""Minimal QwenPaw Chat quickstart.

QwenPaw's local 127.0.0.1 path skips Web login auth, so no token is
required for a local deployment. The base URL defaults to
``http://127.0.0.1:8088``; override the env vars below for a remote or
authenticated setup:

    export QWENPAW_BASE_URL=http://127.0.0.1:8088      # default shown
    export QWENPAW_AUTH_TOKEN=your-token                # optional; set for remote / login auth
    export QWENPAW_AGENT_ID=default                     # optional

Run:

    python examples/qwenpaw_quickstart.py "你好"
"""

from __future__ import annotations

import asyncio
import sys

from openagentio import Bus, InMemoryDriver
from openagentio.bridge import QwenPawChatBridge


TARGET = "qwenpaw.chat"


async def main() -> None:
    message = sys.argv[1] if len(sys.argv) > 1 else "你好"

    bus = Bus(agent_id="quickstart-user", transport=InMemoryDriver())
    await bus.connect()

    bridge = QwenPawChatBridge.from_env(bus, target=TARGET)

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
