"""Minimal Matrix Event bridge quickstart.

Prerequisites:

    # Edit examples/matrix_bridge.yaml with real homeserver_url, access_token,
    # user_id, and room_ids. The bot account must already be in the room.

Run in listen mode:

    env PYTHONPATH=src .venv/bin/python examples/matrix_quickstart.py \
        --config examples/matrix_bridge.yaml

Run and send one message:

    env PYTHONPATH=src .venv/bin/python examples/matrix_quickstart.py \
        --config examples/matrix_bridge.yaml \
        --send "hello from OpenAgentIO" \
        --room "!roomid:example.com"

The quickstart loads the bridge config from YAML, starts the Matrix event
bridge, and prints inbound room messages. When ``--send`` is provided it also
publishes a ``matrix.message.send`` event back to the specified room.

Session propagation:

    If you want a Matrix inbound handler to call ``bus.invoke()`` or
    ``bus.stream_invoke()`` and automatically share ``session_id``,
    ``conversation_id`` and ``traceparent``, create the Bus with
    ``WithSessionPropagation(True)``:

        bus = Bus.new(
            WithAgentID("matrix-quickstart"),
            WithTransport(InMemoryDriver()),
            WithSessionPropagation(True),
        )
"""

from __future__ import annotations

import argparse
import asyncio
import json

from openagentio import (
    Bus,
    Envelope,
    InMemoryDriver,
    WithAgentID,
    WithSessionPropagation,
    WithTransport,
)
from openagentio.bridge import BUILTIN_FACTORIES, BridgeConfig
from openagentio.bridge.runner import BridgeRunner


async def main() -> None:
    parser = argparse.ArgumentParser(description="Matrix event bridge quickstart")
    parser.add_argument(
        "--config",
        default="examples/matrix_bridge.yaml",
        help="path to bridge config (YAML or JSON)",
    )
    parser.add_argument(
        "--send",
        default="",
        help="if set, publish this text to --room and exit after sending",
    )
    parser.add_argument(
        "--room",
        default="",
        help="target Matrix room_id when using --send",
    )
    args = parser.parse_args()

    config = BridgeConfig.from_file(args.config)

    bus = Bus.new(
        WithAgentID("matrix-quickstart"),
        WithTransport(InMemoryDriver()),
        WithSessionPropagation(True),
    )
    await bus.connect()

    runner = BridgeRunner(bus, config, BUILTIN_FACTORIES)
    await runner.start()

    async def on_message(env: Envelope) -> None:
        payload = env.payload_json() or {}
        print(
            f"[{payload.get('room_id')}] {payload.get('sender')}: "
            f"{payload.get('text')}"
        )

    sub = await bus.subscribe("matrix.message.received", on_message)
    try:
        if args.send:
            if not args.room:
                raise SystemExit("--send requires --room")
            send_env = Envelope.new("matrix.message.send")
            send_env.payload = json.dumps(
                {"room_id": args.room, "text": args.send},
                separators=(",", ":"),
            ).encode("utf-8")
            await bus.publish(send_env)
            # Give the bridge a moment to issue the HTTP request.
            await asyncio.sleep(0.5)
        else:
            print("Listening for Matrix messages. Press Ctrl-C to stop.")
            while True:
                await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass
    finally:
        await sub.unsubscribe()
        await runner.stop()
        await bus.close()


if __name__ == "__main__":
    asyncio.run(main())
