"""OpenClaw bridge demo.

Demonstrates how to configure and use the OpenClaw bridge with an
in-memory Bus and OpenClaw Gateway's OpenAI-compatible
``POST /v1/chat/completions`` SSE endpoint.

Run against an OpenClaw Gateway endpoint::

    OPENCLAW_GATEWAY_TOKEN=your-token \
    python examples/openclaw_chat_demo.py \
        --base-url http://localhost:18789/v1 \
        --message "你好"

Phases 1–4 only support ``bus.stream_invoke()``; ``bus.invoke()``
aggregation is deferred to a later phase.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os

from openagentio import Bus, Envelope, InMemoryDriver, MessageReceived
from openagentio.bridge import BridgeRunner, BUILTIN_FACTORIES
from openagentio.bridge.config import BridgeConfig, BridgeDefinition, BridgeMappings


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send one message through the OpenAgentIO OpenClaw bridge."
    )
    parser.add_argument(
        "--target",
        default=os.environ.get("OPENCLAW_TARGET", "openclaw.wechat"),
        help="OpenAgentIO Bus target name. Env: OPENCLAW_TARGET",
    )
    parser.add_argument(
        "--message",
        default=os.environ.get("OPENCLAW_MESSAGE", "Hello from OpenAgentIO!"),
        help="Message text to send. Env: OPENCLAW_MESSAGE",
    )
    parser.add_argument(
        "--session-id",
        default=os.environ.get("OPENCLAW_SESSION_ID"),
        help="Optional session id passed through the Bus envelope.",
    )
    parser.add_argument(
        "--base-url",
        default=os.environ.get("OPENCLAW_GATEWAY_BASE_URL", "http://localhost:18789/v1"),
        help=(
            "OpenClaw Gateway base URL (e.g. http://host:port/v1). "
            "Env: OPENCLAW_GATEWAY_BASE_URL"
        ),
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("OPENCLAW_GATEWAY_TOKEN", ""),
        help="OpenClaw Gateway bearer token. Env: OPENCLAW_GATEWAY_TOKEN",
    )
    parser.add_argument(
        "--model",
        default=os.environ.get("OPENCLAW_GATEWAY_MODEL", "openclaw/default"),
        help="Model name passed to /v1/chat/completions. Env: OPENCLAW_GATEWAY_MODEL",
    )
    parser.add_argument(
        "--request-timeout",
        type=float,
        default=float(os.environ.get("OPENCLAW_REQUEST_TIMEOUT", "60")),
        help="Request timeout in seconds. Env: OPENCLAW_REQUEST_TIMEOUT",
    )
    return parser.parse_args()


def _build_definition(args: argparse.Namespace) -> BridgeDefinition:
    if not args.token:
        raise ValueError(
            "--token or OPENCLAW_GATEWAY_TOKEN is required"
        )
    return BridgeDefinition(
        name=args.target,
        type="openclaw_chat_sse",
        config={
            "base_url": args.base_url,
            "token": args.token,
            "model": args.model,
            "request_timeout": args.request_timeout,
        },
        mappings=BridgeMappings(
            text_field="text",
            session_field="x-openclaw-session-key",
            metadata_prefix="openclaw.",
        ),
    )


async def main() -> None:
    args = _parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    logger = logging.getLogger("openclaw-chat-demo")

    # 1. Create and connect an in-memory Bus.
    bus = Bus(
        agent_id="demo-agent",
        transport=InMemoryDriver(),
        logger=logger,
    )
    await bus.connect()

    # 2. Define the bridge configuration.
    definition = _build_definition(args)
    cfg = BridgeConfig(version="openagentio.bridge/v1", bridges=(definition,))

    # 3. Start the bridge via BridgeRunner.
    runner = BridgeRunner(bus, cfg, BUILTIN_FACTORIES)
    await runner.start()
    try:
        # 4. Send a message via stream_invoke.
        logger.info(
            "Sending message target=%s bridge=openclaw_chat_sse ...",
            args.target,
        )
        payload: dict[str, str] | Envelope = {"text": args.message}
        if args.session_id:
            req = Envelope.new(MessageReceived)
            req.from_ = bus.agent_id
            req.to = args.target
            req.session_id = args.session_id
            req.payload = bus._codec.encode_payload(payload)
            payload = req

        stream = await bus.stream_invoke(args.target, payload)

        async for env in stream:
            if env.event_type == "agent.response.delta":
                delta = env.payload_json().get("delta", "")
                if delta:
                    logger.info("  delta: %s", delta)
            elif env.event_type == "agent.response.final":
                payload = env.payload_json()
                logger.info("  final: %s", payload.get("text", ""))
            elif env.event_type == "agent.response.error":
                payload = env.payload_json()
                logger.error(
                    "  error: code=%s message=%s",
                    payload.get("code"),
                    payload.get("message"),
                )

        logger.info("Done.")
    finally:
        await runner.stop()
        await bus.close()


if __name__ == "__main__":
    asyncio.run(main())
