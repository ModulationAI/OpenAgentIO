"""Minimal MCP Tool bridge quickstart.

Prerequisites:

    export MCP_FS_ROOT=/tmp
    # The example uses the official filesystem MCP server via npx.
    # Make sure Node.js / npx is available, or swap the command for any
    # MCP server that exposes stdio transport.

Run:

    env PYTHONPATH=src .venv/bin/python examples/mcp_quickstart.py \
        --config examples/mcp_bridge.yaml --path /tmp/hello.txt

This loads the bridge configuration from YAML, starts the MCP filesystem
server over stdio, and invokes ``mcp-fs.read_file`` through the Bus.
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from openagentio import Bus, InMemoryDriver
from openagentio.bridge import BUILTIN_FACTORIES, BridgeConfig
from openagentio.bridge.runner import BridgeRunner


def _ensure_sample_file(path: str) -> None:
    """Create a sample file if it does not exist so the demo read_file call succeeds."""
    p = Path(path)
    if not p.exists():
        p.write_text("hello from openagentio\n", encoding="utf-8")
        print(f"created sample file: {path}")


async def main() -> None:
    parser = argparse.ArgumentParser(description="MCP tool bridge quickstart")
    parser.add_argument(
        "--config",
        default="examples/mcp_bridge.yaml",
        help="path to bridge config (YAML or JSON)",
    )
    parser.add_argument(
        "--path",
        default="/tmp/hello.txt",
        help="file path to read via mcp-fs.read_file",
    )
    args = parser.parse_args()

    _ensure_sample_file(args.path)
    config = BridgeConfig.from_file(args.config)

    bus = Bus(agent_id="mcp-quickstart", transport=InMemoryDriver())
    await bus.connect()

    runner = BridgeRunner(bus, config, BUILTIN_FACTORIES)
    await runner.start()
    try:
        resp = await bus.invoke("mcp-fs.read_file", {"path": args.path})
        payload = resp.payload_json()
        print("event_type:", resp.event_type)
        print("payload:", payload)
    finally:
        await runner.stop()
        await bus.close()


if __name__ == "__main__":
    asyncio.run(main())
