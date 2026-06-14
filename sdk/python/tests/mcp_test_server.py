"""Lightweight local MCP server for integration tests.

Run directly for manual smoke testing:

    .venv/bin/python tests/mcp_test_server.py

Or used by ``tests/test_bridge_mcp_tool.py`` via stdio transport.
"""

from __future__ import annotations

import asyncio
import json

import mcp.server.stdio
import mcp.types as types
from mcp.server.lowlevel import NotificationOptions, Server
from mcp.server.models import InitializationOptions

server = Server("openagentio-test-server")


@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="echo",
            description="Echo a message back",
            inputSchema={
                "type": "object",
                "properties": {"message": {"type": "string"}},
                "required": ["message"],
            },
        ),
        types.Tool(
            name="add",
            description="Add two integers",
            inputSchema={
                "type": "object",
                "properties": {
                    "a": {"type": "integer"},
                    "b": {"type": "integer"},
                },
                "required": ["a", "b"],
            },
        ),
        types.Tool(
            name="capture_meta",
            description="Return the JSON-RPC _meta fields received by the server",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
    ]


@server.call_tool()
async def handle_call_tool(
    name: str, arguments: dict[str, object]
) -> list[types.TextContent]:
    if name == "echo":
        message = str(arguments.get("message", ""))
        return [types.TextContent(type="text", text=f"echo: {message}")]
    if name == "add":
        a = int(arguments.get("a", 0))
        b = int(arguments.get("b", 0))
        return [types.TextContent(type="text", text=str(a + b))]
    if name == "capture_meta":
        try:
            meta = server.request_context.meta
            meta_dict = meta.model_dump() if meta is not None else {}
        except LookupError:
            meta_dict = {}
        # progressToken is a standard MCP meta field; remove it so the test can
        # focus on the OpenAgentIO-propagated fields.
        meta_dict.pop("progressToken", None)
        return [types.TextContent(type="text", text=json.dumps(meta_dict))]
    raise ValueError(f"Unknown tool: {name}")


async def main() -> None:
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="openagentio-test-server",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )


if __name__ == "__main__":
    asyncio.run(main())
