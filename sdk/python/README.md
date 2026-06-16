# openagentio (Python SDK)

ACP-compatible **Envelope** protocol on top of NATS / asyncio. The protocol is
shared with the Go SDK at the repo root; transport layers are implemented
independently per language.

> v0.2 skeleton — feature-equivalent surface to Go v0.1.

## Layout

```
sdk/python/
├── pyproject.toml
├── src/openagentio/
│   ├── event/          # Envelope dataclass, event-type constants, payload shapes
│   ├── codec/          # JSON codec
│   ├── transport/      # base ABC + InMemoryDriver + NATSDriver
│   └── bus/            # Bus, Stream, StreamWriter, subject layout
├── tests/              # pytest, pytest-asyncio
└── examples/
    ├── echo_agent.py
    ├── custom_bridge.py
    ├── mcp_bridge.yaml
    ├── mcp_quickstart.py
    ├── matrix_bridge.yaml
    ├── matrix_quickstart.py
    └── openclaw_quickstart.py
```

## Install (development)

```bash
cd sdk/python
python3.11 -m venv .venv          # already done in this repo
.venv/bin/pip install -e ".[dev]"
```

## Run tests

```bash
.venv/bin/pytest                   # unit tests (in-memory transport)
AFB_NATS_URL=nats://localhost:4222 .venv/bin/pytest tests/test_nats_integration.py
```

The NATS integration tests are gated on the `AFB_NATS_URL` env var. Start a
local broker first, e.g. `nats-server -p 4222`.

The MCP bridge tests in `tests/test_bridge_mcp_tool.py` spawn a real stdio
subprocess. If the full suite hangs, run those tests separately; the most recent
verification focused on the relevant bridge test set.

## Example

```python
import asyncio
from openagentio import Bus, InMemoryDriver

async def main() -> None:
    bus = Bus(agent_id="echo", transport=InMemoryDriver())
    await bus.connect()

    async def echo(env):
        return env.payload_json()

    await bus.handle_invoke("echo", echo)
    resp = await bus.invoke("echo", {"msg": "hello"})
    print(resp.event_type, resp.payload_json())
    await bus.close()

asyncio.run(main())
```

See `examples/echo_agent.py` for a runnable version that mirrors
`examples/echo-agent/main.go` from the Go SDK.

Bridge authors can start from `examples/custom_bridge.py`, which shows how to
implement a custom `Bridge`, register a factory, and run it through
`BridgeRunner`. See `<repo>/docs/custom_bridge.md` for the developer guide.

## OpenClaw Quickstart

For a local OpenClaw Gateway exposing the OpenAI-compatible streaming chat
endpoint:

```bash
export OPENCLAW_GATEWAY_BASE_URL=http://127.0.0.1:18789/v1
export OPENCLAW_GATEWAY_TOKEN=your-token

env PYTHONPATH=src .venv/bin/python examples/openclaw_quickstart.py "你好"
```

The quickstart sends one OpenAgentIO `stream_invoke()` request to the
`openclaw_chat_sse` bridge and prints OpenClaw's streamed response.

Application code can use the same bridge without constructing low-level
bridge config objects:

```python
from openagentio import Bus, InMemoryDriver
from openagentio.bridge import OpenClawChatBridge

bus = Bus(agent_id="my-app", transport=InMemoryDriver())
await bus.connect()

bridge = OpenClawChatBridge.from_env(bus, target="openclaw.chat")
await bridge.start()

async for env in await bus.stream_invoke("openclaw.chat", {"text": "你好"}):
    print(env.payload_json())
```

## MCP Tool Quickstart

Expose any MCP server's tools as OpenAgentIO `invoke` targets. First install
the optional `mcp` and `bridge` extras (`bridge` provides PyYAML, which the
quickstart config file uses):

```bash
.venv/bin/pip install -e ".[mcp,bridge]"
```

Example bridge config (`examples/mcp_bridge.yaml`):

```yaml
version: "openagentio.bridge/v1"
bridges:
  - name: "mcp-fs"
    type: "mcp_tool"
    config:
      transport: "stdio"
      command: "npx"
      args:
        - "-y"
        - "@modelcontextprotocol/server-filesystem"
        - "/tmp"
      env:
        NODE_ENV: "production"
      timeout: 10
    mappings:
      target_prefix: "mcp-fs"
```

Run the quickstart (create a sample file first so `read_file` succeeds):

```bash
printf "hello\\n" > /tmp/hello.txt
env PYTHONPATH=src .venv/bin/python examples/mcp_quickstart.py \
    --config examples/mcp_bridge.yaml --path /tmp/hello.txt
```

If the file does not exist, `mcp_quickstart.py` will create a sample file
automatically and print a notice before invoking the tool.

This starts the MCP filesystem server over stdio, registers each discovered
tool as `mcp-fs.<tool_name>`, and invokes `mcp-fs.read_file` through the Bus.

You can also load the same config programmatically:

```python
from openagentio import Bus, InMemoryDriver
from openagentio.bridge import BUILTIN_FACTORIES, BridgeConfig
from openagentio.bridge.runner import BridgeRunner

bus = Bus(agent_id="my-app", transport=InMemoryDriver())
await bus.connect()

config = BridgeConfig.from_file("examples/mcp_bridge.yaml")
runner = BridgeRunner(bus, config, BUILTIN_FACTORIES)
await runner.start()

resp = await bus.invoke("mcp-fs.read_file", {"path": "/tmp/hello.txt"})
print(resp.payload_json())
```

### Streamable HTTP transport

For remote MCP servers that expose the Streamable HTTP transport, use
`examples/mcp_bridge_http.yaml`:

```yaml
version: "openagentio.bridge/v1"
bridges:
  - name: "mcp-http"
    type: "mcp_tool"
    config:
      transport: "streamable_http"
      http_url: "http://localhost:8000/mcp"
      # token: "your-token"          # optional literal Bearer token
      headers:                      # optional extra headers
        X-Custom-Header: "openagentio"
      timeout: 30
    mappings:
      target_prefix: "mcp-http"
```

`BridgeConfig.from_file()` reads literal values; it does not expand environment
variables. Use a literal `token` here, or construct the `BridgeDefinition`
programmatically if you need dynamic values.

When you construct `McpToolBridge` programmatically and provide your own
`http_client`, that client is used as-is: the bridge will not inject the
`token` or `headers` from the config into it. Configure authentication and any
extra headers directly on the client you pass in.

HTTP errors are mapped to OpenAgentIO error codes:

- `401` / `403` -> `AUTH_FAILURE`
- `5xx` -> `TRANSPORT_FAILURE`
- connection errors -> `AGENT_UNAVAILABLE`
- timeouts -> `AGENT_TIMEOUT`

### Session and trace propagation

For every tool call, the bridge forwards the current OpenAgentIO `session_id`
and `traceparent` to the MCP server via the JSON-RPC `_meta` field:

```json
{
  "jsonrpc": "2.0",
  "method": "tools/call",
  "params": {
    "name": "read_file",
    "arguments": { "path": "/tmp/hello.txt" },
    "_meta": {
      "session_id": "<openagentio-session-id>",
      "traceparent": "<w3c-traceparent>"
    }
  }
}
```

This works for both `stdio` and `streamable_http` transports because the
context travels with the MCP message itself. The official MCP Python SDK does
not expose per-request HTTP headers for `streamable_http`; the HTTP request is
issued by a background task created during `start()`, so transport-level
headers cannot carry per-invoke context.

### MCP bridge scope

**Supported:** exposing MCP server **tools** as OpenAgentIO `invoke` targets.
Each discovered tool becomes `{target_prefix}.{tool_name}` on the Bus, and tool
results are returned as `agent.response.final` envelopes.

**Not yet supported:** full MCP host/runtime, resources as event streams,
prompts, sampling, roots, or bidirectional session features.

## Matrix Event Quickstart

Bridge Matrix room text messages into OpenAgentIO publish/subscribe events. This
is an **event bridge**, not a full Matrix client: it maps `m.room.message` /
`m.text` events to `matrix.message.received` and sends `matrix.message.send`
events back to Matrix rooms.

Supported:

- Text messages in configured rooms.
- Inbound `matrix.message.received` events with `session_id`, `conversation_id`,
  `correlation_id`, and `matrix.*` metadata.
- Outbound `matrix.message.send` events to a `room_id`.
- `room` and `room_sender` session strategies.
- Sync-loop reconnect, backoff, and health state.

Not supported:

- E2EE, media, reactions, redactions, federation, presence, typing, or full
  membership management.

Example bridge config (`examples/matrix_bridge.yaml`):

```yaml
version: "openagentio.bridge/v1"
bridges:
  - name: "matrix-main"
    type: "matrix_event"
    config:
      homeserver_url: "https://matrix.example.com"
      access_token: "your-access-token"
      user_id: "@bot:example.com"
      room_ids:
        - "!roomid:example.com"
      sync_timeout: 30
      reconnect_delay: 2
      initial_sync_behavior: "skip"
      outbound_msgtype: "m.text"
    mappings:
      event_prefix: "matrix"
      inbound_message_event: "matrix.message.received"
      outbound_message_event: "matrix.message.send"
      session_strategy: "room"
```

Run the quickstart to listen for messages:

```bash
env PYTHONPATH=src .venv/bin/python examples/matrix_quickstart.py \
    --config examples/matrix_bridge.yaml
```

Send one message back to a room:

```bash
env PYTHONPATH=src .venv/bin/python examples/matrix_quickstart.py \
    --config examples/matrix_bridge.yaml \
    --send "hello from OpenAgentIO" \
    --room "!roomid:example.com"
```

Load the same config programmatically:

```python
from openagentio import (
    Bus,
    InMemoryDriver,
    WithAgentID,
    WithSessionPropagation,
    WithTransport,
)
from openagentio.bridge import BUILTIN_FACTORIES, BridgeConfig
from openagentio.bridge.runner import BridgeRunner

bus = Bus.new(
    WithAgentID("my-app"),
    WithTransport(InMemoryDriver()),
    WithSessionPropagation(True),  # share session/trace with nested invoke/stream
)
await bus.connect()

config = BridgeConfig.from_file("examples/matrix_bridge.yaml")
runner = BridgeRunner(bus, config, BUILTIN_FACTORIES)
await runner.start()

# React to Matrix messages.
async def on_message(env):
    payload = env.payload_json()
    print(payload["room_id"], payload["sender"], payload["text"])

await bus.subscribe("matrix.message.received", on_message)
```

### Session and trace propagation for Matrix handlers

If a Matrix inbound handler calls `bus.invoke()` or `bus.stream_invoke()`,
create the Bus with `WithSessionPropagation(True)` so the new request inherits
`session_id`, `conversation_id`, `trace_id`, `span_id`, and `traceparent` from
the Matrix event. Without this option, nested calls start with a fresh context.

### Matrix bridge scope

**Supported:** text room message events in both directions.

**Not yet supported:** E2EE, media upload/download, federation, reactions,
redactions, thread/reply semantics, presence, typing, or automatic room
membership management.

## Protocol

The wire format is shared with the Go SDK:

- `spec_version = "acp/1.0"`, `schema_version = 1`
- UUIDv7 `event_id` (RFC 9562) with UUIDv4 fallback
- Subject layout `acp.v1.events.<event_type>` and `acp.v1.invoke.<target>`
  (with optional `<tenant>` segment)
- Envelopes published by Go can be decoded in Python and vice versa; the
  golden tests in `tests/test_envelope.py` use shared sample envelopes from
  `<repo>/schema/samples/`.

## Status

| Surface              | Status             |
| -------------------- | ------------------ |
| Envelope (v0.1)      | ✅ wire-compatible |
| In-memory transport  | ✅                 |
| NATS transport       | ✅ (nats-py)       |
| Pub / Sub            | ✅                 |
| Invoke / Reply       | ✅                 |
| Stream Invoke        | ✅                 |
| Middleware framework | ⏭ v0.3            |
| JetStream            | ⏭ v0.3            |
| Sync wrapper         | ⏭ v0.3            |
