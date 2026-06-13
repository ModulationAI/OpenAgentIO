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
