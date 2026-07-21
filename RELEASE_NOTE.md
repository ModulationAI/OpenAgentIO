# openagentio Python SDK v0.3.0 — Bridge the Gap Between Agents, Tools, and Chat

**Your agents just got two new superpowers.**

With v0.3.0, the openagentio Python SDK introduces the **Bridge** subsystem — a plug-and-play integration layer that connects your ACP-compatible agents to the tools and chat platforms you already use. No rewrites, no custom glue code: configure, connect, and go.

---

## 🛠️ Connect Real Tools with the MCP Tool Bridge (`25b320b`)

Turn any MCP-compatible tool into an OpenAgentIO handler in minutes.

- **Instant tool exposure:** Surface MCP tools as first-class `invoke` targets.
- **Flexible transports:** Works with both local stdio and remote HTTP/SSE MCP servers.
- **Declarative config:** One YAML/JSON file under the `openagentio.bridge/v1` schema.
- **Batteries included:** Example configs and a complete quickstart ready to run.

> Deploy an existing MCP tool behind OpenAgentIO without touching its source code.

---

## 💬 Join the Conversation with the Matrix Event Bridge (`43fed4c`)

Bring your agents into Matrix rooms — and bring Matrix messages into your agents.

- **Bidirectional by default:** Ingest room events and send replies over the Matrix Client-Server API.
- **Room-ready agents:** Build chatbots, notification agents, and interactive assistants that live where your team already collaborates.
- **Zero boilerplate:** Configure the bridge, point it at a room, and start handling events.

> Your agents can now listen and respond inside Matrix, just like any other member.

---

## What This Means for You

- **Less integration code.** Bridges handle protocol translation so you don't have to.
- **More places for agents to run.** Tools, chat rooms, and custom runtimes — all through one SDK.
- **Same OpenAgentIO API.** `Bus`, `Transport`, and `Envelope` stay unchanged; existing code keeps working.

---

## Compatibility

- Requires Python >= 3.11.
- No breaking changes to existing APIs.
- New optional dependency: `mcp`.

---

**Ready to bridge your agents?** Check out the `sdk/python/examples/` directory for MCP and Matrix quickstarts.
