<div align="center">
<p align="center">
<img src="https://raw.githubusercontent.com/ModulationAI/openagentio/refs/heads/main/assets/OpenAgentIO.png" width="700" height="350" alt="openagentio preview">

<h3 align="center">
OpenAgentIO
</h3>
<h5 align="center">
Conversation-Aware Runtime Bus for Distributed AI Agents
</h5>

<div align="center">Build conversational distributed systems with streaming, events, sessions, and traces. </div></div>

---

OpenAgentIO is a lightweight runtime bus for distributed AI agents.

It gives distributed agents one runtime API for invoke, streaming, pub/sub, async tasks, session propagation, and trace propagation, while allowing the underlying protocol model to evolve with open agent communication ideas.

OpenAgentIO is designed for conversational distributed systems: systems where agents, workers, tools, and runtimes exchange long-lived context across requests, streams, and events.

The project focuses on the runtime communication substrate beneath agent systems, rather than planning, workflows, RAG, prompt orchestration, or model-specific agent logic.


## Why OpenAgentIO?

Modern AI systems are no longer single agents running inside one process.

They are:
- distributed
- event-driven
- streaming-native
- cross-runtime
- multi-agent
- conversation-oriented

Yet most frameworks primarily focus on:
- prompting
- workflows
- tool calling

 <img src="https://github.com/ModulationAI/openagentio/blob/main/assets/show.png?raw=true" alt="openagentio show">


OpenAgentIO focuses on the runtime communication layer for AI agents and agent-adjacent workers.

It treats conversational context and lifecycle state as part of the runtime contract, not as application-level afterthoughts. Requests, streams, async task updates, and events stay connected through protocol-level context propagation and lifecycle semantics, while the concrete wire shape can evolve as OpenAgentIO absorbs stronger open-protocol ideas.

Designed for distributed runtime collaboration, OpenAgentIO enables agents, workers, and runtimes to communicate consistently across different transports, languages, and execution environments.

## OpenAgentIO is NOT another Agent Framework

Agent frameworks and OpenAgentIO solve different layers of the AI runtime stack.

| Agent Frameworks             | OpenAgentIO                 |
| ---------------------------- | --------------------------- |
| Workflow orchestration       | Runtime communication       |
| Prompt orchestration         | Runtime interoperability    |
| Tool execution               | Distributed messaging       |
| Single runtime coordination  | Cross-runtime collaboration |
| Agent logic                  | Agent networking            |
| Task pipelines               | Streaming communication     |
| In-process workflows         | Distributed runtime systems |


## Why Conversational Distributed Systems?

Message brokers move bytes between services. Agent systems need more than that.

In a conversational distributed system:
- a conversation may span multiple agents, services, tools, and runtimes
- a single user turn may produce request/reply calls, streaming deltas, pub/sub events, and async task updates
- nested agent calls need to preserve conversational, causal, and observability context
- streaming responses need a clear lifecycle: started, delta, final, or error
- transports should be replaceable without rewriting agent communication semantics

OpenAgentIO provides this layer as a small runtime bus: one message model, one bus API, and transport adapters underneath.

## Problem Solved

Distributed Agent Communication Complexity

| Category | OpenAgentIO |
|---|---|
| Positioning | Conversation-Aware Agent Runtime Bus |
| Focus | Distributed Agent Runtime Communication |
| Protocols | invoke / stream / pubsub |
| Solves | Conversational Runtime Coordination |
| Architecture Layer | East-West Communication |
| Core Capabilities | Context / Session / Streaming |
| Message Model | Unified Runtime Messaging |
| Context Propagation | Conversation / Session / Trace Propagation |
| Runtime Support | Cross-Runtime Communication |
| Typical Scenarios | Multi-Agent Runtime |


## Communication Scenarios Coverage

| Scenario | Communication Pattern |
| --- | --- |
| ⭐️ Request-Reply | Synchronous Invocation |
| ⭐️ Streaming | Streaming Response |
| ⭐️ Pub/Sub | Event-Driven Messaging |
| ⭐️ Parallel Execution | Parallel Invocation |
| ⭐️ Agent Handoff | Context Transfer |
| ⭐️ Async Task | Asynchronous Task Processing |
| ⭐️ Browser/WebAPP Integration | HTTP + SSE Streaming |
| ⭐️ Remote Capability Calling | Agent-to-Agent Function Invocation |


## Scenario Demo

[![OpenAgentIO Scenario-1](./assets/s1.gif)][![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2FModulationAI%2Fopenagentio.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2FModulationAI%2Fopenagentio?ref=badge_shield)
()
[![OpenAgentIO Scenario-2](./assets/s2.gif)]()
[![OpenAgentIO Scenario-3](./assets/s3.gif)]()
[![OpenAgentIO Scenario-4](./assets/s4.gif)]()
[![OpenAgentIO Scenario-5](./assets/s5.gif)]()
[![OpenAgentIO Scenario-6](./assets/s6.gif)]()
[![OpenAgentIO Scenario-7](./assets/s7.gif)]()
[![OpenAgentIO Scenario-8](./assets/s8.gif)]()

## Installation and More Examples

For details, please refer to [OpenAgentIO SDK Examples](https://github.com/ModulationAI/OpenAgentIO-example)

## Design Philosophy
OpenAgentIO focuses on the communication runtime layer for distributed AI agents.

Rather than building another agent framework, OpenAgentIO focuses on:
- invoke and request-reply semantics
- streaming agent communication
- async task signaling
- pub/sub patterns
- context propagation
- runtime interoperability
- transport-neutral messaging

The goal is to provide a small, explicit, and composable communication substrate for heterogeneous agent systems.

## Relationship to A2A

OpenAgentIO is not an alternative to A2A. A2A is an emerging interoperability protocol for agents;OpenAgentIO focuses on the runtime substrate that carries agent communication patterns inside distributed systems.
As the A2A ecosystem evolves, OpenAgentIO intends to absorb compatible ideas around:
   - task lifecycle semantics
   - streaming event models
   - interoperability patterns

while keeping the runtime model composable across different transport backends.sport-neutral.

## Roadmap

> [!WARNING]
> OpenAgentIO is under active development and currently in the early 0.2 stage.
>
> The project is being rapidly refined around runtime communication APIs, protocol design, and cross-runtime interoperability.
>
> A more stable and officially usable 0.3 release is expected in early June 2026.

- v0.1: Go runtime, envelope schema, in-memory transport, NATS Core transport, invoke and streaming APIs.
- v0.2.x: HTTP/SSE adapter, Python SDK, session/trace propagation, OpenTelemetry bridge, retry / dead-letter middleware.
- v0.3.x: Reliability, protocol state cleanup, first multi-language SDK.
- v0.4.x: Advanced orchestration scenes, control plane foundation.
- v1.0.x: stable runtime message schema, cross-language compatibility, and production deployment guidance.

## License

OpenAgentIO is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.


[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2FModulationAI%2Fopenagentio.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2FModulationAI%2Fopenagentio?ref=badge_large)