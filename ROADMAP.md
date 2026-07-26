# OpenAgentIO Roadmap

> Living document. Last updated: 2026-05-24.

---

## Overview

OpenAgentIO is a lightweight, ACP-compatible communication runtime for AI agents. The roadmap is organized into six layers, each with incremental milestones. Breaking changes are batched into major version boundaries; minor versions focus on additive features and stability.

| Version | Theme | Timeline |
|---------|-------|----------|
| v0.2 (current) | Stabilize Go SDK, HTTP/SSE adapter, middleware chain | Now |
| v0.3 | Reliability, protocol state cleanup, first multi-language SDK | Q3 2026 |
| v0.4 | Advanced orchestration scenes, control plane foundation | Q4 2026 |
| v1.0 | Production-grade control plane, enterprise features | 2027 |

---

## 1. Protocol Layer: Align with A2A Philosophy

Goal: Gradually absorb A2A (Agent-to-Agent) design principles without premature lock-in to a moving standard. EventType semantic overload is the primary technical debt.

### v0.3 — Experimental `frame_type` Field
- Add optional `frame_type string` to `Envelope` (schema v1) to carry protocol frame kind (`request` / `response.started` / `response.delta` / `response.final` / `response.error` / `tool.call` / `tool.result`).
- Keep `EventType` semantics unchanged during the compatibility period; framework dual-writes both `EventType` and `FrameType`.
- Consumers read `frame_type` first, fallback to `EventType` when absent.
- Defer `Phase` / schema v2 until the Invocation/Task state model is established.
- Update `IsTerminal()` behavior remains based on `EventType`; SSE event names stay derived from `EventType`.
- **Reference**: `prompts/a2a_prot.md` for detailed trade-off analysis (Options A/B/C).

### v0.4 — Task Model Evaluation
- Evaluate introducing a separate `Task` / `TaskUpdate` structure for Invoke/Stream paths, decoupled from the generic `Envelope`.
- `StreamInvoke` may return `TaskStream` instead of `Stream`.
- Decision gate: A2A ecosystem maturity + user demand for A2A interoperability.

### v1.0 — A2A Interoperability (Conditional)
- Full A2A protocol bridge if the standard stabilizes and gains traction.
- Native `TaskState` enum, OneOf-style response wrappers, artifact streaming.

---

## 2. Scene Layer: Richer Orchestration Patterns

Goal: Expand beyond basic Request-Reply, Streaming, and Pub/Sub to cover real-world multi-agent coordination patterns.

### v0.3 — Core Patterns
- **Parallel Execution**: Invoke multiple SubAgents concurrently, aggregate results (fan-out / fan-in).
- **Agent Handoff**: Transfer session context from one agent to another with explicit state passing.
- **Tool Calling**: Structured tool invocation with `ToolCall` / `ToolResult` event types (reserved in v0.2, enabled in v0.3).

### v0.4 — Advanced Patterns
- **Async Task with Callback**: Fire-and-forget task creation, completion notification via Pub/Sub or webhook.
- **Circuit Breaker / Degradation**: Middleware-level circuit breaker for cascading agent failures.
- **Multi-step Workflow**: DAG-style agent chaining with retry policies per edge.

### v1.0 — Enterprise Scenes
- **Human-in-the-loop**: Pause workflow for human approval, resume via API or event.
- **Multi-tenant Agent Marketplace**: Discovery and invocation of third-party agents across tenant boundaries.

---

## 3. SDK Layer: Multi-Language Support

Goal: Go remains the canonical reference implementation. Other languages follow after Go API freeze.

### v0.2 — Go SDK Stabilization (Current)
- Freeze `pkg/bus` public API.
- Complete golden sample tests as the cross-language specification baseline.
- Stabilize middleware interfaces (Recover, Trace, Logging, Retry, DeadLetter).

### v0.3 — Python SDK Alignment (Active)
The Python SDK has successfully resumed active development and has achieved parity for core Bus features (Publish, Subscribe, Invoke, StreamInvoke) along with the first developer preview of the Bridge layer (Matrix, MCP, and HTTP/SSE).

**Python v0.3 scope**:
- Complete parity with Go SDK scenarios (including OTel tracing example scenarios).
- Complete Bridge implementations: `McpToolBridge` and `MatrixEventBridge` (Developer Preview v0.3-alpha).
- Complete standard middleware chain: Recover, Trace, Logging.
- HTTP/SSE adapter parity.


### v0.4 — Java SDK Evaluation
- Evaluate JVM ecosystem demand.
- If pursued, target Kotlin-first with Java interoperability, reusing the same schema and golden samples.

---

## 4. Runtime Layer: Reliability & Persistence

Goal: Move from "at-most-once" (NATS Core) to "at-least-once" with persistence, replay, and observability.

### v0.3 — JetStream Transport
- **Transport implementation**: `pkg/transport/jetstream/` with durable streams and consumer groups.
- **Delivery guarantee**: At-least-once for Pub/Sub; Invoke/Stream remain at-most-once (idempotency is caller responsibility).
- **Replay**: Time-based and sequence-based message replay for audit and debugging.
- **Pull Consumer**: Work-queue pattern for backpressure-sensitive consumers.

### v0.3 — DLQ & Retry Completion
- **Dead Letter Queue**: Exhausted retries automatically republished to `{prefix}.dlq.{event_type}` with full context.
- **Retry middleware**: Backoff policies (fixed, exponential, custom) with `acp.retry.attempt` metadata stamping.
- **JetStream ack/nack**: Integrate with middleware so retries use JetStream redelivery instead of in-process loops where possible.

### v0.4 — Observability Integration
- **Prometheus metrics**: Messages in/out, latency histograms, error rates per target.
- **Grafana dashboard templates**: Agent topology, throughput, latency percentiles.
- **Distributed tracing**: W3C Trace Context full support, OTel span linking across agent boundaries.

---

## 5. Control Plane: Agent Registry

Goal: Move from static configuration (hard-coded target names) to dynamic service discovery and governance.

### v0.4 — Registry Foundation
- **Agent Registry**: Central store of agent metadata (id, capabilities, version, health status).
- **Service Discovery**: `bus.Invoke` resolves target name to actual subject/endpoint via registry lookup.
- **Health Checks**: Heartbeat mechanism with automatic deregistration on timeout.
- **Dynamic Routing**: Route to specific agent instance based on tenant, load, or capability match.

### v1.0 — Enterprise Control Plane
- **Credential Distribution**: Per-agent TLS certificates or JWT signing keys issued by the control plane.
- **Rate Limiting & Quotas**: Per-tenant and per-agent request throttling.
- **Policy Engine**: Allow/deny lists for cross-agent invocation, audit logging.
- **Web Dashboard**: Visual topology map, real-time traffic flow, drill-down tracing.

---

## 6. Monitoring & Dashboard UI

Goal: Operational visibility into the agent mesh.

### v0.3 — Metrics & Logs
- Structured logging (`slog`) standardized across all packages.
- Prometheus exporter for key bus metrics.
- NATS monitoring integration (server stats, connection counts).

### v0.4 — Dashboard (Web UI)
- **Topology view**: Visual graph of agents, subjects, and invocation edges.
- **Live traffic**: Message rate, latency heatmap, error spikes.
- **Trace search**: Filter by TraceID, SessionID, or time range.
- **Agent management**: Register, deregister, update agent metadata via UI.

### v1.0 — Enterprise Observability
- **Alerting rules**: PagerDuty/Slack integration for agent health degradation.
- **Cost attribution**: Per-tenant message volume and latency attribution.
- **Historical analytics**: Trending, capacity planning, anomaly detection.

---

## Cross-Cutting Concerns

### Breaking Change Policy
- **Schema version bump** (`SchemaVersion` field) for envelope structural changes.
- **Dual-write periods** for field migrations (e.g., `Phase` + `EventType` in v0.3).
- **Deprecation notices** logged at `Warn` level one minor version before removal.

### A2A Standard Tracking
- Monitor A2A spec releases quarterly.
- Maintain an "A2A compatibility scorecard" documenting gaps (tracked in `prompts/a2a_prot.md`).
- No commitment to full A2A alignment before v1.0; interoperability bridges are additive, not replacing native protocols.

### Python SDK Restart Gate
The Python SDK will not resume development until the three restart conditions in §3 are met. This is a hard gate to avoid chasing a moving Go API.
