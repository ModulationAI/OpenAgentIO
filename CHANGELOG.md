# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Formal Python Bridge SPI contract documented in `prompts/design.md` §9 and ADR-012.
- `BridgeRunner` now accepts an optional `stop_timeout` parameter (default remains 10.0s).
- `BridgeConfig.resolve_env()` and `BridgeDefinition.resolve_env()` for opt-in `${VAR}` / `${VAR:-default}` environment-variable placeholder resolution.
- Active Event Source supervision: `EventSourceSupervisor`, `RestartPolicy`, `BridgeHealth`, `BridgeHealthSnapshot`, and `PermanentBridgeError`.
- Optional `Bridge.health` hook and `BridgeRunner.health` aggregate snapshot so callers can observe bridge health.
- Bridge capability matrix in `prompts/bridge-capabilities.md`, documenting direction, Bus modes, session/trace/error mapping, timeout/retry/reconnect, auth, unsupported features, testing, and production readiness for all built-in bridges.
- Optional experimental `frame_type` field on `Envelope` in Go, Python, and TypeScript SDKs. The framework now dual-writes `frame_type` alongside `event_type` for protocol frames (request/response/tool); for known protocol event types, `frame_type` is canonical and derived from `event_type`, while unknown event types preserve any explicit `frame_type`.

### Changed

- `prompts/design.md` renumbered: Python SDK design moved to §10, ecosystem/interop to §11, HTTP/SSE adapter to §12, etc.
- `prompts/design.md` §3.2.4, `prompts/a2a_prot.md`, and `ROADMAP.md` updated to reflect the v0.3.x decision: optional experimental `frame_type` is adopted, while `Phase` and a `schema_version` bump to 2 are deferred until the Invocation/Task state model is defined.
- `prompts/design.md` §9.11 and `prompts/adr-012-bridge-spi.md` updated to document active Event Source supervision semantics (`EventSourceSupervisor`, `RestartPolicy`, health snapshots, failure isolation).
- `MatrixEventBridge` now delegates its background sync loop to `EventSourceSupervisor`, with optional `supervision` config for max restarts, exponential backoff, jitter, and health threshold.

### Fixed

- `BridgeRunner.start()` now preserves the original start exception when a bridge's `stop()` raises `CancelledError` during rollback cleanup.
- `BridgeConfig.resolve_env()` / `BridgeDefinition.resolve_env()` now recursively resolve `${VAR}` / `${VAR:-default}` placeholders inside nested mappings, lists, and tuples.
- Active Event Source bridges (e.g., `MatrixEventBridge`) no longer silently stop when their background task exits, and no longer infinite-retry on permanent configuration/authentication errors.
- `EventSourceSupervisor.stop()` now distinguishes child-task cancellation from caller cancellation and propagates external `asyncio.CancelledError`.

## [0.2.3] - 2026-06-10

### Added

- Event Envelope v1 with UUIDv7 IDs, W3C Trace Context (`traceparent`), streaming semantics (`seq`, `is_final`), and `metadata` inheritance for proxy chains.
- JSON Codec as the default wire format, with a pluggable `Codec` interface.
- Transport abstraction with NATS Core and In-Memory implementations for Go and Python.
- Bus runtime API (`Publish`, `Subscribe`, `Invoke`, `StreamInvoke`, `HandleInvoke`, `HandleStream`) with Go/Python parity.
- `StreamWriter` with `Started` / `Delta` / `Final` / `Error` frames and Go 1.23+ `iter.Seq2` iterator support.
- Middleware chain (`Recover`, `Trace`, `Logging`, `Retry`, `DeadLetter`) with metadata-stamped retry attempts (`acp.retry.attempt`).
- OpenTelemetry bridge (`pkg/middleware/otel`, `openagentio.middleware.otel`) as an opt-in subpackage.
- Session and trace context propagation via `context.Context` (Go) and `ContextVar` (Python).
- HTTP/SSE Adapter for Go (`pkg/adapter/http`) and Python (`openagentio.adapter.http`) with BearerAuth, custom `AuthFunc`, timeout/idle-timeout, and SSE retry.
- `transportdial` quick-start factory for zero-config `inmem`/`nats` switching via environment variables.
- EventType constructor differentiation (`event.NewEvent` / `event.NewRequest`) with runtime contract warnings in `Invoke`/`StreamInvoke`.
- Cross-language golden samples (`schema/samples/`) validating Go/Python wire-format compatibility.
- Scene Example demonstrating single-process orchestrator and distributed multi-process modes.
- Echo examples for Go (`examples/echo-agent/`) and Python (`sdk/python/examples/echo_agent.py`).

### Changed

- Unified Go, Python, and TypeScript SDK versions to `0.2.3`.

## [0.2.0-alpha.2] - 2026-05-17

### Added

- Initial alpha release of the Python SDK (`openagentio`) with asyncio Bus, NATS/InMem drivers, and HTTP/SSE adapter.
- TypeScript SSE client (`@openagentio/client`).
