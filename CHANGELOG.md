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
- Standard error-code constants (`CodeAgentTimeout`, `CodeAgentUnavailable`, etc.) are now exported from the TypeScript SDK (`sdk/typescript/src/types.ts`).
- Cross-language contract test matrix added: Go (`pkg/event/golden_test.go`), Python (`sdk/python/tests/test_golden_samples.py`), and TypeScript (`sdk/typescript/test/golden.test.mjs`) now validate golden samples against `schema/envelope.schema.json` and assert required fields, RFC3339 timestamps, UUIDv7 IDs, `seq=0` omission, terminal/`is_final` rules, unknown-field forward compatibility, and `ErrorPayload` shape.

### Changed

- **Breaking (TypeScript SDK)** — `ErrorPayload` shape unified with Go, Python, and the JSON Schema: `details` renamed to `cause` and `retryable` is now required. This is an intentional contract tightening for the TypeScript type system.

  Migration example:
  ```ts
  // Before (0.2.3)
  const error: ErrorPayload = {
    code: "AGENT_TIMEOUT",
    message: "timeout",
    details: { ... },
  };

  // After (0.3.0+)
  const error: ErrorPayload = {
    code: "AGENT_TIMEOUT",
    message: "timeout",
    retryable: true,
    cause: { ... },
  };
  ```

  This release should be versioned as **0.3.0** (or higher) because it changes the TypeScript public API contract.

- Response metadata inheritance in Go and Python now uses key-by-key merge semantics: handler-provided metadata overrides inherited request keys for the same name and adds new keys, while `acp.*` runtime keys are filtered from both inherited and handler-provided metadata. Empty handler metadata maps still inherit request metadata.

- `prompts/design.md` renumbered: Python SDK design moved to §10, ecosystem/interop to §11, HTTP/SSE adapter to §12, etc.
- `prompts/design.md` §3.2.4, `prompts/a2a_prot.md`, and `ROADMAP.md` updated to reflect the v0.3.x decision: optional experimental `frame_type` is adopted, while `Phase` and a `schema_version` bump to 2 are deferred until the Invocation/Task state model is defined.
- `prompts/design.md` §2 architecture diagram replaced with a Mermaid flowchart that includes the Go/Python/TypeScript SDKs, Bridge SPI & BridgeRunner, built-in bridges, HTTP/SSE Adapter, OTel EnvelopePreparer, and JSON Schema/golden samples; Router is now shown as internal to Bus and JetStream is marked Planned.
- `prompts/design.md` metadata and status updated: version bumped to v0.3.0, status changed from Draft to Implemented (with Experimental/Planned chapters), §16.0 snapshot dated 2026-07-26, §16.1 chapter status labels added, and outdated checkmarks for missing Go examples removed.
- `prompts/design.md` SDK module layout updated: §8.1 Go layout reflects the actual repo (no `examples/`, `cmd/`, `pkg/transport/jetstream/`, or `pkg/middleware/metrics/`); §10 status bumped to v0.3.0 alpha; §10.1 Python layout adds `bridge/` and the full examples list; new §10.7 documents the TypeScript HTTP/SSE Client and its limitations; §15 testing strategy updated to cover Go/Python/TypeScript, Bridge, and HTTP/SSE Adapter tests.
- `prompts/design.md` §12 HTTP/SSE API description rewritten: endpoints are all `POST`; `invoke` returns payload JSON; `stream` returns full Envelope SSE with `event`/`id`/`retry`/`data`; headers-before/headers-after error handling and cancellation propagation are documented. The cancellation section was subsequently corrected to state that Adapter cancellation currently stops local waiting/SSE output and closes the local Stream/inbox, but does not propagate an active cancellation control frame across transport to the remote handler.
- `MatrixEventBridge` now delegates its background sync loop to `EventSourceSupervisor`, with optional `supervision` config for max restarts, exponential backoff, jitter, and health threshold.

### Fixed

- TypeScript `isErrorPayload()` type guard now validates the required `retryable` field and rejects invalid `cause` values, matching the unified `ErrorPayload` interface.
- Python HTTP/SSE error responses now always serialize the required `retryable` field in `ErrorPayload`-shaped bodies; `cause` remains omitted when empty.
- Go HTTP/SSE timeout error responses now correctly set `retryable: true` for `AGENT_TIMEOUT`.
- Go HTTP adapter `writeErrorJSON` now includes the required `retryable` field in 4xx/5xx JSON error responses, aligning with the unified `ErrorPayload` contract and the Python adapter.
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
