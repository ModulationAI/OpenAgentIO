# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/ModulationAI/openagentio/compare/v0.2.3...HEAD
[0.2.3]: https://github.com/ModulationAI/openagentio/compare/v0.2.0-alpha.2...v0.2.3
[0.2.0-alpha.2]: https://github.com/ModulationAI/openagentio/releases/tag/v0.2.0-alpha.2
