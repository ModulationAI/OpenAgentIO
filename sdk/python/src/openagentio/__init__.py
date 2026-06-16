"""OpenAgentIO Python SDK — ACP-compatible Envelope protocol on asyncio.

Quickstart::

    import asyncio
    from openagentio import Bus, InMemoryDriver, WithAgentID, WithTransport

    async def main():
        bus = Bus.new(WithAgentID("echo"), WithTransport(InMemoryDriver()))
        await bus.connect()

        async def echo(env):
            return env.payload_json()

        await bus.handle_invoke("echo", echo)
        resp = await bus.invoke("echo", {"msg": "hello"})
        print(resp.event_type, resp.payload_json())
        await bus.close()

    asyncio.run(main())
"""
from openagentio.bus import (
    DEFAULT_SUBJECT_PREFIX,
    Bus,
    ErrIdleTimeout,
    Handler,
    InvokeHandler,
    Stream,
    StreamHandler,
    StreamWriter,
    # Options.
    Options,
    Option,
    SubOption,
    InvokeOption,
    HandleOption,
    WithAgentID,
    WithTransport,
    WithTenant,
    WithSubjectPrefix,
    WithCodec,
    WithLogger,
    WithDefaultTimeout,
    WithMiddleware,
    WithEnvelopePreparer,
    WithSessionPropagation,
    WithQueue,
    WithTimeout,
    WithIdleTimeout,
    WithHandleQueue,
    # Errors.
    BusError,
    AgentTimeoutError,
    AgentUnavailableError,
    NoHandlerError,
    TransportFailureError,
    AuthFailureError,
    CodecFailureError,
    InvalidRequestError,
    BackpressureDropError,
    # DLQ.
    dlq_sink,
)
from openagentio.codec import Codec, JSONCodec
from openagentio.event import (
    SCHEMA_VERSION,
    SPEC_VERSION,
    CodeAgentTimeout,
    CodeAgentUnavailable,
    CodeAuthFailure,
    CodeBackpressureDrop,
    CodeCodecFailure,
    CodeInvalidRequest,
    CodeNoHandler,
    CodeTransportFailure,
    DeltaPayload,
    Envelope,
    ErrorPayload,
    FinalPayload,
    MessageReceived,
    ResponseDelta,
    ResponseError,
    ResponseFinal,
    ResponseStarted,
    StartedPayload,
    TaskCompleted,
    TaskCreated,
    ToolCall,
    ToolResult,
    is_terminal,
    new_id,
)
from openagentio.middleware import Chain, Middleware
from openagentio.middleware.deadletter import DeadLetter, DLQError, DLQSink
from openagentio.middleware.logging import Logging
from openagentio.middleware.recover import Recover
from openagentio.middleware.retry import (
    ConstantBackoff,
    ExponentialBackoff,
    IsRetryableError,
    Retry,
    RetryPolicy,
    Retryable,
)
from openagentio.middleware.trace import Trace
from openagentio.transport import (
    Capabilities,
    DialOption,
    Inbox,
    InMemoryDriver,
    NATSDriver,
    RawMessage,
    Subscription,
    Transport,
    TransportHandler,
    WithNATSName,
    dial,
)
from openagentio import session

# HTTP/SSE adapter — only available when starlette is installed.
try:
    from openagentio.adapter.http import (
        Adapter as HTTPAdapter,
        New as HTTPNew,
        AdapterOptions,
        WithAuth,
        WithIdleTimeout as WithHTTPIdleTimeout,
        WithLogger as WithHTTPLogger,
        WithMiddleware as WithHTTPMiddleware,
        WithSSERetry as WithHTTPSSERetry,
        WithTimeout as WithHTTPTimeout,
        AuthContext,
        AuthFunc,
        BearerAuth,
        ErrUnauthorized,
        ASGIMiddleware,
        Recover as HTTPRecover,
        Logging as HTTPLogging,
    )
except ImportError:
    pass

# OTel Bridge — only available when opentelemetry-api is installed.
try:
    from openagentio.middleware.otel import (
        Trace as OTelTrace,
        envelope_preparer as OTelEnvelopePreparer,
        EnvelopeCarrier,
        Config as OTelConfig,
        Option as OTelOption,
        WithTracerProvider,
        WithPropagator,
    )
except ImportError:
    pass

__version__ = "0.2.3"

__all__ = [
    "__version__",
    # Bus.
    "Bus",
    "Stream",
    "StreamWriter",
    "ErrIdleTimeout",
    "Handler",
    "InvokeHandler",
    "StreamHandler",
    "DEFAULT_SUBJECT_PREFIX",
    # Bus Options.
    "Options",
    "Option",
    "SubOption",
    "InvokeOption",
    "HandleOption",
    "WithAgentID",
    "WithTransport",
    "WithTenant",
    "WithSubjectPrefix",
    "WithCodec",
    "WithLogger",
    "WithDefaultTimeout",
    "WithMiddleware",
    "WithEnvelopePreparer",
    "WithSessionPropagation",
    "WithQueue",
    "WithTimeout",
    "WithIdleTimeout",
    "WithHandleQueue",
    # Bus Errors.
    "BusError",
    "AgentTimeoutError",
    "AgentUnavailableError",
    "NoHandlerError",
    "TransportFailureError",
    "AuthFailureError",
    "CodecFailureError",
    "InvalidRequestError",
    "BackpressureDropError",
    # DLQ.
    "dlq_sink",
    # Middleware.
    "Chain",
    "Middleware",
    "Trace",
    "Recover",
    "Logging",
    "Retry",
    "RetryPolicy",
    "ConstantBackoff",
    "ExponentialBackoff",
    "Retryable",
    "IsRetryableError",
    "DeadLetter",
    "DLQError",
    "DLQSink",
    # Envelope / events.
    "Envelope",
    "new_id",
    "is_terminal",
    "SPEC_VERSION",
    "SCHEMA_VERSION",
    "MessageReceived",
    "ResponseStarted",
    "ResponseDelta",
    "ResponseFinal",
    "ResponseError",
    "ToolCall",
    "ToolResult",
    "TaskCreated",
    "TaskCompleted",
    "StartedPayload",
    "DeltaPayload",
    "FinalPayload",
    "ErrorPayload",
    "CodeAgentTimeout",
    "CodeAgentUnavailable",
    "CodeBackpressureDrop",
    "CodeTransportFailure",
    "CodeCodecFailure",
    "CodeAuthFailure",
    "CodeInvalidRequest",
    "CodeNoHandler",
    # Codec.
    "Codec",
    "JSONCodec",
    # Transport.
    "Transport",
    "RawMessage",
    "Capabilities",
    "Subscription",
    "Inbox",
    "TransportHandler",
    "InMemoryDriver",
    "NATSDriver",
    # Dial helper.
    "dial",
    "DialOption",
    "WithNATSName",
    # Session / trace context (see openagentio.session for full API).
    "session",
    # HTTP/SSE Adapter (requires starlette).
    "HTTPAdapter",
    "HTTPNew",
    "AdapterOptions",
    "WithAuth",
    "WithHTTPIdleTimeout",
    "WithHTTPLogger",
    "WithHTTPMiddleware",
    "WithHTTPSSERetry",
    "WithHTTPTimeout",
    "AuthContext",
    "AuthFunc",
    "BearerAuth",
    "ErrUnauthorized",
    "ASGIMiddleware",
    "HTTPRecover",
    "HTTPLogging",
    # OTel Bridge (requires opentelemetry-api).
    "OTelTrace",
    "OTelEnvelopePreparer",
    "EnvelopeCarrier",
    "OTelConfig",
    "OTelOption",
    "WithTracerProvider",
    "WithPropagator",
]
