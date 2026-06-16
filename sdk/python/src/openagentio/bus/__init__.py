"""Application-facing bus runtime."""
from openagentio.bus.bus import (
    Bus,
    Handler,
    InvokeHandler,
    StreamHandler,
)
from openagentio.bus.dlq import dlq_sink
from openagentio.bus.errors import (
    AgentTimeoutError,
    AgentUnavailableError,
    AuthFailureError,
    BackpressureDropError,
    BusError,
    CodecFailureError,
    InvalidRequestError,
    NoHandlerError,
    TransportFailureError,
)
from openagentio.bus.options import (
    HandleOption,
    InvokeOption,
    Option,
    Options,
    SubOption,
    WithAgentID,
    WithCodec,
    WithDefaultTimeout,
    WithEnvelopePreparer,
    WithHandleQueue,
    WithIdleTimeout,
    WithLogger,
    WithMiddleware,
    WithQueue,
    WithSessionPropagation,
    WithSubjectPrefix,
    WithTenant,
    WithTimeout,
    WithTransport,
)
from openagentio.bus.stream import ErrIdleTimeout, Stream, StreamWriter
from openagentio.bus.subjects import (
    DEFAULT_SUBJECT_PREFIX,
    event_subject,
    invoke_subject,
)

__all__ = [
    # Bus.
    "Bus",
    "Stream",
    "StreamWriter",
    "ErrIdleTimeout",
    "Handler",
    "InvokeHandler",
    "StreamHandler",
    "DEFAULT_SUBJECT_PREFIX",
    "event_subject",
    "invoke_subject",
    # Options.
    "Options",
    "Option",
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
    "SubOption",
    "WithQueue",
    "InvokeOption",
    "WithTimeout",
    "WithIdleTimeout",
    "HandleOption",
    "WithHandleQueue",
    # Errors.
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
]