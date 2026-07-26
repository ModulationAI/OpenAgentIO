"""Event-type and protocol-version constants. Mirrors pkg/event/types.go."""

SPEC_VERSION = "acp/1.0"
SCHEMA_VERSION = 1

# User input.
MessageReceived = "agent.message.received"

# Response lifecycle.
ResponseStarted = "agent.response.started"
ResponseDelta = "agent.response.delta"
ResponseFinal = "agent.response.final"
ResponseError = "agent.response.error"

# Tool calls (reserved, enabled in v0.2+).
ToolCall = "agent.tool.call"
ToolResult = "agent.tool.result"

# Async tasks (reserved, enabled in v0.3+ with JetStream).
TaskCreated = "agent.task.created"
TaskCompleted = "agent.task.completed"

# Frame types — experimental, optional protocol-level discriminator for the kind
# of protocol frame carried by the envelope. They are produced automatically by
# the framework and may be ignored by older consumers.
FrameTypeRequest = "request"
FrameTypeResponseStarted = "response.started"
FrameTypeResponseDelta = "response.delta"
FrameTypeResponseFinal = "response.final"
FrameTypeResponseError = "response.error"
FrameTypeToolCall = "tool.call"
FrameTypeToolResult = "tool.result"

_FRAME_TYPE_FOR_EVENT_TYPE = {
    MessageReceived: FrameTypeRequest,
    ResponseStarted: FrameTypeResponseStarted,
    ResponseDelta: FrameTypeResponseDelta,
    ResponseFinal: FrameTypeResponseFinal,
    ResponseError: FrameTypeResponseError,
    ToolCall: FrameTypeToolCall,
    ToolResult: FrameTypeToolResult,
}

_TERMINAL = frozenset({ResponseFinal, ResponseError, ToolResult, TaskCompleted})


def is_terminal(event_type: str) -> bool:
    """True if this event type closes a streaming response on its correlation_id."""
    return event_type in _TERMINAL


def frame_type_for_event_type(event_type: str) -> str:
    """Return the canonical frame_type for a standard event_type, or '' if unknown."""
    return _FRAME_TYPE_FOR_EVENT_TYPE.get(event_type, "")


def effective_frame_type(env: "Envelope") -> str:
    """Return the canonical frame_type for the envelope's event_type when it is
    a known protocol frame.

    For unknown event_types, fall back to any explicit frame_type. This keeps
    old and new consumers consistent: a known event_type always implies the
    same frame_type, regardless of what a producer may have written into the
    frame_type field.
    """
    ft = frame_type_for_event_type(env.event_type)
    if ft:
        return ft
    return env.frame_type
