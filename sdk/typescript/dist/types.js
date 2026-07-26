export const SPEC_VERSION = "acp/1.0";
export const SCHEMA_VERSION = 1;
export const MessageReceived = "agent.message.received";
export const ResponseStarted = "agent.response.started";
export const ResponseDelta = "agent.response.delta";
export const ResponseFinal = "agent.response.final";
export const ResponseError = "agent.response.error";
export const ToolCall = "agent.tool.call";
export const ToolResult = "agent.tool.result";
export const TaskCreated = "agent.task.created";
export const TaskCompleted = "agent.task.completed";
// Experimental frame types — optional protocol-level discriminator.
export const FrameTypeRequest = "request";
export const FrameTypeResponseStarted = "response.started";
export const FrameTypeResponseDelta = "response.delta";
export const FrameTypeResponseFinal = "response.final";
export const FrameTypeResponseError = "response.error";
export const FrameTypeToolCall = "tool.call";
export const FrameTypeToolResult = "tool.result";
// Standard error codes used by ErrorPayload. Wire-identical to Go/Python SDKs.
export const CodeAgentTimeout = "AGENT_TIMEOUT";
export const CodeAgentUnavailable = "AGENT_UNAVAILABLE";
export const CodeBackpressureDrop = "BACKPRESSURE_DROP";
export const CodeTransportFailure = "TRANSPORT_FAILURE";
export const CodeCodecFailure = "CODEC_FAILURE";
export const CodeAuthFailure = "AUTH_FAILURE";
export const CodeInvalidRequest = "INVALID_REQUEST";
export const CodeNoHandler = "NO_HANDLER";
export function isTerminal(eventType) {
    return (eventType === ResponseFinal ||
        eventType === ResponseError ||
        eventType === ToolResult ||
        eventType === TaskCompleted);
}
const frameTypeForEventTypeMap = {
    [MessageReceived]: FrameTypeRequest,
    [ResponseStarted]: FrameTypeResponseStarted,
    [ResponseDelta]: FrameTypeResponseDelta,
    [ResponseFinal]: FrameTypeResponseFinal,
    [ResponseError]: FrameTypeResponseError,
    [ToolCall]: FrameTypeToolCall,
    [ToolResult]: FrameTypeToolResult,
};
export function frameTypeForEventType(eventType) {
    return frameTypeForEventTypeMap[eventType] ?? "";
}
export function effectiveFrameType(env) {
    const ft = frameTypeForEventType(env.event_type);
    return ft || env.frame_type || "";
}
//# sourceMappingURL=types.js.map