export declare const SPEC_VERSION = "acp/1.0";
export declare const SCHEMA_VERSION = 1;
export declare const MessageReceived = "agent.message.received";
export declare const ResponseStarted = "agent.response.started";
export declare const ResponseDelta = "agent.response.delta";
export declare const ResponseFinal = "agent.response.final";
export declare const ResponseError = "agent.response.error";
export declare const ToolCall = "agent.tool.call";
export declare const ToolResult = "agent.tool.result";
export declare const TaskCreated = "agent.task.created";
export declare const TaskCompleted = "agent.task.completed";
export declare const FrameTypeRequest = "request";
export declare const FrameTypeResponseStarted = "response.started";
export declare const FrameTypeResponseDelta = "response.delta";
export declare const FrameTypeResponseFinal = "response.final";
export declare const FrameTypeResponseError = "response.error";
export declare const FrameTypeToolCall = "tool.call";
export declare const FrameTypeToolResult = "tool.result";
export type StandardEventType = typeof MessageReceived | typeof ResponseStarted | typeof ResponseDelta | typeof ResponseFinal | typeof ResponseError | typeof ToolCall | typeof ToolResult | typeof TaskCreated | typeof TaskCompleted;
export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonObject | JsonArray;
export interface JsonObject {
    [key: string]: JsonValue | undefined;
}
export type JsonArray = JsonValue[];
export interface ErrorPayload {
    code: string;
    message: string;
    retryable?: boolean;
    details?: JsonValue;
}
export interface SSEMetadata {
    event?: string;
    id?: string;
    retry?: number;
}
export interface Envelope<TPayload = JsonValue> {
    spec_version: string;
    schema_version: number;
    event_id: string;
    event_type: string;
    frame_type?: string;
    occurred_at: string;
    trace_id?: string;
    span_id?: string;
    traceparent?: string;
    session_id?: string;
    conversation_id?: string;
    correlation_id?: string;
    reply_to?: string;
    from?: string;
    to?: string;
    channel?: string;
    tenant_id?: string;
    user_id?: string;
    seq?: number;
    is_final?: boolean;
    payload?: TPayload;
    metadata?: Record<string, JsonValue>;
    sse?: SSEMetadata;
}
export interface RequestContextHeaders {
    traceId?: string;
    traceparent?: string;
    tenantId?: string;
    sessionId?: string;
    conversationId?: string;
    userId?: string;
    channel?: string;
}
export declare function isTerminal(eventType: string): boolean;
export declare function frameTypeForEventType(eventType: string): string;
export declare function effectiveFrameType(env: Envelope): string;
//# sourceMappingURL=types.d.ts.map