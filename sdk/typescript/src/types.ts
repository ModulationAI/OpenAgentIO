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

export type StandardEventType =
  | typeof MessageReceived
  | typeof ResponseStarted
  | typeof ResponseDelta
  | typeof ResponseFinal
  | typeof ResponseError
  | typeof ToolCall
  | typeof ToolResult
  | typeof TaskCreated
  | typeof TaskCompleted;

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

export function isTerminal(eventType: string): boolean {
  return (
    eventType === ResponseFinal ||
    eventType === ResponseError ||
    eventType === ToolResult ||
    eventType === TaskCompleted
  );
}

const frameTypeForEventTypeMap: Record<string, string> = {
  [MessageReceived]: FrameTypeRequest,
  [ResponseStarted]: FrameTypeResponseStarted,
  [ResponseDelta]: FrameTypeResponseDelta,
  [ResponseFinal]: FrameTypeResponseFinal,
  [ResponseError]: FrameTypeResponseError,
  [ToolCall]: FrameTypeToolCall,
  [ToolResult]: FrameTypeToolResult,
};

export function frameTypeForEventType(eventType: string): string {
  return frameTypeForEventTypeMap[eventType] ?? "";
}

export function effectiveFrameType(env: Envelope): string {
  const ft = frameTypeForEventType(env.event_type);
  return ft || env.frame_type || "";
}
