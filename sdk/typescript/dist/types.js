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
export function isTerminal(eventType) {
    return (eventType === ResponseFinal ||
        eventType === ResponseError ||
        eventType === ToolResult ||
        eventType === TaskCompleted);
}
//# sourceMappingURL=types.js.map