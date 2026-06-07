export class OpenAgentIOHTTPError extends Error {
    name = "OpenAgentIOHTTPError";
    status;
    code;
    payload;
    constructor(status, message, payload) {
        super(message);
        this.status = status;
        this.payload = payload;
        if (isErrorPayload(payload)) {
            this.code = payload.code;
        }
    }
}
export class OpenAgentIOStreamError extends Error {
    name = "OpenAgentIOStreamError";
    envelope;
    code;
    constructor(envelope) {
        const payload = envelope.payload;
        super(payload?.message || "agent stream error");
        this.envelope = envelope;
        this.code = payload?.code;
    }
}
export class OpenAgentIOSSEError extends Error {
    name = "OpenAgentIOSSEError";
    constructor(message) {
        super(message);
    }
}
export function isErrorPayload(value) {
    return (typeof value === "object" &&
        value !== null &&
        typeof value.code === "string" &&
        typeof value.message === "string");
}
//# sourceMappingURL=errors.js.map