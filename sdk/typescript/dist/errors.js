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
function isJsonObject(value) {
    return (typeof value === "object" &&
        value !== null &&
        !Array.isArray(value));
}
export function isErrorPayload(value) {
    if (typeof value !== "object" || value === null) {
        return false;
    }
    const candidate = value;
    if (typeof candidate.code !== "string") {
        return false;
    }
    if (typeof candidate.message !== "string") {
        return false;
    }
    if (typeof candidate.retryable !== "boolean") {
        return false;
    }
    if ("cause" in candidate && candidate.cause !== undefined && !isJsonObject(candidate.cause)) {
        return false;
    }
    return true;
}
//# sourceMappingURL=errors.js.map