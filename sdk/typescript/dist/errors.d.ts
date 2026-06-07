import type { Envelope, ErrorPayload, JsonValue } from "./types.js";
export declare class OpenAgentIOHTTPError extends Error {
    readonly name = "OpenAgentIOHTTPError";
    readonly status: number;
    readonly code?: string;
    readonly payload?: ErrorPayload | JsonValue;
    constructor(status: number, message: string, payload?: ErrorPayload | JsonValue);
}
export declare class OpenAgentIOStreamError extends Error {
    readonly name = "OpenAgentIOStreamError";
    readonly envelope: Envelope<ErrorPayload>;
    readonly code?: string;
    constructor(envelope: Envelope<ErrorPayload>);
}
export declare class OpenAgentIOSSEError extends Error {
    readonly name = "OpenAgentIOSSEError";
    constructor(message: string);
}
export declare function isErrorPayload(value: unknown): value is ErrorPayload;
//# sourceMappingURL=errors.d.ts.map