import { OpenAgentIOHTTPError, OpenAgentIOStreamError, isErrorPayload } from "./errors.js";
import { parseSSEJSON, parseSSEStream } from "./sse.js";
import { ResponseError, } from "./types.js";
export class OpenAgentIOClient {
    baseUrl;
    headerSource;
    fetchImpl;
    credentials;
    constructor(options) {
        if (!options.baseUrl) {
            throw new TypeError("OpenAgentIOClient requires baseUrl");
        }
        const fetchImpl = options.fetch ?? globalThis.fetch;
        if (!fetchImpl) {
            throw new TypeError("OpenAgentIOClient requires a fetch implementation");
        }
        this.baseUrl = options.baseUrl.replace(/\/+$/, "");
        this.headerSource = options.headers;
        this.fetchImpl = fetchImpl.bind(globalThis);
        this.credentials = options.credentials;
    }
    async invoke(target, payload, options = {}) {
        const response = await this.request(`/v1/agents/${encodeURIComponent(target)}/invoke`, {
            method: "POST",
            body: encodeBody(payload),
            signal: options.signal,
            headers: await this.buildHeaders(options),
        });
        await assertOK(response);
        if (response.status === 204) {
            return null;
        }
        return (await response.json());
    }
    async *streamInvoke(target, payload, options = {}) {
        const headers = await this.buildHeaders(options);
        headers.set("Accept", "text/event-stream");
        const response = await this.request(`/v1/agents/${encodeURIComponent(target)}/stream`, {
            method: "POST",
            body: encodeBody(payload),
            signal: options.signal,
            headers,
        });
        await assertOK(response);
        if (!response.body) {
            throw new OpenAgentIOHTTPError(response.status, "stream response has no body");
        }
        for await (const frame of parseSSEStream(response.body)) {
            const envelope = parseSSEJSON(frame);
            envelope.sse = {
                event: frame.event,
                id: frame.id,
                retry: frame.retry,
            };
            if (options.throwOnResponseError && envelope.event_type === ResponseError) {
                throw new OpenAgentIOStreamError(envelope);
            }
            yield envelope;
            if (envelope.is_final) {
                break;
            }
        }
    }
    async publish(eventType, payload, options = {}) {
        const response = await this.request(`/v1/events/${encodeURIComponent(eventType)}`, {
            method: "POST",
            body: encodeBody(payload),
            signal: options.signal,
            headers: await this.buildHeaders(options),
        });
        await assertOK(response);
    }
    async request(path, init) {
        return this.fetchImpl(`${this.baseUrl}${path}`, {
            ...init,
            credentials: this.credentials,
        });
    }
    async buildHeaders(options) {
        const headers = new Headers();
        headers.set("Content-Type", "application/json");
        if (this.headerSource) {
            mergeHeaders(headers, await resolveHeaders(this.headerSource));
        }
        if (options.headers) {
            mergeHeaders(headers, options.headers);
        }
        applyContextHeaders(headers, options.context);
        return headers;
    }
}
function encodeBody(value) {
    if (value === undefined) {
        return undefined;
    }
    return JSON.stringify(value);
}
async function resolveHeaders(source) {
    if (typeof source === "function") {
        return source();
    }
    return source;
}
function mergeHeaders(target, source) {
    new Headers(source).forEach((value, key) => target.set(key, value));
}
function applyContextHeaders(headers, context) {
    if (!context) {
        return;
    }
    setIfPresent(headers, "X-Trace-Id", context.traceId);
    setIfPresent(headers, "X-Traceparent", context.traceparent);
    setIfPresent(headers, "X-Tenant-Id", context.tenantId);
    setIfPresent(headers, "X-Session-Id", context.sessionId);
    setIfPresent(headers, "X-Conversation-Id", context.conversationId);
    setIfPresent(headers, "X-User-Id", context.userId);
    setIfPresent(headers, "X-Channel", context.channel);
}
function setIfPresent(headers, key, value) {
    if (value) {
        headers.set(key, value);
    }
}
async function assertOK(response) {
    if (response.ok) {
        return;
    }
    const payload = await readErrorPayload(response);
    let message = response.statusText || `HTTP ${response.status}`;
    if (isErrorPayload(payload)) {
        message = payload.message;
    }
    throw new OpenAgentIOHTTPError(response.status, message, payload);
}
async function readErrorPayload(response) {
    const contentType = response.headers.get("content-type") || "";
    if (!contentType.includes("application/json")) {
        return undefined;
    }
    try {
        return (await response.json());
    }
    catch {
        return undefined;
    }
}
//# sourceMappingURL=client.js.map