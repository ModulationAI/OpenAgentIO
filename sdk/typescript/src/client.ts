import { OpenAgentIOHTTPError, OpenAgentIOStreamError, isErrorPayload } from "./errors.js";
import { parseSSEJSON, parseSSEStream } from "./sse.js";
import {
  type Envelope,
  type ErrorPayload,
  type JsonValue,
  type RequestContextHeaders,
  ResponseError,
} from "./types.js";

export type HeaderSource = HeadersInit | (() => HeadersInit | Promise<HeadersInit>);

export interface OpenAgentIOClientOptions {
  baseUrl: string;
  headers?: HeaderSource;
  fetch?: typeof fetch;
  credentials?: RequestCredentials;
}

export interface RequestOptions {
  headers?: HeadersInit;
  context?: RequestContextHeaders;
  signal?: AbortSignal;
}

export interface StreamInvokeOptions extends RequestOptions {
  throwOnResponseError?: boolean;
}

export class OpenAgentIOClient {
  private readonly baseUrl: string;
  private readonly headerSource?: HeaderSource;
  private readonly fetchImpl: typeof fetch;
  private readonly credentials?: RequestCredentials;

  constructor(options: OpenAgentIOClientOptions) {
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

  async invoke<TResponse = JsonValue, TRequest = JsonValue>(
    target: string,
    payload?: TRequest,
    options: RequestOptions = {},
  ): Promise<TResponse | null> {
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
    return (await response.json()) as TResponse;
  }

  async *streamInvoke<TPayload = JsonValue, TRequest = JsonValue>(
    target: string,
    payload?: TRequest,
    options: StreamInvokeOptions = {},
  ): AsyncIterable<Envelope<TPayload>> {
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
      const envelope = parseSSEJSON<Envelope<TPayload>>(frame);
      envelope.sse = {
        event: frame.event,
        id: frame.id,
        retry: frame.retry,
      };
      if (options.throwOnResponseError && envelope.event_type === ResponseError) {
        throw new OpenAgentIOStreamError(envelope as Envelope<ErrorPayload>);
      }
      yield envelope;
      if (envelope.is_final) {
        break;
      }
    }
  }

  async publish<TPayload = JsonValue>(
    eventType: string,
    payload?: TPayload,
    options: RequestOptions = {},
  ): Promise<void> {
    const response = await this.request(`/v1/events/${encodeURIComponent(eventType)}`, {
      method: "POST",
      body: encodeBody(payload),
      signal: options.signal,
      headers: await this.buildHeaders(options),
    });
    await assertOK(response);
  }

  private async request(path: string, init: RequestInit): Promise<Response> {
    return this.fetchImpl(`${this.baseUrl}${path}`, {
      ...init,
      credentials: this.credentials,
    });
  }

  private async buildHeaders(options: RequestOptions): Promise<Headers> {
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

function encodeBody(value: unknown): string | undefined {
  if (value === undefined) {
    return undefined;
  }
  return JSON.stringify(value);
}

async function resolveHeaders(source: HeaderSource): Promise<HeadersInit> {
  if (typeof source === "function") {
    return source();
  }
  return source;
}

function mergeHeaders(target: Headers, source: HeadersInit): void {
  new Headers(source).forEach((value, key) => target.set(key, value));
}

function applyContextHeaders(headers: Headers, context?: RequestContextHeaders): void {
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

function setIfPresent(headers: Headers, key: string, value?: string): void {
  if (value) {
    headers.set(key, value);
  }
}

async function assertOK(response: Response): Promise<void> {
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

async function readErrorPayload(response: Response): Promise<ErrorPayload | JsonValue | undefined> {
  const contentType = response.headers.get("content-type") || "";
  if (!contentType.includes("application/json")) {
    return undefined;
  }
  try {
    return (await response.json()) as ErrorPayload | JsonValue;
  } catch {
    return undefined;
  }
}
