import { type Envelope, type JsonValue, type RequestContextHeaders } from "./types.js";
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
export declare class OpenAgentIOClient {
    private readonly baseUrl;
    private readonly headerSource?;
    private readonly fetchImpl;
    private readonly credentials?;
    constructor(options: OpenAgentIOClientOptions);
    invoke<TResponse = JsonValue, TRequest = JsonValue>(target: string, payload?: TRequest, options?: RequestOptions): Promise<TResponse | null>;
    streamInvoke<TPayload = JsonValue, TRequest = JsonValue>(target: string, payload?: TRequest, options?: StreamInvokeOptions): AsyncIterable<Envelope<TPayload>>;
    publish<TPayload = JsonValue>(eventType: string, payload?: TPayload, options?: RequestOptions): Promise<void>;
    private request;
    private buildHeaders;
}
//# sourceMappingURL=client.d.ts.map