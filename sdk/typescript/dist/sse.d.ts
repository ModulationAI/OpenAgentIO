export interface SSEFrame {
    event?: string;
    id?: string;
    retry?: number;
    data: string;
}
export declare function parseSSEStream(stream: ReadableStream<Uint8Array>): AsyncIterable<SSEFrame>;
export declare function parseSSEJSON<T>(frame: SSEFrame): T;
//# sourceMappingURL=sse.d.ts.map