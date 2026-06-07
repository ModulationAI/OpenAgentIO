import { OpenAgentIOSSEError } from "./errors.js";

export interface SSEFrame {
  event?: string;
  id?: string;
  retry?: number;
  data: string;
}

export async function* parseSSEStream(stream: ReadableStream<Uint8Array>): AsyncIterable<SSEFrame> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let event: string | undefined;
  let id: string | undefined;
  let retry: number | undefined;
  let data: string[] = [];

  const emit = (): SSEFrame | undefined => {
    if (event === undefined && id === undefined && retry === undefined && data.length === 0) {
      return undefined;
    }
    const frame: SSEFrame = { data: data.join("\n") };
    if (event !== undefined) frame.event = event;
    if (id !== undefined) frame.id = id;
    if (retry !== undefined) frame.retry = retry;
    event = undefined;
    id = undefined;
    retry = undefined;
    data = [];
    return frame;
  };

  const processLine = (line: string): SSEFrame | undefined => {
    if (line.endsWith("\r")) {
      line = line.slice(0, -1);
    }
    if (line === "") {
      return emit();
    }
    if (line.startsWith(":")) {
      return undefined;
    }

    const colon = line.indexOf(":");
    const field = colon === -1 ? line : line.slice(0, colon);
    let value = colon === -1 ? "" : line.slice(colon + 1);
    if (value.startsWith(" ")) {
      value = value.slice(1);
    }

    switch (field) {
      case "event":
        event = value;
        break;
      case "id":
        id = value;
        break;
      case "retry": {
        const parsed = Number.parseInt(value, 10);
        if (Number.isFinite(parsed) && parsed >= 0) {
          retry = parsed;
        }
        break;
      }
      case "data":
        data.push(value);
        break;
      default:
        break;
    }
    return undefined;
  };

  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) {
        break;
      }
      buffer += decoder.decode(value, { stream: true });
      let newline = buffer.indexOf("\n");
      while (newline !== -1) {
        const line = buffer.slice(0, newline);
        buffer = buffer.slice(newline + 1);
        const frame = processLine(line);
        if (frame) {
          yield frame;
        }
        newline = buffer.indexOf("\n");
      }
    }

    buffer += decoder.decode();
    if (buffer.length > 0) {
      const frame = processLine(buffer);
      if (frame) {
        yield frame;
      }
    }
    const trailing = emit();
    if (trailing) {
      yield trailing;
    }
  } finally {
    reader.releaseLock();
  }
}

export function parseSSEJSON<T>(frame: SSEFrame): T {
  if (!frame.data) {
    throw new OpenAgentIOSSEError("SSE frame is missing data");
  }
  try {
    return JSON.parse(frame.data) as T;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new OpenAgentIOSSEError(`SSE frame data is not valid JSON: ${message}`);
  }
}
