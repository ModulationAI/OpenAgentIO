import type { Envelope, ErrorPayload, JsonObject, JsonValue } from "./types.js";

export class OpenAgentIOHTTPError extends Error {
  readonly name = "OpenAgentIOHTTPError";
  readonly status: number;
  readonly code?: string;
  readonly payload?: ErrorPayload | JsonValue;

  constructor(status: number, message: string, payload?: ErrorPayload | JsonValue) {
    super(message);
    this.status = status;
    this.payload = payload;
    if (isErrorPayload(payload)) {
      this.code = payload.code;
    }
  }
}

export class OpenAgentIOStreamError extends Error {
  readonly name = "OpenAgentIOStreamError";
  readonly envelope: Envelope<ErrorPayload>;
  readonly code?: string;

  constructor(envelope: Envelope<ErrorPayload>) {
    const payload = envelope.payload;
    super(payload?.message || "agent stream error");
    this.envelope = envelope;
    this.code = payload?.code;
  }
}

export class OpenAgentIOSSEError extends Error {
  readonly name = "OpenAgentIOSSEError";

  constructor(message: string) {
    super(message);
  }
}

function isJsonObject(value: unknown): value is JsonObject {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value)
  );
}

export function isErrorPayload(value: unknown): value is ErrorPayload {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  const candidate = value as Record<string, unknown>;
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
