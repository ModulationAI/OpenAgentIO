import type { Envelope, ErrorPayload, JsonValue } from "./types.js";

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

export function isErrorPayload(value: unknown): value is ErrorPayload {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as ErrorPayload).code === "string" &&
    typeof (value as ErrorPayload).message === "string"
  );
}
