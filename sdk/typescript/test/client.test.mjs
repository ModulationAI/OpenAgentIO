import assert from "node:assert/strict";
import { test } from "node:test";

import {
  OpenAgentIOClient,
  OpenAgentIOHTTPError,
  ResponseDelta,
  ResponseFinal,
  parseSSEStream,
} from "../dist/index.js";

function streamFromText(text) {
  const encoder = new TextEncoder();
  return new ReadableStream({
    start(controller) {
      controller.enqueue(encoder.encode(text));
      controller.close();
    },
  });
}

function jsonResponse(body, init = {}) {
  return new Response(JSON.stringify(body), {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
}

test("parseSSEStream parses OpenAgentIO event, id, retry, and data", async () => {
  const frames = [];
  const body = [
    "event: agent.response.delta",
    "id: evt-1",
    "retry: 3000",
    'data: {"event_type":"agent.response.delta"}',
    "",
    "",
  ].join("\n");

  for await (const frame of parseSSEStream(streamFromText(body))) {
    frames.push(frame);
  }

  assert.equal(frames.length, 1);
  assert.equal(frames[0].event, ResponseDelta);
  assert.equal(frames[0].id, "evt-1");
  assert.equal(frames[0].retry, 3000);
  assert.equal(frames[0].data, '{"event_type":"agent.response.delta"}');
});

test("OpenAgentIOClient.invoke posts payload and returns response JSON", async () => {
  const seen = {};
  const client = new OpenAgentIOClient({
    baseUrl: "http://localhost:8080/",
    headers: { Authorization: "Bearer token" },
    fetch: async (url, init) => {
      seen.url = url;
      seen.method = init.method;
      seen.body = init.body;
      seen.authorization = init.headers.get("authorization");
      return jsonResponse({ ok: true });
    },
  });

  const result = await client.invoke("echo", { msg: "hello" });

  assert.deepEqual(result, { ok: true });
  assert.equal(seen.url, "http://localhost:8080/v1/agents/echo/invoke");
  assert.equal(seen.method, "POST");
  assert.equal(seen.body, '{"msg":"hello"}');
  assert.equal(seen.authorization, "Bearer token");
});

test("OpenAgentIOClient.streamInvoke yields envelopes with SSE metadata", async () => {
  const sse = [
    "event: agent.response.delta",
    "id: evt-delta",
    "retry: 1250",
    'data: {"spec_version":"acp/1.0","schema_version":1,"event_id":"evt-delta","event_type":"agent.response.delta","occurred_at":"2026-06-06T00:00:00Z","payload":{"chunk":"hi"}}',
    "",
    "event: agent.response.final",
    "id: evt-final",
    "retry: 1250",
    'data: {"spec_version":"acp/1.0","schema_version":1,"event_id":"evt-final","event_type":"agent.response.final","occurred_at":"2026-06-06T00:00:01Z","is_final":true,"payload":{"done":true}}',
    "",
    "",
  ].join("\n");

  const client = new OpenAgentIOClient({
    baseUrl: "http://localhost:8080",
    fetch: async () =>
      new Response(streamFromText(sse), {
        status: 200,
        headers: { "Content-Type": "text/event-stream" },
      }),
  });

  const events = [];
  for await (const env of client.streamInvoke("assistant", { text: "hi" })) {
    events.push(env);
  }

  assert.equal(events.length, 2);
  assert.equal(events[0].event_type, ResponseDelta);
  assert.deepEqual(events[0].payload, { chunk: "hi" });
  assert.deepEqual(events[0].sse, {
    event: ResponseDelta,
    id: "evt-delta",
    retry: 1250,
  });
  assert.equal(events[1].event_type, ResponseFinal);
  assert.deepEqual(events[1].payload, { done: true });
});

test("OpenAgentIOClient throws OpenAgentIOHTTPError for adapter JSON errors", async () => {
  const client = new OpenAgentIOClient({
    baseUrl: "http://localhost:8080",
    fetch: async () =>
      jsonResponse(
        { code: "NO_HANDLER", message: "no handler" },
        { status: 404, statusText: "Not Found" },
      ),
  });

  await assert.rejects(
    () => client.invoke("missing", {}),
    (error) => {
      assert.ok(error instanceof OpenAgentIOHTTPError);
      assert.equal(error.status, 404);
      assert.equal(error.code, "NO_HANDLER");
      assert.equal(error.message, "no handler");
      return true;
    },
  );
});
