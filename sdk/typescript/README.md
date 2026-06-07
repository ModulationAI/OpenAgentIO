# OpenAgentIO TypeScript Client

Framework-agnostic TypeScript client for the OpenAgentIO HTTP/SSE adapter.

It depends only on Web platform APIs: `fetch`, `ReadableStream`, and
`AbortController`. React, Vue, Svelte, Solid, and vanilla applications can all
consume the same core client.

## Usage

```ts
import { OpenAgentIOClient, ResponseDelta, ResponseFinal } from "@openagentio/client";

const client = new OpenAgentIOClient({
  baseUrl: "http://localhost:8080",
  headers: () => ({
    Authorization: `Bearer ${localStorage.getItem("token")}`,
  }),
});

const result = await client.invoke("echo", { msg: "hello" });

for await (const env of client.streamInvoke("count", { n: 3 })) {
  if (env.event_type === ResponseDelta) {
    console.log("delta", env.payload);
  }
  if (env.event_type === ResponseFinal) {
    console.log("final", env.payload);
  }
}
```

## Context Headers

The client maps request context to the headers understood by the OpenAgentIO
HTTP adapter.

```ts
await client.publish("order.created", { order_id: "o1" }, {
  context: {
    tenantId: "tenant-a",
    sessionId: "session-1",
    conversationId: "conversation-1",
    userId: "user-1",
    channel: "web",
    traceId: "trace-1",
  },
});
```

## Streaming

`streamInvoke()` performs `POST /v1/agents/{target}/stream` and parses the
OpenAgentIO SSE response. Each yielded value is the envelope JSON from the SSE
`data:` field with an additional `sse` metadata object containing the SSE
`event`, `id`, and `retry` fields.

```ts
const controller = new AbortController();

for await (const env of client.streamInvoke("assistant", { text: "hi" }, {
  signal: controller.signal,
})) {
  console.log(env.event_type, env.payload, env.sse?.retry);
}
```

By default, `agent.response.error` is yielded like any other terminal frame.
Pass `throwOnResponseError: true` to throw `OpenAgentIOStreamError` instead.
