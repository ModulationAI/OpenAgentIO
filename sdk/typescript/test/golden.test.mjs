import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import { basename, join } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";
import { Validator } from "jsonschema";

const __dirname = fileURLToPath(new URL(".", import.meta.url));
const schemaPath = join(__dirname, "..", "..", "..", "schema", "envelope.schema.json");
const samplesDir = join(__dirname, "..", "..", "..", "schema", "samples");

const schema = JSON.parse(readFileSync(schemaPath, "utf-8"));
const validator = new Validator();

const uuidV7RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const rfc3339RE =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/;

const terminalEventTypes = new Set([
  "agent.response.final",
  "agent.response.error",
  "agent.tool.result",
  "agent.task.completed",
]);

function loadSample(name) {
  return JSON.parse(readFileSync(join(samplesDir, name), "utf-8"));
}

const samples = readdirSync(samplesDir).filter((f) => f.endsWith(".json"));

for (const name of samples) {
  test(`sample ${name} validates against schema`, () => {
    const doc = loadSample(name);
    const result = validator.validate(doc, schema);
    assert.equal(result.errors.length, 0, result.errors.map((e) => e.stack).join("\n"));
  });

  test(`sample ${name} has required envelope fields`, () => {
    const doc = loadSample(name);
    assert.equal(typeof doc.spec_version, "string");
    assert.equal(typeof doc.schema_version, "number");
    assert.match(doc.event_id, uuidV7RE);
    assert.equal(typeof doc.event_type, "string");
    assert.match(doc.occurred_at, rfc3339RE);
  });

  test(`sample ${name} ignores extra unknown fields`, () => {
    const doc = loadSample(name);
    doc.future_field = "ignored";
    const result = validator.validate(doc, schema);
    assert.equal(result.errors.length, 0, "schema should allow additional properties");
  });
}

test("response_error payload has required ErrorPayload fields", () => {
  const doc = loadSample("response_error.json");
  assert.ok(doc.payload);
  assert.equal(typeof doc.payload.code, "string");
  assert.equal(typeof doc.payload.message, "string");
  assert.equal(typeof doc.payload.retryable, "boolean");
});

test("terminal samples set is_final true", () => {
  for (const name of samples) {
    const doc = loadSample(name);
    if (terminalEventTypes.has(doc.event_type)) {
      assert.equal(
        doc.is_final,
        true,
        `${name} has terminal event_type ${doc.event_type} but is_final != true`,
      );
    }
  }
});

test("frame_type matches event_type for protocol frames", () => {
  const expected = {
    "agent.message.received": "request",
    "agent.response.started": "response.started",
    "agent.response.delta": "response.delta",
    "agent.response.final": "response.final",
    "agent.response.error": "response.error",
    "agent.tool.call": "tool.call",
    "agent.tool.result": "tool.result",
  };
  for (const name of samples) {
    const doc = loadSample(name);
    const want = expected[doc.event_type];
    if (want) {
      assert.equal(doc.frame_type, want, `${name} frame_type mismatch`);
    }
  }
});
