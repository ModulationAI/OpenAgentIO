# OpenAgentIO 跨语言契约

`schema/` 目录是 OpenAgentIO 在 Go / Python / 其他 SDK 之间共享的**唯一权威**协议描述。任何修改都视为协议变更,需要同步更新 `schema_version` 并通过所有 SDK 的回归测试。

## 文件清单

| 路径 | 角色 |
| --- | --- |
| `envelope.schema.json` | JSON Schema (Draft 2020-12),约束 Envelope 顶层结构、必填字段、`is_final` ⇒ 终态事件、`agent.response.error` 的 payload 形状 |
| `samples/message_received.json` | 用户输入入站消息(对应 `agent.message.received`) |
| `samples/response_started.json` | 流式响应起始帧 |
| `samples/response_delta.json` | 流式响应增量帧 |
| `samples/response_final.json` | 流式响应终态(成功) |
| `samples/response_error.json` | 流式响应终态(失败,含 `ErrorPayload`) |

样本是**字段级真值**——任何 SDK 编解码出的 envelope 都应能字段对齐地解析这些样本。

## 在各 SDK 里如何校验

### Go

`pkg/event/golden_test.go` 已经做了:

1. 用 `github.com/santhosh-tekuri/jsonschema/v5` 编译 `envelope.schema.json`
2. 对每个样本跑一遍 schema 校验
3. 解码样本到 `event.Envelope` → 重新编码 → 用 `map[string]any` 做语义等价比较,确认 round-trip 不丢字段

直接 `go test ./pkg/event/...`。

### Python

```bash
pip install jsonschema
```

```python
import json
from jsonschema import Draft202012Validator

with open("schema/envelope.schema.json") as f:
    schema = json.load(f)
with open("schema/samples/response_delta.json") as f:
    sample = json.load(f)

Draft202012Validator(schema).validate(sample)
```

### Node / TypeScript

```bash
npm i ajv ajv-formats
```

```js
const Ajv = require("ajv/dist/2020").default;
const addFormats = require("ajv-formats");
const schema = require("./envelope.schema.json");
const sample = require("./samples/response_final.json");

const ajv = new Ajv();
addFormats(ajv);
const ok = ajv.compile(schema)(sample);
if (!ok) throw new Error("invalid envelope");
```

## 协议演进规则

- **新增可选字段**:不动 `schema_version`(向前兼容)。例如 v0.3.x 新增的 `frame_type` 是可选实验字段，旧 SDK 可安全忽略，因此 `schema_version` 保持为 1。
- **新增必填字段 / 改变现有字段含义**:`schema_version` +1,旧 SDK 必须能继续解析(忽略新字段),新 SDK 拿到 `schema_version < 当前` 时按降级路径处理。
- **`spec_version` 大版本**:仅在协议出现 breaking 变更(如重命名/删除字段)时调整,需要并发维护多版本 SDK。
