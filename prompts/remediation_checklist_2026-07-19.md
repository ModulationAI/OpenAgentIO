# OpenAgentIO 整改清单

> 日期：2026-07-19  
> 来源：最新代码与 `prompts/design.md`、`prompts/overview.md`、`prompts/publicity.md` 的一致性 Review  
> 用途：作为整改 Issue、PR Review Checklist 和发布前验收依据  
> 建议顺序：P0 → P1 → P2

## 进度速览（更新至 2026-07-21）

- **P0 #1 Python 测试进程不退出 / Bridge 生命周期** — ✅ 完成（早前 session）。
- **P0 #2 流式乱序缓冲与背压** — ✅ 完成。代码 + 文档均落地：
  - Go/Python 均实现有界 pending map + `MaxPendingFrames`（默认 256）+ `MaxSequenceGap`（默认 1024）。
  - Python 拒绝负 seq；两侧 gap 校验采用 `seq - expected >= gap`（对齐 Go 的 uint64 溢出安全写法）。
  - `prompts/design.md` §5.3、`pkg/bus/options.go` 与 `sdk/python/src/openagentio/bus/options.py` 的 docstring 已同步（2026-07-21）。
- **P0 #3 StreamWriter 终态发布失败** — ✅ 完成。writer 引入 `open/closing/closed/failed` 状态机，Go/Python 对齐；runtime fallback + 结构化日志到位。2026-07-21 追加 seq 预留修复：failed terminal 保留其消耗的 seq，fallback `Error` 复用同一 seq，避免客户端 reorder buffer 卡在 seq 空洞上 idle-timeout。含端到端测试 `stream_e2e_failure_test.go` 与 Python 版。
- **下一步**：P0 已全绿，可进入 P1 §4（`Phase` / `FrameType` 决策）。

回归状态：Go `./...` 在 `-race` 下全绿；Python `pytest -q` 报告 408 passed / 10 nats-skipped。

---

## P0：发布前必须处理

### 1. 修复 Python 测试进程不退出

> ✅ 完成（早前 session）。

- [x] 按测试文件二分定位资源泄漏来源。
- [x] 重点检查 Matrix 长轮询后台任务。
- [x] 检查 Bridge `start()` 创建的 task 是否都在 `stop()` 中 cancel 并 await。
- [x] 检查 `httpx.AsyncClient`、NATS connection、subscription 是否全部关闭。
- [x] 检查 pytest async fixture teardown。
- [x] 为 Bridge lifecycle 增加无残留 task 测试。
- [x] CI 增加测试总超时，避免永久挂起。

验收标准：

- [x] `uv run pytest -q` 能自然退出，exit code 为 0。
- [x] 连续执行 3 次均不挂起。
- [x] 测试完成后没有 pending asyncio task 警告。
- [x] 当前预期至少为 `379 passed, 10 skipped`。（实际：408 passed / 10 skipped）

相关位置：

- `sdk/python/src/openagentio/bridge/runner.py`
- `sdk/python/src/openagentio/bridge/matrix_event.py`

### 2. 限制流式乱序缓冲，补齐背压策略

> ✅ 完成。Go 与 Python 均实现，`design.md` §5.3 与两侧 `options.*` 的 docstring 已同步。

Go 和 Python 都存在无界 pending map。

- [x] 增加 `MaxPendingFrames` 配置。
- [x] 增加最大允许 `seq` gap。
- [x] 拒绝负数序号（Python）。
- [x] 超限时终止流。
- [x] 返回统一的 `BACKPRESSURE_DROP` 或新的协议错误码。（`ErrBackpressureDrop` / `BackpressureDropError`）
- [x] 明确重复帧、过期帧、超大序号的处理语义。
- [x] Go/Python 行为保持一致。
- [x] 更新 `design.md` 中的背压描述，避免继续声称使用了实际不存在的"有界 channel"。（2026-07-21）

建议默认值：

- `MaxPendingFrames = 256` ✅ 实施
- `MaxSequenceGap = 1024` ✅ 实施

验收测试：

- [x] 正常乱序能够恢复顺序。
- [x] 重复帧被忽略。
- [x] 缺失 `seq=0` 时，pending 不会无限增长。
- [x] 超大 `seq` 被拒绝。
- [x] 超限返回明确错误，而不是等待 overall timeout。
- [x] Go/Python 对相同帧序列产生相同结果。

相关位置：

- `pkg/bus/stream.go`、`pkg/bus/stream_backpressure_test.go`
- `sdk/python/src/openagentio/bus/stream.py`、`sdk/python/tests/test_stream_backpressure.py`
- `prompts/design.md` §5.3

### 3. 修复 StreamWriter 终态发布失败问题

> ✅ 完成。writer 状态机 + Go/Python 对齐 + runtime fallback + 结构化日志 + seq 预留修复（2026-07-21 follow-up）。

- [x] 不要在编码和 publish 成功前永久设置 `closed=true`。
- [x] 区分 `open`、`closing`、`closed`、`failed` 状态。
- [x] `Final()` payload 编码失败时允许进入错误处理路径。
- [x] `Final()` transport publish 失败时将错误返回给 runtime。
- [x] `Error()` publish 失败时记录结构化日志和指标。
- [x] 不要静默忽略自动 `Final(nil)` / `Error(err)` 的错误。
- [x] 检查 Python `StreamWriter` 是否存在相同行为。（同步修复）

**Follow-up 2026-07-21**：修补 seq 预留 bug —— 失败的终态帧消耗了 seq=N，之前的实现让 fallback `Error` 从 nextSeq 拿 seq=N+1，导致客户端 reorder buffer 永久等 seq=N，尽管终态帧到了仍会 idle-timeout。现在 writer 记录 `reservedSeq` / `_reserved_seq`，fallback `Error` 从 FAILED 出发时复用该 seq。含端到端回归测试 `pkg/bus/stream_e2e_failure_test.go::TestStreamInvoke_FailedFinal_FallbackErrorReachesClient` 与 Python 版 `test_stream_invoke_failed_final_fallback_error_reaches_client`——回归后表现为 idle timeout。

验收测试：

- [x] Final payload codec 失败不会表现为无原因 idle timeout。
- [x] Final publish 失败能够被 handler/runtime 观察。
- [x] Error publish 失败有明确日志。
- [x] 成功发送终态后不能再次发送 delta/final。
- [x] 并发调用 Final/Error 时只有一个终态生效。
- [x] Failed terminal 与 fallback Error 复用同一 seq，客户端不会残留 seq 空洞。（P0#3 follow-up）

相关位置：

- `pkg/bus/stream.go` 中的 `StreamWriter.Final`、`StreamWriter.Error` 和自动终态处理。
- `pkg/bus/stream_writer_failure_test.go`、`pkg/bus/stream_e2e_failure_test.go`。
- `sdk/python/src/openagentio/bus/stream.py`、`sdk/python/tests/test_stream_writer_failure.py`。

---

## P1：协议与跨语言一致性

### 4. 决定 `Phase` / `FrameType` 是否进入当前协议

这是当前最重要的设计决策，不能继续保持多套口径。

- [ ] 明确 v0.3.x 是否实施 `Phase + FrameType`。
- [ ] 明确它们是已接受 ADR、实验特性，还是暂缓方案。
- [ ] 统一 `design.md`、`ROADMAP.md`、schema 和 SDK 口径。

如果决定实施：

- [ ] Envelope 增加 `phase`。
- [ ] Envelope 增加 `frame_type`。
- [ ] `schema_version` 升级为 2。
- [ ] 定义合法 Phase 枚举。
- [ ] 定义合法 FrameType 枚举。
- [ ] 明确 `EventType` 的新语义。
- [ ] 定义 v1/v2 双写、读取优先级和弃用周期。
- [ ] 更新 Go SDK。
- [ ] 更新 Python SDK。
- [ ] 更新 TypeScript SDK。
- [ ] 更新 JSON Schema。
- [ ] 更新全部黄金样本。
- [ ] 更新 HTTP/SSE event name 行为。
- [ ] 更新 `IsTerminal()` 判断规则。
- [ ] 增加 v1/v2 互操作测试。

如果决定延后：

- [ ] 从 v0.3 已承诺项中移除。
- [ ] 改为 v0.4 evaluation 或 unresolved ADR。
- [ ] 保留当前 `EventType` 语义并完整记录其限制。
- [ ] 不再把 `Phase + FrameType` 写成“长期方案确定”。

相关位置：

- `prompts/design.md` §3.2.4、§15、ADR-010。
- `ROADMAP.md` §1。
- `pkg/event/envelope.go`。
- `sdk/python/src/openagentio/event/envelope.py`。
- `sdk/typescript/src/types.ts`。

### 5. 统一三语言 `ErrorPayload`

当前 TypeScript 使用 `details`，Go/Python/schema 使用 `cause`。

- [ ] 选择唯一的标准字段名，建议保留 `cause`。
- [ ] TypeScript 将 `details` 改为 `cause`。
- [ ] 确定 `cause` 的 JSON 类型约束。
- [ ] 检查 `retryable` 是必填还是可选。
- [ ] HTTP 错误响应、SSE 错误帧使用相同结构。
- [ ] 三语言增加同一黄金样本测试。
- [ ] 检查所有标准错误码是否一致。

验收标准：

- [ ] 同一个 `response_error.json` 能被 Go、Python、TypeScript 正确解析。
- [ ] 三端序列化后字段名和语义一致。
- [ ] TypeScript 用户能通过类型访问 `cause`。

相关位置：

- `pkg/event/payload.go`。
- `sdk/python/src/openagentio/event/payload.py`。
- `sdk/typescript/src/types.ts`。
- `schema/envelope.schema.json`。

### 6. 修正 metadata 继承与覆盖规则

建议契约明确为：

```text
response metadata =
    filtered(request metadata)
    merged with response metadata
```

其中 response 同名键覆盖 request，`acp.*` 不从 request 继承。

- [ ] Go `adoptResponse()` 改成逐键合并。
- [ ] Python 对应逻辑同步修改。
- [ ] 明确 handler 是否允许主动返回 `acp.*` metadata。
- [ ] 决定过滤规则是否大小写敏感。
- [ ] 更新设计文档中的准确算法。

验收测试：

- [ ] 请求 metadata 在普通响应中继承。
- [ ] handler 新增键不会导致原 metadata 全部丢失。
- [ ] handler 同名键覆盖请求值。
- [ ] 请求的 `acp.*` 键不继承。
- [ ] nil、空 map、非空 map 行为明确。
- [ ] Go/Python 行为一致。

相关位置：

- `pkg/bus/invoke.go` 中的 `adoptResponse`、`inheritMetadata`。
- `sdk/python/src/openagentio/bus/bus.py`。
- `prompts/design.md` §5.4、§7.1、ADR-009。

### 7. 建立跨语言契约测试矩阵

- [ ] Go 校验全部 schema samples。
- [ ] Python 校验全部 schema samples。
- [ ] TypeScript 增加 schema sample round-trip。
- [ ] 检查 required fields。
- [ ] 检查时间格式。
- [ ] 检查 UUID 格式。
- [ ] 检查 `seq=0` 的省略行为是否符合约定。
- [ ] 检查 terminal event 与 `is_final` 的约束。
- [ ] 检查未知字段的前向兼容。
- [ ] 检查 ErrorPayload。
- [ ] CI 将三语言协议测试设为同一个必过 job。

验收矩阵：

| 能力 | Go | Python | TypeScript |
| --- | ---: | ---: | ---: |
| 读取黄金样本 | [ ] | [ ] | [ ] |
| 写入后 schema 校验 | [ ] | [ ] | [ ] |
| ErrorPayload | [ ] | [ ] | [ ] |
| 未知字段兼容 | [ ] | [ ] | [ ] |
| terminal 判断 | [ ] | [ ] | [ ] |

---

## P1：Bridge 生命周期与可靠性

### 8. 定义 Bridge SPI 正式契约

> ✅ 完成（2026-07-23）。新增 `prompts/design.md` §9「Bridge SPI 与配置驱动集成」、ADR-012，以及配套的代码契约与测试。

- [x] 在 `design.md` 增加 Bridge 专章。
- [x] 定义 `start()` / `stop()` 幂等性。
- [x] 定义 partial-start cleanup。
- [x] 定义 Handler 型 Bridge。
- [x] 定义 Event Source 型 Bridge。
- [x] 定义 Bridge 对 Bus 生命周期的所有权。
- [x] 定义外部 HTTP/NATS/MCP/Matrix client 的所有权。
- [x] 定义 handler 注册如何撤销。
- [x] 定义 Bridge 配置版本兼容规则。
- [x] 定义 factory registry 扩展方式。
- [x] 定义敏感配置和环境变量解析规则。

实现与证据：

- `prompts/design.md` §9（新增）与 §17 ADR 摘要表。
- `prompts/adr-012-bridge-spi.md`。
- `sdk/python/src/openagentio/bridge/base.py`：强化 `Bridge.start()` / `Bridge.stop()` 契约 docstring。
- `sdk/python/src/openagentio/bridge/runner.py`：新增可配置 `stop_timeout`，文档化 Runner 职责边界。
- `sdk/python/src/openagentio/bridge/config.py`：新增 `resolve_env()` 与 `${VAR}` / `${VAR:-default}` 解析。
- `sdk/python/tests/test_bridge_runner.py`：新增 `stop_timeout` 暴露测试与原有机制测试。
- `sdk/python/tests/test_bridge_config.py`：新增 `TestResolveEnv` 覆盖占位符、默认值、缺失变量、非字符串透传。
- `CHANGELOG.md`：记录 Unreleased 变更。

验收状态：

- [x] Python `uv run pytest -q`：416 passed / 10 nats-skipped。
- [x] Go `go test ./... -race -count=1`：全绿。

相关位置：

- `sdk/python/src/openagentio/bridge/base.py`。
- `sdk/python/src/openagentio/bridge/runner.py`。
- `sdk/python/src/openagentio/bridge/config.py`。

### 9. 补齐主动 Event Source 的 supervision

Matrix 与普通 MCP/OpenClaw handler 的生命周期不同。

- [ ] 记录后台 task。
- [ ] task 异常时输出结构化日志。
- [ ] 支持有限次数自动重启。
- [ ] 支持指数退避和 jitter。
- [ ] 区分可重试和永久配置错误。
- [ ] `stop()` 必须 cancel 并 await task。
- [ ] 暴露 Bridge health 状态。
- [ ] 防止一个 Bridge 崩溃静默停止、Runner 仍显示 started。
- [ ] 决定一个 Bridge 永久失败时是否影响其他 Bridge。

验收测试：

- [ ] Matrix sync task 崩溃后按策略重启。
- [ ] 配置错误不进行无限重试。
- [ ] stop 期间不会再次重连。
- [ ] runner stop 后无残留 task。
- [ ] 单个 Bridge 失败不会静默消失。

### 10. 对新增 Bridge 做能力清单和边界说明

对以下 Bridge 分别建立能力矩阵：

- [ ] MCP Tool Bridge。
- [ ] Matrix Event Bridge。
- [ ] OpenClaw Chat SSE Bridge。
- [ ] QwenPaw Chat SSE Bridge。

每个 Bridge 至少记录：

- [ ] 支持的方向：inbound / outbound。
- [ ] 对应 Bus 模式：Invoke / StreamInvoke / Publish / Subscribe。
- [ ] session 映射。
- [ ] trace 映射。
- [ ] error 映射。
- [ ] timeout/retry。
- [ ] reconnect。
- [ ] auth。
- [ ] 不支持能力。
- [ ] 测试方式。
- [ ] production readiness。

特别需要把 QwenPaw 纳入正式设计和定位材料：

- `sdk/python/src/openagentio/bridge/qwenpaw_chat_sse.py`。

---

## P2：`design.md` 文档整改

### 11. 更新文档元信息和版本状态

- [ ] 将 `v0.2-draft` 更新为实际版本。
- [ ] 更新“截至 2026-06-07”的进度快照。
- [ ] 明确哪些章节是规范。
- [ ] 明确哪些章节是提案。
- [ ] 明确哪些章节是未来规划。
- [ ] 避免使用 `[x]` 表示仓库中已不存在的能力。
- [ ] 增加最后验证日期和对应 release/tag。

建议状态标签：

```text
Implemented
Partially implemented
Experimental
Planned
Deferred
Rejected
```

### 12. 更新架构图

当前架构图需要补充：

- [ ] Go Runtime SDK。
- [ ] Python Runtime SDK。
- [ ] TypeScript HTTP/SSE Client。
- [ ] Bridge SPI。
- [ ] BridgeRunner。
- [ ] MCP Tool Bridge。
- [ ] Matrix Event Bridge。
- [ ] OpenClaw/QwenPaw SSE Bridge。
- [ ] HTTP/SSE Adapter。
- [ ] OTel EnvelopePreparer。
- [ ] JSON Schema / golden samples。

同时建议移除或降级尚未实现的独立 Router、Registry、JetStream、Prometheus 等组件，避免架构图让读者误以为已经存在。

### 13. 修正 HTTP/SSE API 描述

- [ ] 统一 stream endpoint 为实际的 POST。
- [ ] 核对 Go、Python Adapter 路由完全一致。
- [ ] 核对 TypeScript client 使用相同路由。
- [ ] 明确 invoke 返回 payload 还是完整 Envelope。
- [ ] 明确 stream 返回完整 Envelope。
- [ ] 记录 SSE `event`、`id`、`retry`、`data` 格式。
- [ ] 明确错误发生在 HTTP headers 前后的不同处理方式。
- [ ] 明确取消请求如何传播到 Bus stream。

实际路由：

```text
POST /v1/agents/{target}/invoke
POST /v1/agents/{target}/stream
POST /v1/events/{event_type}
```

相关位置：

- `pkg/adapter/http/adapter.go`。
- `sdk/python/src/openagentio/adapter/http/adapter.py`。
- `sdk/typescript/src/client.ts`。
- `prompts/design.md` §9.6、§11.1。

### 14. 更新 SDK 模块布局

- [ ] 删除不存在的 example 路径。
- [ ] 补充当前 Python Bridge 模块。
- [ ] 增加 TypeScript SDK 章节。
- [ ] 记录 TypeScript 只是 HTTP/SSE client，不是完整 Bus runtime。
- [ ] 更新 examples 清单。
- [ ] 更新测试目录及覆盖范围。
- [ ] 修复 streaming-llm 同时标记完成和未完成的矛盾。

### 15. 将“已实现”与“规划能力”分开

逐项核对并重新标记：

- [ ] Dropped callback。
- [ ] 有界 stream backpressure。
- [ ] `BACKPRESSURE_DROP` 自动告警。
- [ ] Prometheus metrics。
- [ ] Grafana dashboard。
- [ ] Redactor middleware。
- [ ] JetStream。
- [ ] Replay。
- [ ] Webhook forwarder。
- [ ] mTLS。
- [ ] Registry。
- [ ] Circuit breaker。
- [ ] Async Task runtime。
- [ ] 双向流。
- [ ] OpenAI V1 通用 Adapter。
- [ ] Anthropic Adapter。
- [ ] LangGraph/AgentScope Plugin。

规则：代码中不存在的能力不能使用现在时描述。

### 16. 修正 Go API 和错误模型描述

- [ ] 核对 `Bus` 当前真实接口。
- [ ] 核对 Transport 当前真实接口。
- [ ] 核对 Codec 当前真实接口。
- [ ] 核对所有 exported option。
- [ ] 核对实际哨兵错误名。
- [ ] 明确 transport errors 是否统一包装。
- [ ] 明确 Invoke timeout 返回底层 context error 还是标准 Bus error。
- [ ] 明确 Stream error 是本地错误还是远端 `ResponseError` Envelope。
- [ ] 为文档代码示例增加编译测试。

验收标准：

- [ ] `design.md` 中全部 Go 示例可以编译。
- [ ] Python 示例可以执行 import。
- [ ] TypeScript 示例可以通过 `tsc`。

---

## P2：定位、README 和宣发材料

### 17. 统一项目一句话定位

建议统一采用：

> OpenAgentIO is a bridgeable runtime bus for heterogeneous agent systems.

中文：

> OpenAgentIO 是面向异构 Agent 系统的可桥接运行时总线。

- [ ] README 使用统一定位。
- [ ] `overview.md` 使用统一定位。
- [ ] `publicity.md` 使用统一定位。
- [ ] Python package description 使用统一定位。
- [ ] TypeScript package description 使用统一定位。
- [ ] Release note 使用统一定位。

### 18. 收紧容易夸大的宣传用语

建议审查并修改：

- [ ] “plug-and-play”。
- [ ] “No custom glue code”。
- [ ] “Bidirectional by default”。
- [ ] “one shared session and trace”。
- [ ] “Agent Service Mesh”。
- [ ] “ACP-compatible”。
- [ ] “production-ready”。

建议替换为：

- developer preview。
- minimal bridge implementation。
- mock-backed integration tests。
- service-mesh-like runtime layer。
- orchestration remains in application/router agent。
- shared Envelope fields and optional OTel propagation。

相关位置：

- `RELEASE_NOTE.md`。
- `prompts/publicity.md` §4、§5.2。

### 19. 补齐宣发所需的端到端证据

- [ ] 实现 Matrix → Router → MCP → Matrix demo。
- [ ] 可选加入 OpenClaw/QwenPaw streaming。
- [ ] 补 Python OTel tracing scenario。
- [ ] 使用 Jaeger/OTLP 验证跨组件 trace。
- [ ] 增加 mock-backed E2E 自动测试。
- [ ] 增加真实服务 smoke test 说明。
- [ ] 录制可复现 GIF/视频。
- [ ] README 提供端到端架构图。
- [ ] 明确 Matrix 回写不是天然 token streaming。
- [ ] 明确 router agent 仍负责业务编排。

### 20. 更新 Roadmap、Changelog 和 Release Note

#### ROADMAP

- [ ] 更新最后修改日期。
- [ ] 删除“v0.2 current”。
- [ ] 删除 Python 尚未恢复的旧 gate。
- [ ] 加入 Bridge layer。
- [ ] 加入 TypeScript Client。
- [ ] 统一 `Phase` / `FrameType` 决策。
- [ ] 区分 v0.3 已完成和后续计划。

目前存在直接矛盾：`ROADMAP.md` §3 表示 Python SDK 已恢复开发，但文档末尾仍保留 Python SDK 不得恢复的旧 gate。

#### CHANGELOG

- [ ] 增加 Python 0.3.0。
- [ ] 记录 Bridge SPI/Runner。
- [ ] 记录 MCP Bridge。
- [ ] 记录 Matrix Bridge。
- [ ] 记录 OpenClaw Bridge。
- [ ] 记录 QwenPaw Bridge。
- [ ] 记录 TypeScript Client。
- [ ] 记录兼容性和已知限制。

#### Release Note

- [ ] 加入 OpenClaw。
- [ ] 加入 QwenPaw。
- [ ] 加入 TypeScript Client。
- [ ] 加入 developer-preview 限定。
- [ ] 加入已知限制。
- [ ] 删除不够准确的“无需 glue code”承诺。

---

## 最终回归验收

### 自动化测试

- [ ] `go test ./...`
- [ ] `go test ./... -race`
- [ ] `UV_CACHE_DIR=/tmp/openagentio-uv-cache uv run pytest -q`
- [ ] Python pytest 自然退出。
- [ ] `npm test`
- [ ] NATS Go 集成测试。
- [ ] NATS Python 集成测试。
- [ ] 三语言黄金样本测试。
- [ ] Bridge mock-backed E2E。
- [ ] HTTP/SSE 跨语言测试。

### 文档一致性

- [ ] README、design、overview、publicity 定位一致。
- [ ] design 与实际 API 一致。
- [ ] ROADMAP 不包含内部矛盾。
- [ ] CHANGELOG 覆盖最新功能。
- [ ] Release Note 不夸大能力。
- [ ] 所有文档路径真实存在。
- [ ] 所有示例至少经过编译或 import 验证。

### 完成定义

只有在以下条件全部满足后，才认为本轮整改结束：

1. [ ] 三语言测试全部通过且进程正常退出。
2. [ ] 两个流式 P1 问题已有回归测试。
3. [ ] `Phase` / `FrameType` 已形成唯一明确决策。
4. [ ] ErrorPayload 和 metadata 语义跨语言一致。
5. [ ] `design.md` 准确描述当前 Bridge + 多语言架构。
6. [ ] README、Roadmap、Changelog、Release Note 与实际版本一致。
7. [ ] 宣发内容明确保持 developer-preview 边界。
