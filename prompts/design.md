# OpenAgentIO 设计文档

> 版本: v0.2-draft  
> 状态: Draft  
> 关联需求: `prompts/require.md`  
> 实现语言: Go 1.25（核心运行时）+ Python 3.11+（asyncio SDK）  
> 默认 Transport: NATS 2.10+（Core，JetStream 在 v0.3 启用）

---

## 1. 需求评审（Review）

### 1.1 需求文档优点

- **定位清晰**：聚焦"Agent 通信运行时"，明确不做推理 / 编排 / RAG，避免范围蔓延。
- **协议优先**：以 ACP-compatible Event Envelope 为核心抽象，符合事件驱动思路。
- **Streaming-Native**：把流式当一等公民处理，契合 LLM 的 token 输出场景。
- **Transport Agnostic**：留出抽象层，NATS 之外可扩展 HTTP / Kafka 等。
- **Ecosystem Ready**：定位为 Agent 通信网格（Service Mesh），预留了集成第三方 Agent 框架（LangGraph, AgentScope）与标准协议（OpenAI, Anthropic）的扩展点。

### 1.2 需求文档需要补充 / 修正的点

| 类别 | 问题 | 建议 |
| --- | --- | --- |
| Envelope | 字段缺少 `seq`/`is_final`/`reply_to`/`correlation_id`/`schema_version` | 流式有序、关联请求、协议演进必备 |
| Subject 规范 | `agent.response.delta` 是事件类型而非路由地址，无法定位到具体会话 | 引入"调用 inbox + 广播主题"双层模型 |
| Streaming 语义 | 顺序性、丢失、超时、背压未定义 | 明确 Core NATS（最多一次）vs JetStream（至少一次）的取舍 |
| Request/Reply | 与 Streaming 整合方式未说明 | 用 NATS `_INBOX.{nuid}` 作为多消息流式响应通道 |
| Reliability | retry / DLQ 仅列在 v0.3，但与 ack/timeout 紧耦合 | 接口在 v0.1 留好钩子（中间件 + Transport 能力声明），实现可后置 |
| 多租户 | `tenant_id` 仅在 payload，没有强隔离 | 路由层加 `tenant` 段或使用 NATS Account |
| 错误模型 | `agent.response.error` 没有错误码、是否可重试等字段 | 标准化 `ErrorPayload`：`code` / `message` / `retryable` / `cause` |
| 可观测性 | 仅有 trace_id 文本字段 | 兼容 W3C Trace Context（`traceparent`），原生埋 OpenTelemetry |
| Codec | 未明确序列化格式 | 默认 JSON（互通 + 调试友好），保留 Protobuf 扩展点 |
| SDK 一致性 | Roadmap v0.1 同时上 Go + Python，需避免双端 API 漂移 | **Go 仍作为协议与运行时参考实现；Python SDK 已在 v0.2 alpha 跟进，采用共享 Envelope / Subject / 黄金样本约束跨语言一致性** |
| 命名 | 主题用 `agent.*` 与事件类型同名易混 | 主题统一前缀 `acp.v1.*`，事件类型保留 `agent.*` 语义命名 |
| **EventType 语义** | **同一字段身兼两职：协议状态标记（`ResponseDelta`）+ 业务路由键（`goc.incident.created`），在 `Publish` 和 `Invoke/StreamInvoke` 下语义权重不一致，易混淆** | **短期（v0.2 已落地）：构造器分化（`event.NewEvent`/`event.NewRequest`）+ 运行时契约检查；长期（v0.3 评估）：新增 `Phase` + `FrameType`，分别承载生命周期状态与通信帧类型，EventType 回归纯业务语义，参考 A2A 协议状态机设计** |

### 1.3 范围与里程碑微调建议

- v0.1 MVP 必须包含：Envelope + Codec + NATS Transport + Pub/Sub + 单向 StreamInvoke + Go SDK + 最小 Python 订阅/发布。
- v0.2 重点：Request/Reply 完整化、HTTP/SSE Adapter、Session 中间件、Python asyncio SDK 与 Go v0.1/v0.2 核心能力对齐。
- v0.3 引入 JetStream（持久化 / Replay / DLQ）与 OTel 集成。
- v1.0 再做 Control Plane / Registry。

---

## 2. 总体架构

```
+---------------------------------------------------------------------------------+
|                                 Client Layer                                    |
|   Standard OpenAI SDK  |  Anthropic SDK  |  DingTalk  |  Web UI  |  Custom API  |
+----------------------------------------+----------------------------------------+
                                         |
                                         v
+---------------------------------------------------------------------------------+
|                         Gateway / Protocol Adapters                             |
|    (OpenAI V1 / Anthropic Protocol -> Envelope, AuthN, RateLimit, TenantTag)     |
+----------------------------------------+----------------------------------------+
                                         |
                                         v
+---------------------------------------------------------------------------------+
|                               OpenAgentIO Runtime                               |
|                               (Agent Service Mesh)                              |
|                                                                                 |
|  +--------+      +-----------+      +------------+      +----------+            |
|  | Bus    | <--> | Router    | <--> | Middleware | <--> | Codec    |            |
|  +--------+      +-----------+      +------------+      +----------+            |
|       |                ^                  ^                  ^                  |
|       v                |                  |                  |                  |
|  +---------------------------------------------------------------------------+  |
|  |                          Transport (interface)                            |  |
|  |      NATS (default)  |  JetStream  |  HTTP / SSE  |  In-Memory            |  |
|  +---------------------------------------------------------------------------+  |
+----------------------------------------+----------------------------------------+
                                         |
            +----------------------------+----------------------------+
            v                            v                            v
      +-----------+                +-----------+                +-----------+
      | MainAgent |                | RAGAgent  |                | ToolAgent |
      | (LangGraph)|                | (Scope)   |                | (Go/Raw)  |
      +-----------+                +-----------+                +-----------+
```

核心组件职责：

- **Envelope / Codec**：协议层，跨语言契约，向上支撑 OpenAI/Anthropic 等协议转换。
- **Transport**：通信层抽象，屏蔽 NATS / HTTP 差异。
- **Bus**：面向应用的 API，作为 Agent Service Mesh 的核心总线。
- **Router**：把 `Subject` 与 `event_type` 映射到 handler；支持基于 Agent 名的逻辑寻址。
- **Middleware**：提供全链路 Trace、鉴权、重试等 Service Mesh 特性。
- **Adapter / Bridge**：包含两类适配器：
  - **Protocol Adapter**：将外部标准协议（OpenAI/Anthropic）映射为内部 Envelope。
  - **Framework Plugin**：为 LangGraph、AgentScope 等提供接入插件，使其能作为 OpenAgentIO 的节点运行。

---

## 3. 协议规范

### 3.1 Event Envelope v1

```json
{
  "spec_version": "acp/1.0",
  "schema_version": 1,

  "event_id": "evt_01H...",
  "event_type": "agent.response.delta",
  "occurred_at": "2026-05-02T10:00:00.123Z",

  "trace_id":        "0af7651916cd43dd8448eb211c80319c",
  "span_id":         "b7ad6b7169203331",
  "traceparent":     "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
  "session_id":      "sess_xxx",
  "conversation_id": "conv_xxx",
  "correlation_id":  "req_xxx",
  "reply_to":        "_INBOX.abc123",

  "from": "main-agent",
  "to":   "dingtalk-gateway",

  "channel":  "dingtalk",
  "tenant_id": "tenant_xxx",
  "user_id":   "user_xxx",

  "seq":       3,
  "is_final":  false,

  "payload":  { "delta": "正在分析..." },
  "metadata": { "model": "qwen-max" }
}
```

字段说明（核心补充项加粗）：

| 字段 | 说明 |
| --- | --- |
| `spec_version` | 协议大版本（语义版本，例 `acp/1.0`）。Breaking change 提升主版本。 |
| **`schema_version`** | Envelope 结构小版本，单调递增整数，向前兼容。 |
| `event_id` | 全局唯一，**UUIDv7**（RFC 9562）。时间有序、与各语言/数据库 UUID 类型原生兼容。 |
| `event_type` | 见 §3.2。 |
| `occurred_at` | RFC3339Nano，UTC。原 `created_at` 重命名以贴合事件语义。 |
| `trace_id` / `span_id` / **`traceparent`** | 兼容 W3C Trace Context；中间件自动注入。 |
| `session_id` / `conversation_id` | Agent 会话与对话上下文。 |
| **`correlation_id`** | 跨请求/响应关联（例如：StreamInvoke 的全部 delta 共享同一个 correlation_id）。 |
| **`reply_to`** | 响应应回写的 subject，通常为 NATS `_INBOX.*`。 |
| `from` / `to` | 逻辑 Agent 名（与服务实例无关）。 |
| `channel` / `tenant_id` / `user_id` | 业务上下文。 |
| **`seq`** | 流式中第几条（从 0 开始单调递增）。 |
| **`is_final`** | 流是否结束。配合 `seq` 让消费者完成有序+完整性校验。 |
| `payload` | 业务负载，按 `event_type` 走子 schema。 |
| `metadata` | 自由扩展字段，非协议保留。**级联响应时，非 `acp.*` 键默认继承自请求信封**，业务上下文（如 `dingtalk.*`）可随响应自动回流；`acp.*` 键（如 `acp.retry.attempt`、`acp.dlq.last_error`）为运行时内部状态，不继承。 |

### 3.2 事件类型（EventType）设计

#### 3.2.1 当前标准枚举（框架内部状态）

```
# 用户输入
agent.message.received

# 响应生命周期
agent.response.started   # 流开启，payload 可包含模型/工具元信息
agent.response.delta     # 流式增量
agent.response.final     # 流式终态（也用于一次性响应）
agent.response.error     # 错误终态（与 final 互斥）

# 工具调用（v0.2+ 启用）
agent.tool.call
agent.tool.result

# 任务/异步（v0.3+ 启用，依赖 JetStream）
agent.task.created
agent.task.completed
```

约束：
- `started` / `final` / `error` 在一次 `correlation_id` 中各最多一次；`error` 与 `final` 互斥。
- `delta` 之间允许并发到达，消费者按 `seq` 重排序。
- `is_final=true` ⇒ `event_type ∈ {final, error}`。

#### 3.2.2 已知设计债务：EventType 语义过载

`EventType` 目前同时承担两个不相容的职责：

| 职责 | 表现形式 | 控制方 |
|---|---|---|
| **协议状态标记** | `ResponseDelta`、`ResponseFinal` 等，用于框架内部识别通信阶段 | **框架自动设置**，用户不应修改 |
| **业务路由键** | `goc.incident.created`、`com.mycompany.order.paid`，用于 Pub/Sub 路由到不同消费者 | **用户自定义并手动设置** |

这导致同一字段在不同 API 下权重不一致：
- `Publish`：EventType 是**路由键**，直接决定消息发到哪个 subject。
- `Invoke` / `StreamInvoke`：EventType **不参与路由**，路由由 `target` 参数决定；EventType 仅作为日志/追踪标签。

**典型误用模式**：
1. 用户把 `StreamInvoke` 的 `target` 名填到 `EventType` 里，误以为两者需要对齐。
2. 用户从 Pub/Sub handler 里拿到 envelope，直接转发给 `StreamInvoke`，把业务 EventType（如 `goc.incident.created`）带进了请求链路。

#### 3.2.3 短期缓解方案（v0.2.x）

**1. 构造器分化**（`pkg/event/envelope.go`）

通过 API 命名建立语义边界，不改动结构体：

```go
// 明确用于 Pub/Sub：EventType 必填，因为它决定路由
env := event.NewEvent("goc.incident.created")
env.Payload = payload
b.Publish(ctx, env)

// 明确用于 Invoke/StreamInvoke：EventType 由框架自动设为 MessageReceived，用户无需关心
env := event.NewRequest()
env.Payload = payload
b.StreamInvoke(ctx, "sub-agent-stream", env)
```

保留 `event.New()` 作为底层兼容入口，但示例代码全部迁移到新构造器。

**2. 运行时契约检查**（`pkg/bus/invoke.go`）

在 `Invoke` / `StreamInvoke` 的 `buildRequestEnvelope` 中，如果检测到传入的 `*event.Envelope` 的 EventType 不是 `MessageReceived`，输出结构化 warning（不阻断、不报错）：

```go
if env.EventType != "" && env.EventType != event.MessageReceived {
    b.opts.Logger.Warn("bus: invoke payload envelope carries non-request event type",
        slog.String("event_type", env.EventType),
        slog.String("target", target),
        slog.String("hint", "use event.NewRequest() for invoke/stream, event.NewEvent() for pub/sub"),
    )
}
```

**3. 前缀隔离约定**

框架标准事件统一使用 `agent.*` 前缀（已有），业务事件建议采用 `{domain}.{entity}.{action}` 风格（如 `goc.incident.created`），避免与框架枚举冲突。该约定写入文档，但不强制运行时校验。

#### 3.2.4 长期演进方向（v0.3 评估）

**参考：A2A 协议的设计启示**

Google A2A (Agent-to-Agent) 协议明确移除了 Kind Discriminator（Appendix A.2.1），采用 OneOf/Union 模式区分消息语义（`StreamResponse` 包含互斥字段 `task?` / `message?` / `statusUpdate?`），并通过 `TaskState` 枚举（`SUBMITTED` → `WORKING` → `COMPLETED/FAILED`）表达状态机。A2A 的启示是：**"用一个字符串字段区分消息种类"本身就是被业界淘汰的模式**。

这让我们重新评估 OpenAgentIO 的长期方案。三种演进选项：

**选项 A（推荐，v0.3 落地）**：`Envelope` 新增 `Phase string` + `FrameType string` 字段

- `Phase` 承载生命周期状态（`submitted` / `working` / `completed` / `failed`，遵循 A2A 状态机思想），回答"这次调用/任务处于什么阶段"。
- `FrameType` 承载通信帧类型（`request` / `response.started` / `response.delta` / `response.final` / `response.error` / `tool.call` / `tool.result`），回答"这条 envelope 是哪类协议帧"。
- `EventType` 回归纯业务语义 / Pub/Sub 路由键（如 `goc.incident.created`、`com.mycompany.order.paid`）。
- 双写过渡期：框架同时设置 `FrameType`、`Phase` 和旧的框架 `EventType`（如 `agent.response.delta`）；读取时新 SDK 优先 `FrameType`，旧 SDK 继续读取 `EventType`。
- 成本可控，为 future 向选项 B/C 演进预留接口。
- 注意：`Phase` 是"状态值"而非"类型标记"（不用 `Phase = "ResponseDelta"`，而用 `Phase = "working"`）；`response.delta` 这类帧语义进入 `FrameType`，避免 `Phase` 变成新的语义过载字段。

示例：

```json
{
  "event_type": "goc.incident.created",
  "phase": "working",
  "frame_type": "response.delta",
  "payload": { "delta": "正在分析..." }
}
```

**选项 B（备选）**：Invoke/Stream 路径引入独立的 `Task` / `TaskStatus` / `TaskUpdate` 结构

- `Envelope` 仅用于 Pub/Sub，`EventType` 回归纯业务语义
- `StreamInvoke` 返回 `iter.Seq2[*TaskUpdate, error]` 而非 `*event.Envelope`
- 彻底消除语义过载，但 Bus 接口 breaking change 面大

**选项 C（远期）**：深度对齐 A2A Task 模型

- 以 `Task` 为核心概念，所有通信围绕任务生命周期
- OneOf 风格的响应包装，状态机驱动
- 与行业标准互通，但几乎等于重写协议，标准漂移风险高

**当前推荐**：v0.3 先落地选项 A（`Phase` + `FrameType` 字段 + 双写过渡），理由：
1. API 已稳定，不宜在 v0.3 做选项 B/C 级别的 breaking change
2. `Phase` 可为后续迁移到 `TaskState` 预留平滑通道，`FrameType` 则保留 streaming 所需的 started/delta/final/error 帧语义
3. A2A 仍是早期标准，彻底对齐存在漂移风险

### 3.3 错误负载

`agent.response.error` 与 `agent.tool.result` 的失败场景统一使用：

```json
{
  "code":      "AGENT_TIMEOUT",
  "message":   "main-agent reply timeout after 30s",
  "retryable": true,
  "cause":     { "type": "context.DeadlineExceeded" }
}
```

错误码采用大写下划线命名空间：`AGENT_*` / `TRANSPORT_*` / `CODEC_*` / `AUTH_*`。

---

## 4. Subject / Routing 规范

### 4.1 命名空间

默认前缀 `acp.v1`，可通过 SDK 选项 `WithSubjectPrefix("...")` 覆盖，避免与业务 NATS 命名冲突：

```
# RPC 入口（queue-group 负载均衡到目标 Agent 多实例）
acp.v1.invoke.{target_agent}

# 事件广播 / 订阅（无 ack 语义）
acp.v1.events.{event_type}

# 多租户隔离（推荐）
acp.v1.{tenant_id}.invoke.{target_agent}
acp.v1.{tenant_id}.events.{event_type}

# 流式响应（NATS 自动生成 inbox）
_INBOX.{nuid}
```

通配规则：`acp.v1.events.agent.response.>` 订阅所有响应类事件。

### 4.2 通信模式与 Subject 关系

| 模式 | 客户端动作 | 服务端动作 |
| --- | --- | --- |
| Publish/Subscribe | `PUB acp.v1.events.{type}` | `SUB acp.v1.events.{type}`（可选 queue group） |
| Request/Reply（一次） | `PUB acp.v1.invoke.{target}` ⇒ `SUB _INBOX.x` | `SUB acp.v1.invoke.{target}`（默认 queue group = target），`PUB _INBOX.x`（一次） |
| StreamInvoke（多次响应） | `PUB acp.v1.invoke.{target}` 携带 `reply_to=_INBOX.x`，`SUB _INBOX.x` | `SUB acp.v1.invoke.{target}`（默认 queue group = target），处理后向 `_INBOX.x` 连续 `PUB` started/delta/.../final |
| Streaming Broadcast | 任意发布者 `PUB acp.v1.events.agent.response.delta` | 所有订阅者收到（用于审计/UI 旁路） |
| **级联代理 (Proxy)** | 主Agent `Invoke("sub-agent", req)`，sub-agent 的响应经主Agent聚合/代理后返回上游 | 主Agent作为路由/编排层，Subject 保持 `invoke` 语义，上下文通过 metadata 自动透传 |

### 4.3 多租户

- 默认采用 subject 段隔离：`acp.v1.{tenant}.*`。
- 高隔离场景使用 NATS Account/JWT 多账号方案（控制平面在 v1.0 提供）。

---

## 5. 通信模式详解

### 5.1 Publish / Subscribe

- 默认无确认（at-most-once）。
- `Subscribe` 默认 fan-out（不带 queue group），多实例同时收到；如需负载均衡，显式使用 `WithQueue("workers")`。
- `HandleInvoke` / `HandleStream` 则默认 queue group 名等于 target，多实例自动负载均衡；如需 fan-out，显式使用 `WithHandleQueue("")`。
- 慢消费者：超过 PendingLimit 时 NATS 会丢弃；SDK 暴露 `Dropped` 回调。

### 5.2 Request / Reply

- SDK 自动分配 `_INBOX.{nuid}` 作为 `reply_to`。
- 单次响应：`Bus.Invoke` 发送请求后等待并返回服务端回复的**第一条**消息。Invoke 本身不做帧类型校验（如跳过 `started` 或强制 `final`）——服务语义一致性（如"该 target 是否只接受 Invoke"）由上层注册中心或部署规范保证，通信层保持最小职责。
- 超时由 ctx.Deadline 控制；超时本身映射为 `AGENT_TIMEOUT` 错误。

### 5.3 Stream Invoke（多消息回复）

```
Caller                     Bus / NATS                    Callee
  |  Invoke(target, payload, stream=true)                  |
  |---------------------------- PUB acp.v1.invoke.target ->|
  |                              reply_to=_INBOX.x         |
  |                                                        |
  | <-- _INBOX.x : started (seq=0) ----------------------- |
  | <-- _INBOX.x : delta   (seq=1) ----------------------- |
  | <-- _INBOX.x : delta   (seq=2) ----------------------- |
  | <-- _INBOX.x : final   (seq=3, is_final=true) -------- |
  |                                                        |
  v                                                        v
```

- 消费者按 `seq` 缓冲与重排，遇 `is_final` 关闭流。
- 超时策略：`StreamTimeout`（整体）+ `IdleTimeout`（两条 delta 间最大间隔）。
- 背压：客户端 reorder buffer 是**有界的 pending map**（不是有界 channel），由两个上限共同保护：
  - `MaxPendingFrames`（默认 256）：pending 中同时缓存的乱序帧上限。当 pending 已满且新到帧不是当前期望的 seq 时，客户端立即以 `BACKPRESSURE_DROP` 终止流；如果新到帧恰好是期望 seq，则接受并借由后续 flush 把 pending 排空。
  - `MaxSequenceGap`（默认 1024）：单帧的 seq 距离期望 seq 的最大跳变，用条件 `seq - expected >= gap` 检查（减法方向保证 uint64 不溢出）。防御攻击者或坏 server 用一个巨大 seq 让 pending 无限等待缺失帧。
  - 触达上限时客户端在流迭代器内抛出 `ErrBackpressureDrop` / `BackpressureDropError`；服务端**不感知**这次终止（Bus v0.3 的 NATS Core 不提供 flow control）。Go/Python 两侧均使用同一套阈值，可通过 `WithMaxPendingFrames` / `WithMaxSequenceGap` 覆盖。
  - JetStream 模式的服务端 flow control 仍为 v0.4+ 预留。

### 5.4 级联代理（Proxy / Orchestrator）

当消息需穿过多层 Agent（如 钉钉网关 → 主Agent → subAgent），每层 Agent 既充当消费者又充当生产者：

```
上游调用方
    │ Invoke("master-agent")
    ▼
主Agent (HandleInvoke)
    │ Bus.Invoke("team-sales", original_req)
    │ Bus.Invoke("team-tech",  original_req)
    ▼
subAgent(s) 处理 → 返回 ResponseFinal
    │
主Agent 聚合结果 → 返回 ResponseFinal 给上游
```

**Subject 规划**：
- 网关 → 主Agent：`acp.v1.invoke.master-agent`
- 主Agent → 销售团队：`acp.v1.invoke.team-sales`
- 主Agent → 技术团队：`acp.v1.invoke.team-tech`
- `HandleInvoke` / `HandleStream` 默认 queue group 名等于 target，多实例自动负载均衡；可通过 `WithHandleQueue("custom-queue")` 覆盖，或 `WithHandleQueue("")` 显式关闭负载均衡（fan-out，仅特殊场景使用）。

**上下文保持**：
- 一级字段（`trace_id`、`conversation_id`、`user_id`、`tenant_id`）在 `new_reply_shell` 中自动复制，全链路不会丢失。
- `metadata` 中非 `acp.*` 键同样自动继承，因此钉钉的 `openConversationId`、`conversationToken` 等自定义上下文在级联链路中无需手动搬运即可透传回上游。

### 5.5 双向流（v0.3 预留）

通过两个 inbox（`reply_to` + `client_inbox`）实现，协议层无需改动，仅在 SDK 增加 API。

---

## 6. Transport 抽象

### 6.1 接口定义（Go）

```go
// pkg/transport/transport.go
type Transport interface {
    // 生命周期
    Connect(ctx context.Context) error
    Close() error
    Capabilities() Capabilities

    // 单条发布
    Publish(ctx context.Context, subject string, msg *RawMessage) error

    // 订阅；queue 为空表示 fan-out
    Subscribe(ctx context.Context, subject, queue string, h Handler) (Subscription, error)

    // 单次 request/reply
    Request(ctx context.Context, subject string, msg *RawMessage) (*RawMessage, error)

    // 多消息响应通道（streaming）
    OpenInbox(ctx context.Context) (Inbox, error)
}

type Capabilities struct {
    Streaming    bool
    Persistence  bool   // JetStream
    QueueGroup   bool
    Headers      bool
}

type Inbox interface {
    Subject() string                // 用作 envelope.reply_to
    Recv(ctx context.Context) (*RawMessage, error)
    Close() error
}
```

`RawMessage` 仅承载字节 + headers，序列化由 Codec 负责，方便后续接入非 NATS Transport。

### 6.2 Transport 快速接入（transportdial）

为降低使用者配置成本，`pkg/transport/dial` 提供基于环境变量的工厂函数，是推荐的快速启动路径：

```go
package transportdial

// Dial creates a Transport from environment variables.
// Env vars:
//   - OPENAGENTIO_TRANSPORT: "nats" (default) or "inmem"
//   - NATS_URL: NATS server URL, default "nats://localhost:4222"
//
// Example:
//   tp, err := transportdial.Dial(ctx, transportdial.WithNATSName("echo-agent"))
func Dial(ctx context.Context, opts ...Option) (transport.Transport, error)
```

- `inmem` 用于单进程零依赖测试或 `cmd/orchestrator` 本地快速运行。
- `nats` 用于真实分布式部署，每个 agent 进程独立创建连接。
- 高级用户（需要 TLS、自定义连接池）应直接构造 `nats.New(...)`。

### 6.3 实现矩阵

| Transport | Pub/Sub | Req/Reply | Streaming | Persistence | 阶段 |
| --- | --- | --- | --- | --- | --- |
| `nats` (Core) | ✅ | ✅ | ✅(Inbox) | ❌ | v0.1 |
| `nats-jetstream` | ✅ | ✅ | ✅ | ✅(at-least-once) | v0.3 |
| `inmem` | ✅ | ✅ | ✅ | ❌ | v0.1（测试 / 单进程 demo） |
| `http` | ✅(SSE) | ✅(REST) | ✅(SSE) | ❌ | v0.2 适配器 |

---

## 7. Session / Trace / Context

### 7.1 Go 侧上下文

```go
// pkg/session/context.go
type ctxKey int
const (
    keyEnvelope ctxKey = iota
    keySession
)

func Inject(ctx context.Context, e *event.Envelope) context.Context
func From(ctx context.Context) *event.Envelope        // 取出当前事件
func Session(ctx context.Context) (string, bool)      // session_id
func Trace(ctx context.Context) (string, bool)        // trace_id
```

中间件链上的 handler 调用下游 `Bus.Invoke` 时，自动从 ctx 透传 `trace_id` / `session_id` / `conversation_id` / `tenant_id` / `user_id`。

**Metadata 级联透传**：`new_reply_shell`（构造响应信封）默认浅拷贝请求信封的 `metadata`，但过滤掉 `acp.*` 前缀键。这保证了：
- 业务自定义上下文（如 `dingtalk.conversation_token`、`channel.source_message_id`）在请求-响应链中自动继承，无需每层 handler 手动搬运。
- 运行时内部状态（如 `acp.retry.attempt`、`acp.dlq.last_error`）不会随响应泄露给上游调用方。
- Handler 仍可显式覆盖 `metadata`；以 handler 显式设置的值为准。

### 7.2 与 OpenTelemetry 集成

- 入站：从 envelope.`traceparent` 重建 SpanContext。
- 出站：`otel.GetTextMapPropagator().Inject` 写回 envelope。
- Span 命名：`acp.invoke {target}` / `acp.subscribe {event_type}`。

---

## 8. Go SDK 设计（核心实现）

### 8.1 模块布局

```
openagentio/
├── go.mod                       # module github.com/<org>/openagentio, go 1.25
├── pkg/
│   ├── event/                   # Envelope / 事件类型常量 / payload 子结构
│   ├── codec/                   # Codec 接口 + JSON 实现（默认）
│   ├── transport/               # Transport 接口
│   │   ├── nats/                # Core NATS 实现
│   │   ├── inmem/               # 内存实现（测试 / 单进程 demo）
│   │   ├── dial/                # 基于环境变量的快速工厂（transportdial）
│   │   └── jetstream/           # v0.3
│   ├── bus/                     # Bus 接口与默认实现（聚合 Transport+Codec+MW）
│   ├── middleware/              # recover / log / trace / retry / metrics / otel
│   ├── session/                 # ctx 透传
│   └── adapter/
│       └── http/                # REST + SSE Gateway
├── cmd/
│   └── openagentio/            # CLI（健康检查、主题嗅探）
├── examples/
│   ├── scene_example/           # Agentic Agent 通信演示（Request-Reply / Streaming / Pub-Sub）
│   ├── echo-agent/
│   ├── streaming-llm/
│   └── dingtalk-gateway/
└── test/                        # 集成测试（含 docker-compose-nats）
```

### 8.2 Bus API（用户视角）

```go
package bus

type Bus interface {
    Publish(ctx context.Context, e *event.Envelope) error
    Subscribe(ctx context.Context, eventType string, h Handler, opts ...SubOption) (Subscription, error)

    Invoke(ctx context.Context, target string, payload any, opts ...InvokeOption) (*event.Envelope, error)
    StreamInvoke(ctx context.Context, target string, payload any, opts ...InvokeOption) (Stream, error)

    HandleInvoke(target string, h InvokeHandler, opts ...HandleOption) error
    HandleStream(target string, h StreamHandler, opts ...HandleOption) error

    Close() error
}

type Handler        func(ctx context.Context, e *event.Envelope) error
type InvokeHandler  func(ctx context.Context, e *event.Envelope) (any, error)
type StreamHandler  func(ctx context.Context, e *event.Envelope, w StreamWriter) error

type StreamWriter interface {
    Started(meta any) error
    Delta(chunk any) error
    Final(result any) error
    Error(err error) error            // 与 Final 互斥
}

type Stream interface {
    // Go 1.23+ iterator：for evt := range stream.Events() { ... }
    Events() iter.Seq2[*event.Envelope, error]
    Close() error
}
```

**`Invoke` / `StreamInvoke` 的 `payload any` 语义说明：**

运行时通过类型断言自适应处理两种输入：

| 传入类型 | 内部行为 | 适用场景 |
|---|---|---|
| `*event.Envelope` | **克隆复用**：保留原信封的 `metadata`、`session_id`、`trace_id`、`tenant_id` 等上下文，仅覆写 `from`/`to`/`reply_to` 等路由字段 | 级联调用（如 MainAgent 将上游请求透传给 SubAgent） |
| 其他 `any`（struct / map / string 等） | **新建信封**：自动调用 `Codec.EncodePayload` 编码为 JSON 放入 `payload` 字段，其他上下文字段为空或由 option/默认配置填充 | 简单调用，无需携带额外上下文 |

这种设计让级联代理无需手动搬运上下文——直接把收到的 `*event.Envelope` 转发给下游即可；同时简单场景仍可传普通对象，降低入门成本。

### 8.3 构造与选项

**推荐方式（transportdial，零配置快速启动）：**

```go
tp, err := transportdial.Dial(ctx, transportdial.WithNATSName("main-agent"))
if err != nil { log.Fatal(err) }

b, err := bus.New(
    bus.WithAgentID("main-agent"),
    bus.WithTransport(tp),
    bus.WithMiddleware(
        middleware.Recover(),
        middleware.Trace(),
        middleware.Logging(slog.Default()),
    ),
)
```

- 设置 `OPENAGENTIO_TRANSPORT=inmem` 可零依赖本地运行；默认 `nats` 连接 `NATS_URL`（默认 `nats://localhost:4222`）。
- 每个 agent 进程**独立创建自己的 transport 连接**，与真实分布式部署行为一致。

**高级方式（手动构造 Transport）：**

```go
b, err := bus.New(
    bus.WithAgentID("main-agent"),
    bus.WithTransport(natsx.New(natsx.URL("nats://localhost:4222"))),
    bus.WithCodec(codec.JSON()),
    bus.WithSubjectPrefix("acp.v1"),    // 默认值，可省略；遗留集群可改为 "agent.v1"
    bus.WithTenant("tenant_default"),
    bus.WithMiddleware(
        middleware.Recover(),
        middleware.OTelTrace(),
        middleware.Logging(slog.Default()),
        middleware.Retry(middleware.RetryPolicy{MaxAttempts: 3}),
    ),
)
```

### 8.4 利用 Go 1.25 的几个点

- **`iter.Seq2`**：`Stream.Events()` 直接返回迭代器，业务侧 `for evt, err := range stream.Events()` 自然消费。
- **`testing/synctest`**：单测中模拟超时 / 重试，避免 wall-clock 抖动。
- **`encoding/json/v2`（实验）**：在 v0.2 评估开启，提升序列化性能与 `omitzero` 行为。
- **容器感知 GOMAXPROCS**：默认行为，已无需手工设置。
- **`log/slog`**：作为 SDK 统一日志门面（不强加 logger 实现）。

### 8.5 中间件模型

```go
type Middleware func(next Handler) Handler

// 责任链：编码后顺序为 outer -> inner -> handler
b.Use(Recover, Logging, Trace, Retry)
```

服务端订阅与客户端 Invoke 各有一条独立链；`Bus.Invoke` 内部把 client middleware 应用在序列化前的 envelope 上。

### 8.6 Codec

```go
type Codec interface {
    Name() string
    EncodeEnvelope(*event.Envelope) ([]byte, error)
    DecodeEnvelope([]byte) (*event.Envelope, error)
    EncodePayload(v any) (json.RawMessage, error)
    DecodePayload(raw json.RawMessage, v any) error
}
```

**v0.1 仅落 JSON 实现**，Codec 接口稳定后保留 protobuf 等扩展点（与 `schema/*.proto` 同源生成 Envelope，`schema_version` 配套升级）。是否启用 protobuf 在 v0.3 阶段基于压测与流式吞吐数据决策；过渡期可通过在 `payload` 内 base64 嵌入二进制实现局部优化。

### 8.7 错误处理

- 所有面向用户的错误均实现 `errors.Is` 友好的哨兵：`ErrTimeout`、`ErrUnavailable`、`ErrCodec`、`ErrNoHandler`。
- Transport 错误包装 `error.Cause`，不丢底层细节。
- Stream 错误通过迭代器第二个返回值传出，调用方决定是否中断。

---

## 9. Bridge SPI 与配置驱动集成

> **状态：v0.3 Developer Preview，Python SDK 已实现**。Bridge 是连接 OpenAgentIO Bus 与外部 Agent 框架 / 协议的适配层。本章定义 Bridge SPI 的正式契约，供新增 Bridge 实现时遵循。

### 9.1 定位与范围

Bridge 是 Bus 的**客户端**，而不是 Bus 的所有者：

* 它接收一个已经 ``await bus.connect()`` 过的 ``Bus`` 实例；
* 在 ``start()`` 中通过 ``bus.handle_invoke`` / ``bus.handle_stream`` / ``bus.subscribe`` 注册 handler；
* 在 ``stop()`` 中撤销这些 handler 并释放外部资源；
* **不得**调用 ``bus.close()``。

Bridge 让外部系统（MCP server、Matrix homeserver、OpenAI-compatible SSE gateway 等）以"配置驱动"方式接入 Bus，而无需修改 Bus 核心代码。

### 9.2 Bridge 类型

按数据流向分为两类：

| 类型 | 数据流向 | 注册方式 | 代表实现 |
|---|---|---|---|
| Handler 型 | 外部请求 → Bus → 外部系统 → 响应回 Bus | ``handle_invoke`` / ``handle_stream`` | ``McpToolBridge``、``OpenClawChatSSEBridge``、``QwenPawChatSSEBridge`` |
| 主动 Event Source 型 | 外部系统主动产生事件 → Bus；Bus 也可 outbound → 外部系统 | ``subscribe`` + 后台 task | ``MatrixEventBridge`` |

Bridge SPI 基类只要求 ``start()`` / ``stop()``；健康状态、重连策略、速率限制等属于具体 Bridge 的扩展。

### 9.3 ``Bridge.start()`` / ``Bridge.stop()`` 语义契约

#### ``start()``

* 负责连接外部系统并在 Bus 上注册 handler。
* 可以抛出异常；抛出时 ``BridgeRunner`` 仍会调用该 bridge 的 ``stop()`` 做 best-effort 清理。
* 不应假设 ``stop()`` 已经被调用过。

#### ``stop()``

* 必须**幂等**：多次调用不抛异常。
* 必须**安全**：在 ``start()`` 完全未调用、或 ``start()`` 执行到一半失败时也能安全运行。
* 必须**完整**：撤销所有在 ``start()`` 中注册的 Bus handler / subscription。
* 必须**收尾**：关闭所有外部资源（HTTP client、subprocess、background task、session 等）。
* 不得调用 ``bus.close()``。

### 9.4 ``BridgeRunner`` 职责边界

``BridgeRunner`` 是一个编排器，职责有限：

* 接收已连接的 ``Bus``、已解析的 ``BridgeConfig``、以及 ``type -> BridgeFactory`` 映射；
* 按配置顺序调用每个 bridge 的 ``start()``；
* **在 ``start()`` 之前**把 bridge 加入内部列表，确保 partial-start 也能调用 ``stop()``；
* ``stop()`` 时按**反向顺序**停止每个 bridge，每个 ``stop()`` 受 ``stop_timeout`` 限制；
* ``stop()`` 期间单个 bridge 抛出的异常被记录并吞掉；``CancelledError`` 在全部 bridge 停止后重新抛出；
* ``start()`` 失败时按反向顺序 rollback；rollback 期间若某个 bridge 的 ``stop()`` 抛出 ``CancelledError``，该取消异常被抑制，始终向上抛出**原始的启动异常**；
* 不内置 factory registry，调用方显式传入 factory 映射。

生命周期时序：

```text
bus = Bus(...)
await bus.connect()
runner = BridgeRunner(bus, config, factories)
await runner.start()
# ... runtime ...
await runner.stop()
await bus.close()
```

### 9.5 外部资源所有权

| 资源 | 拥有者 | 关闭时机 |
|---|---|---|
| ``Bus`` | 调用方 | ``bus.close()`` |
| Bridge 注册的 ``Subscription`` | Bridge | ``Bridge.stop()`` |
| HTTP client (httpx) | Bridge | ``Bridge.stop()`` |
| MCP ``ClientSession`` / subprocess | ``McpToolBridge`` | ``Bridge.stop()`` |
| Matrix sync loop task | ``MatrixEventBridge`` | ``Bridge.stop()`` |

### 9.6 Bus Handler 注册与撤销

Bridge 必须在 ``stop()`` 中显式 unsubscribe 自己注册的 handler。``Bus.close()`` 只是安全网：runner 可能在 ``bus.close()`` 之前先 ``runner.stop()``，此时如果 bridge 没有主动撤销，handler 会继续接收消息直到 Bus 关闭。

### 9.7 配置模式与版本

Bridge 配置使用 YAML/JSON 文档：

```yaml
version: "openagentio.bridge/v1"
bridges:
  - name: "openclaw.wechat"
    type: "openclaw_chat_sse"
    config:
      base_url: "https://gateway.example/v1"
      token: "${OPENCLAW_GATEWAY_TOKEN}"
    mappings:
      text_field: "text"
      session_field: "x-openclaw-session-key"
      metadata_prefix: "openclaw."
```

* ``version`` 当前严格为 ``openagentio.bridge/v1``；未知版本直接拒绝。
* ``name`` 在同一个配置文档内必须唯一。
* ``type`` 对应传入 ``BridgeRunner`` 的 factory 映射。
* ``config`` 是 bridge 类型特定的键值映射。
* ``mappings`` 提供字段映射提示；未知键保留到 ``extra``，允许未来 Bridge 类型扩展模式而不破坏旧解析器。

### 9.8 敏感配置与环境变量解析

``config`` 中的字符串值（包括嵌套 mapping、list、tuple 中的字符串）可以包含环境变量占位符：

* ``${VAR}`` — 从环境变量读取。
* ``${VAR:-default}`` — 环境变量缺失时使用 ``default``（``default`` 可以为空字符串）。

解析是**可选的**（opt-in），通过 ``BridgeDefinition.resolve_env()`` / ``BridgeConfig.resolve_env()`` 触发；``from_dict()`` / ``from_file()`` 不自动解析，以避免与现有 Bridge 的本地解析冲突，并保留原始值可审计。缺失且无默认值的变量抛出 ``BridgeConfigError``。

### 9.9 Factory Registry 扩展

当前使用显式 ``BUILTIN_FACTORIES: dict[str, BridgeFactory]``，由调用方合并自定义 factories 后传入 ``BridgeRunner``：

```python
from openagentio.bridge import BridgeRunner, BUILTIN_FACTORIES

factories = {**BUILTIN_FACTORIES, "my_bridge": my_bridge_factory}
runner = BridgeRunner(bus, config, factories)
```

全局 ``register_bridge()`` 注册表按计划推迟到 v0.4+，避免在 SPI 尚未稳定前引入全局状态。

### 9.10 错误映射约定

Bridge 应把外部错误映射为 ``openagentio.bus.errors.BusError`` 子类（如 ``AgentUnavailableError``、``AuthFailureError``、``InvalidRequestError``），使上游 middleware（Retry / DeadLetter）能正确决策。无法映射的异常可以向上传播，由 runner 或调用方记录。

---

## 10. Python SDK 设计概要

> **状态：v0.2.0a2 alpha，已实现**。Python SDK 位于 `sdk/python/`，以 Python 3.11+ / asyncio 为运行时基线，协议层与 Go SDK 共享 Envelope、Subject 规范和黄金样本。

### 10.1 模块布局

```
sdk/python/
├── pyproject.toml              # package openagentio, Python >=3.11
├── src/openagentio/
│   ├── event/                  # Envelope dataclass / event constants / payload shapes
│   ├── codec/                  # JSONCodec，Envelope wire format 与 Go 对齐
│   ├── transport/              # Transport Protocol + InMemoryDriver + NATSDriver + dial
│   ├── bus/                    # Bus / Stream / StreamWriter / subject layout / DLQ errors
│   ├── middleware/             # Recover / Logging / Trace / Retry / DeadLetter / OTel bridge
│   ├── adapter/http/           # Starlette ASGI HTTP/SSE Adapter
│   └── session.py              # ContextVar 会话与 trace 上下文
├── tests/                      # pytest + pytest-asyncio
└── examples/
    └── echo_agent.py
```

### 10.2 Python Bus API（用户视角）

Python SDK 采用 async-first 设计，方法名使用 Python snake_case，但语义与 Go SDK 对齐：

```python
from openagentio import (
    Bus,
    InMemoryDriver,
    WithAgentID,
    WithTransport,
    WithMiddleware,
    Recover,
    Trace,
    Logging,
)

bus = Bus.new(
    WithAgentID("echo"),
    WithTransport(InMemoryDriver()),
    WithMiddleware(Recover(), Trace(), Logging()),
)
await bus.connect()

async def echo(env):
    return env.payload_json()

await bus.handle_invoke("echo", echo)
resp = await bus.invoke("echo", {"msg": "hello"})

await bus.close()
```

核心 API：

| Go | Python | 说明 |
| --- | --- | --- |
| `Publish` | `publish` | 发布 `Envelope` 到 `acp.v1.events.{event_type}` |
| `Subscribe` | `subscribe` | 订阅事件，支持 `WithQueue` |
| `Invoke` | `invoke` | 单次请求 / 响应，返回第一条响应 Envelope |
| `StreamInvoke` | `stream_invoke` | 返回 `Stream`，通过 async iterator 消费多条响应 |
| `HandleInvoke` | `handle_invoke` | 注册目标 Agent 的一次响应 handler，默认 queue group = target |
| `HandleStream` | `handle_stream` | 注册流式 handler，使用 `StreamWriter.started/delta/final/error` 输出 |
| `Close` | `close` | 取消订阅、取消 handler task、关闭 transport |

### 10.3 协议兼容约束

- `Envelope` 使用 dataclass 表达，JSON key 与 Go wire format 一致；Python 属性 `from_` 映射到 JSON `"from"`。
- `payload` 保持为 JSON bytes，等价于 Go `json.RawMessage`，避免二次编码。
- `event_id` 默认 UUIDv7（RFC 9562），运行环境不支持时回退 UUIDv4。
- `spec_version = "acp/1.0"`，`schema_version = 1`。
- `codec.JSONCodec` 负责 Envelope / payload 编解码；`tests/test_envelope.py` 使用 `schema/samples/` 黄金样本验证跨语言兼容。
- Subject 布局与 Go 一致：`acp.v1.events.{event_type}`、`acp.v1.invoke.{target}`，可选 tenant 段。

### 10.4 Transport 与快速接入

Python transport 是 `typing.Protocol`，使用 coroutine 表达生命周期和 IO：

```python
class Transport(Protocol):
    async def connect(self) -> None: ...
    async def close(self) -> None: ...
    def capabilities(self) -> Capabilities: ...
    async def publish(self, msg: RawMessage) -> None: ...
    async def subscribe(self, subject: str, queue: str, handler: TransportHandler) -> Subscription: ...
    async def request(self, msg: RawMessage, timeout: float | None = None) -> RawMessage: ...
    async def open_inbox(self) -> Inbox: ...
```

实现矩阵：

| Driver | Pub/Sub | Req/Reply | Streaming | Persistence | 依赖 |
| --- | --- | --- | --- | --- | --- |
| `InMemoryDriver` | ✅ | ✅ | ✅ | ❌ | 标准库 asyncio |
| `NATSDriver` | ✅ | ✅ | ✅ | ❌ | `nats-py>=2.6` |
| JetStream | ⏭ v0.3 | ⏭ v0.3 | ⏭ v0.3 | ✅ | 待设计 |

快速工厂 `openagentio.dial()` 与 Go `transportdial.Dial` 对齐：

- `OPENAGENTIO_TRANSPORT=inmem`：返回并连接 `InMemoryDriver`。
- `OPENAGENTIO_TRANSPORT=nats` 或空：连接 `NATS_URL`，默认 `nats://localhost:4222`。
- `WithNATSName(name)`：设置 NATS connection name。

### 10.5 中间件、Session 与 OTel

- `WithMiddleware(...)` 在 `subscribe` / `handle_invoke` / `handle_stream` 的入站 dispatch 路径上应用责任链。
- 已实现 `Recover`、`Logging`、`Trace`、`Retry`、`DeadLetter`，错误码与 Go 的 `ErrorPayload` 语义对齐。
- `session.py` 使用 `ContextVar` 保存当前 Envelope / session / trace 上下文，适配 asyncio task 隔离。
- OTel bridge 为可选依赖：安装 `openagentio[otel]` 后可使用 `OTelTrace` 与 `OTelEnvelopePreparer` 注入 / 提取 `traceparent`。

### 10.6 HTTP / SSE Adapter

Python HTTP adapter 是可选 Starlette ASGI 应用，安装 `openagentio[http]` 后启用：

```python
from openagentio import HTTPNew, WithHTTPTimeout

adapter = HTTPNew(bus, WithHTTPTimeout(30.0))
app = adapter.app
```

路由与 Go adapter 对齐：

| Route | 语义 |
| --- | --- |
| `POST /v1/agents/{target}/invoke` | HTTP JSON 请求转 `Bus.invoke` |
| `POST /v1/agents/{target}/stream` | HTTP 请求转 `Bus.stream_invoke` |
| `POST /v1/events/{event_type}` | HTTP JSON 请求转 `Bus.publish` |

支持 BearerAuth、自定义 AuthFunc、ASGI middleware、Recover / Logging HTTP middleware、timeout / idle timeout / SSE retry 配置。

---

## 11. 生态集成与协议互操作 (Ecosystem & Interoperability)

OpenAgentIO 的核心愿景是成为 Agent 界的 "Service Mesh"，通过协议适配和框架插件实现异构 Agent 的互联互通。

### 11.1 协议适配器 (Protocol Adapters)

通过在 `Gateway` 层提供标准协议转换，使得现有的端侧入口可以无缝接入 OpenAgentIO 基座。

- **OpenAI V1 Adapter**：
  - 对外暴露 `/v1/chat/completions`。
  - 将 OpenAI 的 `messages` 包装为 Envelope Payload。
  - 将 `stream=true` 映射为 `Bus.StreamInvoke`，实现 Token 级的 SSE 转发。
  - **价值**：IDE 插件（Cursor/Continue）、Web UI 等无需修改即可调用后端的分布式 Agent 集群。
- **Anthropic Adapter**：
  - 对外暴露 Anthropic Messages API 格式。
  - 适配 Anthropic 的 Tool Use 语义到 OpenAgentIO 的 `ToolCall` 规范。

### 11.2 框架插件 (Framework Plugins)

针对主流 Agent 编排框架提供深度集成，打破其“单体运行”或“闭环通信”的局限。

- **LangGraph Plugin**：
  - 提供 `OpenAgentIONode`，使 Graph 中的节点可以通过 Bus 远程调用其他 Agent。
  - 映射 LangGraph 的 `thread_id` 到 OpenAgentIO 的 `session_id`。
- **AgentScope Plugin**：
  - 为 AgentScope 提供 `OpenAgentIOService`。
  - 支持 AgentScope 的多 Agent 协同消息通过总线进行分布式路由。

### 11.3 价值定位：Agent Service Mesh

通过基础设施层的统一通信，解决以下核心问题：
- **异构寻址**：无论 Agent 是用 Python (LangGraph) 还是 Go 编写，通过逻辑名称（Target）统一寻址。
- **状态同步**：跨框架透传 Session 与 Context，确保对话历史不丢失。
- **全链路追踪**：利用 OTel 实现从 OpenAI 协议入口到最末端 Tool Agent 的全链路 Trace。

---

## 12. HTTP / SSE Adapter（v0.2）

### 12.1 入站（外部 → Bus）

```
POST /v1/agents/{target}/invoke           # 一次响应
GET  /v1/agents/{target}/stream (SSE)     # 流式响应
POST /v1/events/{event_type}              # 发布事件
```

- 鉴权：Bearer Token / mTLS / 自定义中间件。
- Body → Envelope.payload；Header → metadata（`X-Trace-Id`、`X-Tenant-Id` 等）。
- SSE 格式：每个 NATS message 序列化为 `data: <envelope-json>\n\n`，`event: <event_type>`。

### 12.2 出站（Bus → 外部 webhook，v0.3+）

通过 Subscribe + HTTP Forwarder，把指定 subject 的事件 POST 到外部 URL；带签名与重试（DLQ 走 JetStream）。

---

## 13. 可观测性

| 维度 | 方案 |
| --- | --- |
| Trace | OpenTelemetry，使用 `traceparent` 字段；每个 Bus 操作都是 Span。 |
| Metrics | Prometheus exposition：`acp_publish_total`、`acp_invoke_duration_seconds`、`acp_stream_inflight`、`acp_dropped_total`。 |
| Log | `slog` 结构化输出；中间件自动附 `event_id`/`trace_id`/`session_id`。 |
| Audit | 可选订阅 `acp.v1.events.>`，写入审计存储。 |

---

## 14. 安全性

- **传输**：NATS TLS + JWT/NKEY 鉴权；HTTP 走 TLS + Bearer/mTLS。
- **租户隔离**：subject 段隔离 + NATS Account（v1.0 控制平面分发凭据）。
- **PII**：metadata/payload 不强约束，但 SDK 提供 `Redactor` 中间件，按 key 脱敏后再写日志。
- **Replay 攻击**：Envelope 中 `event_id` + `occurred_at`，Adapter 层做去重窗口。

---

## 15. 测试策略

- **单元**：`pkg/event`、`pkg/codec`、`pkg/middleware` 全覆盖。
- **集成**：使用 `transport/inmem` 跑端到端逻辑；NATS 路径用 `nats-server` 嵌入式或 testcontainers。
- **协议**：维护 `testdata/envelopes/*.json` 黄金样本，Go/Python 两端各自校验，保证跨语言一致。
- **流式**：`testing/synctest` 控制时间，验证超时、idle、乱序到达重排。
- **基准**：`go test -bench` 跑 publish / invoke / streaming 吞吐基线。

---

## 16. Roadmap 细化

### 16.0 实现进度快照（截至 2026-06-07）

> 详细代码导读见 `prompts/codex_0.1_report.md`。下方 `[x]` = 完成，`[~]` = 部分完成，`[ ]` = 未启动。

| 模块 | Go | Python | 备注 |
| --- | --- | --- | --- |
| Envelope / Codec / Schema | [x] | [x] | Go `pkg/event` + `pkg/codec`；Python `openagentio.event` / `openagentio.codec`，共享 `schema/samples/` 黄金样本 |
| Transport (NATS Core + InMem + Dial) | [x] | [x] | Go：`pkg/transport/{nats,inmem,dial}`；Python：`NATSDriver` / `InMemoryDriver` / `dial()` |
| Bus 核心 API | [x] | [x] | 两端均覆盖 Pub/Sub/Invoke/StreamInvoke/HandleInvoke/HandleStream |
| 基础中间件 (Recover/Logging/Trace) | [x] | [x] | Python 额外包含 Retry / DeadLetter / OTel bridge alpha |
| Session ctx 透传 | [x] | [x] | Go `context.Context`；Python `ContextVar` |
| Stream 双超时 (overall + idle) | [x] | [x] | 两端均支持 overall timeout 与 idle timeout |
| 黄金样本 + 跨语言校验 | [x] | [x] | Go `pkg/event/golden_test.go`；Python `tests/test_envelope.py` |
| Scene Example（Request-Reply / Streaming / Pub-Sub） | [x] | [ ] | Go `examples/scene_example/`：单进程 orchestrator + 分布式多进程双模式 |
| Echo example | [x] | [x] | Go `examples/echo-agent/main.go`；Python `sdk/python/examples/echo_agent.py` |
| streaming-llm example | [ ] | [ ] | 待补 |
| HTTP/SSE Adapter | [x] | [x] | Go `pkg/adapter/http`；Python `openagentio.adapter.http`（Starlette ASGI，可选依赖） |
| NATS 集成测试 | [x] | [x] | Go 依赖外部 `nats-server`；Python 通过 `AFB_NATS_URL` 门控 |
| EventType 构造器分化 + 运行时检查 | [x] | [~] | Go 已落地 `event.NewEvent` / `event.NewRequest` + warn log；Python 当前保留 `Envelope.new(event_type)` 与请求自动构造，构造器分化待补 |

### v0.1 MVP（4–6 周）
- [x] Envelope v1 + JSON Codec
- [x] Transport 接口 + NATS Core 实现 + InMem 实现
- [x] Bus：Publish / Subscribe / Invoke / StreamInvoke / HandleStream
- [x] 基础中间件：Recover / Logging / Trace（无 OTel SDK 依赖也可用） — *Go / Python 均已落地*
- [x] Go SDK 文档 + 2 个 example（echo ✓ / streaming-llm ✓）
- [x] Python SDK 最小集（实际已超额至 Bus 全 API + NATS/InMem + middleware alpha）
- [x] 黄金样本 + 跨语言协议校验

### v0.2（4 周）
- [x] HTTP/SSE Adapter（Go，`pkg/adapter/http` + `examples/http-gateway`；§12.2 出站 Webhook 顺延 v0.3）
- [x] Session / Trace 中间件 + OTel 桥接（Go：`pkg/middleware/otel` 子包，opt-in OTel 依赖；W3C `tracestate` 顺延 v0.3）
- [x] Retry / DeadLetter 中间件接口（实现以 NATS Core 为基础）
- [x] transportdial 快速接入（`pkg/transport/dial`，环境变量自动切换 inmem/nats）
- [x] EventType 构造器分化（`event.NewEvent` / `event.NewRequest`）+ 运行时契约检查（Go 已落地）
- [x] Scene Example 完整演示（Request-Reply / Streaming / Pub-Sub，单进程 + 分布式多进程双模式）
- [x] **Python SDK v0.2.0a2 alpha**：Bus 全 API、NATS/InMem、middleware、HTTP/SSE adapter、pytest 覆盖

### v0.3（4–6 周）
- [ ] EventType 解耦：Envelope 新增 `phase` / `frame_type`，Go/Python 双写过渡，SDK 读取优先 `frame_type`
- [ ] JetStream Transport（持久化、Replay、Pull Consumer）
- [ ] Tool 事件启用 + Task 事件启用
- [ ] 出站 Webhook Forwarder
- [ ] Prometheus / Grafana Dashboard 模板
- [ ] Python SDK 稳定化：构造器分化（`new_event` / `new_request`）、API freeze、发布包流程
- [ ] **OpenAI V1 Protocol Adapter**：支持标准 Chat Completions 接口调用后端 Agent

### v0.4（4–8 周）
- [ ] **LangGraph / AgentScope 接入插件**：支持异构框架节点挂载到总线
- [ ] **Anthropic Protocol Adapter**：支持 Anthropic 风格的消息接口
- [ ] **高级编排场景演示**：跨框架（Go + Python LangGraph）的分布式协作 Demo

### v1.0
- [ ] Control Plane（Agent Registry、凭据下发、限流策略）
- [ ] Web Dashboard（topo、流量、追踪查询）
- [ ] 多租户账户管理（NATS Account 集成）

---

## 17. 关键决策记录（ADR 摘要）

| 编号 | 决策 | 取舍 |
| --- | --- | --- |
| ADR-001 | Codec：v0.1 仅 JSON，接口可插拔 | 调试体验 + 跨语言成本最低；性能瓶颈出现后再切 protobuf，迁移由 `WithCodec` 一行完成 |
| ADR-002 | Streaming 用 NATS Inbox 而非 JetStream | v0.1 轻量化；持久化场景在 v0.3 升级 |
| ADR-003 | Subject 前缀默认 `acp.v1`，可配置 | SDK 暴露 `WithSubjectPrefix`，与业务命名解耦，又能兼容遗留 `agent.*` 集群 |
| ADR-004 | Go 1.25 作为运行时基线 | 利用 `iter.Seq2`、`testing/synctest`、`slog` 等现代特性 |
| ADR-005 | Transport 接口承载字节 + headers | 序列化由 Codec 负责，Transport 可替换 |
| ADR-006 | Envelope 内嵌 `traceparent` | 与 OTel/W3C 兼容，不绑定具体 tracing 库 |
| ADR-007 | Reliability 在 v0.3 才落地 | MVP 聚焦最小可用；接口在中间件层提前预留 |
| ADR-008 | `event_id` 使用 UUIDv7 | RFC 9562 标准、时间有序、与各语言/数据库 UUID 类型原生兼容；放弃 ULID 的 10 字符长度优势换取标准化与生态 |
| ADR-009 | `metadata` 响应时默认继承（过滤 `acp.*`） | 业务自定义上下文（如钉钉字段）在级联链路中自动透传，避免每层 handler 手动搬运；`acp.*` 前缀保留给运行时内部状态（retry、dlq），防止泄露给上游调用方。以 handler 显式覆盖值为准。 |
| ADR-012 | Python Bridge SPI 正式契约 | 明确 Bridge 生命周期、Bus 所有权边界、handler 撤销、配置版本与 env 解析规则，降低新增 Bridge 的实现不一致风险。 |
| ADR-010 | EventType 构造器分化 + 运行时契约检查（v0.2 已落地）；长期 `Phase` + `FrameType` 字段引入（v0.3 评估） | 短期：`event.NewEvent()` / `event.NewRequest()` + `Invoke`/`StreamInvoke` 内部 warn log，已落地。长期：新增 `Phase` 承载生命周期状态（`submitted`/`working`/`completed`/`failed`），新增 `FrameType` 承载协议帧类型（`response.started`/`response.delta`/`response.final`/`response.error`），EventType 回归纯业务语义；参考 A2A 协议状态机设计，双写过渡期保障兼容。**详细分析见 [`prompts/a2a_prot.md`](./a2a_prot.md)**。 |
| ADR-011 | transportdial 作为默认接入方式 | 通过环境变量（`OPENAGENTIO_TRANSPORT`、`NATS_URL`）自动选择 inmem/nats，降低入门成本；每个 agent 进程独立 `Dial`，与真实分布式部署行为一致；高级场景保留手动构造 `nats.New` 的扩展点。 |

---

## 18. 风险与开放问题

1. **NATS Core 流式无持久化**：若 Agent 在流中崩溃，客户端只会收到 idle timeout，需要在文档中明确"v0.1 不保证完整性"。
2. **Envelope 体积**：当前字段较多，纯 ASCII 也有 ~500B；高 QPS 场景需要评估 protobuf 切换成本。
3. **EventType 语义债务**：短期（v0.2）已通过构造器分化和运行时检查缓解。长期方案确定为 v0.3 引入 `Phase` + `FrameType`：前者承载生命周期状态（参考 A2A 状态机设计），后者承载 started/delta/final/error 等通信帧类型，EventType 回归纯业务语义；双写过渡期保障向后兼容。需维护好文档和示例，避免用户建立错误心智模型。
4. **跨语言 Schema 漂移**：Python SDK 已跟进后，需持续以 `schema/samples/` 黄金样本和双端测试约束 wire format；任何 Envelope / Subject 变更都必须同时更新 Go、Python、样本和设计文档。
5. **多租户隔离强度**：subject 前缀方案依赖客户端守规矩，真正强隔离需要 NATS Account（v1.0）。

---

## 19. 附录：示例代码

### 19.1 Go：Echo Agent

```go
func main() {
    ctx := context.Background()
    tp, err := transportdial.Dial(ctx, transportdial.WithNATSName("echo"))
    if err != nil { log.Fatal(err) }

    b, err := bus.New(
        bus.WithAgentID("echo"),
        bus.WithTransport(tp),
    )
    if err != nil { log.Fatal(err) }
    defer b.Close()

    b.HandleInvoke("echo", func(ctx context.Context, e *event.Envelope) (any, error) {
        return e.Payload, nil
    })

    <-ctx.Done()
}
```

### 19.2 Go：流式 LLM Agent

```go
b.HandleStream("main-agent", func(ctx context.Context, e *event.Envelope, w bus.StreamWriter) error {
    if err := w.Started(map[string]string{"model": "qwen-max"}); err != nil {
        return err
    }
    for tok := range llm.Stream(ctx, e.Payload) {
        if err := w.Delta(map[string]string{"delta": tok}); err != nil {
            return err
        }
    }
    return w.Final(map[string]any{"finished": true})
})
```

### 19.3 Go：Pub/Sub 发布事件

```go
// SubAgent (GOC) 主动发布业务事件
incident := GOCIncidentPayload{...}
payload, _ := json.Marshal(incident)

env := event.NewEvent("goc.incident.created")
env.SessionID = incident.IncidentID
env.TraceID = incident.IncidentID
env.Metadata = map[string]any{
    "source_system":               "goc",
    "dingtalk.conversation_token": "token_xyz789",
}
env.Payload = payload

if err := b.Publish(ctx, env); err != nil {
    log.Fatal(err)
}
```

### 19.4 Go：消费流（StreamInvoke 客户端）

```go
req := event.NewRequest()
req.Payload = []byte(`{"text": "你好"}`)

stream, err := b.StreamInvoke(ctx, "main-agent", req)
if err != nil { return err }
defer stream.Close()

for evt, err := range stream.Events() {
    if err != nil { return err }
    switch evt.EventType {
    case event.ResponseDelta:
        fmt.Print(evt.Payload["delta"])
    case event.ResponseFinal:
        fmt.Println("\n[done]")
    }
}
```

---

> 本文档为 Draft，待评审后冻结 v1，作为后续 v0.1 ~ v1.0 实现的契约。
