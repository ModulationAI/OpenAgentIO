# ADR-012: Python Bridge SPI 正式契约

## 状态

已接受（Accepted），2026-07-23。

## 上下文

Python SDK 在 v0.3 引入了 Bridge 层，用于把外部 Agent 框架 / 协议接入 OpenAgentIO Bus。目前 `Bridge`、`BridgeRunner`、`BridgeConfig` 已实现，但行为散落在代码注释里，没有形成正式契约。随着 MCP Tool Bridge、Matrix Event Bridge、OpenClaw/QwenPaw Chat SSE Bridge 等实现增多，需要把边界、所有权、失败处理写成明确规则，方便新增 Bridge 时有一致可遵循的规范。

## 决策

### 1. Bridge 是 Bus 客户端，不拥有 Bus 生命周期

`Bridge` 通过构造函数接收一个**已经 connected** 的 `Bus`，在 `start()` 中注册 handler，在 `stop()` 中撤销 handler 并释放外部资源。它**不得**调用 `bus.close()`。

正确的生命周期顺序：

```text
bus = Bus(...)
await bus.connect()
runner = BridgeRunner(bus, config, factories)
await runner.start()
...
await runner.stop()
await bus.close()
```

### 2. `start()` / `stop()` 的语义契约

* `start()` 可以抛出异常；抛出时，`BridgeRunner` 仍会调用该 bridge 的 `stop()` 做 best-effort 清理。
* `stop()` 必须满足：
  * 幂等：多次调用不抛异常；
  * 安全：在 `start()` 完全未调用、或 `start()` 执行到一半失败时也能安全运行；
  * 完整：撤销所有在 `start()` 中注册的 Bus handler/subscription；
  * 收尾：关闭所有外部资源（HTTP client、subprocess、background task 等）。

### 3. Bridge 分类

| 类型 | 职责 | 代表实现 |
|---|---|---|
| Handler 型 | 把外部调用映射为 Bus 的 `handle_invoke` / `handle_stream` 目标 | `McpToolBridge`, `OpenClawChatSSEBridge`, `QwenPawChatSSEBridge` |
| 主动 Event Source 型 | 主动监听外部系统并通过 `bus.publish()` 向 Bus 推送事件；可能同时订阅 outbound 事件 | `MatrixEventBridge` |

Bridge SPI 基类只要求 `start()` / `stop()`；健康状态、重连策略等属于具体 Bridge 的扩展。

### 4. 外部资源所有权

| 资源 | 拥有者 | 关闭时机 |
|---|---|---|
| `Bus` | 调用方 / Runner 的使用者 | `bus.close()` |
| Bridge 注册的 `Subscription` | Bridge 自身 | `Bridge.stop()` 中 unsubscribe |
| HTTP client (httpx) | Bridge | `Bridge.stop()` |
| MCP `ClientSession` / subprocess | `McpToolBridge` | `Bridge.stop()` |
| Matrix sync loop task | `MatrixEventBridge` | `Bridge.stop()` |

### 5. `BridgeRunner` 职责边界

* 接收已连接的 Bus、已解析的 `BridgeConfig`、以及 `type -> BridgeFactory` 映射。
* 按配置顺序 `start()` 每个 bridge；在 `start()` 前先把 bridge 加入内部列表，确保 partial-start 也能 `stop()`。
* `stop()` 时按**反向顺序**停止每个 bridge，每个 `stop()` 受 `stop_timeout` 限制。
* `stop()` 期间单个 bridge 抛出的异常被记录并吞掉；`CancelledError` 在全部 bridge 停止后重新抛出。
* `start()` 失败 rollback 时，若某个 bridge 的 `stop()` 抛出 `CancelledError`，该取消异常被抑制，始终向上抛出原始的启动异常。
* 不内置 factory registry；调用方显式传入 `BUILTIN_FACTORIES` 或自定义映射。

### 6. 配置版本与兼容性

`BridgeConfig` 严格接受 `openagentio.bridge/v1`。遇到未知版本直接拒绝，直到 v2 被正式设计。`mappings` 中的未知键保留到 `extra`，允许新 Bridge 类型扩展模式而不破坏旧解析器。

### 7. 敏感配置 / 环境变量解析

`config` 中的字符串值可以包含 `${VAR}` 或 `${VAR:-default}` 占位符。解析是**可选的**（opt-in），通过 `BridgeDefinition.resolve_env()` / `BridgeConfig.resolve_env()` 触发；`from_dict()` / `from_file()` 不自动解析，以避免与现有 Bridge 的本地解析冲突，并保留原始值可审计。

缺失且无默认值的变量抛出 `BridgeConfigError`。

### 8. Factory Registry 扩展

当前使用显式 `BUILTIN_FACTORIES: dict[str, BridgeFactory]`，由调用方合并自定义 factories 后传入 `BridgeRunner`。全局 `register_bridge()` 注册表按计划推迟到 v0.4+，避免在 SPI 尚未稳定前引入全局状态。

## 后果

* 新增 Bridge 实现时有明确契约可依，降低生命周期 bug（如 handler 未撤销、stop 不安全）。
* `BridgeRunner` 的 `stop_timeout` 变为可配置，但默认行为不变。
* `BridgeConfig` 提供集中式 env 解析，但现有 Bridge 可以逐步迁移，不强制一次性重构。
* 需要更新 `prompts/design.md` 增加 Bridge SPI 专章，并在 ADR 摘要表中引用本文。

## 相关文件

* `sdk/python/src/openagentio/bridge/base.py`
* `sdk/python/src/openagentio/bridge/runner.py`
* `sdk/python/src/openagentio/bridge/config.py`
* `sdk/python/src/openagentio/bridge/__init__.py`
* `sdk/python/tests/test_bridge_runner.py`
* `sdk/python/tests/test_bridge_config.py`
* `prompts/design.md`
