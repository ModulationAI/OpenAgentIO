"""OpenAgentIO bridge subpackage.

Bridges connect the ACP/Envelope bus to external agent frameworks and
protocols (HTTP/SSE gateways, OpenAPI services, custom systems). Built-in
bridge types currently include the OpenClaw Chat SSE bridge and the
QwenPaw Chat SSE bridge.

Public surface is intentionally narrow: importing this module does **not**
add new symbols to top-level ``openagentio`` — callers explicitly import
from ``openagentio.bridge``.
"""

from openagentio.bridge.base import Bridge, BridgeFactory
from openagentio.bridge.config import (
    SUPPORTED_VERSION,
    BridgeConfig,
    BridgeConfigError,
    BridgeDefinition,
    BridgeMappings,
)
from openagentio.bridge.health import BridgeHealth, BridgeHealthSnapshot
from openagentio.bridge.matrix_event import MatrixEventBridge, matrix_event_factory
from openagentio.bridge.mcp_tool import McpToolBridge, mcp_tool_factory
from openagentio.bridge.openclaw_chat_sse import (
    OpenClawChatBridge,
    OpenClawChatSSEBridge,
    openclaw_chat_sse_factory,
)
from openagentio.bridge.qwenpaw_chat_sse import (
    QwenPawChatBridge,
    QwenPawChatSSEBridge,
    qwenpaw_chat_sse_factory,
)
from openagentio.bridge.runner import BridgeRunner, RunnerHealthSnapshot
from openagentio.bridge.supervisor import EventSourceSupervisor, RestartPolicy

#: Built-in bridge type -> factory mapping. Callers can pass this
#: directly to :class:`BridgeRunner`, or merge their own custom
#: factories on top of it.
BUILTIN_FACTORIES: dict[str, BridgeFactory] = {
    "matrix_event": matrix_event_factory,
    "mcp_tool": mcp_tool_factory,
    "openclaw_chat_sse": openclaw_chat_sse_factory,
    "qwenpaw_chat_sse": qwenpaw_chat_sse_factory,
}

__all__ = [
    "BUILTIN_FACTORIES",
    "Bridge",
    "BridgeConfig",
    "BridgeConfigError",
    "BridgeDefinition",
    "BridgeFactory",
    "BridgeHealth",
    "BridgeHealthSnapshot",
    "BridgeMappings",
    "BridgeRunner",
    "EventSourceSupervisor",
    "MatrixEventBridge",
    "McpToolBridge",
    "OpenClawChatBridge",
    "OpenClawChatSSEBridge",
    "QwenPawChatBridge",
    "QwenPawChatSSEBridge",
    "RestartPolicy",
    "RunnerHealthSnapshot",
    "SUPPORTED_VERSION",
    "matrix_event_factory",
    "mcp_tool_factory",
    "openclaw_chat_sse_factory",
    "qwenpaw_chat_sse_factory",
]
