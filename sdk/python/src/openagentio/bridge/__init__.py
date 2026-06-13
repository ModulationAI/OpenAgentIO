"""OpenAgentIO bridge subpackage.

Bridges connect the ACP/Envelope bus to external agent frameworks and
protocols (HTTP/SSE gateways, OpenAPI services, custom systems). The
current built-in bridge scope is the Python OpenClaw Chat SSE bridge.

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
from openagentio.bridge.openclaw_chat_sse import (
    OpenClawChatBridge,
    OpenClawChatSSEBridge,
    openclaw_chat_sse_factory,
)
from openagentio.bridge.runner import BridgeRunner

#: Built-in bridge type -> factory mapping. Callers can pass this
#: directly to :class:`BridgeRunner`, or merge their own custom
#: factories on top of it.
BUILTIN_FACTORIES: dict[str, BridgeFactory] = {
    "openclaw_chat_sse": openclaw_chat_sse_factory,
}

__all__ = [
    "BUILTIN_FACTORIES",
    "Bridge",
    "BridgeConfig",
    "BridgeConfigError",
    "BridgeDefinition",
    "BridgeFactory",
    "BridgeMappings",
    "BridgeRunner",
    "OpenClawChatBridge",
    "OpenClawChatSSEBridge",
    "SUPPORTED_VERSION",
    "openclaw_chat_sse_factory",
]
