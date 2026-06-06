"""HTTP/SSE adapter for the OpenAgentIO Bus.

Provides a thin REST gateway that translates external HTTP traffic into Bus
calls. Requires the ``starlette`` extra: ``pip install openagentio[http]``.
"""
from openagentio.adapter.http.adapter import Adapter, New
from openagentio.adapter.http.auth import (
    AuthContext,
    AuthFunc,
    BearerAuth,
    ErrUnauthorized,
)
from openagentio.adapter.http.middleware import ASGIMiddleware, Logging, Recover
from openagentio.adapter.http.options import (
    AdapterOptions,
    Option,
    WithAuth,
    WithIdleTimeout,
    WithLogger,
    WithMiddleware,
    WithSSERetry,
    WithTimeout,
)

__all__ = [
    "Adapter",
    "New",
    "AdapterOptions",
    "Option",
    "WithAuth",
    "WithLogger",
    "WithTimeout",
    "WithIdleTimeout",
    "WithSSERetry",
    "WithMiddleware",
    "AuthContext",
    "AuthFunc",
    "BearerAuth",
    "ErrUnauthorized",
    "ASGIMiddleware",
    "Recover",
    "Logging",
]
