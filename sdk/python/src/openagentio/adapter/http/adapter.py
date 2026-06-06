"""HTTP/SSE Adapter mirroring pkg/adapter/http/adapter.go.

Construct with :func:`New`, then pass ``adapter.app`` to any ASGI server
(uvicorn, hypercorn, etc.).
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from starlette.applications import Starlette
from starlette.routing import Route
from starlette.types import ASGIApp

from openagentio.adapter.http.auth import AuthFunc
from openagentio.adapter.http.handlers import handle_invoke, handle_stream, handle_publish
from openagentio.adapter.http.middleware import ASGIMiddleware
from openagentio.adapter.http.options import AdapterOptions, Option

if TYPE_CHECKING:
    from openagentio.bus.bus import Bus


class Adapter:
    """HTTP/SSE adapter wrapping a :class:`~openagentio.Bus`.

    Use the :func:`New` factory; do not construct directly.
    """

    def __init__(
        self,
        bus: Bus,
        opts: AdapterOptions,
    ) -> None:
        self._bus = bus
        self._auth: AuthFunc | None = opts.auth
        self._log: logging.Logger = opts.logger or logging.getLogger("openagentio.http")
        self._timeout: float = opts.timeout
        self._idle: float = opts.idle_timeout
        self._sse_retry: float = opts.sse_retry

        # Curried handlers — each receives the adapter instance via closure.
        async def _invoke(request) -> ...:
            return await handle_invoke(self, request)

        async def _stream(request) -> ...:
            return await handle_stream(self, request)

        async def _publish(request) -> ...:
            return await handle_publish(self, request)

        routes = [
            Route("/v1/agents/{target}/invoke", _invoke, methods=["POST"]),
            Route("/v1/agents/{target}/stream", _stream, methods=["POST"]),
            Route("/v1/events/{event_type}", _publish, methods=["POST"]),
        ]

        app: ASGIApp = Starlette(routes=routes)

        # Apply middleware in reverse so outermost runs first.
        for mw in reversed(opts.middleware):
            app = mw(app)

        self.app = app


def New(bus: Bus, *options: Option) -> Adapter:
    """Factory aligned with Go SDK's ``http.New(bus, opts...)``."""
    opts = AdapterOptions()
    for o in options:
        o(opts)
    return Adapter(bus, opts)
