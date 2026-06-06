"""Adapter options mirroring pkg/adapter/http/options.go."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from openagentio.adapter.http.auth import AuthFunc
    from openagentio.adapter.http.middleware import ASGIMiddleware


@dataclass
class AdapterOptions:
    auth: AuthFunc | None = None
    logger: logging.Logger | None = None
    timeout: float = 30.0
    idle_timeout: float = 0.0
    sse_retry: float = 3.0
    middleware: list[ASGIMiddleware] = field(default_factory=list)


Option = Callable[[AdapterOptions], None]


def WithAuth(fn: AuthFunc) -> Option:
    def apply(o: AdapterOptions) -> None:
        o.auth = fn
    return apply


def WithLogger(l: logging.Logger) -> Option:
    def apply(o: AdapterOptions) -> None:
        if l is not None:
            o.logger = l
    return apply


def WithTimeout(d: float) -> Option:
    def apply(o: AdapterOptions) -> None:
        o.timeout = d
    return apply


def WithIdleTimeout(d: float) -> Option:
    def apply(o: AdapterOptions) -> None:
        o.idle_timeout = d
    return apply


def WithSSERetry(d: float) -> Option:
    def apply(o: AdapterOptions) -> None:
        o.sse_retry = d
    return apply


def WithMiddleware(*mws: ASGIMiddleware) -> Option:
    def apply(o: AdapterOptions) -> None:
        o.middleware.extend(mws)
    return apply
