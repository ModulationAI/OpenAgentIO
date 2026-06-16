"""Functional option pattern for Bus construction. Mirrors pkg/bus/options.go.

Provides :class:`Options`, the :data:`Option` type alias, and all ``With*``
helper functions. Per-call option types (:data:`SubOption`, :data:`InvokeOption`,
:data:`HandleOption`) are also defined here so downstream code can import them
from a single location.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol, runtime_checkable

from openagentio.bus.subjects import DEFAULT_SUBJECT_PREFIX
from openagentio.codec.json_codec import Codec, JSONCodec


# --- EnvelopePreparer ---------------------------------------------------------

EnvelopePreparer = Callable[[Any], None]


# --- Bus-level options --------------------------------------------------------

@dataclass
class Options:
    """Every Bus-level setting in one place."""

    agent_id: str = ""
    tenant: str = ""
    subject_prefix: str = DEFAULT_SUBJECT_PREFIX
    codec: Codec | None = None
    transport: Any = None  # Transport protocol instance
    logger: logging.Logger | None = None
    middleware: list[Callable] = field(default_factory=list)
    envelope_preparers: list[EnvelopePreparer] = field(default_factory=list)
    default_timeout: float = 30.0
    propagate_session_context: bool = False


Option = Callable[[Options], None]


def WithAgentID(id: str) -> Option:
    def apply(o: Options) -> None:
        o.agent_id = id
    return apply


def WithTransport(t: Any) -> Option:
    def apply(o: Options) -> None:
        o.transport = t
    return apply


def WithTenant(t: str) -> Option:
    def apply(o: Options) -> None:
        o.tenant = t
    return apply


def WithSubjectPrefix(p: str) -> Option:
    def apply(o: Options) -> None:
        o.subject_prefix = p
    return apply


def WithCodec(c: Codec) -> Option:
    def apply(o: Options) -> None:
        o.codec = c
    return apply


def WithLogger(l: logging.Logger) -> Option:
    def apply(o: Options) -> None:
        o.logger = l
    return apply


def WithMiddleware(*mws: Callable) -> Option:
    def apply(o: Options) -> None:
        o.middleware.extend(mws)
    return apply


def WithEnvelopePreparer(*preparers: EnvelopePreparer) -> Option:
    def apply(o: Options) -> None:
        o.envelope_preparers.extend(preparers)
    return apply


def WithDefaultTimeout(d: float) -> Option:
    def apply(o: Options) -> None:
        o.default_timeout = d
    return apply


def WithSessionPropagation(propagate: bool = True) -> Option:
    """Control whether nested ``invoke`` / ``stream_invoke`` inherit session,
    conversation, and trace fields from the currently dispatched envelope.

    Default is ``False`` to preserve existing core bus semantics; enable when
    you want handlers triggered by external bridges to share context with
    downstream calls.
    """

    def apply(o: Options) -> None:
        o.propagate_session_context = propagate

    return apply


# --- Per-call options ---------------------------------------------------------

@dataclass
class _SubOpts:
    queue: str = ""


SubOption = Callable[[_SubOpts], None]


def WithQueue(q: str) -> SubOption:
    def apply(o: _SubOpts) -> None:
        o.queue = q
    return apply


def collect_sub_opts(opts: list[SubOption]) -> _SubOpts:
    o = _SubOpts()
    for f in opts:
        f(o)
    return o


@dataclass
class _InvokeOpts:
    timeout: float | None = None
    idle_timeout: float | None = None


InvokeOption = Callable[[_InvokeOpts], None]


def WithTimeout(d: float) -> InvokeOption:
    def apply(o: _InvokeOpts) -> None:
        o.timeout = d
    return apply


def WithIdleTimeout(d: float) -> InvokeOption:
    def apply(o: _InvokeOpts) -> None:
        o.idle_timeout = d
    return apply


def collect_invoke_opts(opts: list[InvokeOption]) -> _InvokeOpts:
    o = _InvokeOpts()
    for f in opts:
        f(o)
    return o


@dataclass
class _HandleOpts:
    queue: str = ""
    queue_set: bool = False


HandleOption = Callable[[_HandleOpts], None]


def WithHandleQueue(q: str) -> HandleOption:
    def apply(o: _HandleOpts) -> None:
        o.queue = q
        o.queue_set = True
    return apply


def collect_handle_opts(opts: list[HandleOption]) -> _HandleOpts:
    o = _HandleOpts()
    for f in opts:
        f(o)
    return o