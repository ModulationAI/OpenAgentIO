"""Error-to-HTTP mapping mirroring pkg/adapter/http/errors.go."""
from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING

from starlette.responses import JSONResponse, Response

from openagentio.bus.errors import BusError
from openagentio.bus.stream import ErrIdleTimeout
from openagentio.event.envelope import Envelope
from openagentio.event.payload import (
    CodeAgentTimeout,
    CodeAgentUnavailable,
    CodeAuthFailure,
    CodeBackpressureDrop,
    CodeCodecFailure,
    CodeInvalidRequest,
    CodeNoHandler,
    CodeTransportFailure,
    ErrorPayload,
)
from openagentio.event.types import ResponseError

if TYPE_CHECKING:
    pass


_CODE_TO_STATUS = {
    CodeAuthFailure: 401,
    CodeInvalidRequest: 400,
    CodeNoHandler: 404,
    CodeAgentTimeout: 504,
    CodeAgentUnavailable: 502,
    CodeTransportFailure: 502,
    CodeBackpressureDrop: 429,
    CodeCodecFailure: 500,
}


def status_for_code(code: str) -> int:
    return _CODE_TO_STATUS.get(code, 500)


def status_for_bus_error(exc: BaseException) -> tuple[int, str]:
    if isinstance(exc, (asyncio.TimeoutError, ErrIdleTimeout)):
        return 504, CodeAgentTimeout
    if isinstance(exc, asyncio.CancelledError):
        return 499, CodeInvalidRequest
    return 502, CodeAgentUnavailable


def _retryable_for_bus_error(exc: BaseException) -> bool:
    if isinstance(exc, (asyncio.TimeoutError, ErrIdleTimeout)):
        return True
    if isinstance(exc, asyncio.CancelledError):
        return False
    return False


def _error_payload_content(ep: ErrorPayload) -> dict[str, Any]:
    """Return the canonical JSON shape for an ErrorPayload-shaped body."""
    content: dict[str, Any] = {
        "code": ep.code,
        "message": ep.message,
        "retryable": ep.retryable,
    }
    if ep.cause:
        content["cause"] = ep.cause
    return content


def write_error_json(
    status: int, code: str, message: str, retryable: bool = False
) -> JSONResponse:
    ep = ErrorPayload(code=code, message=message, retryable=retryable)
    return JSONResponse(
        status_code=status,
        content=_error_payload_content(ep),
    )


def write_bus_error(exc: BaseException) -> JSONResponse:
    status, code = status_for_bus_error(exc)
    retryable = _retryable_for_bus_error(exc)
    ep = ErrorPayload(code=code, message=str(exc), retryable=retryable)
    return JSONResponse(
        status_code=status,
        content=_error_payload_content(ep),
    )


def write_envelope_error(env: Envelope) -> JSONResponse:
    ep = ErrorPayload()
    if env.payload:
        try:
            data = json.loads(env.payload)
            ep = ErrorPayload(
                code=data.get("code", ""),
                message=data.get("message", ""),
                retryable=data.get("retryable", False),
                cause=data.get("cause", {}),
            )
        except (json.JSONDecodeError, ValueError):
            pass
    if not ep.code:
        ep.code = CodeAgentUnavailable
    if not ep.message:
        ep.message = "agent error"
    status = status_for_code(ep.code)
    return JSONResponse(status_code=status, content=_error_payload_content(ep))
