"""Route handlers mirroring pkg/adapter/http/handlers.go."""
from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING

from starlette.requests import Request
from starlette.responses import Response, StreamingResponse

from openagentio.bus.options import WithTimeout, WithIdleTimeout
from openagentio.bus.stream import ErrIdleTimeout
from openagentio.event.envelope import Envelope
from openagentio.event.payload import (
    CodeAgentTimeout,
    CodeAgentUnavailable,
    CodeAuthFailure,
    CodeInvalidRequest,
    ErrorPayload,
)
from openagentio.event.types import ResponseError

from openagentio.adapter.http.auth import AuthContext
from openagentio.adapter.http.envelope import read_envelope
from openagentio.adapter.http.errors import (
    write_error_json,
    write_bus_error,
    write_envelope_error,
)

if TYPE_CHECKING:
    from openagentio.adapter.http.adapter import Adapter

_DEFAULT_PUBLISH_WAIT = 5.0


async def _authenticate(adapter: Adapter, request: Request) -> tuple[AuthContext | None, Response | None]:
    """Run the configured AuthFunc. Returns (ctx, error_response)."""
    if adapter._auth is None:
        return None, None
    try:
        ac = await adapter._auth(request)
    except Exception as exc:
        return None, write_error_json(401, CodeAuthFailure, str(exc))
    return ac, None


async def handle_invoke(adapter: Adapter, request: Request) -> Response:
    ac, err = await _authenticate(adapter, request)
    if err is not None:
        return err

    target = request.path_params.get("target", "")
    if not target:
        return write_error_json(400, CodeInvalidRequest, "missing target")

    try:
        env = await read_envelope(request, "", ac)
    except ValueError as exc:
        return write_error_json(400, CodeInvalidRequest, str(exc))

    invoke_opts: list = []
    if adapter._timeout > 0:
        invoke_opts.append(WithTimeout(adapter._timeout))

    try:
        resp = await adapter._bus.invoke(target, env, *invoke_opts)
    except Exception as exc:
        return write_bus_error(exc)

    if resp.event_type == ResponseError:
        return write_envelope_error(resp)

    if not resp.payload:
        return Response(status_code=204)

    return Response(
        content=resp.payload,
        status_code=200,
        media_type="application/json",
    )


async def handle_stream(adapter: Adapter, request: Request) -> Response:
    ac, err = await _authenticate(adapter, request)
    if err is not None:
        return err

    target = request.path_params.get("target", "")
    if not target:
        return write_error_json(400, CodeInvalidRequest, "missing target")

    try:
        env = await read_envelope(request, "", ac)
    except ValueError as exc:
        return write_error_json(400, CodeInvalidRequest, str(exc))

    stream_opts: list = []
    if adapter._timeout > 0:
        stream_opts.append(WithTimeout(adapter._timeout))
    if adapter._idle > 0:
        stream_opts.append(WithIdleTimeout(adapter._idle))

    try:
        stream = await adapter._bus.stream_invoke(target, env, *stream_opts)
    except Exception as exc:
        return write_bus_error(exc)

    async def event_generator():
        retry_ms = int(adapter._sse_retry * 1000)
        try:
            async for frame in stream:
                yield _format_sse_envelope(frame, retry_ms)
                if frame.is_final:
                    break
        except (asyncio.TimeoutError, ErrIdleTimeout) as exc:
            yield _format_sse_error(exc, retry_ms)
        except Exception as exc:
            yield _format_sse_error(exc, retry_ms)
        finally:
            await stream.close()

    return StreamingResponse(
        event_generator(),
        status_code=200,
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


async def handle_publish(adapter: Adapter, request: Request) -> Response:
    ac, err = await _authenticate(adapter, request)
    if err is not None:
        return err

    event_type = request.path_params.get("event_type", "")
    if not event_type:
        return write_error_json(400, CodeInvalidRequest, "missing event_type")

    try:
        env = await read_envelope(request, event_type, ac)
    except ValueError as exc:
        return write_error_json(400, CodeInvalidRequest, str(exc))

    wait = _DEFAULT_PUBLISH_WAIT
    if adapter._timeout > 0 and adapter._timeout < wait:
        wait = adapter._timeout

    try:
        await asyncio.wait_for(adapter._bus.publish(env), timeout=wait)
    except Exception as exc:
        return write_bus_error(exc)

    return Response(status_code=202)


def _format_sse_envelope(env: Envelope, retry_ms: int = 0) -> bytes:
    body = env.to_bytes()
    parts = [f"event: {env.event_type}\n"]
    if env.event_id:
        parts.append(f"id: {env.event_id}\n")
    if retry_ms > 0:
        parts.append(f"retry: {retry_ms}\n")
    parts.append(f"data: {body.decode('utf-8')}\n\n")
    return "".join(parts).encode("utf-8")


def _format_sse_error(exc: BaseException, retry_ms: int = 0) -> bytes:
    code = CodeAgentUnavailable
    if isinstance(exc, (asyncio.TimeoutError, ErrIdleTimeout)):
        code = CodeAgentTimeout
    frame = Envelope.new(ResponseError)
    frame.is_final = True
    payload = ErrorPayload(code=code, message=str(exc))
    frame.payload = json.dumps({"code": payload.code, "message": payload.message}).encode("utf-8")
    return _format_sse_envelope(frame, retry_ms)
