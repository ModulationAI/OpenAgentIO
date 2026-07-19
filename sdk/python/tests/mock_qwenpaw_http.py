"""Mock QwenPaw HTTP server for tests.

Exposes QwenPaw's ``POST /api/console/chat`` endpoint and returns
configurable SSE streams in QwenPaw's event format (``status`` plus
``output[].content[].text``) or HTTP error responses. Intended to be
mounted via :class:`httpx.ASGITransport` in unit tests.

Unlike :mod:`mock_openclaw_http`, this mock does **not** enforce auth:
QwenPaw's local 127.0.0.1 path skips Web login auth, so an empty
``Authorization`` header is the expected default and must not yield a
401. HTTP 401/403 are driven explicitly via :meth:`set_error`.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterable
from typing import Any

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response, StreamingResponse
from starlette.routing import Route


def assistant_event(text: str, status: str = "in_progress") -> dict[str, Any]:
    """Build a QwenPaw SSE event carrying assistant ``text``.

    QwenPaw nests model output under ``output[].content[]``; only
    ``role == "assistant"`` / ``type == "text"`` parts carry text. The
    bridge extracts and deduplicates these via
    ``_extract_assistant_text``; passing cumulative ``text`` values
    across events exercises the dedup path.
    """
    return {
        "status": status,
        "output": [
            {
                "role": "assistant",
                "content": [{"type": "text", "text": text}],
            }
        ],
    }


def text_delta_event(text: str) -> dict[str, Any]:
    """Build QwenPaw's top-level text delta event shape."""
    return {
        "type": "text",
        "delta": True,
        "index": 0,
        "status": None,
        "object": "content",
        "msg_id": "msg_test",
        "text": text,
    }


def _sse_data(payload: Any) -> str:
    return f"data: {json.dumps(payload)}\n\n"


async def _stream_events(
    events: list[dict[str, Any]],
    append_done: bool,
) -> AsyncIterable[bytes]:
    for event in events:
        yield _sse_data(event).encode("utf-8")
    # QwenPaw terminates on ``status: "completed"`` rather than [DONE];
    # the OpenAI terminator is appended only to exercise the bridge's
    # defensive [DONE] handling.
    if append_done:
        yield b"data: [DONE]\n\n"


class MockQwenPawServer:
    """Configurable mock QwenPaw console-chat server for SSE bridge tests."""

    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.status_code = 200
        self.error_body = ""
        self.events: list[dict[str, Any]] = []
        self.append_done = False
        self.app = Starlette(
            routes=[Route("/api/console/chat", self._chat, methods=["POST"])]
        )

    def set_stream(
        self,
        events: list[dict[str, Any]],
        *,
        append_done: bool = False,
    ) -> None:
        """Configure a successful SSE response streaming the given events."""
        self.status_code = 200
        self.error_body = ""
        self.events = list(events)
        self.append_done = append_done

    def set_error(self, status_code: int, body: str = "") -> None:
        """Configure an HTTP error response (400/401/403/404/500/...)."""
        self.status_code = status_code
        self.error_body = body
        self.events = []
        self.append_done = False

    async def _chat(self, request: Request) -> Response:
        body = await request.body()
        self.requests.append(
            {
                "headers": dict(request.headers),
                "body": json.loads(body) if body else None,
            }
        )

        if self.status_code != 200:
            return PlainTextResponse(
                self.error_body, status_code=self.status_code
            )

        return StreamingResponse(
            _stream_events(self.events, self.append_done),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache"},
        )


__all__ = ["MockQwenPawServer", "assistant_event"]
