"""Mock OpenClaw Gateway HTTP server for tests.

Exposes an OpenAI-compatible ``POST /v1/chat/completions`` endpoint that
returns configurable SSE streams or HTTP error responses. Intended to be
mounted via :class:`httpx.ASGITransport` in unit tests.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterable
from typing import Any

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, Response, StreamingResponse
from starlette.routing import Route


def _sse_chunk(content: str) -> str:
    data = json.dumps(
        {
            "choices": [
                {"index": 0, "delta": {"role": None, "content": content}}
            ]
        }
    )
    return f"data: {data}\n\n"


async def _stream_chunks(
    chunks: list[str],
    delays: list[float],
) -> AsyncIterable[bytes]:
    for idx, content in enumerate(chunks):
        if idx > 0 and idx - 1 < len(delays):
            import asyncio

            await asyncio.sleep(delays[idx - 1])
        yield _sse_chunk(content).encode("utf-8")
    yield b"data: [DONE]\n\n"


class MockOpenClawGateway:
    """Configurable mock OpenClaw Gateway for testing the SSE bridge."""

    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.status_code = 200
        self.error_body = ""
        self.chunks: list[str] = []
        self.delays: list[float] = []
        self.app = Starlette(routes=[Route("/v1/chat/completions", self._chat, methods=["POST"])])

    def set_stream(self, chunks: list[str], delays: list[float] | None = None) -> None:
        """Configure a successful SSE response."""
        self.status_code = 200
        self.error_body = ""
        self.chunks = list(chunks)
        self.delays = list(delays) if delays else []

    def set_error(self, status_code: int, body: str = "") -> None:
        """Configure an HTTP error response."""
        self.status_code = status_code
        self.error_body = body
        self.chunks = []
        self.delays = []

    async def _chat(self, request: Request) -> Response:
        body = await request.body()
        self.requests.append(
            {
                "headers": dict(request.headers),
                "body": json.loads(body) if body else None,
            }
        )

        auth = request.headers.get("authorization", "")
        if not auth.startswith("Bearer "):
            return JSONResponse({"error": "missing token"}, status_code=401)

        if self.status_code != 200:
            return PlainTextResponse(
                self.error_body, status_code=self.status_code
            )

        return StreamingResponse(
            _stream_chunks(self.chunks, self.delays),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache"},
        )


__all__ = ["MockOpenClawGateway"]
