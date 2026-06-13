"""Server-Sent Events (SSE) parser.

Provides a small, dependency-free async generator that consumes raw
byte chunks from an HTTP response and yields ``data:`` payload strings.

Only the ``data`` field is surfaced to callers; ``event``, ``id``,
``retry`` and comment lines are tolerated but ignored. Multi-line data
fields are joined with newline characters per the SSE spec.
"""

from __future__ import annotations

from collections.abc import AsyncIterable
from typing import Any


async def parse_sse(
    chunks: AsyncIterable[bytes | str],
    *,
    encoding: str = "utf-8",
    errors: str = "replace",
) -> AsyncIterable[str]:
    """Yield decoded event ``data`` payloads from an SSE byte stream.

    The caller is expected to drive the iterator with ``async for``.
    Multi-line ``data:`` fields are joined with newlines per the SSE
    specification. The terminator ``data: [DONE]`` is yielded as the
    literal string ``"[DONE]"`` so the consumer can detect end-of-stream.

    Args:
        chunks: An async iterable of text or byte chunks (e.g.
            ``response.aiter_text()`` or ``response.aiter_bytes()``).
        encoding: Character encoding used to decode bytes to text.
        errors: Decoder error handling strategy.

    Yields:
        Decoded SSE event ``data`` payload strings.
    """
    text_buffer = ""
    data_lines: list[str] = []

    async for chunk in chunks:
        if isinstance(chunk, bytes):
            text_buffer += chunk.decode(encoding, errors)
        else:
            text_buffer += str(chunk)

        while True:
            line, sep, rest = text_buffer.partition("\n")
            if not sep:
                break
            text_buffer = rest
            event_data = _append_line(line, data_lines)
            if event_data is not None:
                yield event_data
                data_lines = []

    if text_buffer:
        event_data = _append_line(text_buffer, data_lines)
        if event_data is not None:
            yield event_data
            data_lines = []

    # Flush any buffered data lines even if the stream did not end with a
    # blank line (some implementations omit the final newline).
    if data_lines:
        yield "\n".join(data_lines)


def _append_line(line: str, data_lines: list[str]) -> str | None:
    """Process one SSE line and return the completed event data, if any.

    * ``data: ...`` lines append to the current event buffer.
    * Empty lines (after stripping ``\r``) dispatch the buffered event.
    * Comment lines and other fields are ignored.
    """
    line = line.rstrip("\r")
    if not line:
        if data_lines:
            return "\n".join(data_lines)
        return None
    if line.startswith(":"):
        # Comment line.
        return None
    if line.startswith("data:"):
        payload = line[5:]
        if payload.startswith(" "):
            payload = payload[1:]
        data_lines.append(payload)
    return None


async def _aiter_text(
    response: Any,
    *,
    chunk_size: int | None = None,
) -> AsyncIterable[str]:
    """Compatibility helper for ``httpx.Response.aiter_text``.

    ``httpx`` 0.27+ supports ``aiter_text(chunk_size=None)`` but older
    versions used a different signature. We call it with no arguments,
    which is the safest common denominator for the versions we support.
    """
    async for text in response.aiter_text():
        yield text


__all__ = ["parse_sse", "_aiter_text"]
