"""Tests for the SSE parser."""

from __future__ import annotations

import pytest

from openagentio.bridge.sse_parser import parse_sse


async def _collect(chunks):
    async def gen():
        if hasattr(chunks, "__aiter__"):
            async for item in chunks:
                yield item
        else:
            for item in chunks:
                yield item

    return [p async for p in parse_sse(gen())]


async def test_parse_simple_data_lines():
    chunks = [b"data: hello\n\ndata: world\n\n"]
    assert await _collect(chunks) == ["hello", "world"]


async def test_parse_done_terminator():
    chunks = [b"data: hello\n\ndata: [DONE]\n\n"]
    assert await _collect(chunks) == ["hello", "[DONE]"]


async def test_parse_multiline_data():
    chunks = [b"data: line one\ndata: line two\n\n"]
    assert await _collect(chunks) == ["line one\nline two"]


async def test_parse_across_chunk_boundaries():
    chunks = [b"data: hel", b"lo\n\ndata: wor", b"ld\n\n"]
    assert await _collect(chunks) == ["hello", "world"]


async def test_parse_ignores_comments_and_other_fields():
    chunks = [
        b": comment\n",
        b"event: message\n",
        b"id: 123\n",
        b"data: payload\n\n",
    ]
    assert await _collect(chunks) == ["payload"]


async def test_parse_crlf():
    chunks = [b"data: hello\r\n\r\ndata: world\r\n\r\n"]
    assert await _collect(chunks) == ["hello", "world"]


async def test_parse_text_chunks():
    async def gen():
        yield "data: hello\n\n"
        yield "data: world\n\n"

    assert await _collect(gen()) == ["hello", "world"]


async def test_parse_empty_stream():
    assert await _collect([]) == []


async def test_parse_trailing_line_without_newline():
    chunks = [b"data: trailing"]
    assert await _collect(chunks) == ["trailing"]
