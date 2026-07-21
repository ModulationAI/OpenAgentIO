"""Client-side stream reorder buffer backpressure tests.

Directly exercises the ``Stream`` iterator with a scripted fake inbox — the
end-to-end path assigns monotonic Seq via ``StreamWriter`` and cannot
naturally reach the negative-Seq / large-Seq-gap / pending-cap branches.

Mirrors ``pkg/bus/stream_backpressure_test.go``.
"""
from __future__ import annotations

import asyncio

import pytest

from openagentio import (
    BackpressureDropError,
    Envelope,
    ResponseDelta,
    ResponseFinal,
)
from openagentio.bus.stream import Stream
from openagentio.codec.json_codec import JSONCodec
from openagentio.transport.base import Inbox, RawMessage


class _FakeInbox(Inbox):
    """Scripted Inbox that yields pre-encoded messages, then blocks forever."""

    def __init__(self, messages: list[RawMessage]) -> None:
        self._messages = list(messages)
        self._closed = asyncio.Event()

    @property
    def subject(self) -> str:
        return "test.inbox"

    async def recv(self, timeout: float | None = None) -> RawMessage:
        if self._messages:
            return self._messages.pop(0)
        # No more scripted frames: block until closed or timeout, matching
        # a real inbox's semantics.
        try:
            await asyncio.wait_for(self._closed.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            raise
        raise RuntimeError("fake inbox: closed")

    async def close(self) -> None:
        self._closed.set()


def _env_with_seq(seq: int, *, final: bool = False) -> Envelope:
    e = Envelope.new(ResponseFinal if final else ResponseDelta)
    e.seq = seq
    e.is_final = final
    return e


def _mk_stream(
    envs: list[Envelope],
    *,
    max_pending: int | None = None,
    max_gap: int | None = None,
) -> Stream:
    codec = JSONCodec()
    messages = [RawMessage(subject="", data=codec.encode_envelope(e)) for e in envs]
    return Stream(
        inbox=_FakeInbox(messages),
        codec=codec,
        max_pending_frames=max_pending,
        max_sequence_gap=max_gap,
    )


async def test_stream_happy_path_reorders_frames() -> None:
    """Out-of-order arrival must be reordered by seq before yielding."""
    envs = [
        _env_with_seq(2),
        _env_with_seq(0),
        _env_with_seq(1),
        _env_with_seq(3, final=True),
    ]
    stream = _mk_stream(envs)
    seqs = [env.seq async for env in stream]
    assert seqs == [0, 1, 2, 3]


async def test_stream_drops_duplicate_frames() -> None:
    """Duplicate seq must be silently dropped."""
    envs = [
        _env_with_seq(0),
        _env_with_seq(0),  # duplicate
        _env_with_seq(1, final=True),
    ]
    stream = _mk_stream(envs)
    seqs = [env.seq async for env in stream]
    assert seqs == [0, 1]


async def test_stream_rejects_negative_seq() -> None:
    """Negative seq is a protocol violation; must raise BackpressureDropError.

    Regression: without the explicit guard, ``env.seq < 0`` compared against
    ``expected == 0`` would pass the ``seq < expected`` check and get silently
    dropped as "a late frame" — which hides the bug.
    """
    envs = [_env_with_seq(-1)]
    stream = _mk_stream(envs)
    with pytest.raises(BackpressureDropError):
        async for _ in stream:
            pass


async def test_stream_backpressure_drop_on_sequence_gap() -> None:
    """A frame more than max_gap ahead of expected terminates the stream."""
    envs = [_env_with_seq(100)]
    stream = _mk_stream(envs, max_gap=8)
    with pytest.raises(BackpressureDropError):
        async for _ in stream:
            pass


async def test_stream_backpressure_drop_on_pending_cap() -> None:
    """A frame that would overflow the pending buffer terminates the stream.

    Seq=0 is missing, so all four frames land in pending. With max_pending=3,
    the fourth must trigger the drop.
    """
    envs = [_env_with_seq(1), _env_with_seq(2), _env_with_seq(3), _env_with_seq(4)]
    stream = _mk_stream(envs, max_pending=3)
    with pytest.raises(BackpressureDropError):
        async for _ in stream:
            pass


async def test_stream_accepts_expected_frame_even_when_pending_full() -> None:
    """A frame with seq == expected must be accepted even at buffer capacity.

    Otherwise a stream that arrived pathologically out-of-order would never
    recover. Feeding 1, 2, 3 fills pending; the arrival of Seq=0 drains it.
    """
    envs = [
        _env_with_seq(1),
        _env_with_seq(2),
        _env_with_seq(3),
        _env_with_seq(0),
        _env_with_seq(4, final=True),
    ]
    stream = _mk_stream(envs, max_pending=3)
    seqs = [env.seq async for env in stream]
    assert seqs == [0, 1, 2, 3, 4]


async def test_stream_gap_check_ok_near_large_seq() -> None:
    """Mirror of the Go MaxUint64 test — Python ints don't overflow, but the
    subtraction form still needs to work at large Seq values that would trip
    a Go uint64 addition. We seed expected past 2**63 to prove the check is
    additive-independent.
    """
    base = (1 << 64) - 4
    envs = [_env_with_seq(base), _env_with_seq(base + 1, final=True)]
    stream = _mk_stream(envs, max_gap=8)
    stream._expected = base  # type: ignore[attr-defined]
    seqs = [env.seq async for env in stream]
    assert seqs == [base, base + 1]


async def test_stream_gap_check_rejects_over_gap_at_large_seq() -> None:
    """Mirror the negative test near the boundary: a genuinely-over-gap frame
    at large Seq still terminates the stream. Guards the reduction from
    'no overflow' to 'gap check still works' as a stand-alone contract.
    """
    expected = (1 << 64) - 100
    envs = [_env_with_seq((1 << 64) - 1)]  # differs by 99 > gap
    stream = _mk_stream(envs, max_gap=8)
    stream._expected = expected  # type: ignore[attr-defined]
    with pytest.raises(BackpressureDropError):
        async for _ in stream:
            pass
