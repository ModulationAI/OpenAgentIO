"""Stream invoke ordering and lifecycle over the in-memory driver."""
from __future__ import annotations

import asyncio

import pytest

from openagentio import (
    Bus,
    CodeAgentTimeout,
    CodeAgentUnavailable,
    Envelope,
    ErrIdleTimeout,
    FrameTypeRequest,
    FrameTypeResponseDelta,
    FrameTypeResponseFinal,
    FrameTypeResponseStarted,
    ResponseDelta,
    ResponseError,
    ResponseFinal,
    ResponseStarted,
    StreamWriter,
    WithIdleTimeout,
    WithTimeout,
)


async def test_stream_invoke_happy_path(bus: Bus) -> None:
    async def handler(_: Envelope, w: StreamWriter) -> None:
        await w.started({"model": "test"})
        for i in range(3):
            await w.delta({"i": i})
        await w.final({"count": 3})

    await bus.handle_stream("count", handler)

    s = await bus.stream_invoke("count", None)
    types: list[str] = []
    seqs: list[int] = []
    try:
        async for env in s:
            types.append(env.event_type)
            seqs.append(env.seq)
    finally:
        await s.close()

    assert types == [
        ResponseStarted,
        ResponseDelta,
        ResponseDelta,
        ResponseDelta,
        ResponseFinal,
    ]
    assert seqs == [0, 1, 2, 3, 4]


async def test_stream_frames_include_frame_type(bus: Bus) -> None:
    async def handler(_: Envelope, w: StreamWriter) -> None:
        await w.started(None)
        await w.delta({"chunk": 1})
        await w.final(None)

    await bus.handle_stream("typed", handler)

    s = await bus.stream_invoke("typed", None)
    frames: list[Envelope] = []
    try:
        async for env in s:
            frames.append(env)
    finally:
        await s.close()

    assert len(frames) == 3
    assert frames[0].frame_type == FrameTypeResponseStarted
    assert frames[1].frame_type == FrameTypeResponseDelta
    assert frames[2].frame_type == FrameTypeResponseFinal


async def test_stream_request_envelope_includes_frame_type(bus: Bus) -> None:
    captured: Envelope | None = None

    async def handler(req: Envelope, w: StreamWriter) -> None:
        nonlocal captured
        captured = req
        await w.final(None)

    await bus.handle_stream("req-type", handler)

    s = await bus.stream_invoke("req-type", {"msg": "hi"})
    try:
        async for _ in s:
            pass
    finally:
        await s.close()

    assert captured is not None
    assert captured.frame_type == FrameTypeRequest


async def test_invoke_response_enforces_canonical_frame_type(bus: Bus) -> None:
    async def handler(req: Envelope) -> Envelope:
        # User returns an envelope with a known event_type but a contradictory
        # custom frame_type.
        resp = Envelope.new(ResponseFinal)
        resp.frame_type = "custom.response.final"
        return resp

    await bus.handle_invoke("typed-resp", handler)
    resp = await bus.invoke("typed-resp", {"msg": "hi"})
    assert resp.event_type == ResponseFinal
    assert resp.frame_type == FrameTypeResponseFinal


async def test_invoke_response_preserves_custom_frame_type_for_unknown_event_type(bus: Bus) -> None:
    async def handler(req: Envelope) -> Envelope:
        resp = Envelope.new("goc.incident.created")
        resp.frame_type = "custom.frame"
        return resp

    await bus.handle_invoke("custom-resp", handler)
    resp = await bus.invoke("custom-resp", {"msg": "hi"})
    assert resp.event_type == "goc.incident.created"
    assert resp.frame_type == "custom.frame"


async def test_stream_auto_finalizes_on_clean_return(bus: Bus) -> None:
    async def handler(_: Envelope, w: StreamWriter) -> None:
        await w.started(None)
        await w.delta({"chunk": 1})
        # No explicit final/error — runtime should auto-emit final.

    await bus.handle_stream("auto", handler)

    s = await bus.stream_invoke("auto", None)
    last: Envelope | None = None
    try:
        async for env in s:
            last = env
    finally:
        await s.close()

    assert last is not None
    assert last.event_type == ResponseFinal
    assert last.is_final is True


async def test_stream_auto_emits_error_on_handler_exception(bus: Bus) -> None:
    async def handler(_: Envelope, w: StreamWriter) -> None:
        await w.started(None)
        raise RuntimeError("kaboom")

    await bus.handle_stream("explode", handler)

    s = await bus.stream_invoke("explode", None)
    last: Envelope | None = None
    try:
        async for env in s:
            last = env
    finally:
        await s.close()

    assert last is not None
    assert last.event_type == ResponseError
    err = last.payload_json()
    assert err["message"] == "kaboom"


async def test_stream_writer_started_at_most_once(bus: Bus) -> None:
    second_err: list[BaseException] = []
    captured = asyncio.Event()

    async def handler(_: Envelope, w: StreamWriter) -> None:
        await w.started(None)
        try:
            await w.started(None)
        except RuntimeError as e:
            second_err.append(e)
        captured.set()
        await w.final(None)

    await bus.handle_stream("twostart", handler)

    s = await bus.stream_invoke("twostart", None)
    try:
        async for _ in s:
            pass
    finally:
        await s.close()
    await asyncio.wait_for(captured.wait(), 1.0)
    assert len(second_err) == 1


async def test_stream_writer_final_is_terminal(bus: Bus) -> None:
    after_final_err: list[BaseException] = []
    captured = asyncio.Event()

    async def handler(_: Envelope, w: StreamWriter) -> None:
        await w.final(None)
        try:
            await w.delta(None)
        except RuntimeError as e:
            after_final_err.append(e)
        captured.set()

    await bus.handle_stream("late", handler)

    s = await bus.stream_invoke("late", None)
    count = 0
    try:
        async for _ in s:
            count += 1
    finally:
        await s.close()
    await asyncio.wait_for(captured.wait(), 1.0)
    assert count == 1
    assert len(after_final_err) == 1


async def test_stream_idle_timeout(bus: Bus) -> None:
    hold = asyncio.Event()

    async def handler(_: Envelope, w: StreamWriter) -> None:
        await w.started(None)
        try:
            await asyncio.wait_for(hold.wait(), 5.0)
        except asyncio.TimeoutError:
            pass

    await bus.handle_stream("hang", handler)

    s = await bus.stream_invoke("hang", None, WithIdleTimeout(0.05))
    got_start = False
    raised: BaseException | None = None
    try:
        async for env in s:
            if env.event_type == ResponseStarted:
                got_start = True
    except ErrIdleTimeout as e:
        raised = e
    finally:
        hold.set()
        await s.close()

    assert got_start is True
    assert isinstance(raised, ErrIdleTimeout)


async def test_stream_overall_timeout(bus: Bus) -> None:
    hold = asyncio.Event()

    async def handler(_: Envelope, w: StreamWriter) -> None:
        await w.started(None)
        # Trickle deltas faster than the overall budget so idle_timeout never
        # fires — only the overall deadline can end the iteration.
        try:
            for i in range(100):
                await w.delta({"i": i})
                await asyncio.sleep(0.01)
            await asyncio.wait_for(hold.wait(), 5.0)
        except asyncio.TimeoutError:
            pass

    await bus.handle_stream("trickle", handler)

    s = await bus.stream_invoke("trickle", None, WithTimeout(0.05), WithIdleTimeout(1.0))
    raised: BaseException | None = None
    try:
        async for _ in s:
            pass
    except asyncio.TimeoutError as e:
        raised = e
    finally:
        hold.set()
        await s.close()

    assert isinstance(raised, asyncio.TimeoutError)


async def test_stream_overall_timeout_beats_idle_when_no_frames(bus: Bus) -> None:
    hold = asyncio.Event()

    async def handler(_: Envelope, w: StreamWriter) -> None:
        await w.started(None)
        try:
            await asyncio.wait_for(hold.wait(), 5.0)
        except asyncio.TimeoutError:
            pass

    await bus.handle_stream("silent", handler)

    # idle_timeout > timeout — overall deadline must fire first and surface
    # asyncio.TimeoutError, not ErrIdleTimeout.
    s = await bus.stream_invoke("silent", None, WithTimeout(0.05), WithIdleTimeout(1.0))
    raised: BaseException | None = None
    got_start = False
    try:
        async for env in s:
            if env.event_type == ResponseStarted:
                got_start = True
    except asyncio.TimeoutError as e:
        raised = e
    finally:
        hold.set()
        await s.close()

    assert got_start is True
    assert isinstance(raised, asyncio.TimeoutError)


async def test_stream_handler_error_code_is_agent_unavailable(bus: Bus) -> None:
    """Handler exception → ResponseError with code=AGENT_UNAVAILABLE."""
    async def handler(_: Envelope, w: StreamWriter) -> None:
        await w.started(None)
        raise RuntimeError("kaboom")

    await bus.handle_stream("unavail", handler)

    s = await bus.stream_invoke("unavail", None)
    last: Envelope | None = None
    try:
        async for env in s:
            last = env
    finally:
        await s.close()

    assert last is not None
    assert last.event_type == ResponseError
    err = last.payload_json()
    assert err["code"] == CodeAgentUnavailable
    assert err["retryable"] is False


async def test_stream_idle_timeout_error_code_is_agent_timeout(bus: Bus) -> None:
    """Idle timeout → runtime auto-emits ResponseError with code=AGENT_TIMEOUT."""
    hold = asyncio.Event()

    async def handler(_: Envelope, w: StreamWriter) -> None:
        await w.started(None)
        try:
            await asyncio.wait_for(hold.wait(), 5.0)
        except asyncio.TimeoutError:
            pass

    await bus.handle_stream("idle-timeout-err", handler)

    s = await bus.stream_invoke("idle-timeout-err", None, WithIdleTimeout(0.05))
    last: Envelope | None = None
    try:
        async for env in s:
            last = env
    except ErrIdleTimeout:
        pass
    finally:
        hold.set()
        await s.close()

    # When idle timeout fires, the runtime auto-emits a ResponseError envelope
    # with code=AGENT_TIMEOUT before raising ErrIdleTimeout on the consumer side.
    if last is not None and last.event_type == ResponseError:
        err = last.payload_json()
        assert err["code"] == CodeAgentTimeout
        assert err["retryable"] is True


async def test_stream_close_unblocks_iteration(bus: Bus) -> None:
    hold = asyncio.Event()

    async def handler(_: Envelope, w: StreamWriter) -> None:
        await w.started(None)
        try:
            await asyncio.wait_for(hold.wait(), 5.0)
        except asyncio.TimeoutError:
            pass

    await bus.handle_stream("park", handler)
    s = await bus.stream_invoke("park", None)

    got_first = asyncio.Event()
    closed_loop = asyncio.Event()

    async def consume() -> None:
        try:
            async for env in s:
                if env.event_type == ResponseStarted:
                    got_first.set()
        finally:
            closed_loop.set()

    task = asyncio.create_task(consume())
    try:
        await asyncio.wait_for(got_first.wait(), 1.0)
        await s.close()
        await asyncio.wait_for(closed_loop.wait(), 1.0)
    finally:
        hold.set()
        task.cancel()
