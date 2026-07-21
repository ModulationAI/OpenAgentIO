"""NATS Core driver for OpenAgentIO. Mirrors pkg/transport/nats/nats.go.

Wraps the async ``nats-py`` client. Exposes Publish, Subscribe (with optional
queue groups), Request/Reply, and an _INBOX-backed :py:class:`Inbox` for
streaming responses. JetStream is reserved for v0.3.
"""
from __future__ import annotations

import asyncio
from typing import Any

import nats
from nats.aio.client import Client as NATSClient
from nats.aio.msg import Msg

from openagentio.transport.base import (
    Capabilities,
    Inbox,
    RawMessage,
    Subscription,
    Transport,
    TransportHandler,
)

_CLOSED = object()


class NATSDriver:
    """NATS Core async driver."""

    def __init__(
        self,
        url: str = "nats://localhost:4222",
        *,
        name: str = "",
        connect_timeout: float = 2.0,
    ) -> None:
        self._url = url
        self._name = name
        self._connect_timeout = connect_timeout
        self._nc: NATSClient | None = None
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        """Idempotent: a second call while already connected is a no-op.

        ``connect_timeout`` bounds the *entire* initial dial, including any
        internal reconnect attempts nats-py performs when the first server(s)
        are unreachable. Without this outer bound, a DNS failure fans out to
        ``max_reconnect_attempts * reconnect_time_wait`` (~120s by default),
        which manifests as the test suite appearing to hang.
        """
        async with self._lock:
            if self._nc is not None:
                return
            kwargs: dict[str, Any] = {
                "servers": [self._url],
                "connect_timeout": self._connect_timeout,
            }
            if self._name:
                kwargs["name"] = self._name
            try:
                self._nc = await asyncio.wait_for(
                    nats.connect(**kwargs),
                    timeout=self._connect_timeout,
                )
            except asyncio.TimeoutError as e:
                raise TimeoutError(
                    f"nats: connect to {self._url} timed out after "
                    f"{self._connect_timeout:g}s"
                ) from e

    async def close(self) -> None:
        """Drain in-flight handlers, then release the connection. Idempotent."""
        async with self._lock:
            nc = self._nc
            self._nc = None
        if nc is not None:
            try:
                await nc.drain()
            except Exception:
                pass

    def capabilities(self) -> Capabilities:
        return Capabilities(
            streaming=True,
            persistence=False,
            queue_group=True,
            headers=True,
        )

    async def publish(self, msg: RawMessage) -> None:
        if msg is None:
            raise ValueError("nats: nil message")
        nc = self._require_conn()
        await nc.publish(
            subject=msg.subject,
            payload=msg.data,
            reply=msg.reply_to or "",
            headers=msg.headers,
        )

    async def subscribe(
        self,
        subject: str,
        queue: str,
        handler: TransportHandler,
    ) -> Subscription:
        if handler is None:
            raise ValueError("nats: nil handler")
        nc = self._require_conn()

        async def cb(m: Msg) -> None:
            await handler(_from_nats(m))

        sub = await nc.subscribe(subject=subject, queue=queue or "", cb=cb)
        return _NATSSubscription(sub)

    async def request(
        self,
        msg: RawMessage,
        timeout: float | None = None,
    ) -> RawMessage:
        if msg is None:
            raise ValueError("nats: nil message")
        nc = self._require_conn()
        resp = await nc.request(
            subject=msg.subject,
            payload=msg.data,
            timeout=timeout if timeout is not None else 2.0,
            headers=msg.headers,
        )
        return _from_nats(resp)

    async def open_inbox(self) -> Inbox:
        nc = self._require_conn()
        subject = nc.new_inbox()
        return await _NATSInbox.create(nc, subject)

    async def flush(self) -> None:
        """Block until the server has acknowledged all queued protocol writes.

        Useful before exiting a process to ensure the last publish actually hit
        the wire, and in tests to make subscribe-then-publish on the same
        connection race-free.
        """
        nc = self._require_conn()
        await nc.flush()

    def _require_conn(self) -> NATSClient:
        if self._nc is None:
            raise RuntimeError("nats: not connected")
        return self._nc


def _from_nats(m: Msg) -> RawMessage:
    headers: dict[str, str] | None = None
    raw_headers = getattr(m, "headers", None) or getattr(m, "header", None)
    if raw_headers:
        headers = {str(k): str(v) for k, v in raw_headers.items()}
    return RawMessage(
        subject=m.subject,
        data=m.data,
        headers=headers,
        reply_to=m.reply or "",
    )


class _NATSSubscription:
    def __init__(self, sub: Any) -> None:
        self._sub = sub
        self._closed = False

    async def unsubscribe(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            await self._sub.unsubscribe()
        except Exception:
            pass


class _NATSInbox:
    def __init__(self, subject: str) -> None:
        self._subject = subject
        self._queue: asyncio.Queue = asyncio.Queue()
        self._sub: Any | None = None
        self._closed = False

    @classmethod
    async def create(cls, nc: NATSClient, subject: str) -> "_NATSInbox":
        box = cls(subject)

        async def cb(m: Msg) -> None:
            if not box._closed:
                await box._queue.put(_from_nats(m))

        box._sub = await nc.subscribe(subject=subject, cb=cb)
        return box

    @property
    def subject(self) -> str:
        return self._subject

    async def recv(self, timeout: float | None = None) -> RawMessage:
        getter = self._queue.get()
        if timeout is not None:
            getter = asyncio.wait_for(getter, timeout)
        item = await getter
        if item is _CLOSED:
            await self._queue.put(_CLOSED)
            raise RuntimeError("nats inbox: closed")
        return item

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._sub is not None:
            try:
                await self._sub.unsubscribe()
            except Exception:
                pass
        await self._queue.put(_CLOSED)
