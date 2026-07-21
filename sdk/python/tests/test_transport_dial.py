"""Transport dial helper tests."""
from __future__ import annotations

import asyncio
import os

from openagentio import InMemoryDriver, NATSDriver, dial, WithNATSName
from openagentio.transport.nats import NATSDriver as _NATSDriver


async def test_dial_inmem() -> None:
    os.environ["OPENAGENTIO_TRANSPORT"] = "inmem"
    try:
        tp = await dial()
        try:
            assert isinstance(tp, InMemoryDriver)
        finally:
            await tp.close()
    finally:
        del os.environ["OPENAGENTIO_TRANSPORT"]


async def test_dial_default_is_nats_url_from_env() -> None:
    """When OPENAGENTIO_TRANSPORT is unset or empty, dial creates a NATSDriver."""
    os.environ.pop("OPENAGENTIO_TRANSPORT", None)
    os.environ["NATS_URL"] = "nats://localhost:4222"
    try:
        tp = await dial()
        try:
            assert isinstance(tp, NATSDriver)
        finally:
            await tp.close()
    except RuntimeError:
        # NATS server not running — expected in CI without NATS.
        pass
    finally:
        del os.environ["NATS_URL"]


async def test_dial_explicit_nats() -> None:
    os.environ["OPENAGENTIO_TRANSPORT"] = "nats"
    try:
        tp = await dial()
        try:
            assert isinstance(tp, NATSDriver)
        finally:
            await tp.close()
    except RuntimeError:
        pass
    finally:
        del os.environ["OPENAGENTIO_TRANSPORT"]


async def test_dial_unsupported_mode() -> None:
    os.environ["OPENAGENTIO_TRANSPORT"] = "kafka"
    try:
        try:
            await dial()
        except ValueError as e:
            assert "kafka" in str(e)
        else:
            raise AssertionError("Expected ValueError for unsupported transport")
    finally:
        del os.environ["OPENAGENTIO_TRANSPORT"]


async def test_dial_with_nats_name() -> None:
    os.environ["OPENAGENTIO_TRANSPORT"] = "nats"
    os.environ["NATS_URL"] = "nats://localhost:4222"
    try:
        tp = await dial(WithNATSName("test-agent"))
        try:
            assert isinstance(tp, NATSDriver)
            assert tp._name == "test-agent"
        finally:
            await tp.close()
    except RuntimeError:
        pass
    finally:
        del os.environ["OPENAGENTIO_TRANSPORT"]
        del os.environ["NATS_URL"]


async def test_dial_nats_connect_failure_wraps_error() -> None:
    """If NATS connection fails, dial wraps the error with URL info."""
    os.environ["OPENAGENTIO_TRANSPORT"] = "nats"
    os.environ["NATS_URL"] = "nats://no-such-host:4222"
    try:
        try:
            await dial()
        except RuntimeError as e:
            assert "no-such-host" in str(e)
        else:
            raise AssertionError("Expected RuntimeError on connect failure")
    finally:
        del os.environ["OPENAGENTIO_TRANSPORT"]
        del os.environ["NATS_URL"]


async def test_nats_driver_connect_respects_connect_timeout() -> None:
    """NATSDriver.connect must not exceed connect_timeout on unreachable hosts.

    Regression: nats-py's internal reconnect loop can otherwise stall dial for
    ~120s (max_reconnect_attempts * reconnect_time_wait). The outer wait_for in
    NATSDriver.connect bounds the entire initial dial. Any regression here
    reappears as pytest-suite hangs (see remediation checklist P0 #1).
    """
    driver = _NATSDriver(url="nats://no-such-host:4222", connect_timeout=1.0)
    loop = asyncio.get_event_loop()
    start = loop.time()
    try:
        await driver.connect()
    except (TimeoutError, Exception):  # noqa: BLE001 — either wrap is fine
        pass
    finally:
        elapsed = loop.time() - start
        await driver.close()
    # 1.0s budget + generous slack for CI schedulers; hard-fail well under the
    # 120s worst-case that we are guarding against.
    assert elapsed < 5.0, f"connect took {elapsed:.2f}s, expected < 5s"


async def test_dial_nats_name_ignored_for_inmem() -> None:
    """WithNATSName should be silently ignored when transport is inmem."""
    os.environ["OPENAGENTIO_TRANSPORT"] = "inmem"
    try:
        tp = await dial(WithNATSName("should-be-ignored"))
        try:
            assert isinstance(tp, InMemoryDriver)
        finally:
            await tp.close()
    finally:
        del os.environ["OPENAGENTIO_TRANSPORT"]
