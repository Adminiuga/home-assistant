"""Test zha queue."""
import asyncio
import time
from unittest.mock import AsyncMock, patch, sentinel

import pytest

from homeassistant.components.zha.core import queue


@pytest.fixture
def cmd_queue(loop):
    """Command queue fixture."""
    return queue.RetryableQueue()


def test_deferred_cmd_expiration():
    """Test DeferredCommand expiration."""

    coro = AsyncMock()
    cmd = queue.DeferredCommand(
        sentinel.tag, coro, (sentinel.arg1, sentinel.arg2), {"retries": 1}, ttl=10
    )

    assert cmd.expired is False
    with patch.object(time, "time", return_value=time.time() + 5):
        assert cmd.expired is False
    with patch.object(time, "time", return_value=time.time() + 15):
        assert cmd.expired is True

    assert coro.call_count == 0
    assert coro.await_count == 0


async def test_queue_enqueue_single_run(cmd_queue):
    """Test enqueuing single command."""

    coro = AsyncMock(return_value=sentinel.result)
    tag = "command_tag"

    assert len(cmd_queue) == 0
    res = cmd_queue.enqueue(
        tag,
        coro,
        sentinel.arg1,
        sentinel.arg2,
        retries=2,
        tsn=22,
        high_priority=True,
        ttl=20,
    )
    assert res.done() is False
    assert coro.call_count == 0
    assert coro.await_count == 0

    # yield loop
    for i in range(0, 5):
        await asyncio.sleep(0)

    assert coro.call_count == 1
    assert coro.await_count == 1
    assert len(cmd_queue) == 0
    assert res.done()
    assert res.result() is sentinel.result
    args, kwargs = coro.await_args
    assert args[0] is sentinel.arg1
    assert args[1] is sentinel.arg2
    assert kwargs == {"retries": 2, "tsn": 22}


async def test_queue_enqueue_retry(cmd_queue):
    """Test retry of the command."""

    coro = AsyncMock(side_effect=asyncio.TimeoutError)

    tag = "command_tag"

    assert len(cmd_queue) == 0
    res = cmd_queue.enqueue(
        tag,
        coro,
    )
    assert res.done() is False
    assert coro.call_count == 0
    assert coro.await_count == 0

    # yield loop
    for i in range(0, 5):
        await asyncio.sleep(0)

    assert coro.call_count == 1
    assert coro.await_count == 1
    assert len(cmd_queue) == 1
    assert res.done() is False


async def test_queue_enqueue_hard_fail(cmd_queue):
    """Test retry of the command."""

    coro = AsyncMock(side_effect=ValueError)

    tag = "command_tag"

    assert len(cmd_queue) == 0
    res = cmd_queue.enqueue(
        tag,
        coro,
    )
    assert res.done() is False
    assert coro.call_count == 0
    assert coro.await_count == 0

    # yield loop
    for i in range(0, 5):
        await asyncio.sleep(0)

    assert coro.call_count == 1
    assert coro.await_count == 1
    assert len(cmd_queue) == 0
    assert res.done() is True
    assert res.exception()


async def test_queue_enqueue_expire(cmd_queue):
    """Test command expiration."""

    coro = AsyncMock(side_effect=asyncio.TimeoutError)
    tag = "command_tag"

    assert len(cmd_queue) == 0
    res = cmd_queue.enqueue(
        tag,
        coro,
    )
    assert res.done() is False
    assert coro.call_count == 0
    assert coro.await_count == 0

    # yield loop
    for i in range(0, 5):
        await asyncio.sleep(0)

    assert coro.call_count == 1
    assert coro.await_count == 1
    assert len(cmd_queue) == 1
    assert res.done() is False

    res = cmd_queue.enqueue(
        tag,
        coro,
    )
    with patch.object(time, "time", return_value=time.time() + 3601):
        for i in range(0, 5):
            await asyncio.sleep(0)

    assert coro.call_count == 2
    assert coro.await_count == 2
    assert len(cmd_queue) == 0
    assert res.done() is True
    with pytest.raises(asyncio.TimeoutError):
        res.result()


async def test_queue_enqueue_cancelled(cmd_queue):
    """Test command expiration."""

    coro = AsyncMock(side_effect=asyncio.CancelledError)
    tag = "command_tag"

    assert len(cmd_queue) == 0
    res = cmd_queue.enqueue(
        tag,
        coro,
    )

    # yield loop
    for i in range(0, 5):
        await asyncio.sleep(0)

    assert coro.await_count == 1
    assert len(cmd_queue) == 0
    assert res.done() is True
    with pytest.raises(asyncio.CancelledError):
        res.result()
