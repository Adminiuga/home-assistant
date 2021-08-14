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
    assert coro.call_count == 0
    assert coro.await_count == 0

    # yield loop
    for i in range(0, 5):
        await asyncio.sleep(0)

    assert coro.call_count == 1
    assert coro.await_count == 1
    assert len(cmd_queue) == 0
    assert await res is sentinel.result
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
    assert coro.call_count == 0
    assert coro.await_count == 0
    assert not res.done()

    # yield loop
    for i in range(0, 5):
        await asyncio.sleep(0)

    assert coro.call_count == 1
    assert coro.await_count == 1
    assert len(cmd_queue) == 1
    assert not res.done()
    res.cancel()


async def test_queue_enqueue_hard_fail(cmd_queue):
    """Test retry of the command."""

    coro = AsyncMock(side_effect=ValueError)

    tag = "command_tag"

    assert len(cmd_queue) == 0
    res = cmd_queue.enqueue(
        tag,
        coro,
    )
    assert coro.call_count == 0
    assert coro.await_count == 0

    # yield loop
    for i in range(0, 5):
        await asyncio.sleep(0)

    assert coro.call_count == 1
    assert coro.await_count == 1
    assert len(cmd_queue) == 0
    with pytest.raises(Exception):
        await res


async def test_queue_enqueue_expire(cmd_queue):
    """Test command expiration."""

    coro = AsyncMock(side_effect=asyncio.TimeoutError)
    tag = "command_tag"

    assert len(cmd_queue) == 0
    res1 = cmd_queue.enqueue(
        tag,
        coro,
    )
    assert coro.call_count == 0
    assert coro.await_count == 0

    # yield loop
    for i in range(0, 5):
        await asyncio.sleep(0)

    assert coro.call_count == 1
    assert coro.await_count == 1
    assert len(cmd_queue) == 1

    res2 = cmd_queue.enqueue(
        tag,
        coro,
    )
    with patch.object(time, "time", return_value=time.time() + 3601):
        for i in range(0, 5):
            await asyncio.sleep(0)

    assert coro.call_count == 2
    assert coro.await_count == 2
    assert len(cmd_queue) == 0
    with pytest.raises(asyncio.CancelledError):
        await res1
    with pytest.raises(asyncio.TimeoutError):
        await res2


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
    with pytest.raises(asyncio.CancelledError):
        await res


async def test_queue_enqueue_deplete(cmd_queue):
    """Test queue depltion."""

    success = False

    async def _coro():
        if not success:
            raise asyncio.TimeoutError
        return sentinel.value_3

    coro1 = AsyncMock(side_effect=asyncio.TimeoutError)
    coro2 = AsyncMock(side_effect=asyncio.TimeoutError)
    tag = "command_tag_{}"

    assert len(cmd_queue) == 0
    res1 = cmd_queue.enqueue(
        tag.format(1),
        coro1,
    )
    res2 = cmd_queue.enqueue(
        tag.format(2),
        coro2,
    )

    # yield loop
    for i in range(0, 5):
        await asyncio.sleep(0)

    assert coro1.await_count == 1
    assert coro2.await_count == 1
    assert len(cmd_queue) == 2

    coro1.reset_mock()
    coro2.reset_mock()
    cmd_queue.deplete_queue()
    cmd_queue.deplete_queue()
    for i in range(0, 5):
        await asyncio.sleep(0)

    assert coro1.await_count == 1
    assert coro2.await_count == 0
    assert len(cmd_queue) == 2

    coro1.reset_mock()
    coro2.reset_mock()
    cmd_queue.deplete_queue()
    for i in range(0, 5):
        await asyncio.sleep(0)

    assert coro1.await_count == 0
    assert coro2.await_count == 1
    assert len(cmd_queue) == 2

    coro1.reset_mock()
    coro2.reset_mock()
    coro1.side_effect = (sentinel.value_3,)
    coro2.side_effect = (sentinel.value_4,)
    success = True
    cmd_queue.deplete_queue()
    await cmd_queue._deplete_queue()
    for i in range(0, 15):
        await asyncio.sleep(0)

    done, pending = await asyncio.wait(
        (res1, res2), timeout=0.1, return_when=asyncio.ALL_COMPLETED
    )

    assert coro1.await_count == 1
    assert coro2.await_count == 1
    assert len(cmd_queue) == 0
    assert len(pending) == 0
    assert len(done) == 2
    assert res1.done()
    assert res2.done()
    assert await res1 == sentinel.value_3
    assert await res2 == sentinel.value_4
