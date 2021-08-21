"""Deferred command queue helpers."""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import time
from typing import Any, Awaitable, Callable

import zigpy.exceptions

_LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass
class DeferredCommand:
    """Container for the command."""

    tag: str
    func: Callable
    args: tuple[Any]
    kwargs: dict[str, Any]
    high_priority: bool = False
    ttl: int = 3600
    future: asyncio.Future = dataclasses.field(
        default_factory=asyncio.Future, init=False
    )
    _created_on: float = dataclasses.field(default_factory=time.time)

    @property
    def expired(self) -> bool:
        """Return True if exceeding TTL."""
        return time.time() - self._created_on > self.ttl


class RetryableQueue:
    """A queue of commands to retry."""

    def __init__(self) -> None:
        """Initialize instance."""
        self._queue: dict[str, DeferredCommand] = {}
        self._deplete_task: asyncio.Task | None = None

    def __len__(self) -> int:
        """Return queue length."""
        return len(self._queue)

    def enqueue(
        self,
        tag: str,
        func: Callable,
        *args,
        high_priority: bool = False,
        ttl: int = 3600,
        **kwargs,
    ) -> Awaitable:
        """Enqueue command."""

        cmd = DeferredCommand(
            tag, func, args, kwargs, high_priority=high_priority, ttl=ttl
        )

        # new command cancels all previous instances
        existing_cmd = self._queue.pop(tag, None)
        if existing_cmd and not existing_cmd.future.done():
            existing_cmd.future.cancel()

        asyncio.create_task(self.catching_run(cmd))
        return asyncio.create_task(self._unwrap(cmd, ttl))

    async def _unwrap(self, cmd: DeferredCommand, ttl: int) -> Any:
        """Unwrap future result or raise TimeoutError if exceed TTL."""
        try:
            await asyncio.wait_for(cmd.future, timeout=ttl + 1)
        except asyncio.TimeoutError:
            self._queue.pop(cmd.tag, None)
            raise

        return cmd.future.result()

    async def catching_run(self, cmd: DeferredCommand) -> None:
        """Run cmd but catch any exceptions."""
        try:
            await self._run(cmd)
        except asyncio.CancelledError:
            raise
        except (asyncio.TimeoutError, zigpy.exceptions.DeliveryError):
            # these are allowed to fail
            pass
        except Exception as exc:  # pylint: disable=broad-except
            _LOGGER.warning(
                "Unexpected failure executing %s(%s, %s)",
                cmd.tag,
                cmd.args,
                cmd.kwargs,
                exc_info=exc,
            )

    async def _run(self, cmd: DeferredCommand) -> None:
        """Run the deferred command."""

        self._queue.pop(cmd.tag, None)
        try:
            res = await cmd.func(*cmd.args, **cmd.kwargs)
        except (asyncio.TimeoutError, zigpy.exceptions.DeliveryError) as exc:
            # well, maybe better luck next time, unless ttl expired
            if cmd.expired:
                _LOGGER.debug(
                    "Command '%s' has expired, not retrying", cmd.tag, exc_info=exc
                )
                cmd.future.set_exception(exc)
            else:
                self._queue[cmd.tag] = cmd
            raise
        except (Exception, asyncio.CancelledError) as exc:
            _LOGGER.debug(
                "Fatal exception running %s(%s, %s)",
                cmd.tag,
                cmd.args,
                cmd.kwargs,
                exc_info=exc,
            )
            cmd.future.set_exception(exc)
            raise
        else:
            _LOGGER.debug(
                "Success running %s(%s, %s) -> %s", cmd.tag, cmd.args, cmd.kwargs, res
            )
            cmd.future.set_result(res)

    def deplete_queue(self) -> None:
        """Schedule queue depletion."""

        if self._deplete_task and not self._deplete_task.done():
            return

        self._deplete_task = asyncio.create_task(self._deplete_queue())

    async def _deplete_queue(self) -> None:
        """Deplete queue as long as task are successful."""

        for cmd in [*self._queue.values()]:
            try:
                await self._run(cmd)
            except asyncio.CancelledError:
                raise
            except Exception:  # pylint: disable=broad-except
                break
