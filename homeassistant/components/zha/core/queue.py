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
        return cmd.future

    async def catching_run(self, cmd: DeferredCommand) -> None:
        """Run cmd but catch any exceptions."""
        try:
            await self._run(cmd)
        except asyncio.CancelledError:
            raise
        except Exception:  # pylint: disable=broad-except
            pass

    async def _run(self, cmd: DeferredCommand) -> None:
        """Run the deferred command."""

        self._queue.pop(cmd.tag, None)
        try:
            res = await cmd.func(*cmd.args, **cmd.kwargs)
        except (asyncio.TimeoutError, zigpy.exceptions.DeliveryError) as exc:
            # well, maybe better luck next time, unless ttl expired
            if cmd.expired:
                _LOGGER.debug("Command %s has expired, not retrying: %s", cmd.tag, exc)
                cmd.future.set_exception(exc)
            else:
                self._queue[cmd.tag] = cmd
            raise
        except (Exception, asyncio.CancelledError) as exc:
            _LOGGER.debug(
                "Fatal exception running %s(%s, %s): %s",
                cmd.tag,
                cmd.args,
                cmd.kwargs,
                exc,
            )
            cmd.future.set_exception(exc)
            raise
        else:
            _LOGGER.debug(
                "Success running %s(%s, %s) -> %s", cmd.tag, cmd.args, cmd.kwargs, res
            )
            cmd.future.set_result(res)
