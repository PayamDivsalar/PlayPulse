"""A file whose modification time proves a pipeline is still turning.

Liveness without an HTTP server. Each worker touches its own file every loop,
and ``storage_consumer/healthcheck.sh`` checks that every expected file is
recent. That keeps the container healthcheck honest per pipeline: a process
whose reviews thread has wedged is not healthy just because it is still
running.

Touching is rate-limited, so a busy loop does not turn into a syscall per poll.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from pathlib import Path

logger = logging.getLogger(__name__)


class Heartbeat:
    """Periodically refreshes one file's mtime.

    Every failure is swallowed after a single warning. A full or read-only
    ``/tmp`` is a monitoring problem; letting it stop a pipeline from writing
    to Postgres would turn a cosmetic fault into an outage.
    """

    def __init__(
        self,
        directory: str | Path,
        name: str,
        *,
        interval_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.path = Path(directory) / f"{name}.heartbeat"
        self._interval_seconds = interval_seconds
        self._clock = clock
        self._next_touch_at = 0.0
        self._warned = False

    def touch(self, *, force: bool = False) -> None:
        """Refresh the file if the interval has elapsed.

        ``force`` writes unconditionally, which the worker uses once at startup
        so the file exists before the first poll returns and the healthcheck
        does not fail a container that is merely idle.
        """

        now = self._clock()
        if not force and now < self._next_touch_at:
            return
        self._next_touch_at = now + self._interval_seconds

        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.touch()
        except OSError as exc:
            if not self._warned:
                logger.warning(
                    "Cannot write the heartbeat file %s (%s). Ingestion is "
                    "unaffected, but the container healthcheck will fail.",
                    self.path,
                    exc,
                )
                self._warned = True

    def remove(self) -> None:
        """Delete the file on a clean shutdown.

        A stopped pipeline should read as absent rather than stale, which is
        the difference between "it exited" and "it hung".
        """

        try:
            self.path.unlink(missing_ok=True)
        except OSError:
            logger.debug("Could not remove %s.", self.path, exc_info=True)
