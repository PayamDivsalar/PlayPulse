"""Thread lifecycle for the pipelines, and the process exit code.

The entire cost of the threading model is this file. Threads are used for
isolation rather than speed: separate poll loops mean a 200,000-message reviews
burst cannot delay a ``network-metrics`` message by minutes, and a database
backoff triggered by one topic cannot stall the other two.

The supervisor exists mainly to enforce one rule: **the process is alive only
while all of its pipelines are.** A container still running with two of three
pipelines silently dead would keep its healthy-looking uptime while data
quietly stopped arriving, which is worse than crashing.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable

from storage_consumer.config import Settings
from storage_consumer.messaging.pipelines import PipelineSpec
from storage_consumer.runtime.worker import Worker

logger = logging.getLogger(__name__)

WorkerFactory = Callable[[PipelineSpec, threading.Event], Worker]


class Supervisor:
    """Starts one thread per pipeline and joins them on shutdown."""

    def __init__(
        self,
        specs: tuple[PipelineSpec, ...],
        settings: Settings,
        worker_factory: WorkerFactory,
    ) -> None:
        self._specs = specs
        self._settings = settings
        self._worker_factory = worker_factory
        self._stop = threading.Event()
        # Set by any pipeline that ends badly; the process exit code is derived
        # from it. An Event is already thread-safe, so the whole subsystem
        # still needs no locks. Which pipeline failed and why is logged where
        # it happens, with a traceback, rather than accumulated here.
        self._failed = threading.Event()

    @property
    def stop_event(self) -> threading.Event:
        return self._stop

    def request_stop(self, reason: str) -> None:
        """Ask every pipeline to finish its current batch and exit.

        Safe to call from a signal handler: setting an ``Event`` is all it
        does, and the workers notice at their next poll boundary.
        """

        if not self._stop.is_set():
            logger.info("Shutdown requested (%s).", reason)
        self._stop.set()

    def run(self) -> int:
        """Run every pipeline until one fails or a shutdown is requested.

        Returns the process exit code: zero for a clean shutdown, one if any
        pipeline ended in an error.
        """

        threads = [
            threading.Thread(
                target=self._run_pipeline,
                args=(spec,),
                name=f"pipeline-{spec.name}",
                daemon=False,
            )
            for spec in self._specs
        ]

        logger.info(
            "Starting %s pipeline(s): %s",
            len(threads),
            ", ".join(spec.name for spec in self._specs),
        )
        for thread in threads:
            thread.start()

        # Block here rather than joining straight away, so the main thread
        # stays free to run signal handlers. Python delivers signals only to
        # the main thread, and a blocking join would delay SIGTERM until a
        # worker happened to finish.
        self._stop.wait()

        for thread in threads:
            thread.join(timeout=self._settings.shutdown_timeout_seconds)
            if thread.is_alive():
                logger.error(
                    "Pipeline thread %s did not stop within %.0fs; exiting anyway. "
                    "Its in-flight batch was not committed and will be "
                    "redelivered.",
                    thread.name,
                    self._settings.shutdown_timeout_seconds,
                )

        return self._exit_code()

    def _run_pipeline(self, spec: PipelineSpec) -> None:
        """Body of one pipeline thread.

        Any failure stops the whole process. Dying quietly and leaving the
        siblings running is precisely the half-alive state this design rules
        out; ``restart: unless-stopped`` then brings everything back, and
        because the write path is idempotent, resuming from the last committed
        offset is safe.
        """

        worker: Worker | None = None
        try:
            worker = self._worker_factory(spec, self._stop)
            worker.run()
        except BaseException:  # noqa: BLE001 - deliberately total
            self._failed.set()
            logger.error("[%s] Pipeline failed.", spec.name, exc_info=True)
        else:
            if not self._stop.is_set():
                self._failed.set()
                logger.error(
                    "[%s] Pipeline returned unexpectedly while still running.",
                    spec.name,
                )
        finally:
            if worker is not None:
                try:
                    worker.close()
                except Exception:
                    logger.warning(
                        "[%s] Worker did not close cleanly.", spec.name, exc_info=True
                    )
            # Whether this thread failed or finished, no pipeline outlives
            # another.
            self.request_stop(f"pipeline {spec.name} finished")

    def _exit_code(self) -> int:
        if self._failed.is_set():
            return 1

        logger.info("All pipelines stopped cleanly.")
        return 0
