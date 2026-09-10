"""Tests for thread lifecycle and the process exit code.

The property worth protecting here is that the process is alive only while all
of its pipelines are. A container still running with two of three pipelines
silently dead keeps a healthy-looking uptime while data stops arriving, which
is harder to notice than a crash loop.
"""

from __future__ import annotations

import threading
import unittest

from storage_consumer.config import Settings
from storage_consumer.decoders import decode_app_stats
from storage_consumer.pipelines import PipelineSpec, build_pipelines
from storage_consumer.supervisor import Supervisor


def _spec(name: str) -> PipelineSpec:
    return PipelineSpec(
        name=name,
        topic=name,
        group_id=f"storage-consumer.{name}",
        decoder=decode_app_stats,
        repository=None,  # type: ignore[arg-type]
        conflict_key="(id)",
        max_poll_records=10,
        poll_timeout_ms=10,
    )


class FakeWorker:
    """Blocks until stopped, unless told to fail or to return early."""

    def __init__(
        self,
        stop_event: threading.Event,
        *,
        failure: BaseException | None = None,
        return_early: bool = False,
    ) -> None:
        self.stop_event = stop_event
        self.failure = failure
        self.return_early = return_early
        self.ran = threading.Event()
        self.closed = threading.Event()

    def run(self) -> None:
        self.ran.set()
        if self.failure is not None:
            raise self.failure
        if self.return_early:
            return
        self.stop_event.wait(timeout=5.0)

    def close(self) -> None:
        self.closed.set()


class SupervisorTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = Settings.for_testing(shutdown_timeout_seconds=5.0)
        self.workers: dict[str, FakeWorker] = {}

    def _factory(self, **worker_kwargs):
        def build(spec: PipelineSpec, stop_event: threading.Event) -> FakeWorker:
            worker = FakeWorker(stop_event, **worker_kwargs.get(spec.name, {}))
            self.workers[spec.name] = worker
            return worker

        return build


class CleanShutdownTests(SupervisorTestCase):
    def test_every_pipeline_gets_its_own_thread(self) -> None:
        specs = tuple(_spec(name) for name in ("app-stats", "reviews"))
        supervisor = Supervisor(specs, self.settings, self._factory())

        stopper = threading.Timer(0.2, supervisor.request_stop, args=("test",))
        stopper.start()
        exit_code = supervisor.run()
        stopper.cancel()

        self.assertEqual(exit_code, 0)
        self.assertEqual(set(self.workers), {"app-stats", "reviews"})
        for worker in self.workers.values():
            self.assertTrue(worker.ran.is_set())

    def test_a_clean_shutdown_exits_zero(self) -> None:
        supervisor = Supervisor((_spec("app-stats"),), self.settings, self._factory())

        stopper = threading.Timer(0.2, supervisor.request_stop, args=("SIGTERM",))
        stopper.start()
        exit_code = supervisor.run()
        stopper.cancel()

        self.assertEqual(exit_code, 0)

    def test_every_worker_is_closed_on_shutdown(self) -> None:
        specs = tuple(_spec(name) for name in ("app-stats", "reviews"))
        supervisor = Supervisor(specs, self.settings, self._factory())

        stopper = threading.Timer(0.2, supervisor.request_stop, args=("test",))
        stopper.start()
        supervisor.run()
        stopper.cancel()

        for name, worker in self.workers.items():
            with self.subTest(pipeline=name):
                self.assertTrue(worker.closed.is_set())

    def test_requesting_stop_before_running_returns_immediately(self) -> None:
        supervisor = Supervisor((_spec("app-stats"),), self.settings, self._factory())
        supervisor.request_stop("early")

        self.assertEqual(supervisor.run(), 0)


class FailurePropagationTests(SupervisorTestCase):
    def test_one_failing_pipeline_stops_the_others(self) -> None:
        """A half-alive process is the outcome this design rules out."""

        specs = tuple(_spec(name) for name in ("app-stats", "reviews"))
        supervisor = Supervisor(
            specs,
            self.settings,
            self._factory(**{"app-stats": {"failure": RuntimeError("boom")}}),
        )

        exit_code = supervisor.run()

        self.assertEqual(exit_code, 1)
        self.assertTrue(self.workers["reviews"].closed.is_set())

    def test_a_failure_produces_a_non_zero_exit_code(self) -> None:
        supervisor = Supervisor(
            (_spec("app-stats"),),
            self.settings,
            self._factory(**{"app-stats": {"failure": RuntimeError("boom")}}),
        )

        self.assertEqual(supervisor.run(), 1)

    def test_a_pipeline_returning_early_counts_as_a_failure(self) -> None:
        """A worker may only return once a stop has been requested."""

        supervisor = Supervisor(
            (_spec("app-stats"),),
            self.settings,
            self._factory(**{"app-stats": {"return_early": True}}),
        )

        self.assertEqual(supervisor.run(), 1)

    def test_a_worker_that_cannot_be_built_is_a_failure(self) -> None:
        def failing_factory(spec, stop_event):
            raise RuntimeError("no broker")

        supervisor = Supervisor(
            (_spec("app-stats"),), self.settings, failing_factory
        )

        self.assertEqual(supervisor.run(), 1)

    def test_a_failing_close_does_not_mask_a_clean_run(self) -> None:
        class BadCloseWorker(FakeWorker):
            def close(self) -> None:
                super().close()
                raise RuntimeError("close failed")

        def factory(spec, stop_event):
            worker = BadCloseWorker(stop_event)
            self.workers[spec.name] = worker
            return worker

        supervisor = Supervisor((_spec("app-stats"),), self.settings, factory)
        stopper = threading.Timer(0.2, supervisor.request_stop, args=("test",))
        stopper.start()
        exit_code = supervisor.run()
        stopper.cancel()

        self.assertEqual(exit_code, 0)


class PipelineSelectionTests(unittest.TestCase):
    def test_all_three_pipelines_are_built_by_default(self) -> None:
        specs = build_pipelines(Settings.for_testing())

        self.assertEqual(
            [spec.name for spec in specs],
            ["app-stats", "reviews", "network-metrics"],
        )

    def test_only_the_selected_pipeline_is_built(self) -> None:
        specs = build_pipelines(Settings.for_testing(pipelines=("reviews",)))

        self.assertEqual([spec.name for spec in specs], ["reviews"])

    def test_each_pipeline_has_its_own_consumer_group(self) -> None:
        """Which is what lets one topic be reset without replaying the others."""

        groups = {spec.group_id for spec in build_pipelines(Settings.for_testing())}

        self.assertEqual(len(groups), 3)

    def test_reviews_batch_bigger_than_network_metrics(self) -> None:
        """Throughput for the burst topic, latency for the on-demand one."""

        specs = {spec.name: spec for spec in build_pipelines(Settings.for_testing())}

        self.assertGreater(
            specs["reviews"].max_poll_records,
            specs["network-metrics"].max_poll_records,
        )

    def test_an_override_applies_to_every_pipeline(self) -> None:
        specs = build_pipelines(
            Settings.for_testing(max_poll_records_override=25)
        )

        self.assertEqual({spec.max_poll_records for spec in specs}, {25})

    def test_a_per_pipeline_override_leaves_siblings_on_their_defaults(self) -> None:
        specs = {
            spec.name: spec
            for spec in build_pipelines(
                Settings.for_testing(
                    max_poll_records_by_pipeline={"reviews": 1000}
                )
            )
        }

        self.assertEqual(specs["reviews"].max_poll_records, 1000)
        self.assertEqual(specs["app-stats"].max_poll_records, 200)
        self.assertEqual(specs["network-metrics"].max_poll_records, 50)

    def test_a_per_pipeline_override_wins_over_the_global_one(self) -> None:
        specs = {
            spec.name: spec
            for spec in build_pipelines(
                Settings.for_testing(
                    max_poll_records_override=25,
                    max_poll_records_by_pipeline={"reviews": 1000},
                )
            )
        }

        self.assertEqual(specs["reviews"].max_poll_records, 1000)
        self.assertEqual(specs["network-metrics"].max_poll_records, 25)

    def test_each_pipeline_declares_the_key_that_makes_replay_safe(self) -> None:
        for spec in build_pipelines(Settings.for_testing()):
            with self.subTest(pipeline=spec.name):
                self.assertTrue(spec.conflict_key.startswith("("))


if __name__ == "__main__":
    unittest.main()
