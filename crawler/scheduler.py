"""Hourly crawl scheduling with APScheduler."""

from __future__ import annotations

import logging
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler

from crawler.crawler_service import CrawlerService

logger = logging.getLogger(__name__)

_CRAWL_JOB_ID = "crawl_cycle"


def build_scheduler(
    crawler_service: CrawlerService,
    *,
    interval_hours: int,
) -> BlockingScheduler:
    """Create a BlockingScheduler that runs crawl cycles on a fixed interval.

    BlockingScheduler is used (instead of BackgroundScheduler) because this
    process exists only to keep the crawler alive: start() blocks the main
    thread and replaces an idle sleep loop. BackgroundScheduler would need an
    extra ``while True: sleep(...)`` just to keep the process from exiting.
    """

    def _run_cycle() -> None:
        try:
            crawler_service.run_crawl_cycle()
        except Exception:
            # Keep the scheduler alive even if a whole cycle blows up
            # (e.g. App API unreachable after retries).
            logger.exception("Crawl cycle failed with an unexpected error")

    scheduler = BlockingScheduler()
    scheduler.add_job(
        _run_cycle,
        trigger="interval",
        hours=interval_hours,
        id=_CRAWL_JOB_ID,
        max_instances=1,
        coalesce=True,
        next_run_time=datetime.now(),
    )
    return scheduler
