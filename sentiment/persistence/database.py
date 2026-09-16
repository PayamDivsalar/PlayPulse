"""PostgreSQL connection ownership for the sentiment batch job.

Copied from the ``storage_consumer.persistence.database`` pattern: one lazily
opened connection, explicit transactions, session-level timeouts. The job is
single-threaded, so there is no pool and no locking.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg2
import psycopg2.extensions

from sentiment.config import Settings

logger = logging.getLogger(__name__)


class Database:
    """A single lazily-opened Postgres connection with explicit transactions."""

    def __init__(
        self, settings: Settings, *, application_name: str = "sentiment"
    ) -> None:
        self._settings = settings
        self._application_name = application_name
        self._connection: psycopg2.extensions.connection | None = None

    @property
    def is_connected(self) -> bool:
        return self._connection is not None and self._connection.closed == 0

    def connect(self) -> psycopg2.extensions.connection:
        """Return the open connection, opening one if needed."""

        if self.is_connected:
            assert self._connection is not None
            return self._connection

        settings = self._settings
        logger.info(
            "Connecting to postgres %s:%s/%s as %s",
            settings.postgres_host,
            settings.postgres_port,
            settings.postgres_db,
            self._application_name,
        )
        self._connection = psycopg2.connect(
            dbname=settings.postgres_db,
            user=settings.postgres_user,
            password=settings.postgres_password,
            host=settings.postgres_host,
            port=settings.postgres_port,
            connect_timeout=settings.db_connect_timeout_seconds,
            application_name=self._application_name,
            options=(
                f"-c statement_timeout={settings.db_statement_timeout_ms}"
                " -c idle_in_transaction_session_timeout="
                f"{settings.db_idle_in_transaction_timeout_ms}"
            ),
        )
        self._connection.autocommit = False
        return self._connection

    @contextmanager
    def transaction(self) -> Iterator[Any]:
        """Run a block in one transaction, committing only if it returns."""

        connection = self.connect()
        cursor = connection.cursor()
        try:
            yield cursor
        except BaseException:
            try:
                connection.rollback()
            except psycopg2.Error:
                logger.warning("Rollback failed; the connection will be replaced.")
            raise
        else:
            connection.commit()
        finally:
            try:
                cursor.close()
            except psycopg2.Error:
                pass

    def close(self) -> None:
        """Close the connection, ignoring a connection that is already gone."""

        if self._connection is None:
            return
        try:
            self._connection.close()
        except psycopg2.Error:
            logger.debug("Ignoring error while closing the connection.", exc_info=True)
        finally:
            self._connection = None
