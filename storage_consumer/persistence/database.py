"""PostgreSQL connection ownership for one pipeline thread.

``psycopg2`` connections are **not** thread-safe, so exactly one ``Database``
instance belongs to exactly one thread. That strict ownership is what lets the
whole subsystem run without a single lock; a shared pool would buy nothing here
because each pipeline issues one batch insert at a time.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg2
import psycopg2.extensions

from storage_consumer.config import Settings

logger = logging.getLogger(__name__)


class Database:
    """A single lazily-opened Postgres connection with explicit transactions.

    Session-level timeouts are set at connect time rather than per statement:
    a hung write must not be able to pin a pipeline forever, and
    ``idle_in_transaction_session_timeout`` guarantees an abandoned transaction
    cannot hold row locks against the other pipelines indefinitely.
    """

    def __init__(self, settings: Settings, *, application_name: str) -> None:
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
            # Shows up in pg_stat_activity, so an operator can tell which
            # pipeline is holding a lock without guessing from the query text.
            application_name=self._application_name,
            options=(
                f"-c statement_timeout={settings.db_statement_timeout_ms}"
                " -c idle_in_transaction_session_timeout="
                f"{settings.db_idle_in_transaction_timeout_ms}"
            ),
        )
        # Explicit transactions only: the batch boundary is the transaction
        # boundary, and section 4's ordering guarantee depends on it.
        self._connection.autocommit = False
        return self._connection

    def reconnect(self) -> None:
        """Drop the current connection and open a fresh one.

        Called from the batch retry path on ``OperationalError``: after the
        server has gone away the existing connection is useless, and retrying
        the batch on it would fail immediately with ``InterfaceError`` for the
        rest of the retry budget.
        """

        self.close()
        self.connect()

    @contextmanager
    def transaction(self) -> Iterator[Any]:
        """Run a block in one transaction, committing only if it returns.

        One batch is one transaction. Good rows and their dead-letter siblings
        therefore land atomically, so a crash can never leave a poisoned
        message recorded-but-unadvanced or advanced-but-unrecorded.
        """

        connection = self.connect()
        cursor = connection.cursor()
        try:
            yield cursor
        except BaseException:
            # Includes KeyboardInterrupt on purpose: a Ctrl-C mid-batch must
            # not leave a transaction open on the server.
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
