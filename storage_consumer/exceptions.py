"""Exception types, one per failure class.

Confusing these classes is how a Kafka consumer either loses data or wedges
forever, so the taxonomy is deliberately small:

* ``MessageDecodeError`` is **permanent, per message**. The record is written
  to ``dead_letter_events``, the offset advances past it, and the pipeline
  keeps going. Retrying would block the partition for good.
* ``ConfigError`` and ``MigrationError`` are **fatal**. The process exits
  non-zero and the container restart policy decides what happens next.
* Transient faults deliberately have *no* type here. They arrive as
  ``psycopg2.OperationalError`` / ``kafka.errors.KafkaError`` and are retried
  in place by ``retry_policy``; wrapping them in a subsystem exception would
  only make the retryable set harder to see.

An unregistered ``package_name`` is the fourth case, and it raises nothing:
``batching.bind_application_ids`` turns it straight into a ``RejectedRecord``
because it is discovered for a whole batch at once, not per message.
"""

from __future__ import annotations


class StorageConsumerError(Exception):
    """Base exception for storage consumer failures."""


class ConfigError(StorageConsumerError):
    """Raised when a required configuration value is missing or invalid."""


class MessageDecodeError(StorageConsumerError):
    """Raised when a Kafka record cannot be turned into a valid event.

    Covers malformed JSON, a non-object payload, a missing identity field, a
    field of the wrong type, an unparseable timestamp, and an unknown
    ``scenario`` -- all producer-side contract violations that no amount of
    retrying will fix.

    The message text is stored verbatim in ``dead_letter_events.error_reason``
    and is therefore the only thing an operator has to go on. It must name the
    offending field and what was wrong with it.
    """


class MigrationError(StorageConsumerError):
    """Raised when the schema cannot be brought up to date.

    Includes the pre-flight failure for a missing ``apps_registry_application``
    table, which is reported explicitly so the operator is told to run the
    ``app_api`` migrations rather than being handed an opaque foreign-key
    error.
    """
