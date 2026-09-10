"""Everything in this subsystem that touches PostgreSQL.

The split between this package and the flat modules above it is the same one
``network_analyzer/analysis/`` keeps against Kafka and HTTP: ``events.py``,
``decoders.py`` and ``batching.py`` are pure functions over dicts and DTOs,
while every connection, cursor and SQL string lives here. That boundary is what
makes the wire contract testable without a broker or a database.
"""
