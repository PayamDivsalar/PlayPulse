"""Pure classification core.

Only the Python standard library and ``transformers`` are allowed here.
Never import ``psycopg2``, ``sentiment.config``, or any persistence module —
that boundary is what keeps the classifier unit-testable without a database.
"""
