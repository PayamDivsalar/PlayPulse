"""PostgreSQL access for the sentiment batch job.

Everything that opens a connection or runs SQL lives here. The classifier
core must never import this package.
"""
