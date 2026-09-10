# Build context is ./storage_consumer (see the compose service). Unlike the
# network-analyzer image this needs nothing from the repository root: the SQL
# migrations ship inside the package, at storage_consumer/persistence/sql/.

FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Package lives at /app/storage_consumer so `from storage_consumer...` imports
# resolve via PYTHONPATH.
COPY . /app/storage_consumer/

RUN cp /app/storage_consumer/docker-entrypoint.sh \
       /app/storage_consumer/healthcheck.sh \
       /usr/local/bin/ \
    && chmod +x /usr/local/bin/docker-entrypoint.sh /usr/local/bin/healthcheck.sh \
    && rm -rf /app/storage_consumer/tests \
       /app/storage_consumer/venv \
       /app/storage_consumer/.env \
       /app/storage_consumer/.env.example \
       /app/storage_consumer/Dockerfile \
       /app/storage_consumer/.dockerignore \
       /app/storage_consumer/README.md \
       /app/storage_consumer/docker-entrypoint.sh \
       /app/storage_consumer/healthcheck.sh

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
# Where each worker touches its liveness file, and where healthcheck.sh looks.
ENV STORAGE_HEARTBEAT_DIRECTORY=/tmp/storage-consumer

# Migrate, then consume. Both are idempotent, so an unhealthy container can
# simply be restarted. Passing another subcommand (`migrate`, `--help`) runs
# that instead; see docker-entrypoint.sh.
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["run"]
