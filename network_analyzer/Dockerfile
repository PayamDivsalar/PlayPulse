# Build from the repository root so both the package and scripts/analyze_pcaps.sh
# land in the image (the compose service sets context: . for this reason):
#
#   docker compose --profile tools build network-analyzer
#
# Captures are NOT baked in; compose bind-mounts ./data/pcap -> /data/pcap.

FROM python:3.12-slim

WORKDIR /app

COPY network_analyzer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Package lives at /app/network_analyzer so `from network_analyzer...` imports
# resolve via PYTHONPATH.
COPY network_analyzer/ /app/network_analyzer/

# Batch runner used by the default --batch entrypoint mode.
COPY scripts/analyze_pcaps.sh /usr/local/bin/analyze_pcaps.sh
COPY network_analyzer/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/analyze_pcaps.sh /usr/local/bin/docker-entrypoint.sh \
    && rm -rf /app/network_analyzer/tests /app/network_analyzer/.env.example \
       /app/network_analyzer/Dockerfile /app/network_analyzer/.dockerignore \
       /app/network_analyzer/docker-entrypoint.sh

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
# Inbox layout inside the container; override only if the compose mount changes.
ENV ANALYZER_PCAP_ROOT=/data/pcap

# Dispatcher: default job is batch over the bind-mounted inbox; pass --file …
# (or any main.py flag) to analyze a single capture instead.
#
#   docker compose run --rm network-analyzer
#   docker compose run --rm network-analyzer --batch --dry-run --keep
#   docker compose run --rm network-analyzer --file /data/pcap/inbox/<name>.pcap
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["--batch"]
