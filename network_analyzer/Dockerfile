FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Package lives at /app/network_analyzer so `from network_analyzer...` imports
# resolve via PYTHONPATH.
COPY . /app/network_analyzer/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# No long-running CMD: unlike the crawler, this subsystem is an on-demand batch
# job over manually recorded captures, so there is nothing to schedule. The
# entrypoint makes the container behave like the command itself:
#
#   docker compose run --rm network-analyzer --file /data/pcap/inbox/<name>.pcap
#   docker compose run --rm network-analyzer --help
ENTRYPOINT ["python", "-m", "network_analyzer.main"]
CMD ["--help"]
