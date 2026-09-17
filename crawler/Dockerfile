FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Package lives at /app/crawler so `from crawler...` imports resolve via PYTHONPATH.
COPY . /app/crawler/

# Strip test/dev artifacts from the runtime image; compose test services
# remount ./crawler/tests from the host.
RUN rm -rf /app/crawler/tests \
       /app/crawler/venv \
       /app/crawler/.env \
       /app/crawler/.env.example \
       /app/crawler/Dockerfile \
       /app/crawler/.dockerignore \
       /app/crawler/README.md

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "crawler.main"]
