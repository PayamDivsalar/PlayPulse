# Build context is ./app_api (see the compose service). Django's manage.py,
# config/, and apps_registry/ all live at the image WORKDIR so `manage.py`
# resolves without a PYTHONPATH dance.

FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/

RUN cp /app/docker-entrypoint.sh /app/healthcheck.sh /usr/local/bin/ \
    && chmod +x /usr/local/bin/docker-entrypoint.sh /usr/local/bin/healthcheck.sh \
    && rm -rf /app/venv \
       /app/.venv \
       /app/.env \
       /app/.env.example \
       /app/db.sqlite3 \
       /app/Dockerfile \
       /app/.dockerignore \
       /app/docker-entrypoint.sh \
       /app/healthcheck.sh

ENV PYTHONUNBUFFERED=1
# DJANGO_SETTINGS_MODULE is also set by manage.py; keep it explicit for
# non-manage entrypoints (e.g. future gunicorn).
ENV DJANGO_SETTINGS_MODULE=config.settings

EXPOSE 8000

# Migrate, then serve. Passing another subcommand (`migrate`, `test`, `shell`)
# runs that instead; see docker-entrypoint.sh.
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["run"]
