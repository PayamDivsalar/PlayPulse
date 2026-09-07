FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Package lives at /app/crawler so `from crawler...` imports resolve via PYTHONPATH.
COPY . /app/crawler/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "crawler.main"]
