# Build context is ./sentiment (see the compose service).
#
#   docker compose --profile tools build sentiment
#   docker compose run --rm sentiment run

FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

# CPU-only torch first — several GB smaller than the default CUDA wheel.
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu \
 && pip install --no-cache-dir -r requirements.txt

# Package lives at /app/sentiment so `from sentiment...` imports resolve via
# PYTHONPATH.
COPY . /app/sentiment/

# Bake the model into the image so the first `docker compose run` does not
# wait on a several-hundred-MB download.
RUN python -c "from transformers import pipeline; pipeline('sentiment-analysis', model='cardiffnlp/twitter-xlm-roberta-base-sentiment')"

RUN rm -rf /app/sentiment/tests \
       /app/sentiment/venv \
       /app/sentiment/.env \
       /app/sentiment/.env.example \
       /app/sentiment/Dockerfile \
       /app/sentiment/.dockerignore \
       /app/sentiment/README.md \
       /app/sentiment/*.csv \
       /app/sentiment/main2.py

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
# Hugging Face cache stays inside the image layer created by the RUN above.
ENV HF_HOME=/root/.cache/huggingface

ENTRYPOINT ["python", "-m", "sentiment.main"]
CMD ["run"]
