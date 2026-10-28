FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml ./
COPY app ./app
COPY migrations ./migrations
COPY scripts ./scripts
COPY alembic.ini ./

RUN python -m pip install --no-cache-dir . \
    && useradd --create-home --uid 10001 scheduler

USER scheduler

CMD ["python", "-m", "app.consumers"]
