# One image for collector / processor / etl / tests; the command selects the service.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml ./
COPY src ./src
RUN pip install ".[dev]"

COPY tests ./tests

RUN useradd --create-home --uid 1000 app && mkdir -p /data/lake && chown app /data/lake
USER app

CMD ["python", "-m", "telemetry.processor"]
