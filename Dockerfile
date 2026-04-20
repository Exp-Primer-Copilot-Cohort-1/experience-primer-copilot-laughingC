FROM python:3.11-slim AS builder
WORKDIR /app
COPY requirements.txt .
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /app/wheels -r requirements.txt

FROM python:3.11-slim
WORKDIR /app

RUN addgroup --system worker && adduser --system --group worker

COPY --from=builder /app/wheels /wheels
COPY --from=builder /app/requirements.txt .
RUN pip install --no-cache /wheels/*

COPY src/ ./src/
RUN chown -R worker:worker /app

USER worker

HEALTHCHECK --interval=30s --timeout=5s CMD python -c "import boto3, google.cloud.storage; exit(0)"

CMD ["python", "src/worker.py"]
