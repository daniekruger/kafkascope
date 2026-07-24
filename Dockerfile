FROM python:3.12-slim

LABEL org.opencontainers.image.title="kafkascope" \
      org.opencontainers.image.description="Web UI to inspect and operate Apache Kafka: browse/produce/search messages, manage consumer groups and topics." \
      org.opencontainers.image.source="https://github.com/daniekruger/kafkascope" \
      org.opencontainers.image.licenses="Apache-2.0"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/

# Run as a non-root user.
RUN useradd --system --uid 10001 appuser
USER appuser

EXPOSE 9000

# Liveness: the app answers /healthz without needing a broker.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:9000/healthz').status==200 else 1)"]

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9000"]
