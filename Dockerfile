FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /srv

COPY pyproject.toml ./
COPY app ./app
RUN pip install .

RUN useradd --create-home --uid 10001 svc && chown -R svc /srv
USER svc

EXPOSE 8003
HEALTHCHECK --interval=10s --timeout=6s --start-period=20s --retries=5 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8003/health', timeout=5)"
# Several worker processes per container (WEB_CONCURRENCY); scale further with more replicas.
# Each worker keeps its own pool: DB_POOL_SIZE + DB_MAX_OVERFLOW connections at most.
ENV WEB_CONCURRENCY=4
CMD ["sh", "-c", "exec uvicorn app.main:app --host 0.0.0.0 --port 8003 --workers ${WEB_CONCURRENCY} --proxy-headers --timeout-keep-alive 30"]
