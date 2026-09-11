# Build from repo root:
#   docker build -f api/Dockerfile -t recording-transcription-api .
FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    API_HOST=0.0.0.0 \
    API_PORT=8000

# HTTPS（调 LLM）等需要；slim 镜像显式保证证书完整
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 仅安装运行依赖（测试见 requirements-dev.txt）
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY alembic.ini .
COPY alembic ./alembic
COPY api ./api

RUN mkdir -p /app/logs /app/uploads \
    && useradd --create-home --uid 1000 --shell /usr/sbin/nologin appuser \
    && chown -R appuser:appuser /app \
    && chmod +x /app/api/docker-entrypoint.sh

EXPOSE 8000

# 以 root 进 entrypoint 修挂载目录权限后，再降权为 appuser 跑服务
ENTRYPOINT ["/app/api/docker-entrypoint.sh"]
CMD ["sh", "-c", "exec uvicorn api.app:app --host ${API_HOST} --port ${API_PORT}"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3)"]
