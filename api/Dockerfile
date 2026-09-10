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

# 先装依赖，改代码时复用该层缓存
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY api ./api

RUN mkdir -p /app/logs /app/uploads \
    && useradd --create-home --uid 1000 --shell /usr/sbin/nologin appuser \
    && chown -R appuser:appuser /app

USER appuser

EXPOSE 8000

# 注意：需 api.app:app 已定义；当前 api/app.py 为空时容器会启动失败
CMD ["sh", "-c", "exec uvicorn api.app:app --host ${API_HOST} --port ${API_PORT}"]
