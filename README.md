# 录音转写与智能摘要 API

客户端上传音频后，服务端异步完成 **Mock ASR 转写** → **LLM 结构化摘要**，并提供任务查询、列表/详情、失败重试、删除，以及摘要 SSE 流式接口。

## 一键启动（推荐）

依赖：Docker + Docker Compose。

```bash
# 仓库根目录
cp .env.example .env
# 评分/演示请填写 LLM_API_KEY（勿加引号，写成 LLM_API_KEY=sk-...）——有 Key 时走真实摘要
# 未填写时 .env.example 默认 WORKER_ALLOW_MOCK_LLM=true，仅用于无 Key 一键跑通（此项会降分）

docker compose -f docker/docker-compose.yaml up --build
```

- API：http://127.0.0.1:8000
- OpenAPI 文档：http://127.0.0.1:8000/docs
- Postgres：宿主机 `localhost:5433`（容器内 `db:5432`）

停止：

```bash
docker compose -f docker/docker-compose.yaml down
```

### 本地开发（不用 Docker 跑 API）

依赖：Python 3.9+（Docker 镜像为 3.12）。

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
pip install -r requirements.txt
# 跑测试时：pip install -r requirements-dev.txt
cp .env.example .env
# 启动 Postgres（可用上面的 compose 只起 db）后设置 DATABASE_URL
# DATABASE_URL=postgresql+asyncpg://postgres:postgres@127.0.0.1:5433/recording_transcription
# 填写 LLM_API_KEY 后摘要走真实 LLM；不填则需 WORKER_ALLOW_MOCK_LLM=true

alembic upgrade head
uvicorn api.app:app --reload --host 127.0.0.1 --port 8000
```

单元测试（HTTP 接口 + 状态机 / Worker，不连真实 DB / LLM）：

```bash
pip install -r requirements-dev.txt
python -m pytest -v
```

接口调试：导入根目录 [`api.http`](api.http)，上传样例为 [`testdata/short.wav`](testdata/short.wav)。

## 架构说明

```text
                 +------------------+
   multipart     |     FastAPI      |  POST /v1/recordings → 立即返回 pending
   upload ------>|  /v1 API + SSE   |  GET 任务/录音 / retry / delete / summary/stream
                 +--------+---------+
                          | 写 recordings + tasks(pending)
                          v
                 +------------------+
                 |    PostgreSQL    |  租约 / 状态 / transcript / summary(JSONB)
                 +--------+---------+
                          | FOR UPDATE SKIP LOCKED 抢占
                          v
                 +------------------+     5~15s / ~20% 失败
                 |   TaskWorker     |---> Mock ASR ---> summarizing
                 | dispatcher+exec  |---> LLM JSON  ---> done | failed
                 +------------------+
                          | 失败：指数退避（最多失败 3 次后再 failed，共最多 4 次执行）
                          | 重启 / 周期：回收本进程僵尸与过期租约
```

**异步选型**：进程内 `asyncio` Worker（`dispatcher` 抢占调度 + `executor` 执行），非 Celery/Redis。理由：题量小、部署简单、与 FastAPI 同事件循环即可。

**未完成任务如何恢复**：

- 启动时：重置本 `worker_id` 仍持有的、或 `lease_expires_at` 已过期的 in-flight 任务为 `pending`
- 运行中：按约 `max(30s, lease/2)` 周期回收**过期租约**（多副本下无需等对端重启）
- 长任务期间周期性续约；续约失败则中止本机处理，避免与回收方双写

## 表结构（Alembic）

迁移目录：`alembic/versions/`。启动时自动 `upgrade head`。

| 表 | 要点 |
|----|------|
| `recordings` | 文件元数据；`file_hash` 唯一（上传幂等） |
| `tasks` | 与录音 1:1；`status` / `transcript` / `summary(JSONB)` / `retry_count` / `next_retry_at` / `locked_by` / `lease_expires_at` |

状态机：`pending → transcribing → summarizing → done`，任一环节可进入 `failed`。查询接口用细粒度 `status` 体现当前阶段（不另造 `processing`）。

## API 一览

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | /health | 健康检查 |
| POST | /v1/recordings | 上传音频（wav/mp3/m4a/aac，≤50MB）；新建 `201`，幂等命中 `200` |
| GET | /v1/recordings | 录音列表，分页（`page`/`page_size`），按创建时间倒序，含任务状态 |
| GET | /v1/recordings/{id} | 录音详情；`done` 时含 `transcript` 与 `summary` |
| DELETE | /v1/recordings/{id} | 删除录音、关联任务与本地文件（`204`） |
| GET | /v1/recordings/{id}/summary/stream | SSE 流式摘要（需已有 `transcript`；不写库） |
| GET | /v1/tasks/{task_id} | 查询任务状态与结果字段 |
| POST | /v1/tasks/{task_id}/retry | 仅 `failed` 可重试；已排队/处理中幂等返回 |

统一错误体：

```json
{ "error": { "code": "recording_not_found", "message": "...", "details": null } }
```

常见状态码：`400`（参数/上传不合法）、`404`（录音/任务不存在）、`409`（任务不可重试）、`503`（无 LLM 且未开 mock，无法流式摘要）。

调试集合：根目录 [`api.http`](api.http)（VS Code REST Client / IDEA HTTP Client 可导入）。

### SSE 事件约定

```text
event: delta
data: {"text":"<chunk>"}

event: done
data: {"summary":"...","key_points":[],"todos":[]}

event: error
data: {"message":"..."}
```

## 技术取舍

- **ASR**：按题目要求 Mock（随机 5~15s，约 20% 失败），未接真实 ASR。
- **LLM**：OpenAI 兼容 Chat Completions（默认 DeepSeek）；强制 JSON + 多层解析兜底 + 超时/限流/5xx 重试。配置 `LLM_API_KEY` 后 worker 与 SSE 均走真实模型。无 Key 时 compose 默认 `WORKER_ALLOW_MOCK_LLM=true` 仅作占位摘要（题目说明此项会降分）。
- **队列**：DB 状态 + `SKIP LOCKED` 抢占，免额外部署 Redis。
- **并发**：`WORKER_MAX_CONCURRENCY`（默认 3）。
- **上传幂等**：基于文件 SHA-256 去重。
- **日志**：关键路径 INFO/WARNING/ERROR；上传后绑定 `task-{id}`，可用 `X-Request-ID` / `task_id` 串生命周期。
- **测试**：`tests/` 覆盖核心 HTTP 接口（mock 仓储）以及 Worker 状态机（mock ASR/LLM，不等待 5~15s）。

## 已完成的加分项

- 失败自动重试（指数退避，`WORKER_MAX_RETRIES`）
- 服务重启 / 租约过期恢复
- LLM SSE 流式摘要
- 上传哈希幂等
- Worker 并发上限
- 核心接口与状态机单元测试

## 环境变量（常用）

见 `.env.example` 与 `api/config/settings.py`。

| 变量 | 说明 |
|------|------|
| `DATABASE_URL` | `postgresql+asyncpg://...` |
| `LLM_API_KEY` / `LLM_BASE_URL` / `LLM_MODEL` | 摘要模型；**有 Key 即走真实 LLM** |
| `WORKER_ALLOW_MOCK_LLM` | 仅无 Key 时的占位摘要（含 SSE mock 流）；有 Key 时忽略 |
| `WORKER_MAX_CONCURRENCY` / `WORKER_LEASE_SECONDS` | 并发与租约 |
| `LOG_JSON` / `LOG_LEVEL` | 日志 |

## 已知问题与未完成项

- 未提供公网部署地址（本地 / Docker 一键启动已支持）
- Mock ASR 失败率/耗时为随机，联调时任务可能多次自动重试后才 `done`
- SSE 流式摘要为「现算一遍」，**不落库**；与 worker 流水线写入的 `summary` 相互独立
- 多 API 副本时写同一日志文件需改用并发安全 handler（当前单进程 compose 足够）
- `WORKER_MAX_RETRIES=3` 表示失败计数达到 3 仍回 `pending`，第 4 次失败置 `failed`（初始尝试 + 最多 3 次再入队）
- `users` 表在初始迁移中预留，鉴权不在考察范围，业务未使用

## 目录结构（简）

```text
api/
  app.py              # FastAPI 入口
  controller/         # HTTP
  service/            # 业务（recording / task / summarizer / transcriber）
  repository/         # 仓储 + DAO
  worker/             # dispatcher / executor / common
  core/               # db / logger / llm / errors
  schema/             # 响应模型
tests/                # pytest 单元测试（接口 + 状态机）
testdata/short.wav    # api.http 上传样例
alembic/              # 迁移
docker/               # compose
api.http              # 接口调试
```
