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
# 编辑 .env：DATABASE_URL 默认已指向宿主机 5433；填写 LLM_API_KEY 走真实摘要

alembic upgrade head
uvicorn api.app:app --reload --host 127.0.0.1 --port 8000
```

单元测试（HTTP 接口 + 状态机 / Worker，不连真实 DB / LLM，默认全离线）：

```bash
pip install -r requirements-dev.txt
python -m pytest -v
```

集成测试（真实 PostgreSQL，验证 SQL 语义：`SKIP LOCKED` 抢占、退避数值、租约回收
过滤、同哈希竞态）。**未提供 `TEST_DATABASE_URL` 时整组自动 skip**，不影响上面的离线
套件：

```bash
# 起一个临时实例（或直接用 compose 的 db）
docker run -d --rm -p 55432:5432 -e POSTGRES_PASSWORD=postgres postgres:16-alpine

TEST_DATABASE_URL=postgresql+asyncpg://postgres:postgres@127.0.0.1:55432/recording_transcription_test \
  python -m pytest tests/integration -v
```

测试库会被自动创建（库名必须含 `test`，否则拒绝执行——teardown 会 `downgrade base`
清空所有表），schema 由 Alembic 迁移建立，因此迁移的 up/down 路径也一并被覆盖。

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

- 启动时：重置本 `worker_id` 仍持有的、或 `lease_expires_at` 已过期的 in-flight 任务为 `pending`。`worker_id` 默认持久化到 `WORKER_STATE_DIR/worker_id`（容器内 bind-mount `.worker/`），**重启后复用同一标识**，因此本实例中断的任务可立即回收，无需等租约过期；显式设置 `WORKER_ID` 则完全固定
- 运行中：按约 `max(30s, lease/2)` 周期回收**过期租约**（多副本下无需等对端重启）
- 长任务期间周期性续约；续约失败则中止本机处理，避免与回收方双写

> **多副本部署注意**：持久化的 `worker_id` 存放在共享的 `WORKER_STATE_DIR` 中。若运行多个副本（如 `--scale`），每个副本必须显式设置**不同的** `WORKER_ID`，或为每副本挂载独立的 state 目录——否则所有副本会复用同一标识，启动时会把彼此正在处理的任务误判为“自身任务”而重置，导致重复处理。

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
| GET | /v1/recordings/{id}/summary/stream | SSE 流式摘要（需已有 `transcript`）；任务 `done` 且已存摘要时直接复用回放，否则实时生成（不落库） |
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
- **上传落盘**：按 1MiB 分块流式写盘并增量计算 SHA-256，阻塞 I/O 走 `asyncio.to_thread`，避免整文件读入内存；幂等命中直接丢弃 `.part` 暂存件。
- **日志**：loguru 原生 + 扁平结构化单行 JSON（`LOG_JSON=false` 时为彩色文本）。每条含 `ts/level/logger/event/message`、进程级 `instance_id`（每条都有）与链路标识 `request_id`/`task_id`/`recording_id`/`worker_id`（`worker_id` 仅在处理任务时有值）；域字段归入 `extra`，异常归入 `error`。按 `task_id` 字段可一次捞出任务全生命周期，按 `instance_id` 可定位到具体实例。标准库 logging（uvicorn / sqlalchemy / openai）经 `InterceptHandler` 转发到同一 schema。
- **测试**：`tests/` 覆盖核心 HTTP 接口（mock 仓储）、Worker 状态机与失败/竞态路径（mock ASR/LLM，不等待 5~15s），默认离线可跑；`tests/integration/` 另有一组真实 PostgreSQL 集成测试，覆盖 SQL 层语义（`SKIP LOCKED` 并发抢占、重试退避数值、租约回收过滤、fencing 守卫、同哈希竞态），未配置 `TEST_DATABASE_URL` 时自动 skip。

## 已完成的加分项

- 失败自动重试（指数退避，`WORKER_MAX_RETRIES`）
- 服务重启 / 租约过期恢复
- LLM SSE 流式摘要
- 上传哈希幂等
- Worker 并发上限
- 核心接口与状态机单元测试

## 环境变量（常用）

配置优先级：**环境变量 > 仓库根 `.env` > `settings.py` 兜底**。  
完整推荐项见 [`.env.example`](.env.example)；复制为 `.env` 后按需修改（`.env` 勿提交）。

| 变量 | 说明 |
|------|------|
| `DATABASE_URL` | 本地默认宿主机 `5433`；Docker 内由 compose 覆盖为 `db:5432` |
| `LLM_API_KEY` / `LLM_BASE_URL` / `LLM_MODEL` | 摘要模型；**有 Key 即走真实 LLM** |
| `WORKER_ALLOW_MOCK_LLM` | 仅无 Key 时的占位摘要（含 SSE mock 流）；有 Key 时 worker 仍用真实模型 |
| `WORKER_MAX_CONCURRENCY` / `WORKER_LEASE_SECONDS` | 并发与租约 |
| `WORKER_ID` / `WORKER_STATE_DIR` | worker 标识：显式固定，或持久化到 `WORKER_STATE_DIR`（默认 `.worker/`）以便重启复用 |
| `LOG_JSON` / `LOG_LEVEL` / `LOG_TO_FILE` | 日志：JSON 或彩色文本、级别、是否写文件（轮转由 `LOG_MAX_BYTES` / `LOG_BACKUP_COUNT` 控制） |

## 已知问题与未完成项

- 未提供公网部署地址（本地 / Docker 一键启动已支持）
- Mock ASR 失败率/耗时为随机，联调时任务可能多次自动重试后才 `done`
- SSE 摘要与 worker 流水线共用同一份 `summary`，但**只有流水线会写它**：`tasks.summary` 仅由 `mark_done` 写入、`requeue_failed` 清空，因此「库里有摘要」等价于「任务已 `done`」。SSE 只在 `done` 时复用回放，其余情况实时生成且不回写——这样它和详情接口（仅 `done` 暴露结果）对同一条数据的判断始终一致。代价是转写完成但尚未 `done` 期间，每次 SSE 请求都会各自调用一次 LLM（若需去重，可加按 `task_id` 的进程内单飞锁）
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
tests/                # pytest 离线单元测试（接口 + 状态机 + 失败/竞态路径）
tests/integration/    # 真实 PostgreSQL 集成测试（SQL 语义；无库时自动 skip）
testdata/short.wav    # api.http 上传样例
alembic/              # 迁移
docker/               # compose
api.http              # 接口调试
```
