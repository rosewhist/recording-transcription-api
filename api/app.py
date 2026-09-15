import asyncio
import time
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, Request, Response

from api.config.settings import get_settings
from api.core.db.db_connector import engine
from api.core.db.migrate import run_migrations
from api.core.errors import register_exception_handlers
from api.core.logger import (
    bind_request_id,
    get_logger,
    request_id_var,
    set_instance_id,
    setup_logging,
)
from api.controller.recording import router as recording_router
from api.controller.task import router as task_router
from api.repository.recording import RecordingRepository
from api.repository.task import TaskRepository
from api.service.recording import RecordingService
from api.service.task import TaskService
from api.service.transcriber import TranscriberService
from api.worker import TaskWorker

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    Path(settings.UPLOAD_DIR).mkdir(parents=True, exist_ok=True)
    Path(settings.LOG_DIR).mkdir(parents=True, exist_ok=True)

    # LLM_API_KEY 未配置时：除非 WORKER_ALLOW_MOCK_LLM=true，否则摘要阶段会失败重试
    from api.service.summarizer import SummarizerService

    worker_summarizer = None
    if settings.LLM_API_KEY:
        worker_summarizer = SummarizerService(settings=settings)
    # HTTP SSE 始终持有 SummarizerService（无 key 时可走 mock）
    http_summarizer = worker_summarizer or SummarizerService(settings=settings)

    # worker 自行解析并持久化标识（单进程架构下同时作为进程级 instance_id）。
    # 尽量提前创建，使迁移等启动日志也能归因到实例；asyncio.Event 在构造时绑定
    # 事件循环（Python 3.9），故仍留在 lifespan 内而非 create_app。
    worker = TaskWorker(
        summarizer=worker_summarizer,
        transcriber=TranscriberService(),
        settings=settings,
    )
    set_instance_id(worker.worker_id)

    # Alembic async env uses asyncio.run(); must not run on uvicorn's loop
    await asyncio.to_thread(run_migrations)

    await worker.start()

    recording_repo = RecordingRepository()
    app.state.recording_service = RecordingService(
        repo=recording_repo,
        worker=worker,
        summarizer=http_summarizer,
    )
    app.state.task_service = TaskService(repo=TaskRepository(), worker=worker)
    app.state.worker = worker
    app.state.summarizer = http_summarizer

    yield

    await worker.stop()
    await engine.dispose()


def create_app() -> FastAPI:
    settings = get_settings()
    setup_logging(
        level=settings.LOG_LEVEL,
        log_json=settings.LOG_JSON,
        log_file=settings.log_file_path,
        max_bytes=settings.LOG_MAX_BYTES,
        backup_count=settings.LOG_BACKUP_COUNT,
    )

    app = FastAPI(
        title="recording-transcription-api",
        version="0.1.0",
        lifespan=lifespan,
    )
    register_exception_handlers(app)

    @app.middleware("http")
    async def request_context(request: Request, call_next) -> Response:
        incoming = request.headers.get("X-Request-ID") or request.headers.get("X-Trace-ID")
        request_id = (incoming or "").strip() or uuid4().hex
        token = bind_request_id(request_id)
        start = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            # 穿透到 ServerErrorMiddleware 的异常：这里只记访问记录，异常详情由
            # errors.py 的兜底 handler 记录（它在本次 re-raise 之后才执行）。
            # 注意不 reset request_id：请求上下文随本 task 结束销毁，而保留它
            # 能让兜底 handler 也读到，完成异常日志的链路关联。
            logger.error(
                "HTTP 请求异常",
                event="http.request.error",
                method=request.method,
                path=request.url.path,
                status=500,
                duration_ms=round((time.perf_counter() - start) * 1000, 1),
            )
            raise
        if request.url.path != "/health":
            logger.info(
                "HTTP 请求完成",
                event="http.request",
                method=request.method,
                path=request.url.path,
                status=response.status_code,
                duration_ms=round((time.perf_counter() - start) * 1000, 1),
            )
        response.headers["X-Request-ID"] = request_id
        request_id_var.reset(token)
        return response

    @app.get("/health", tags=["health"])
    async def health():
        return {"status": "ok"}

    app.include_router(recording_router, prefix="/v1")
    app.include_router(task_router, prefix="/v1")
    

    return app


app = create_app()
