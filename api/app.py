from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, Request, Response

from api.config.settings import get_settings
from api.core.db.db_connector import engine
from api.core.db.migrate import run_migrations
from api.core.errors import register_exception_handlers
from api.core.logger import setup_logging, trace_id_var
from api.controller.recording import router as recording_router
from api.controller.task import router as task_router
from api.repository.recording import RecordingRepository
from api.repository.task import TaskRepository
from api.service.recording import RecordingService
from api.service.task import TaskService
from api.service.transcriber import TranscriberService
from api.worker import TaskWorker


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    Path(settings.UPLOAD_DIR).mkdir(parents=True, exist_ok=True)
    Path(settings.LOG_DIR).mkdir(parents=True, exist_ok=True)

    run_migrations()

    # LLM_API_KEY 未配置时：除非 WORKER_ALLOW_MOCK_LLM=true，否则摘要阶段会失败重试
    summarizer = None
    if settings.LLM_API_KEY:
        from api.service.summarizer import SummarizerService

        summarizer = SummarizerService(settings=settings)

    worker = TaskWorker(
        summarizer=summarizer,
        transcriber=TranscriberService(),
        settings=settings,
    )
    await worker.start()

    recording_repo = RecordingRepository()
    app.state.recording_service = RecordingService(repo=recording_repo, worker=worker)
    app.state.task_service = TaskService(repo=TaskRepository())
    app.state.worker = worker

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
    async def bind_trace_id(request: Request, call_next) -> Response:
        incoming = request.headers.get("X-Request-ID") or request.headers.get("X-Trace-ID")
        trace_id = (incoming or "").strip() or uuid4().hex
        token = trace_id_var.set(trace_id)
        try:
            response = await call_next(request)
        finally:
            trace_id_var.reset(token)
        response.headers["X-Request-ID"] = trace_id
        return response

    @app.get("/health", tags=["health"])
    async def health():
        return {"status": "ok"}

    app.include_router(recording_router, prefix="/v1")
    app.include_router(task_router, prefix="/v1")
    

    return app


app = create_app()
