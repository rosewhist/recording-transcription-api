from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from api.config.settings import get_settings
from api.core.db.db_connector import engine
from api.repository.dao import create_db_and_tables
from api.repository.recording import RecordingRepository
from api.repository.task import TaskRepository
from api.service.recording import RecordingService
from api.service.task import TaskService
from api.worker import TaskWorker


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    Path(settings.UPLOAD_DIR).mkdir(parents=True, exist_ok=True)
    Path(settings.LOG_DIR).mkdir(parents=True, exist_ok=True)

    await create_db_and_tables()

    worker = TaskWorker()
    await worker.start()

    recording_repo = RecordingRepository()
    task_repo = TaskRepository()
    app.state.recording_service = RecordingService(repo=recording_repo, worker=worker)
    app.state.task_service = TaskService(repo=task_repo, worker=worker)
    app.state.worker = worker

    yield

    await worker.stop()
    await engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(
        title="recording-transcription-api",
        version="0.1.0",
        lifespan=lifespan,
    )

    @app.get("/health", tags=["health"])
    async def health():
        return {"status": "ok"}

    # 业务路由就绪后挂载，例如：
    # from api.controller.recording import router as recording_router
    # from api.controller.task import router as task_router
    # application.include_router(recording_router, prefix="/v1")
    # application.include_router(task_router, prefix="/v1")

    return app


app = create_app()
