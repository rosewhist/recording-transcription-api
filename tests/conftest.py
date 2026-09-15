from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from api.config.settings import get_settings
from api.controller.recording import router as recording_router
from api.core.errors import register_exception_handlers
from api.depends import get_recording_service
from api.repository.dao.recording import Recording
from api.repository.dao.task import Task
from api.repository.dao.task_types import TaskStatus
from api.service.recording import RecordingService


@pytest.fixture
def upload_dir(tmp_path, monkeypatch):
    path = tmp_path / "uploads"
    path.mkdir()
    monkeypatch.setenv("UPLOAD_DIR", str(path))
    get_settings.cache_clear()
    yield path
    get_settings.cache_clear()


@pytest.fixture
def sample_recording() -> Recording:
    return Recording(
        id=uuid4(),
        file_name="demo.mp3",
        file_path="/tmp/demo.mp3",
        file_size=12,
        file_hash="a" * 64,
    )


@pytest.fixture
def sample_task(sample_recording: Recording) -> Task:
    return Task(
        id=uuid4(),
        recording_id=sample_recording.id,
        status=TaskStatus.PENDING.value,
    )


@pytest.fixture
def mock_repo() -> MagicMock:
    repo = MagicMock()
    repo.get_by_hash = AsyncMock(return_value=(None, None))
    repo.create_recording_and_task = AsyncMock()
    repo.set_task_summary_if_absent = AsyncMock(return_value=True)
    return repo


@pytest.fixture
def mock_worker() -> MagicMock:
    worker = MagicMock()
    worker.submit = AsyncMock()
    return worker


@pytest.fixture
def recording_service(upload_dir, mock_repo, mock_worker) -> RecordingService:
    return RecordingService(repo=mock_repo, worker=mock_worker)


@pytest.fixture
def app(recording_service: RecordingService) -> FastAPI:
    application = FastAPI()
    register_exception_handlers(application)
    application.include_router(recording_router, prefix="/v1")
    application.dependency_overrides[get_recording_service] = lambda: recording_service
    return application


@pytest.fixture
async def client(app: FastAPI):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
