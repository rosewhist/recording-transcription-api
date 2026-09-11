from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from api.controller.task import router as task_router
from api.core.errors import register_exception_handlers
from api.depends import get_task_service
from api.repository.dao.task import Task
from api.repository.dao.task_types import TaskStatus
from api.service.task import TaskService


@pytest.fixture
def sample_task() -> Task:
    return Task(
        id=uuid4(),
        recording_id=uuid4(),
        status=TaskStatus.TRANSCRIBING.value,
        retry_count=1,
        transcript=None,
        error_msg=None,
        summary=None,
    )


@pytest.fixture
def mock_task_repo() -> MagicMock:
    return MagicMock()


@pytest.fixture
def task_service(mock_task_repo: MagicMock) -> TaskService:
    return TaskService(repo=mock_task_repo)


@pytest.fixture
def app(task_service: TaskService) -> FastAPI:
    application = FastAPI()
    register_exception_handlers(application)
    application.include_router(task_router, prefix="/v1")
    application.dependency_overrides[get_task_service] = lambda: task_service
    return application


@pytest.fixture
async def client(app: FastAPI):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.mark.asyncio
async def test_get_task_returns_current_stage(
    client: AsyncClient,
    mock_task_repo: MagicMock,
    sample_task: Task,
):
    mock_task_repo.get_by_id = AsyncMock(return_value=sample_task)

    resp = await client.get(f"/v1/tasks/{sample_task.id}")

    assert resp.status_code == 200
    body = resp.json()
    assert body["task_id"] == str(sample_task.id)
    assert body["recording_id"] == str(sample_task.recording_id)
    assert body["status"] == TaskStatus.TRANSCRIBING.value
    assert body["retry_count"] == 1
    assert body["error_msg"] is None
    assert body["transcript"] is None
    assert body["summary"] is None
    mock_task_repo.get_by_id.assert_awaited_once_with(sample_task.id)


@pytest.mark.asyncio
async def test_get_task_done_includes_results(
    client: AsyncClient,
    mock_task_repo: MagicMock,
):
    task = Task(
        id=uuid4(),
        recording_id=uuid4(),
        status=TaskStatus.DONE.value,
        retry_count=0,
        transcript="hello world",
        summary={"summary": "一句话", "key_points": ["a"], "todos": []},
    )
    mock_task_repo.get_by_id = AsyncMock(return_value=task)

    resp = await client.get(f"/v1/tasks/{task.id}")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == TaskStatus.DONE.value
    assert body["transcript"] == "hello world"
    assert body["summary"]["summary"] == "一句话"


@pytest.mark.asyncio
async def test_get_task_failed_includes_error(
    client: AsyncClient,
    mock_task_repo: MagicMock,
):
    task = Task(
        id=uuid4(),
        recording_id=uuid4(),
        status=TaskStatus.FAILED.value,
        retry_count=3,
        error_msg="mock asr failed",
    )
    mock_task_repo.get_by_id = AsyncMock(return_value=task)

    resp = await client.get(f"/v1/tasks/{task.id}")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == TaskStatus.FAILED.value
    assert body["error_msg"] == "mock asr failed"


@pytest.mark.asyncio
async def test_get_task_not_found(client: AsyncClient, mock_task_repo: MagicMock):
    missing_id = uuid4()
    mock_task_repo.get_by_id = AsyncMock(return_value=None)

    resp = await client.get(f"/v1/tasks/{missing_id}")

    assert resp.status_code == 404
    err = resp.json()["error"]
    assert err["code"] == "task_not_found"
