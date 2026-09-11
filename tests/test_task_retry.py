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
def mock_task_repo() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mock_worker() -> MagicMock:
    worker = MagicMock()
    worker.submit = AsyncMock()
    return worker


@pytest.fixture
def task_service(mock_task_repo: MagicMock, mock_worker: MagicMock) -> TaskService:
    return TaskService(repo=mock_task_repo, worker=mock_worker)


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
async def test_retry_failed_requeues_and_submits(
    client: AsyncClient,
    mock_task_repo: MagicMock,
    mock_worker: MagicMock,
):
    task_id = uuid4()
    requeued = Task(
        id=task_id,
        recording_id=uuid4(),
        status=TaskStatus.PENDING.value,
        retry_count=0,
        error_msg=None,
        transcript=None,
        summary=None,
    )
    mock_task_repo.requeue_failed = AsyncMock(return_value=requeued)

    resp = await client.post(f"/v1/tasks/{task_id}/retry")

    assert resp.status_code == 200
    body = resp.json()
    assert body["task_id"] == str(task_id)
    assert body["status"] == TaskStatus.PENDING.value
    assert body["retry_count"] == 0
    mock_task_repo.requeue_failed.assert_awaited_once_with(task_id)
    mock_worker.submit.assert_awaited_once_with(task_id)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status",
    [
        TaskStatus.PENDING,
        TaskStatus.TRANSCRIBING,
        TaskStatus.SUMMARIZING,
    ],
)
async def test_retry_idempotent_when_already_recoverable(
    client: AsyncClient,
    mock_task_repo: MagicMock,
    mock_worker: MagicMock,
    status: TaskStatus,
):
    task_id = uuid4()
    current = Task(
        id=task_id,
        recording_id=uuid4(),
        status=status.value,
        retry_count=0,
    )
    mock_task_repo.requeue_failed = AsyncMock(return_value=None)
    mock_task_repo.get_by_id = AsyncMock(return_value=current)

    resp = await client.post(f"/v1/tasks/{task_id}/retry")

    assert resp.status_code == 200
    assert resp.json()["status"] == status.value
    mock_worker.submit.assert_not_called()


@pytest.mark.asyncio
async def test_retry_rejects_done(
    client: AsyncClient,
    mock_task_repo: MagicMock,
):
    task_id = uuid4()
    done = Task(
        id=task_id,
        recording_id=uuid4(),
        status=TaskStatus.DONE.value,
        retry_count=0,
        transcript="ok",
    )
    mock_task_repo.requeue_failed = AsyncMock(return_value=None)
    mock_task_repo.get_by_id = AsyncMock(return_value=done)

    resp = await client.post(f"/v1/tasks/{task_id}/retry")

    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "task_not_retryable"


@pytest.mark.asyncio
async def test_retry_not_found(client: AsyncClient, mock_task_repo: MagicMock):
    task_id = uuid4()
    mock_task_repo.requeue_failed = AsyncMock(return_value=None)
    mock_task_repo.get_by_id = AsyncMock(return_value=None)

    resp = await client.post(f"/v1/tasks/{task_id}/retry")

    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "task_not_found"
