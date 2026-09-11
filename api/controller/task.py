"""任务路由：查询"""
from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter

from api.depends import TaskServiceDep
from api.schema.task import TaskStatusResponse, to_task_status_response

router = APIRouter(tags=["tasks"])


@router.get("/tasks/{task_id}", response_model=TaskStatusResponse)
async def get_task(task_id: UUID, service: TaskServiceDep):
    """查询任务状态；processing 中 status 即为当前阶段。"""
    task = await service.get(task_id)
    return to_task_status_response(task)
