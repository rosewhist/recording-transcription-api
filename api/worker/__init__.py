
class TaskWorker:
    """异步任务工人（后续实现：排队、并发控制、重启恢复）。"""

    def __init__(self) -> None:
        self._started = False

    async def start(self) -> None:
        self._started = True

    async def stop(self) -> None:
        self._started = False

    async def enqueue(self, task_id) -> None:
        """将任务投入处理队列（占位）。"""
        if not self._started:
            raise RuntimeError("TaskWorker 尚未启动")
