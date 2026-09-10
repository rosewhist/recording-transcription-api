
from typing import Optional

from api.config.settings import Settings, get_settings
from api.repository.recording import RecordingRepository
from api.worker import TaskWorker


class RecordingService:
    def __init__(self, repo: RecordingRepository, worker: Optional[TaskWorker] = None):
        self.repo = repo
        self.worker = worker
        self.settings: Settings = get_settings()
