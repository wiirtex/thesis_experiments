import json
from typing import Any
from uuid import UUID

from library.models.tasks import TaskStatus
from .storage import Storage


class LocalStorage(Storage):
    def __init__(self, statuses: dict[UUID, TaskStatus] | None = None, responses: dict[UUID, str] | None = None):
        self._statuses = dict()
        if statuses is not None:
            self._statuses = statuses
        self._responses = dict()
        if responses is not None:
            self._responses = responses

    async def get_task_status(self, task_id: UUID) -> TaskStatus | None:
        return self._statuses.get(task_id)

    async def save_task_status(self, task_id: UUID, status: TaskStatus):
        self._statuses[task_id] = status

    async def save_task_result(self, task_id: UUID, reqs: list[str]):
        self._statuses[task_id] = TaskStatus.SUCCESS
        self._responses[task_id] = json.dumps({"requirements": reqs})

    async def save_task_error(
            self,
            task_id: UUID,
            task_status: TaskStatus,
            *,
            err_code: str,
            err_msg: str,
            err_details: Any | None = None
    ):
        data = {
            'error': {
                'code': err_code,
                'message': err_msg
            }
        }
        if err_details is not None:
            data['error']['details'] = err_details

        self._statuses[task_id] = task_status
        self._responses[task_id] = json.dumps(data)

    async def close(self):
        self._statuses.clear()
        self._responses.clear()
