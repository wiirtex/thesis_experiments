import json
from dataclasses import dataclass
from datetime import timedelta
from uuid import UUID

import aioredis

from library.models.tasks import TaskStatus
from .storage import Storage


class RedisClient(Storage):
    @dataclass(slots=True)
    class Config:
        url: str
        port: str
        secure: bool
        password: str
        ca_file: str

    KEYS_TTL = timedelta(hours=12)

    def __init__(self, config: Config):
        params = {'port': config.port}
        if config.secure:
            params.update(
                {
                    'password': config.password,
                    'ssl_ca_certs': config.ca_file
                }
            )
        self._client = aioredis.from_url(config.url, **params)

    async def get_task_status(self, task_id: UUID) -> TaskStatus | None:
        key = self._get_task_status_key(task_id)
        raw_status = await self._client.get(key)
        if raw_status is None:
            return None
        return TaskStatus(raw_status.decode('utf-8'))

    async def save_task_status(self, task_id: UUID, status: TaskStatus):
        key = self._get_task_status_key(task_id)
        await self._client.set(key, status, ex=self.KEYS_TTL)

    async def save_task_result(self, task_id: UUID, reqs: list[str]):
        status_key = self._get_task_status_key(task_id)
        response_key = self._get_task_response_key(task_id)

        encoded_reqs = json.dumps({"requirements": reqs})

        async with self._client.pipeline(transaction=True) as pipe:
            pipe.set(status_key, TaskStatus.SUCCESS, ex=self.KEYS_TTL)
            pipe.set(response_key, encoded_reqs, ex=self.KEYS_TTL)
            await pipe.execute()

    async def save_task_error(
            self,
            task_id: UUID,
            task_status: TaskStatus,
            *,
            err_code: str,
            err_msg: str,
            err_details: bytes | None = None
    ):
        status_key = self._get_task_status_key(task_id)
        response_key = self._get_task_response_key(task_id)

        data = {
            'error': {
                'code': err_code,
                'message': err_msg
            }
        }
        if err_details is not None:
            data['error']['details'] = err_details

        async with self._client.pipeline(transaction=True) as pipe:
            pipe.set(status_key, task_status, ex=self.KEYS_TTL)
            pipe.set(response_key, json.dumps(data), ex=self.KEYS_TTL)
            await pipe.execute()

    @classmethod
    def _get_task_status_key(cls, task_id: UUID) -> str:
        return f'status:{str(task_id)}'

    @classmethod
    def _get_task_response_key(cls, task_id: UUID) -> str:
        return str(task_id)

    async def close(self):
        await self._client.close()
