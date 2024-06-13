from aioredis import RedisError
from loguru import logger
from pydantic import ValidationError

from app.domain import Extractor
from app.models import ExtractRequest
from app.storage import Storage
from library.kafka import GenericConsumer, GenericTask
from library.models.tasks import TaskStatus


class Consumer(GenericConsumer):
    def __init__(self, config: GenericConsumer.Config, extractor: Extractor, storage: Storage):
        super().__init__(logger, config)
        self._extractor = extractor
        self._storage = storage

    async def run(self):
        async for task in super().run():
            task_status = await self._storage.get_task_status(task.id)
            if task_status is None:
                logger.warning('no task status in storage')
                continue
            if task_status == TaskStatus.CANCELLED:
                logger.info('ignoring task because it was cancelled')
                continue

            try:
                await self._process_task(task)
            except Exception:
                self._logger.exception(f'failed to process task: {str(task.id)}')
                await self._storage.save_task_error(
                    task.id,
                    TaskStatus.SERVER_ERROR,
                    err_code='INTERNAL_ERROR',
                    err_msg='failed to process task'
                )

    async def _process_task(self, task: GenericTask):
        try:
            request = ExtractRequest.parse_raw(task.raw_request)
        except ValidationError as err:
            self._logger.warning('validation error')
            await self._storage.save_task_error(
                task.id,
                TaskStatus.CLIENT_ERROR,
                err_code='VALIDATION_ERROR',
                err_msg='request does not match request schema',
                err_details=err.errors()
            )
            return

        try:
            reqs = await self._extractor.extract(request)
        except Exception:
            self._logger.exception('failed to extract requirements')
            await self._storage.save_task_error(
                task.id,
                TaskStatus.SERVER_ERROR,
                err_code='INTERNAL_ERROR',
                err_msg='failed to extract requirements'
            )
            return

        try:
            await self._storage.save_task_result(task.id, reqs)
        except RedisError:
            self._logger.exception('failed to save task result')
