import logging
import sys
from functools import partial

from aiohttp import web

from .api.http import MIDDLEWARES, register_handlers
from .api.kafka import Consumer
from .config import read_config
from .domain import model
from .storage import Storage, LocalStorage, RedisClient
from .tracing import setup_tracer

logger.add(sys.stderr, level=logging.INFO, serialize=True, backtrace=True)


async def setup_storage(app: web.Application, redis_config: RedisClient.Config | None):
    storage: Storage
    if redis_config is not None:
        storage = RedisClient(redis_config)
        logger.info('created redis client')
    else:
        storage = LocalStorage()
        logger.info('created local storage')
    app["Storage"] = storage
    yield
    await storage.close()


async def setup_stub_model(app: web.Application):
    stub_model = model.StubModel()

    app["StubModel"] = stub_model

    yield

    pass


async def setup_kafka_consumer(app: web.Application, config: Consumer.Config):
    consumer = Consumer(config, app["StubModel"], app["Storage"])
    logger.info('created kafka consumer')
    await consumer.run()
    yield
    await consumer.close()


def main():
    config = read_config()

    if config.tracing.enabled:
        setup_tracer(config.tracing)

    app = web.Application(logger=logger, middlewares=MIDDLEWARES)
    register_handlers(app.router)

    app.cleanup_ctx.extend([partial(setup_storage, redis_config=config.redis), setup_stub_model])

    if config.kafka is not None:
        logger.info('kafka consumer is enabled')
        app.cleanup_ctx.append(partial(setup_kafka_consumer, config=config.kafka))
    else:
        logger.info('kafka consumer is disabled, will not consume tasks from kafka')

    with logger.contextualize(service_name='stub_model'):
        web.run_app(app, host=config.api.host, port=config.api.port)

    logger.info('stopped application')
