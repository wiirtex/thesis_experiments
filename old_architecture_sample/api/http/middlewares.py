import json
from time import monotonic_ns
from typing import Callable, Any

from aiohttp import web
from loguru import logger
from opentelemetry import trace
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from pydantic import ValidationError

tracer = trace.get_tracer(__name__)

REQUEST_ID_HEADER = 'X-Arqan-Request-Id'


@web.middleware
async def tracing_middleware(request: web.Request, handler: Callable) -> web.Response:
    with tracer.start_as_current_span(
        request.path,
        context=TraceContextTextMapPropagator().extract(request.headers),
        attributes={
            SpanAttributes.HTTP_URL: str(request.url),
            SpanAttributes.HTTP_METHOD: request.method,
            SpanAttributes.HTTP_SCHEME: request.scheme,
            SpanAttributes.HTTP_HOST: request.host
        }
    ) as span:
        response = await handler(request)
        span.set_attribute(SpanAttributes.HTTP_STATUS_CODE, response.status)
        return response


@web.middleware
async def logging_middleware(request: web.Request, handler: Callable) -> web.Response:
    t_start = monotonic_ns()
    request_params = await _build_request_params(request)

    with logger.contextualize(**request_params):
        response: web.Response = await handler(request)
        t_end = monotonic_ns()

        response_params = {
            'response.status': response.status,
            'response.time_ms': round((t_end - t_start) / 1e6, 3)
        }

        if response.status >= 500:
            logger.error('responded with 5xx', **response_params)
        elif response.status >= 400:
            logger.warning('responded with 4xx', **response_params)
        elif response.status >= 300:
            logger.info('responded with 3xx', **response_params)
        else:
            logger.info('responded with 2xx', **response_params)

    return response


async def _build_request_params(request: web.Request) -> dict[str, Any]:
    params = {
        'request.id': request.headers.get(REQUEST_ID_HEADER),
        'request.path': request.path,
        'request.method': request.method
    }
    if not request.body_exists or request.content_type != 'application/json':
        return params

    try:
        params['request.body'] = await request.json()
    except json.JSONDecodeError:
        response = web.json_response(status=web.HTTPBadRequest.status_code, text='invalid json request')
        response_params = {
            'response.status': response.status,
            'response.body': response.body,
        }
        logger.warning('responded with 4xx', **params, **response_params)
        raise response

    return params


@web.middleware
async def error_middleware(request: web.Request, handler: Callable) -> web.Response:
    try:
        return await handler(request)
    except ValidationError as e:
        return web.json_response(status=web.HTTPBadRequest.status_code, text=e.json())
    except web.HTTPException as e:
        return web.json_response(status=e.status_code, body=e.body)
    except Exception:
        logger.exception('unexpected error')
        return web.json_response(status=web.HTTPInternalServerError.status_code)
