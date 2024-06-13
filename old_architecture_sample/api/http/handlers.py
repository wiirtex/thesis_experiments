from aiohttp import web

from app.domain import Extractor
from app.models import ExtractRequest


class PingHandler(web.View):
    async def get(self) -> web.Response:
        return web.json_response(status=web.HTTPOk.status_code)


class ExtractHandler(web.View):
    @property
    def extractor(self) -> Extractor:
        return self.request.app[Extractor.NAME]

    async def post(self) -> web.Response:
        request_body = await self.request.json()
        request = ExtractRequest(**request_body)
        reqs = await self.extractor.extract(request)
        return web.json_response(status=web.HTTPOk.status_code, data={'requirements': reqs})
