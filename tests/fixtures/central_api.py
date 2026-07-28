# Copyright 2021 - 2026 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A mock of the GHGA Central API, built on the service commons `MockRouter`."""

__all__ = [
    "CentralApiMock",
    "ResponseHandler",
    "fail_to_connect",
    "get_mocked_httpx_client",
    "respond",
]

from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import httpx2
from ghga_service_commons.api.mock_router import MockRouter

from dhfs.adapters.outbound.central import CentralClientConfig
from dhfs.adapters.outbound.http import HttpClientConfig, get_configured_httpx_client

ResponseHandler = Callable[
    [httpx2.Request], httpx2.Response | Awaitable[httpx2.Response]
]

_NO_BODY = object()


def respond(status_code: int, json: Any = _NO_BODY) -> ResponseHandler:
    """Make a handler that always answers with the same status code and JSON body."""

    def handler(request: httpx2.Request) -> httpx2.Response:
        if json is _NO_BODY:
            return httpx2.Response(status_code=status_code)
        return httpx2.Response(status_code=status_code, json=json)

    return handler


def fail_to_connect(reason: str = "All connection attempts failed") -> ResponseHandler:
    """Make a handler that simulates the Central API being unreachable."""

    def handler(request: httpx2.Request) -> httpx2.Response:
        raise httpx2.ConnectError(reason, request=request)

    return handler


class CentralApiMock:
    """A mock of the GHGA Central API endpoints that the DHFS talks to.

    Each endpoint answers with the handler assigned to `on_fetch_new_uploads`,
    `on_get_removable_files` or `on_submit_report`. Tests can swap those out with
    `respond(...)`, `fail_to_connect(...)` or any other callable taking the request -
    both sync and async callables are supported. Every request that reaches the mock is
    recorded in `requests`.
    """

    def __init__(self, *, config: CentralClientConfig) -> None:
        self._base_url = str(config.central_api_url).rstrip("/")
        base_path = httpx2.URL(self._base_url).path

        self.requests: list[httpx2.Request] = []
        self.on_fetch_new_uploads: ResponseHandler = respond(200, json=[])
        self.on_get_removable_files: ResponseHandler = respond(200, json=[])
        self.on_submit_report: ResponseHandler = respond(201, json={})

        router: MockRouter = MockRouter()

        @router.get(f"{base_path}/storages/{{storage_alias}}/uploads")
        async def fetch_new_uploads(
            storage_alias: str, request: httpx2.Request
        ) -> httpx2.Response:
            return await self._handle(request, self.on_fetch_new_uploads)

        @router.post(f"{base_path}/storages/{{storage_alias}}/uploads/can_remove")
        async def get_removable_files(
            storage_alias: str, request: httpx2.Request
        ) -> httpx2.Response:
            return await self._handle(request, self.on_get_removable_files)

        @router.post(f"{base_path}/storages/{{storage_alias}}/interrogation-reports")
        async def submit_interrogation_report(
            storage_alias: str, request: httpx2.Request
        ) -> httpx2.Response:
            return await self._handle(request, self.on_submit_report)

        self._router = router

    async def _handle(
        self, request: httpx2.Request, handler: ResponseHandler
    ) -> httpx2.Response:
        """Record the request and let the currently assigned handler answer it."""
        self.requests.append(request)
        response = handler(request)
        if isinstance(response, httpx2.Response):
            return response
        return await response

    def as_transport(self) -> httpx2.MockTransport:
        """Return a transport that answers every request with this mock.

        Only use this where all traffic is Central API traffic - otherwise use
        `as_routing_transport()`.
        """
        return self._router.as_transport()

    def as_routing_transport(self) -> httpx2.AsyncBaseTransport:
        """Return a transport that answers Central API requests with this mock and
        sends everything else (e.g. S3 traffic) over the network as usual.
        """
        return _CentralApiRoutingTransport(
            mock_transport=self.as_transport(), base_url=self._base_url
        )


class _CentralApiRoutingTransport(httpx2.AsyncBaseTransport):
    """Splits traffic between a Central API mock transport and the actual network."""

    def __init__(self, *, mock_transport: httpx2.MockTransport, base_url: str) -> None:
        self._mock_transport = mock_transport
        self._base_url = base_url
        self._network_transport = httpx2.AsyncHTTPTransport()

    async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
        """Dispatch the request based on whether it targets the Central API."""
        if str(request.url).startswith(self._base_url):
            return await self._mock_transport.handle_async_request(request)
        return await self._network_transport.handle_async_request(request)

    async def aclose(self) -> None:
        """Close the transport used for the requests that aren't mocked."""
        await self._network_transport.aclose()


@asynccontextmanager
async def get_mocked_httpx_client(
    *, config: HttpClientConfig, central_api: CentralApiMock
) -> AsyncGenerator[httpx2.AsyncClient]:
    """Drop-in for `get_configured_httpx_client` that answers Central API calls with
    `central_api` while leaving all other traffic untouched.
    """
    async with get_configured_httpx_client(
        config=config, base_transport=central_api.as_routing_transport()
    ) as client:
        yield client
