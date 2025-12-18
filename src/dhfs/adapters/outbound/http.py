# Copyright 2021 - 2025 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

# TODO: Define port
# TODO: Write module doc string
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import httpx
from ghga_service_commons.transports import (
    CompositeCacheConfig,
    CompositeTransportFactory,
)


@asynccontextmanager
async def get_configured_httpx_client(
    *, config: CompositeCacheConfig
) -> AsyncGenerator[httpx.AsyncClient]:
    """Produce an httpx AsyncClient with configured caching and rate limiting behavior"""
    transport = CompositeTransportFactory.create_cached_ratelimiting_retry_transport(
        config=config
    )
    async with httpx.AsyncClient(transport=transport) as client:
        yield client
