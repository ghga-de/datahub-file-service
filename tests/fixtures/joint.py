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

"""Join the functionality of all fixtures for API-level integration testing."""

__all__ = ["JointFixture", "joint_fixture"]

from collections.abc import AsyncGenerator
from dataclasses import dataclass

import pytest_asyncio
from ghga_service_commons.utils.jwt_helpers import generate_jwk
from hexkit.providers.s3.provider import S3Config
from hexkit.providers.s3.testutils import FederatedS3Fixture

from dhfs.config import Config
from dhfs.inject import prepare_interrogation_bucket_cleaner, prepare_interrogator
from dhfs.ports.outbound.cleaner import S3CleanerPort
from dhfs.ports.outbound.interrogator import InterrogatorPort
from tests.fixtures.config import get_config


@dataclass
class JointFixture:
    """Returned by the `joint_fixture`."""

    config: Config
    s3_cleaner: S3CleanerPort
    interrogator: InterrogatorPort
    federated_s3: FederatedS3Fixture


def patch_config_for_alias(
    alias: str, s3_config: S3Config, original_config: Config
) -> Config:
    """Update the full config instance with the given s3 config for the given alias."""
    dumped = original_config.model_dump()
    dumped["object_storages"][alias]["credentials"] = s3_config
    return Config(**dumped)


@pytest_asyncio.fixture(scope="function")
async def joint_fixture(
    federated_s3: FederatedS3Fixture,
) -> AsyncGenerator[JointFixture]:
    """A fixture that embeds all other fixtures for API-level integration testing."""
    central_jwk = generate_jwk()
    central_pub_key = central_jwk.export(private_key=False)
    dhfs_jwk = generate_jwk()
    dhfs_private_key = dhfs_jwk.export_private()

    # merge configs from different sources with the default one:
    config = get_config(
        sources=[],
        data_hub_private_key=dhfs_private_key,
        central_api_public_key=central_pub_key,
    )

    # Patch the config so the URL/credentials point to the actual S3 testcontainers
    for storage_alias, s3_config in federated_s3.get_configs_by_alias().items():
        config = patch_config_for_alias(
            storage_alias, s3_config=s3_config, original_config=config
        )

    # Assemble joint fixture with config injection
    async with (
        prepare_interrogation_bucket_cleaner(config=config) as s3_cleaner,
        prepare_interrogator(config=config) as interrogator,
    ):
        yield JointFixture(
            config=config,
            s3_cleaner=s3_cleaner,
            interrogator=interrogator,
            federated_s3=federated_s3,
        )
