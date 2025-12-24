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

"""Import necessary test fixtures."""

import pytest
from ghga_service_commons.utils import jwt_helpers
from hexkit.providers.s3.testutils import (  # noqa: F401
    federated_s3_fixture,
    s3_multi_container_fixture,
)

from tests.fixtures import ConfigFixture
from tests.fixtures.config import DEFAULT_TEST_CONFIG, get_config
from tests.fixtures.joint import joint_fixture  # noqa: F401


@pytest.fixture(name="config")
def config_fixture() -> ConfigFixture:
    """Generate config from test yaml along with an auth key and JWK"""
    jwk = jwt_helpers.generate_jwk()
    signing_key = jwt_helpers.generate_jwk().export_private()
    config = get_config(data_hub_private_key=signing_key)
    return ConfigFixture(config=config, jwk=jwk)


@pytest.fixture(scope="session")
def storage_aliases():
    """Supplies the aliases to the federated S3 fixture.

    This tells it how many S3 storages to spin up and what names to associate with them.
    """
    return [alias for alias in DEFAULT_TEST_CONFIG.object_storages]
