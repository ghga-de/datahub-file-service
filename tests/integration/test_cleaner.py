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
"""Integration tests for the S3Cleaner class"""

import pytest
from pytest_httpx import HTTPXMock

from tests.fixtures.joint import JointFixture

pytestmark = pytest.mark.asyncio()


@pytest.mark.parametrize(
    "removable_files",
    [
        [
            "18d50867-fbef-4a32-8f70-e81766383980",
            "1969264c-3abe-44e6-8db9-65612d6c6a90",
            "a7084f3d-f4cb-4333-853c-bc1e400f14ba",
        ],
        [
            "18d50867-fbef-4a32-8f70-e81766383980",
            "1969264c-3abe-44e6-8db9-65612d6c6a90",
        ],
        [],
    ],
    ids=["All", "AllButOne", "None"],
)
async def test_cleaner_successful(
    joint_fixture: JointFixture, httpx_mock: HTTPXMock, removable_files: list[str]
):
    """Test that files can be removed from the interrogation bucket."""
    # Pre-populate some objects in the interrogation bucket
    interrogation = joint_fixture.config.interrogation_storage_alias
    bucket = joint_fixture.config.object_storages[interrogation].bucket
    file_ids = [
        "18d50867-fbef-4a32-8f70-e81766383980",
        "1969264c-3abe-44e6-8db9-65612d6c6a90",
        "a7084f3d-f4cb-4333-853c-bc1e400f14ba",
    ]
    contents = {bucket: file_ids}
    await joint_fixture.federated_s3.populate_dummy_items(
        alias=interrogation, contents=contents
    )

    # Now verify that the expected items appear in the interrogation bucket
    storage = joint_fixture.federated_s3.storages[interrogation].storage
    assert set(await storage.list_all_object_ids(bucket)) == set(file_ids)

    # Create a mock response from the central API
    params = "&".join([f"file_id={file_id}" for file_id in file_ids])
    url = f"{joint_fixture.config.central_api_url}/uploads/can_remove?{params}"
    httpx_mock.add_response(
        status_code=200, json=removable_files, url=url, method="GET"
    )

    # Run the scan and clean operation
    await joint_fixture.s3_cleaner.scan_and_clean()

    # Check that only the removable_files were deleted from the bucket
    remaining_files = await storage.list_all_object_ids(bucket)
    assert set(remaining_files) == set(file_ids) - set(removable_files)
