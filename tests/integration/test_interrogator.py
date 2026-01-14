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

"""Integration tests for the Interrogator class"""

from uuid import UUID, uuid4

import pytest
from pytest_httpx import HTTPXMock

from dhfs.adapters.outbound.s3 import S3Client
from dhfs.models import FileUpload
from tests.fixtures.joint import JointFixture
from tests.fixtures.utils import get_encrypted_object, upload_encrypted_object

PART_SIZE = 6 * (1024**2)  # 6291456 bytes


pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.httpx_mock(
        assert_all_responses_were_requested=False,
        assert_all_requests_were_expected=False,
        can_send_already_matched_responses=True,
        should_mock=lambda request: "docker" not in request.url.host,
    ),
]


async def test_interrogate_new_files(
    joint_fixture: JointFixture, httpx_mock: HTTPXMock
):
    """Test the interrogation process for a single file"""
    # Create the inbox bucket
    config = joint_fixture.config
    inbox = config.inbox_storage_alias
    bucket_id = config.object_storages[inbox].bucket
    storage = joint_fixture.federated_s3.storages[inbox].storage
    await storage.create_bucket(bucket_id)

    # Add files to the inbox
    object_ids = sorted([str(uuid4()) for _ in range(2)])
    file_uploads: list[FileUpload] = []
    for object_id in object_ids:
        encrypted_object = get_encrypted_object(
            part_size=PART_SIZE, file_size=int(PART_SIZE * 2.5)
        )
        await upload_encrypted_object(
            bucket_id=inbox,
            object_id=object_id,
            storage=storage,
            encrypted_object=encrypted_object,
        )
        file_uploads.append(
            FileUpload(
                id=UUID(object_id),
                decrypted_sha256=encrypted_object.checksums.unencrypted_sha256.hexdigest(),
                storage_alias=inbox,
                decrypted_size=encrypted_object.unencrypted_size,
                encrypted_size=encrypted_object.encrypted_size,
                part_size=PART_SIZE,
            )
        )

    # Create the interrogation bucket so the re-encrypted files have a place to go
    interrogation = joint_fixture.config.interrogation_storage_alias
    bucket_id = joint_fixture.config.object_storages[interrogation].bucket
    storage = joint_fixture.federated_s3.storages[interrogation].storage
    await storage.create_bucket(bucket_id)

    # Serialize the file uploads we prepared in advance to JSON
    serialized_file_uploads = [x.model_dump(mode="json") for x in file_uploads]

    # Add callback for when we request the list of new files that need interrogation
    url_for_new_files = f"{config.central_api_url}/storages/{inbox}/uploads"
    httpx_mock.add_response(
        url=url_for_new_files, status_code=200, json=serialized_file_uploads
    )

    # Add callback for when we upload the file interrogation report
    url_for_reports = (
        f"{config.central_api_url}/storages/{interrogation}/interrogation-reports"
    )
    httpx_mock.add_response(url=url_for_reports, status_code=201)

    # Process all files
    await joint_fixture.interrogator.interrogate_new_files()

    # Check the interrogation bucket (first patch central api call)
    # TODO: Expose S3Client
    s3_client: S3Client = joint_fixture.interrogator._s3_client  # type: ignore
    interrogation_files = await s3_client.list_files_in_interrogation_bucket()

    assert interrogation_files == object_ids, "Interrogation bucket contents differed"
