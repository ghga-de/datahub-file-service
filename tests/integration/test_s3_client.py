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

"""Integration tests for the S3Client class"""

import base64
import hashlib
from collections.abc import AsyncGenerator

import httpx
import pytest
import pytest_asyncio
from ghga_service_commons.utils.multinode_storage import S3ObjectStorages
from hexkit.providers.s3.provider import S3ObjectStorage

from dhfs.adapters.outbound.http import get_configured_httpx_client
from dhfs.adapters.outbound.s3 import S3Client, get_s3_client
from tests.fixtures.joint import JointFixture

pytestmark = pytest.mark.asyncio()
from uuid import uuid4


@pytest_asyncio.fixture(name="s3_client")
async def _s3_client(joint_fixture: JointFixture) -> AsyncGenerator[S3Client]:
    config = joint_fixture.config
    object_storages = S3ObjectStorages(config=config)
    async with get_configured_httpx_client(
        config=joint_fixture.config, cached=False
    ) as client:
        s3_client = await get_s3_client(
            config=config, object_storages=object_storages, httpx_client=client
        )
        yield s3_client


async def upload_object(*, bucket_id: str, object_id: str, storage: S3ObjectStorage):
    """Upload dummy data to the S3 storage for a given bucket and object ID"""
    upload_id = await storage.init_multipart_upload(
        bucket_id=bucket_id, object_id=object_id
    )
    url = await storage.get_part_upload_url(
        upload_id=upload_id, bucket_id=bucket_id, object_id=object_id, part_number=1
    )
    httpx.put(url, content=b"this is some object content. " * 2000)
    await storage.complete_multipart_upload(
        upload_id=upload_id, bucket_id=bucket_id, object_id=object_id
    )


async def test_get_is_file_in_inbox(joint_fixture: JointFixture, s3_client: S3Client):
    """Test the functionality of `S3Client.get_is_file_in_inbox()`"""
    config = joint_fixture.config
    file_id = uuid4()
    is_in_inbox = await s3_client.get_is_file_in_inbox(file_id=file_id)
    assert isinstance(is_in_inbox, bool)
    assert not is_in_inbox
    inbox = config.inbox_storage_alias
    bucket_id = joint_fixture.config.object_storages[inbox].bucket
    contents = {bucket_id: [str(file_id)]}
    await joint_fixture.federated_s3.populate_dummy_items(inbox, contents)
    assert await s3_client.get_is_file_in_inbox(file_id=file_id)


async def test_list_files_in_interrogation_bucket(
    joint_fixture: JointFixture, s3_client: S3Client
):
    """Test the functionality of `S3Client.list_files_in_interrogation_bucket()`"""
    # Get file list before creating the bucket
    with pytest.raises(s3_client.BucketNotFoundError):
        files = await s3_client.list_files_in_interrogation_bucket()

    # Create the bucket
    interrogation = joint_fixture.config.interrogation_storage_alias
    bucket_id = joint_fixture.config.object_storages[interrogation].bucket
    storage = joint_fixture.federated_s3.storages[interrogation].storage
    await storage.create_bucket(bucket_id)

    # Get the file list -- this time should get no error - should get an empty list
    files = await s3_client.list_files_in_interrogation_bucket()
    assert isinstance(files, list)
    assert not files

    # Now generate some file IDs and upload a dummy object for each one
    object_ids = [str(uuid4()) for _ in range(3)]
    for object_id in object_ids:
        await upload_object(bucket_id=bucket_id, object_id=object_id, storage=storage)

    # Get the file list again and verify that this time it's correct
    files = await s3_client.list_files_in_interrogation_bucket()
    assert set(files) == set(object_ids)


async def test_fetch_file_content_range(
    joint_fixture: JointFixture, s3_client: S3Client
):
    """Test the functionality of `S3Client.fetch_file_content_range()`"""
    # Try when the inbox bucket doesn't exist yet.
    object_id = str(uuid4())
    with pytest.raises(S3Client.BucketNotFoundError):
        await s3_client.fetch_file_content_range(object_id=object_id, start=0, stop=115)

    # Create the bucket
    inbox = joint_fixture.config.inbox_storage_alias
    bucket_id = joint_fixture.config.object_storages[inbox].bucket
    storage = joint_fixture.federated_s3.storages[inbox].storage
    await storage.create_bucket(bucket_id)

    # Try to get a file part for a non-existent file
    with pytest.raises(S3Client.ObjectNotFoundError):
        await s3_client.fetch_file_content_range(object_id=object_id, start=0, stop=115)

    # Upload some test data
    await upload_object(bucket_id=bucket_id, object_id=object_id, storage=storage)

    # Fetch and inspect data
    content = await s3_client.fetch_file_content_range(
        object_id=object_id, start=0, stop=115
    )
    assert content == b"this is some object content. " * 4


async def test_init_interrogation_bucket_upload(
    joint_fixture: JointFixture, s3_client: S3Client
):
    """Test the functionality of `S3Client.init_interrogation_bucket_upload()`"""
    object_id = str(uuid4())
    with pytest.raises(S3Client.BucketNotFoundError):
        await s3_client.init_interrogation_bucket_upload(object_id=object_id)

    # Create the interrogation bucket
    interrogation = joint_fixture.config.interrogation_storage_alias
    bucket_id = joint_fixture.config.object_storages[interrogation].bucket
    storage = joint_fixture.federated_s3.storages[interrogation].storage
    await storage.create_bucket(bucket_id)

    # Now successfully init the upload
    upload_id = await s3_client.init_interrogation_bucket_upload(object_id=object_id)
    assert isinstance(upload_id, str)

    # Repeat the operation to get an error
    with pytest.raises(S3Client.UploadInitError):
        upload_id = await s3_client.init_interrogation_bucket_upload(
            object_id=object_id
        )

    # Verify that we can now upload something
    url = await storage.get_part_upload_url(
        upload_id=upload_id, bucket_id=bucket_id, object_id=object_id, part_number=1
    )
    response = httpx.put(url, content=b"some content but not too much")
    assert response.status_code == 200


async def test_upload_file_part(joint_fixture: JointFixture, s3_client: S3Client):
    """Test the functionality of `S3Client.upload_file_part()`"""
    object_id = str(uuid4())
    part = b"some content but not too much"
    md5 = base64.b64encode(hashlib.md5(part).digest()).decode("ascii")
    bogus_md5 = base64.b64encode(hashlib.md5(b"junk").digest()).decode("ascii")

    # Try to upload file part when neither upload nor bucket yet exist
    with pytest.raises(S3Client.BucketNotFoundError):
        await s3_client.upload_file_part(
            upload_id="bogus",
            object_id=object_id,
            part_no=1,
            part=part,
            part_md5=md5,
        )

    # Create the bucket
    interrogation = joint_fixture.config.interrogation_storage_alias
    bucket_id = joint_fixture.config.object_storages[interrogation].bucket
    storage = joint_fixture.federated_s3.storages[interrogation].storage
    await storage.create_bucket(bucket_id)

    # Try to upload file part when bucket exists but upload does not
    with pytest.raises(S3Client.UploadError):
        await s3_client.upload_file_part(
            upload_id="bogus",
            object_id=object_id,
            part_no=1,
            part=part,
            part_md5=md5,
        )

    # Init the upload
    upload_id = await s3_client.init_interrogation_bucket_upload(object_id=object_id)

    # Supply part but wrong md5 (should get rejected)
    with pytest.raises(S3Client.BadPartMD5Error):
        await s3_client.upload_file_part(
            upload_id=upload_id,
            object_id=object_id,
            part_no=1,
            part=part,
            part_md5=bogus_md5,
        )

    # Now try to do it and hope for success
    await s3_client.upload_file_part(
        upload_id=upload_id,
        object_id=object_id,
        part_no=1,
        part=part,
        part_md5=md5,
    )

    # Now complete the upload and inspect storage to verify the part is there
    await storage.complete_multipart_upload(
        upload_id=upload_id,
        bucket_id=bucket_id,
        object_id=object_id,
    )
    url = await storage.get_object_download_url(
        bucket_id=bucket_id, object_id=object_id
    )
    uploaded_data = httpx.get(url)
    assert uploaded_data.content == part


async def test_complete_upload(joint_fixture: JointFixture, s3_client: S3Client):
    """Test the functionality of `S3Client.complete_upload()`"""
    # Try to complete an upload when neither upload nor bucket yet exist
    object_id = str(uuid4())
    with pytest.raises(S3Client.BucketNotFoundError):
        _ = await s3_client.complete_upload(
            upload_id="bogus", object_id=object_id, part_count=1
        )

    # Create the bucket
    interrogation = joint_fixture.config.interrogation_storage_alias
    bucket_id = joint_fixture.config.object_storages[interrogation].bucket
    storage = joint_fixture.federated_s3.storages[interrogation].storage
    await storage.create_bucket(bucket_id)

    # Complete an upload when the bucket exists but still no matching upload
    with pytest.raises(S3Client.UploadCompletionError):
        _ = await s3_client.complete_upload(
            upload_id="bogus", object_id=object_id, part_count=1
        )

    # Now create an upload
    upload_id = await s3_client.init_interrogation_bucket_upload(object_id=object_id)
    part = b"some content but not too much"
    unencoded_md5 = hashlib.md5(part).digest()
    md5 = base64.b64encode(unencoded_md5).decode("ascii")
    await s3_client.upload_file_part(
        upload_id=upload_id,
        object_id=object_id,
        part_no=1,
        part=part,
        part_md5=md5,
    )

    # Try to complete upload but use wrong part count
    # TODO: Make sure Interrogator class performs object deletion in all necessary cases
    with pytest.raises(S3Client.UploadCompletionError):
        _ = await s3_client.complete_upload(
            upload_id=upload_id, object_id=object_id, part_count=5
        )

    # Complete the upload and verify that the returned whole-object MD5 (etag)
    #  calculated by S3 is the same as what we expect.
    #  For reference, S3 calculates this by calculating a separate MD5 on each file
    #  part, then concatenating them, calculating the MD5 of THAT string, then appending
    #  '-{part_count}'. E.g. "28d90cb7156323004732ff359e54e659-1" for a 1-part object
    expected_object_md5 = hashlib.md5(unencoded_md5, usedforsecurity=False).hexdigest()
    expected_object_md5 += "-1"
    etag = await s3_client.complete_upload(
        upload_id=upload_id, object_id=object_id, part_count=1
    )
    etag = etag.strip('"')
    assert etag == expected_object_md5


async def test_abort_upload(joint_fixture: JointFixture, s3_client: S3Client):
    """Test the functionality of `S3Client.abort_upload()`"""
    # Try to abort an upload when neither upload nor bucket yet exist
    object_id = str(uuid4())
    with pytest.raises(S3Client.BucketNotFoundError):
        await s3_client.abort_upload(upload_id="bogus", object_id=object_id)

    # Create the bucket
    interrogation = joint_fixture.config.interrogation_storage_alias
    bucket_id = joint_fixture.config.object_storages[interrogation].bucket
    storage = joint_fixture.federated_s3.storages[interrogation].storage
    await storage.create_bucket(bucket_id)

    # Abort upload when the bucket exists but still no matching upload (should get no error)
    await s3_client.abort_upload(upload_id="bogus", object_id=object_id)

    # Carry out an upload but don't complete it yet
    upload_id = await s3_client.init_interrogation_bucket_upload(object_id=object_id)
    part = b"some content but not too much"
    unencoded_md5 = hashlib.md5(part).digest()
    md5 = base64.b64encode(unencoded_md5).decode("ascii")
    await s3_client.upload_file_part(
        upload_id=upload_id,
        object_id=object_id,
        part_no=1,
        part=part,
        part_md5=md5,
    )

    # Now abort but use wrong object ID (should get no error)
    await s3_client.abort_upload(upload_id=upload_id, object_id=str(uuid4()))

    # Abort but for real this time
    await s3_client.abort_upload(upload_id=upload_id, object_id=object_id)

    # Verify that the object is not in the bucket
    assert not await storage.does_object_exist(bucket_id=bucket_id, object_id=object_id)

    # Assert the multipart upload does not exist
    await storage._assert_no_multipart_upload(bucket_id=bucket_id, object_id=object_id)


async def test_remove_file_from_interrogation(
    joint_fixture: JointFixture, s3_client: S3Client
):
    """Test the functionality of `S3Client.remove_file()`"""
    # Try to remove a file for a bucket that doesn't exist
    object_id = str(uuid4())
    with pytest.raises(S3Client.BucketNotFoundError):
        await s3_client.remove_file(object_id=object_id)

    # Create the bucket
    interrogation = joint_fixture.config.interrogation_storage_alias
    bucket_id = joint_fixture.config.object_storages[interrogation].bucket
    storage = joint_fixture.federated_s3.storages[interrogation].storage
    await storage.create_bucket(bucket_id)

    # Try to remove the file, but expect no error
    await s3_client.remove_file(object_id=object_id)

    # Upload the file and assert it now exists
    await upload_object(bucket_id=bucket_id, object_id=object_id, storage=storage)
    assert object_id in await s3_client.list_files_in_interrogation_bucket()

    # Remove the file for real and check that it's gone
    await s3_client.remove_file(object_id=object_id)
    assert object_id not in await s3_client.list_files_in_interrogation_bucket()
