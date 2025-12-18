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

import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass

import httpx
from ghga_service_commons.utils.multinode_storage import S3ObjectStorages
from hexkit.protocols.objstorage import ObjectStorageProtocol
from pydantic import UUID4

from dhfs.adapters.outbound.http import get_configured_httpx_client
from dhfs.config import Config
from dhfs.ports.outbound.s3 import S3ClientPort

log = logging.getLogger(__name__)


class StorageAliasNotConfiguredError(RuntimeError):
    """Raised when looking up an object storage configuration by alias fails."""

    def __init__(self, *, alias: str):
        message = (
            f"Could not find a storage configuration for alias {alias}.\n"
            + "Check íf your multi node configuration contains a corresponding entry."
        )
        super().__init__(message)


async def _get_bucket_and_storage(
    object_storages: S3ObjectStorages, storage_alias: str
) -> tuple[str, ObjectStorageProtocol]:
    """Returns the bucket ID and object storage instance for an alias with error handling"""
    try:
        return object_storages.for_alias(storage_alias)
    except KeyError as exc:
        storage_alias_not_configured = StorageAliasNotConfiguredError(
            alias=storage_alias
        )
        log.critical(storage_alias_not_configured)
        raise storage_alias_not_configured from exc


@dataclass
class Storage:
    """A model encapsulating a bucket ID and object storage instance"""

    bucket_id: str
    object_storage: ObjectStorageProtocol


class S3Client(S3ClientPort):
    """Performs S3 upload/download operations with error handling"""

    @asynccontextmanager
    @classmethod
    async def construct(
        cls,
        *,
        config: Config,
        object_storages: S3ObjectStorages,
        httpx_client: httpx.AsyncClient,
    ):
        """Construct a configured S3Client instance"""
        inbox = await _get_bucket_and_storage(
            object_storages=object_storages, storage_alias=config.inbox_storage_alias
        )
        inbox_storage = Storage(*inbox)
        interrogation = await _get_bucket_and_storage(
            object_storages=object_storages,
            storage_alias=config.interrogation_storage_alias,
        )
        interrogation_storage = Storage(*interrogation)
        yield cls(
            config=config,
            inbox_storage=inbox_storage,
            interrogation_storage=interrogation_storage,
            httpx_client=httpx_client,
        )

    def __init__(
        self,
        *,
        config: Config,
        inbox_storage: Storage,
        interrogation_storage: Storage,
        httpx_client: httpx.AsyncClient,
    ) -> None:
        self._config = config
        self._inbox_bucket_id = inbox_storage.bucket_id
        self._inbox_storage = inbox_storage.object_storage
        self._interrogation_bucket_id = interrogation_storage.bucket_id
        self._interrogation_storage = interrogation_storage.object_storage
        self._httpx_client = httpx_client

    async def get_is_file_in_inbox(self, *, file_id: UUID4) -> bool:
        """Return a bool indicating whether the file exists in the inbox"""
        return await self._inbox_storage.does_object_exist(
            bucket_id=self._inbox_bucket_id, object_id=str(file_id)
        )

    async def _get_download_url(self, *, object_id: str) -> str:
        """Generate a download URL for an object in the inbox bucket.

        Relies on cache to prevent excessive outbound calls.
        """
        download_url = await self._inbox_storage.get_object_download_url(
            bucket_id=self._inbox_bucket_id, object_id=object_id
        )
        return download_url

    async def fetch_file_part(self, *, object_id: str, start: int, stop: int) -> bytes:
        """Download a single file part"""
        download_url = await self._get_download_url(object_id=object_id)
        headers = httpx.Headers(
            {
                "Range": f"bytes={start}-{stop}",
                "Cache-Control": "no-store",  # don't cache part downloads
            }
        )
        # TODO: error handling
        response = await self._httpx_client.get(download_url, headers=headers)
        return response.content

    async def init_interrogation_bucket_upload(self, *, object_id: str) -> str:
        """Start a multipart upload to the interrogation bucket"""
        # TODO: Handle errors (including object already exists)
        upload_id = await self._interrogation_storage.init_multipart_upload(
            bucket_id=self._interrogation_bucket_id, object_id=object_id
        )
        log.info("Created multipart upload ID %s for file ID %s", upload_id, object_id)
        return upload_id

    async def upload_file_part(
        self,
        *,
        upload_id: str,
        object_id: str,
        part_no: int,
        part_md5: str,
        part: bytes,
    ) -> None:
        """Upload a single re-encrypted file part"""
        upload_url = await self._interrogation_storage.get_part_upload_url(
            upload_id=upload_id,
            bucket_id=self._interrogation_bucket_id,
            object_id=object_id,
            part_number=part_no,
            part_md5=part_md5,
        )
        # TODO: error handling
        response = await self._httpx_client.put(upload_url, content=part)

    async def complete_upload(self, *, upload_id: str, object_id: str) -> str:
        """Complete a multipart upload for an object in the interrogation bucket.

        Returns the object's ETag (which is the MD5 checksum of the encrypted file).

        If an error occurs, the file will be removed from the interrogation bucket and
        interrogation will need to be repeated. This process should be made more
        intelligent in a future update.
        """
        try:
            await self._interrogation_storage.complete_multipart_upload(
                upload_id=upload_id,
                bucket_id=self._interrogation_bucket_id,
                object_id=object_id,
            )
            # Compare S3 ETag with locally calculated MD5
            etag = await self._interrogation_storage.get_object_etag(
                bucket_id=self._interrogation_bucket_id, object_id=object_id
            )
        except Exception as err:
            error = self.S3Error(
                f"A problem occurred trying to complete multipart upload {upload_id}"
                + f" for file {object_id} in the interrogation bucket"
                + f" ({self._interrogation_bucket_id})."
            )
            log.error(
                error,
                exc_info=True,
                extra={
                    "upload_id": upload_id,
                    "bucket_id": self._interrogation_bucket_id,
                    "object_id": object_id,
                },
            )
            raise error from err

        return etag.strip('"')

    async def abort_upload(self, *, upload_id: str, object_id: str) -> None:
        """Abort a multipart upload for an object in the interrogation bucket"""
        try:
            await self._interrogation_storage.abort_multipart_upload(
                upload_id=upload_id,
                bucket_id=self._interrogation_bucket_id,
                object_id=object_id,
            )
        except Exception as err:
            error = self.S3Error(
                f"Failed to abort multipart upload {upload_id} for file {object_id}"
                + f" in the interrogation bucket ({self._interrogation_bucket_id})."
            )
            log.error(
                error,
                exc_info=True,
                extra={
                    "upload_id": upload_id,
                    "bucket_id": self._interrogation_bucket_id,
                    "object_id": object_id,
                },
            )
            raise error from err

    async def remove_file(self, *, object_id: str) -> None:
        """Remove a file from the interrogation bucket"""
        try:
            await self._interrogation_storage.delete_object(
                bucket_id=self._interrogation_bucket_id,
                object_id=object_id,
            )
        except Exception as err:
            error = self.S3Error(
                f"Failed to delete {object_id} from the interrogation bucket"
                + f" ({self._interrogation_bucket_id})."
            )
            log.error(
                error,
                exc_info=True,
                extra={
                    "bucket_id": self._interrogation_bucket_id,
                    "object_id": object_id,
                },
            )
            raise error from err
