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

"""Outbound adapter that interacts directly with S3 object storage"""

import logging
from dataclasses import dataclass

import httpx
from ghga_service_commons.utils.multinode_storage import S3ObjectStorages
from hexkit.protocols.objstorage import ObjectStorageProtocol
from pydantic import UUID4
from tenacity import RetryError

from dhfs.adapters.outbound.http import check_for_request_errors
from dhfs.config import Config
from dhfs.ports.outbound.s3 import S3ClientPort

log = logging.getLogger(__name__)

__all__ = ["S3Client", "StorageAliasNotConfiguredError"]


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

    async def list_files_in_interrogation_bucket(self) -> list[str]:
        """Returns a list of object IDs from the interrogation bucket"""
        try:
            object_ids = await self._interrogation_storage.list_all_object_ids(
                bucket_id=self._interrogation_bucket_id
            )
            log.info(
                "Retrieved list of %i object IDs from bucket ID '%s'",
                len(object_ids),
                self._interrogation_bucket_id,
            )
            return object_ids
        except ObjectStorageProtocol.BucketNotFoundError as err:
            bucket_error = self.BucketNotFoundError(
                f"Failed to get list of files in the {self._interrogation_bucket_id}"
                + " bucket because the bucket doesn't exist."
            )
            log.error(
                bucket_error,
                exc_info=True,
                extra={"bucket_id": self._interrogation_bucket_id},
            )
            raise bucket_error from err
        # TODO: Generic error catch here (or in the core?)

    async def _get_download_url(self, *, object_id: str) -> str:
        """Generate a download URL for an object in the inbox bucket.

        Relies on cache to prevent excessive outbound calls.
        """
        try:
            download_url = await self._inbox_storage.get_object_download_url(
                bucket_id=self._inbox_bucket_id, object_id=object_id
            )
            return download_url
        except ObjectStorageProtocol.BucketNotFoundError as err:
            bucket_error = self.BucketNotFoundError(
                f"Cannot get download URL for file {object_id} because"
                + f" the bucket {self._inbox_bucket_id} does not exist."
            )
            log.error(bucket_error, exc_info=True)
            raise bucket_error from err
        except ObjectStorageProtocol.ObjectNotFoundError as err:
            object_missing_error = self.ObjectNotFoundError(
                f"Cannot get download URL for file {object_id} because it doesn't exist."
            )
            log.error(object_missing_error, exc_info=True)
            raise object_missing_error from err
        except Exception as err:
            # Catch-all
            error = self.DownloadError(
                "An unexpected error occurred while trying to generate a download URL"
                + f" for file {object_id} in bucket {self._inbox_bucket_id}."
            )
            log.error(error, exc_info=True)
            raise error from err

    async def fetch_file_content_range(
        self, *, object_id: str, start: int, stop: int
    ) -> bytes:
        """Download a single file part for the bytes in range `start` - `stop` (inclusive)."""
        download_url = await self._get_download_url(object_id=object_id)
        headers = httpx.Headers(
            {
                "Range": f"bytes={start}-{stop}",
                "Cache-Control": "no-store",  # don't cache part downloads
            }
        )
        response = await self._httpx_client.get(download_url, headers=headers)

        status_code = response.status_code
        if status_code in (200, 206):
            return response.content

        # Raise a generic download error if the status code is not 200
        error = self.DownloadError(
            f"Received a {status_code} error when trying to download file {object_id}"
            + f" from bucket {self._inbox_bucket_id}."
        )
        log.error(error, extra={"response_detail": response.content.decode("ascii")})
        raise error

    async def init_interrogation_bucket_upload(self, *, object_id: str) -> str:
        """Start a multipart upload to the interrogation bucket"""
        try:
            upload_id = await self._interrogation_storage.init_multipart_upload(
                bucket_id=self._interrogation_bucket_id, object_id=object_id
            )
            log.info(
                "Created multipart upload ID %s for file ID %s", upload_id, object_id
            )
            return upload_id
        except ObjectStorageProtocol.MultiPartUploadAlreadyExistsError as err:
            upload_exists_error = self.UploadInitError(
                f"Cannot create a multipart upload for file {object_id} because an"
                + " upload already exists."
            )
            log.error(upload_exists_error)
            raise upload_exists_error from err
        except ObjectStorageProtocol.ObjectAlreadyExistsError as err:
            object_exists_error = self.UploadInitError(
                f"Cannot create a multipart upload for file {object_id} because"
                + " the object already exists."
            )
            log.error(object_exists_error)
            raise object_exists_error from err
        except ObjectStorageProtocol.BucketNotFoundError as err:
            # This is logged as critical because the bucket should definitely exist
            bucket_error = self.BucketNotFoundError(
                f"Cannot create a multipart upload for file {object_id} because"
                + f" the bucket {self._interrogation_bucket_id} does not exist."
            )
            log.critical(bucket_error)
            raise bucket_error from err
        except Exception as err:
            # Catch-all
            error = self.UploadInitError(
                "An unexpected error occurred trying to initiate a multipart upload"
                + f" for file {object_id} in bucket {self._interrogation_bucket_id}."
            )
            log.error(error, exc_info=True)
            raise error from err

    async def _get_part_upload_url(
        self,
        *,
        upload_id: str,
        object_id: str,
        part_no: int,
        part_md5: str,
    ) -> str:
        """Retrieve a presigned part upload URL for a given file part"""
        try:
            return await self._interrogation_storage.get_part_upload_url(
                upload_id=upload_id,
                bucket_id=self._interrogation_bucket_id,
                object_id=object_id,
                part_number=part_no,
                part_md5=part_md5,
            )
        except ObjectStorageProtocol.BucketNotFoundError as err:
            # This is logged as critical because the bucket should definitely exist
            bucket_error = self.BucketNotFoundError(
                f"Failed to get part upload URL for upload {upload_id} because the"
                + f" interrogation bucket {self._interrogation_bucket_id} does not exist."
            )
            log.critical(
                bucket_error,
                exc_info=True,
                extra={
                    "upload_id": upload_id,
                    "object_id": object_id,
                    "part_no": part_no,
                },
            )
            raise bucket_error from err
        except ObjectStorageProtocol.MultiPartUploadNotFoundError as err:
            error = self.UploadError(
                f"Failed to get part upload URL for upload {upload_id} because the"
                + " upload does not exist."
            )
            log.critical(
                error,
                exc_info=True,
                extra={
                    "upload_id": upload_id,
                    "object_id": object_id,
                    "part_no": part_no,
                },
            )
            raise error from err

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
        upload_url = await self._get_part_upload_url(
            upload_id=upload_id,
            object_id=object_id,
            part_no=part_no,
            part_md5=part_md5,
        )

        try:
            log.debug("Uploading file part number %i for %s", part_no, object_id)
            response = await self._httpx_client.put(upload_url, content=part)
        except RetryError as retry_error:
            check_for_request_errors(retry_error, upload_url)
            response = retry_error.last_attempt.result()

        status_code = response.status_code
        if status_code == 400:
            md5_error = self.BadPartMD5Error(part_no=part_no, object_id=object_id)
            log.error(md5_error)
            raise md5_error
        if response.status_code != 200:
            upload_error = self.UploadError(
                f"Failed to upload part {part_no} for file {object_id}. Status"
                + f" code is {response.status_code}. Detail: {response.content}"
            )
            log.error(upload_error)
            raise upload_error

    async def complete_upload(
        self, *, upload_id: str, object_id: str, part_count: int
    ) -> str:
        """Complete a multipart upload for an object in the interrogation bucket.

        Returns the object's ETag (which is the MD5 checksum of the encrypted file).

        If an error occurs, the file will be removed from the interrogation bucket and
        interrogation will need to be repeated. This process should be made more
        intelligent in a future update.
        """
        extra = {  # for logging purposes
            "upload_id": upload_id,
            "bucket_id": self._interrogation_bucket_id,
            "object_id": object_id,
            "part_count": part_count,
        }
        try:
            await self._interrogation_storage.complete_multipart_upload(
                upload_id=upload_id,
                bucket_id=self._interrogation_bucket_id,
                object_id=object_id,
                anticipated_part_quantity=part_count,
            )
            # Compare S3 ETag with locally calculated MD5
            etag = await self._interrogation_storage.get_object_etag(
                bucket_id=self._interrogation_bucket_id, object_id=object_id
            )
            return etag.strip('"')
        except ObjectStorageProtocol.BucketNotFoundError as err:
            # This is logged as critical because the bucket should definitely exist
            bucket_error = self.BucketNotFoundError(
                f"Couldn't complete upload {upload_id} for file {object_id} because"
                + f" the bucket {self._interrogation_bucket_id} does not exist."
            )
            log.critical(bucket_error, exc_info=True, extra=extra)
            raise bucket_error from err
        except ObjectStorageProtocol.MultiPartUploadConfirmError as err:
            error = self.UploadCompletionError(
                f"S3 rejected upload {upload_id} for file {object_id} due to a difference"
                + f" in the expected part count {part_count}."
            )
            log.error(error, exc_info=True, extra=extra)
            raise error from err
        except ObjectStorageProtocol.MultiPartUploadNotFoundError as err:
            error = self.UploadCompletionError(
                f"Couldn't complete upload {upload_id} for file {object_id} because"
                + " the upload doesn't exist.",
            )
            log.error(error, exc_info=True, extra=extra)
            raise error from err
        except Exception as err:
            error = self.UploadCompletionError(
                f"A problem occurred trying to complete multipart upload {upload_id}"
                + f" for file {object_id} in the interrogation bucket"
                + f" ({self._interrogation_bucket_id})."
            )
            log.error(error, exc_info=True, extra=extra)
            raise error from err

    async def abort_upload(self, *, upload_id: str, object_id: str) -> None:
        """Abort a multipart upload for an object in the interrogation bucket"""
        extra = {  # only used for logging
            "upload_id": upload_id,
            "bucket_id": self._interrogation_bucket_id,
            "object_id": object_id,
        }

        try:
            await self._interrogation_storage.abort_multipart_upload(
                upload_id=upload_id,
                bucket_id=self._interrogation_bucket_id,
                object_id=object_id,
            )
        except ObjectStorageProtocol.BucketNotFoundError as err:
            # This is logged as critical because the bucket should definitely exist
            bucket_error = self.BucketNotFoundError(
                f"Couldn't abort upload {upload_id} for file {object_id} because"
                + f" the bucket {self._interrogation_bucket_id} does not exist."
            )
            log.critical(bucket_error, exc_info=True, extra=extra)
            raise bucket_error from err
        except ObjectStorageProtocol.MultiPartUploadNotFoundError:
            # If not found, log warning and assume it was already aborted.
            log.warning(
                "Tried to abort multipart upload with ID %s (for file %s), but S3 said it doesn't exist.",
                upload_id,
                object_id,
            )
        except Exception as err:
            error = self.S3Error(
                f"Failed to abort multipart upload {upload_id} for file {object_id}"
                + f" in the interrogation bucket ({self._interrogation_bucket_id})."
            )
            log.error(
                error,
                exc_info=True,
                extra=extra,
            )
            raise error from err

    async def remove_file(self, *, object_id: str) -> None:
        """Remove a file from the interrogation bucket"""
        try:
            await self._interrogation_storage.delete_object(
                bucket_id=self._interrogation_bucket_id,
                object_id=object_id,
            )
            log.info(
                "Successfully removed %s from bucket ID %s",
                object_id,
                self._interrogation_bucket_id,
            )
        except ObjectStorageProtocol.BucketNotFoundError as err:
            # This is logged as critical because the bucket should definitely exist
            bucket_id = self._interrogation_bucket_id
            bucket_error = self.BucketNotFoundError(
                f"The bucket {bucket_id} was not found while trying to remove"
                + f" file {object_id}. This error should not have occurred."
            )
            log.critical(
                bucket_error,
                exc_info=True,
                extra={"bucket_id": bucket_id, "object_id": object_id},
            )
            raise bucket_error from err
        except ObjectStorageProtocol.ObjectNotFoundError:
            # If not found, assume the object was already deleted but log a warning
            log.warning(
                "Tried to delete file %s from bucket %s, but it's already deleted",
                object_id,
                self._interrogation_bucket_id,
            )
        except Exception as err:
            error = self.S3CleanupError(
                bucket_id=self._interrogation_bucket_id, object_id=object_id
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


async def get_s3_client(
    *,
    config: Config,
    object_storages: S3ObjectStorages,
    httpx_client: httpx.AsyncClient,
) -> S3Client:
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
    return S3Client(
        config=config,
        inbox_storage=inbox_storage,
        interrogation_storage=interrogation_storage,
        httpx_client=httpx_client,
    )
