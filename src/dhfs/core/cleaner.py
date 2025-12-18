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

"""Post-interrogation S3 bucket cleanup logic"""

import logging

from ghga_service_commons.utils.multinode_storage import S3ObjectStorages

from dhfs.config import Config
from dhfs.ports.outbound.central import CentralClientPort
from dhfs.ports.outbound.cleaner import S3CleanerPort

log = logging.getLogger(__name__)

__all__ = ["S3Cleaner"]


class S3Cleaner(S3CleanerPort):
    """Performs cleanup for the interrogation bucket"""

    def __init__(
        self,
        *,
        central_client: CentralClientPort,
        object_storages: S3ObjectStorages,
        config: Config,
    ):
        self._client = central_client
        self._object_storages = object_storages
        self._interrogation_storage_alias = config.interrogation_storage_alias

    class S3CleanupError(RuntimeError):
        """Raised when there's a problem deleting an object from S3 during cleanup"""

        def __init__(self, *, bucket_id: str, object_id: str):
            msg = f"Failed to delete object {object_id} from the {bucket_id} bucket."
            super().__init__(msg)

    async def scan_and_clean(self):
        """Get a list of all objects in the 'interrogation' bucket, then query the
        GHGA Central API and delete the objects which that API says may be deleted.
        """
        # TODO: Incomplete MPU cleanup - hexkit currently lacks a 'list ongoing MPUs' method
        try:
            bucket_id, object_storage = self._object_storages.for_alias(
                self._interrogation_storage_alias
            )
        except KeyError as exc:
            storage_alias_not_configured = self.StorageAliasNotConfiguredError(
                alias=self._interrogation_storage_alias
            )
            log.critical(storage_alias_not_configured)
            raise storage_alias_not_configured from exc

        object_ids = await object_storage.list_all_object_ids(bucket_id)
        log.info(
            "Retrieved list of %i object IDs from bucket ID '%s'",
            len(object_ids),
            bucket_id,
        )

        # No need to convert file IDs to UUID here because they are serialized to string
        #  in the outbound request, and S3 expects strings. In short, we don't need the
        #  UUID properties, even for validation.
        removable_objects = await self._client.get_removable_files(file_ids=object_ids)

        for object_id in removable_objects:
            try:
                await object_storage.delete_object(
                    bucket_id=bucket_id, object_id=str(object_id)
                )
                log.info(
                    "Successfully removed %s from bucket ID %s", object_id, bucket_id
                )
            except Exception as err:
                error = self.S3CleanupError(bucket_id=bucket_id, object_id=object_id)
                log.error(error, exc_info=True)
                raise error from err
