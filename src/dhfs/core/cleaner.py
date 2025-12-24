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

from dhfs.config import Config
from dhfs.ports.outbound.central import CentralClientPort
from dhfs.ports.outbound.cleaner import S3CleanerPort
from dhfs.ports.outbound.s3 import S3ClientPort

log = logging.getLogger(__name__)

__all__ = ["S3Cleaner"]


# TODO: Consolidate this class with the interrogator class
class S3Cleaner(S3CleanerPort):
    """Performs cleanup for the interrogation bucket"""

    def __init__(
        self,
        *,
        config: Config,
        central_client: CentralClientPort,
        s3_client: S3ClientPort,
    ):
        self._client = central_client
        self._s3_client = s3_client
        self._interrogation_storage_alias = config.interrogation_storage_alias

    async def scan_and_clean(self):
        """Get a list of all objects in the 'interrogation' bucket, then query the
        GHGA Central API and delete the objects which that API says may be deleted.
        """
        # TODO: Incomplete MPU cleanup - hexkit currently lacks a 'list ongoing MPUs' method
        object_ids = await self._s3_client.list_files_in_interrogation_bucket()

        # No need to convert file IDs to UUID here because they are serialized to string
        #  in the outbound request, and S3 expects strings. In short, we don't need the
        #  UUID properties, even for validation.
        removable_objects = await self._client.get_removable_files(file_ids=object_ids)

        for object_id in removable_objects:
            await self._s3_client.remove_file(object_id=object_id)
