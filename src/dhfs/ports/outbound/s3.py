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

"""Port definition for a class that communicates with S3"""

from abc import ABC, abstractmethod

from pydantic import UUID4


class S3ClientPort(ABC):
    """Performs S3 upload/download operations with error handling"""

    class S3Error(RuntimeError):
        """Raised when there's a problem with an operation in S3"""

    @abstractmethod
    async def get_is_file_in_inbox(self, *, file_id: UUID4) -> bool:
        """Return a bool indicating whether the object exists in the inbox"""

    @abstractmethod
    async def fetch_file_part(self, *, object_id: str, start: int, stop: int) -> bytes:
        """Download a single file part"""
        ...

    @abstractmethod
    async def init_interrogation_bucket_upload(self, *, object_id: str) -> str:
        """Start a multipart upload to the interrogation bucket"""
        ...

    @abstractmethod
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
        ...

    @abstractmethod
    async def complete_upload(self, *, upload_id: str, object_id: str) -> str:
        """Complete a multipart upload for an object in the interrogation bucket"""
        ...

    @abstractmethod
    async def abort_upload(self, *, upload_id: str, object_id: str) -> None:
        """Abort a multipart upload for an object in the interrogation bucket"""
        ...

    @abstractmethod
    async def remove_file(self, *, object_id: str) -> None:
        """Remove a file from the interrogation bucket"""
        ...
