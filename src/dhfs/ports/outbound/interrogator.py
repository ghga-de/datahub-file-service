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

"""Contains a port definition for file interrogation class"""

from abc import ABC, abstractmethod

from pydantic import UUID4, SecretBytes

from dhfs.models import FileUpload


class InterrogatorPort(ABC):
    """A class that inspects and re-encrypts files and places them in the interrogation bucket."""

    class CantCompleteError(RuntimeError):
        """Base error class for errors that prevent the interrogation process from
        completing before a conclusion can be reached about the outcome.
        """

    class FileNotFoundError(CantCompleteError):
        """Raised when a file isn't found in the inbox"""

        def __init__(self, *, file_id: UUID4):
            msg = f"The file {file_id} was not found in the inbox"
            super().__init__(msg)

    class ReencryptionError(CantCompleteError):
        """Raised when there's a problem during re-encryption. This is more likely
        caused by a code flaw than a problem with the file itself.
        """

    class InterrogationError(RuntimeError):
        """Base error class for errors that ultimately signal interrogation failure"""

    class FileEnvelopeDecryptionError(InterrogationError):
        """Raised when the file envelope can't be decrypted"""

    class DecryptionError(InterrogationError):
        """Raised when a file part can't be decrypted"""

    class ChecksumMismatchError(InterrogationError):
        """Raised when the MD5 checksum of the encrypted content doesn't match the expected value"""

    @abstractmethod
    async def interrogate_new_files(self) -> None:
        """Query the GHGA Central API for new files that need to be re-encrypted"""
        ...

    @abstractmethod
    async def interrogate_file(self, file_upload: FileUpload) -> None:
        """Inspect and re-encrypt an newly uploaded file"""

    @abstractmethod
    async def report_success(
        self,
        *,
        file_id: UUID4,
        secret: SecretBytes,
        encrypted_parts_md5: list[str],
        encrypted_parts_sha256: list[str],
    ) -> None:
        """Submit an InterrogationReport for a successful interrogation"""
        ...

    @abstractmethod
    async def report_failure(self, *, file_id: UUID4, reason: str) -> None:
        """Submit an InterrogationReport for an unsuccessful interrogation"""
        ...
