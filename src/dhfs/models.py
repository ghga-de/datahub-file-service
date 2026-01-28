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

"""Models for objects in the DHFS"""

import logging
from collections.abc import Generator
from dataclasses import dataclass
from functools import cached_property

import crypt4gh.lib
from ghga_service_commons.utils.utc_dates import UTCDatetime
from pydantic import UUID4, BaseModel, SecretBytes, computed_field

from dhfs.constants import AUTH_TAG_LENGTH, NONCE_LENGTH

__all__ = ["FileUpload", "InterrogationReport", "PartRange"]


log = logging.getLogger(__name__)


@dataclass
class PartRange:
    """Container for download ranges"""

    start: int
    stop: int


class FileUpload(BaseModel):
    """Represents a file that needs to be interrogated and re-encrypted"""

    id: UUID4
    data_hub: str
    storage_alias: str
    decrypted_sha256: str
    decrypted_size: int
    encrypted_size: int
    part_size: int

    @computed_field  # type: ignore[prop-decorator]
    @cached_property
    def offset(self) -> int:
        """Calculate the size of the file encryption envelope/where content begins"""
        # The number of encrypted chunks produced during encryption depends on the file
        #  size. ChaCha20Poly1305 encrypts SEGMENT_SIZE bytes at a time
        chunks = self.decrypted_size // crypt4gh.lib.SEGMENT_SIZE

        # Each full-length encrypted chunk in the file is CIPHER_SEGMENT_SIZE bytes long
        # The difference is 28 bytes. This comes from a 12-byte NONCE and a 16-byte auth tag.
        chunk_size = crypt4gh.lib.CIPHER_SEGMENT_SIZE

        # The last bytes of the file, which are less than SEGMENT_SIZE, are encrypted as
        #  is - no magic padding. So if there are 40 straggler bytes, it's 68 when encrypted.
        unencrypted_remainder = self.decrypted_size - crypt4gh.lib.SEGMENT_SIZE * chunks
        size_sans_envelope = chunk_size * chunks
        if unencrypted_remainder:
            size_sans_envelope += unencrypted_remainder + NONCE_LENGTH + AUTH_TAG_LENGTH

        # We can therefore calculate the encrypted file size given the decrypted size,
        #  and use that to calculate the size of the envelope / offset of the content.
        return self.encrypted_size - size_sans_envelope

    def calc_encrypted_part_ranges(self) -> Generator[PartRange]:
        """Calculate file part ranges that align with the Crypt4GH segment size"""
        # Each encrypted segment consists of: nonce + encrypted_data + auth_tag
        encrypted_segment_size = (
            NONCE_LENGTH + crypt4gh.lib.SEGMENT_SIZE + AUTH_TAG_LENGTH
        )

        # Adjust part_size to be a multiple of the encrypted segment size
        # This ensures we download complete segments that can be decrypted
        segments_per_part = max(1, self.part_size // encrypted_segment_size)
        adjusted_part_size = segments_per_part * encrypted_segment_size

        if adjusted_part_size != self.part_size:
            log.info(
                "Adjusted part size from %d to %d bytes to align with Crypt4GH segment boundaries for file %s",
                self.part_size,
                adjusted_part_size,
                self.id,
            )

        processed = self.offset
        while processed < self.encrypted_size:
            start = processed
            processed += adjusted_part_size
            end = min(processed, self.encrypted_size)
            yield PartRange(start, end)


class InterrogationReport(BaseModel):
    """Model representing the outcome of a file interrogation"""

    file_id: UUID4
    storage_alias: str
    interrogated_at: UTCDatetime
    passed: bool
    secret: SecretBytes | None = None
    encrypted_parts_md5: list[str] | None = None
    encrypted_parts_sha256: list[str] | None = None
    reason: str | None = None
