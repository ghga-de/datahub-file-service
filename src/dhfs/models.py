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
from math import ceil

import crypt4gh.lib
from ghga_service_commons.utils.utc_dates import UTCDatetime
from pydantic import UUID4, BaseModel, SecretBytes, computed_field

from dhfs.constants import AUTH_TAG_LENGTH, NONCE_LENGTH

__all__ = ["FileUpload", "InterrogationReport", "PartRange"]


log = logging.getLogger(__name__)


@dataclass
class PartRange:
    """Container for inclusive download ranges"""

    start: int
    stop: int

    # segment_boundaries:


class FileUpload(BaseModel):
    """Represents a file that needs to be interrogated and re-encrypted"""

    id: UUID4
    storage_alias: str
    decrypted_sha256: str
    decrypted_size: int
    encrypted_size: int
    part_size: int

    @computed_field  # type: ignore[prop-decorator]
    @cached_property
    def encrypted_part_count(self) -> int:
        """Calculate the number of file parts in the re-encrypted object"""
        x = (self.decrypted_size - self.offset) / self.part_size
        return ceil(x)

    @computed_field  # type: ignore[prop-decorator]
    @cached_property
    def offset(self) -> int:
        """Calculate the size of the file encryption envelope/where content begins"""
        chunk_size = NONCE_LENGTH + crypt4gh.lib.SEGMENT_SIZE + AUTH_TAG_LENGTH
        chunks = self.decrypted_size // crypt4gh.lib.SEGMENT_SIZE
        unencrypted_remainder = self.decrypted_size - crypt4gh.lib.SEGMENT_SIZE * chunks
        size_sans_envelope = chunk_size * chunks
        if unencrypted_remainder:
            size_sans_envelope += unencrypted_remainder + NONCE_LENGTH + AUTH_TAG_LENGTH
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
        ranges = []
        while processed < self.encrypted_size:
            start = processed
            processed += adjusted_part_size
            end = min(processed, self.encrypted_size)
            ranges.append(PartRange(start, end))
        yield from ranges


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
