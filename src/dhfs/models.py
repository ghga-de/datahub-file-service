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

from collections.abc import Generator
from dataclasses import dataclass
from functools import cached_property
from math import ceil

import crypt4gh.lib
from ghga_service_commons.utils.utc_dates import UTCDatetime
from pydantic import UUID4, BaseModel, SecretBytes, computed_field

from dhfs.constants import AUTH_TAG_LENGTH, NONCE_LENGTH

__all__ = ["FileUpload", "InterrogationReport", "PartRange"]


@dataclass
class PartRange:
    """Container for inclusive download ranges"""

    start: int
    stop: int


class FileUpload(BaseModel):
    """Represents a file that needs to be interrogated and re-encrypted"""

    id: UUID4
    storage_alias: str
    decrypted_sha256: str
    decrypted_size: int
    encrypted_size: int
    part_size: int

    @cached_property
    @computed_field
    def encrypted_part_count(self) -> int:
        """Calculate the number of file parts in the re-encrypted object"""
        x = (self.decrypted_size - self.offset) / self.part_size
        return ceil(x)

    @cached_property
    @computed_field
    def offset(self) -> int:
        """Calculate the size of the file encryption envelope/where content begins"""
        chunk_size = NONCE_LENGTH + crypt4gh.lib.SEGMENT_SIZE + AUTH_TAG_LENGTH
        chunks = self.decrypted_size // crypt4gh.lib.SEGMENT_SIZE
        unencrypted_remainder = self.decrypted_size - crypt4gh.lib.SEGMENT_SIZE * chunks
        size_sans_envelope = (chunk_size * chunks) + (
            unencrypted_remainder + NONCE_LENGTH + AUTH_TAG_LENGTH
        )
        return self.encrypted_size - size_sans_envelope

    def calc_encrypted_part_ranges(self) -> Generator[PartRange]:
        """Calculate file part ranges that align with the Crypt4GH segment size"""
        processed = 0
        ranges = []
        file_size = self.encrypted_size - self.offset
        while processed < file_size:
            start = processed
            processed += NONCE_LENGTH  # nonce
            processed += crypt4gh.lib.SEGMENT_SIZE
            processed += AUTH_TAG_LENGTH  # auth tag
            end = min(processed, file_size)
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
