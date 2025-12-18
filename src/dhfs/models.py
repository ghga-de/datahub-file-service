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

import crypt4gh.lib
from ghga_service_commons.utils.utc_dates import UTCDatetime
from pydantic import UUID4, BaseModel

__all__ = ["FileUpload", "InterrogationReport"]


@dataclass
class PartRange:
    """Container for inclusive download ranges"""

    start: int
    stop: int


@dataclass
class FileUpload:
    """Represents a file that needs to be interrogated and re-encrypted"""

    id: UUID4
    storage_alias: str
    decrypted_sha256: str
    decrypted_size: int
    encrypted_size: int
    part_size: int

    def calc_encrypted_part_ranges(self, *, envelope_size: int) -> Generator[PartRange]:
        """Calculate file part ranges that align with the Crypt4GH segment size"""
        processed = 0
        ranges = []
        file_size = self.encrypted_size - envelope_size
        while processed < file_size:
            start = processed
            processed += 12  # nonce
            processed += crypt4gh.lib.SEGMENT_SIZE
            processed += 16  # auth tag
            end = min(processed, file_size)
            ranges.append((start, end))
        yield from ranges


class InterrogationReport(BaseModel):
    """Model representing the outcome of a file interrogation"""

    file_id: UUID4
    storage_alias: str
    interrogated_at: UTCDatetime
    passed: bool
    encrypted_parts_md5: list[str] | None = None
    encrypted_parts_sha256: list[str] | None = None
    reason: str | None = None
