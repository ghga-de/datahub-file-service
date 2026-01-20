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

"""Unit tests for models"""

from uuid import uuid4

import crypt4gh.lib

from dhfs.models import FileUpload
from tests.fixtures.utils import get_encrypted_object


def test_file_upload_offset():
    """Test the computed properties of a FileUpload"""
    part_size = 5 * 1024**2
    encrypted_object = get_encrypted_object(part_size=part_size)
    f = FileUpload(
        id=uuid4(),
        storage_alias="inbox",
        decrypted_sha256="test",
        decrypted_size=encrypted_object.unencrypted_size,
        encrypted_size=encrypted_object.encrypted_size,
        part_size=part_size,
    )

    assert f.offset == encrypted_object.offset


def test_calc_part_ranges():
    """Test the calc_encrypted_part_ranges() method"""
    # Test with a file that has multiple parts
    part_size = 5 * 1024**2  # 5 MiB
    file_size = int(part_size * 2.5)  # 2.5 parts worth of data
    encrypted_object = get_encrypted_object(part_size=part_size, file_size=file_size)

    f = FileUpload(
        id=uuid4(),
        storage_alias="inbox",
        decrypted_sha256="test",
        decrypted_size=encrypted_object.unencrypted_size,
        encrypted_size=encrypted_object.encrypted_size,
        part_size=part_size,
    )

    # Calculate the encrypted segment size
    encrypted_segment_size = crypt4gh.lib.CIPHER_SEGMENT_SIZE

    # Calculate the expected adjusted part size
    segments_per_part = max(1, part_size // encrypted_segment_size)
    expected_adjusted_part_size = segments_per_part * encrypted_segment_size

    # Get all part ranges
    ranges = list(f.calc_encrypted_part_ranges())

    # Verify we got the expected number of parts (3 parts for 2.5 parts worth)
    assert len(ranges) == 3, f"Expected 3 parts, got {len(ranges)}"

    # Verify first range starts at the offset (after the envelope)
    assert ranges[0].start == f.offset

    # Verify last range ends at the encrypted size
    assert ranges[-1].stop == encrypted_object.encrypted_size

    # Verify ranges are contiguous (no gaps or overlaps)
    for i in range(len(ranges) - 1):
        assert ranges[i].stop == ranges[i + 1].start

    # Verify all ranges except possibly the last are the expected size
    for r in ranges[:-1]:
        size = r.stop - r.start
        assert size == expected_adjusted_part_size

    # Verify the last part size is <= expected_adjusted_part_size
    last_size = ranges[-1].stop - ranges[-1].start
    assert last_size <= expected_adjusted_part_size

    # Verify total coverage (all ranges cover encrypted_size - offset bytes)
    total_coverage = sum(r.stop - r.start for r in ranges)
    expected_coverage = encrypted_object.encrypted_size - f.offset
    assert total_coverage == expected_coverage


def test_calc_part_ranges_single_part():
    """Test calc_encrypted_part_ranges() with a small file that fits in one part"""
    part_size = 10 * 1024**2  # 10 MiB
    file_size = 1024**2  # 1 MiB (much smaller than part_size)
    encrypted_object = get_encrypted_object(part_size=part_size, file_size=file_size)

    f = FileUpload(
        id=uuid4(),
        storage_alias="inbox",
        decrypted_sha256="test",
        decrypted_size=encrypted_object.unencrypted_size,
        encrypted_size=encrypted_object.encrypted_size,
        part_size=part_size,
    )

    ranges = list(f.calc_encrypted_part_ranges())

    # Should have exactly one part
    assert len(ranges) == 1

    # Should cover the entire encrypted content (minus envelope)
    assert ranges[0].start == f.offset
    assert ranges[0].stop == encrypted_object.encrypted_size
