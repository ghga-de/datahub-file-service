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

from dhfs.models import FileUpload
from tests.fixtures.utils import get_encrypted_object


def test_file_upload():
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
