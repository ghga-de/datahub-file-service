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

"""Interface definition of a class that communicates with GHGA Central"""

from abc import ABC, abstractmethod

from pydantic import UUID4

from dhfs.models import FileUpload

__all__ = ["CentralClientPort"]


class CentralClientPort(ABC):
    """This class communicates with GHGA Central to learn about new file uploads"""

    @abstractmethod
    async def fetch_new_uploads(self) -> list[FileUpload]:
        """Fetches a list of files that need to be interrogated and re-encrypted"""
        ...

    @abstractmethod
    async def check_removability(self, *, file_id: UUID4) -> bool:
        """Asks the GHGA Central API if the object corresponding to the given file ID
        can be removed from `interrogation` bucket.
        """
        ...
