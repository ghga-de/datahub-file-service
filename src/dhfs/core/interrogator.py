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

import asyncio
import base64
import io
import logging
import math
import os
from collections.abc import Generator
from typing import Any

import crypt4gh.header
import crypt4gh.lib
from ghga_service_commons.utils.multinode_storage import S3ObjectStorages
from hexkit.protocols.objstorage import ObjectStorageProtocol
from hexkit.utils import now_utc_ms_prec
from nacl.bindings import (
    crypto_aead_chacha20poly1305_ietf_decrypt as decrypt_algo,
)
from nacl.bindings import (
    crypto_aead_chacha20poly1305_ietf_encrypt as encrypt_algo,
)
from pydantic import UUID4

from dhfs.config import Config
from dhfs.core.checksums import Checksums
from dhfs.models import FileUpload, InterrogationReport, PartRange
from dhfs.ports.outbound.central import CentralClientPort
from dhfs.ports.outbound.interrogator import InterrogatorPort
from dhfs.ports.outbound.s3 import S3ClientPort

log = logging.getLogger(__name__)


class Interrogator(InterrogatorPort):
    """Inspects and re-encrypts newly uploaded files"""

    def __init__(
        self,
        *,
        config: Config,
        central_client: CentralClientPort,
        s3_client: S3ClientPort,
    ):
        """Initialize the Interrogator"""
        self._inbox_storage_alias = config.inbox_storage_alias
        self._interrogation_storage_alias = config.interrogation_storage_alias
        self._central_client = central_client
        self._data_hub_private_key = config.data_hub_private_key
        self._s3_client = s3_client

    async def interrogate_new_files(self):
        new_files = await self._central_client.fetch_new_uploads()
        for file in new_files:
            try:
                await self.interrogate_file(file)
                # TODO: Make InterrogationError real
            except self.InterrogationError as err:
                await self.report_failure(file_id=file.id, reason=str(err))

    def _extract_envelope_content(self, *, file_part: bytes) -> tuple[bytes, int]:
        """Extract file encryption/decryption secret and file content offset from envelope"""
        envelope_stream = io.BytesIO(file_part)

        server_private_key = self._data_hub_private_key.get_secret_value()
        keys = [(0, server_private_key, None)]
        session_keys, _ = crypt4gh.header.deconstruct(
            infile=envelope_stream, keys=keys, sender_pubkey=None
        )

        original_file_secret = session_keys[0]
        offset = envelope_stream.tell()

        return original_file_secret, offset

    async def _compare_unencrypted_checksums(
        self, *, file_upload: FileUpload, new_checksum: str
    ):
        """Compare the decrypted-content SHA-256 with the one calculated by DHFS"""
        # TODO: Remove files that fail
        if new_checksum != file_upload.decrypted_sha256:
            log.warning(
                "SHA-256 checksum over unencrypted content for file %s does not match the value submitted with the file",
                file_upload.id,
            )
            await self.report_failure(
                file_id=file_upload.id,
                reason="SHA-256 checksum over unencrypted content does not match the value submitted with the file",
            )

    async def interrogate_file(self, file_upload: FileUpload):
        """Inspect and re-encrypt an newly uploaded file"""
        # Verify that the file exists in the inbox
        object_id = str(file_upload.id)
        if not await self._s3_client.get_is_file_in_inbox(object_id=object_id):
            raise self.FileNotFoundError(file_id=file_upload.id)

        # Get first file part in order to obtain the envelope
        envelope = await self._s3_client.get_file_envelope(
            object_id=object_id, part_size=file_upload.part_size
        )

        # Extract the file encryption secret (use logic from EKSS)
        original_secret, offset = self._extract_envelope_content(file_part=envelope)

        # Initiate multipart upload
        upload_id = await self._s3_client.init_interrogation_bucket_upload(
            object_id=object_id
        )

        # Calculate part ranges (start/end offsets for each part)
        part_ranges = file_upload.calc_encrypted_part_ranges(envelope_size=offset)
        # TODO: Find way to avoid re-downloading section of file in first part after offset. Maybe we can calculate part sizes backwards from end of file and then assume the envelope is the remainder

        # Generate new file encryption secret
        new_secret = os.urandom(32)

        # Establish Checksums object to track decrypted and encrypted content checksums
        checksums = Checksums()

        # Download, re-encrypt, and upload object part-by-part
        for part_no, part_range in enumerate(part_ranges):
            part = await self._s3_client.fetch_file_part(
                object_id=object_id, start=part_range.start, stop=part_range.stop
            )
            decrypted_part = decrypt_algo(part[12:], None, part[:12], original_secret)
            log.debug("Decrypted part number %i for file %s", part_no, object_id)

            # re-encrypt
            encrypted_part = encrypt_algo(
                decrypted_part, None, os.urandom(12), new_secret
            )
            log.debug("Re-encrypted part %i for file %s", part_no, object_id)

            # calculate part's encrypted md5 and sha256
            checksums.update_encrypted(encrypted_part)

            # decrypt again and update running sha256 on final decrypted content
            new_nonce = encrypted_part[:12]
            decrypted_part = decrypt_algo(
                encrypted_part[12:], None, new_nonce, new_secret
            )
            checksums.update_unencrypted(decrypted_part)

            # upload part
            part_md5 = checksums.encrypted_md5[-1]
            await self._s3_client.upload_file_part(
                upload_id=upload_id,
                object_id=object_id,
                part_no=part_no,
                part_md5=part_md5,
                part=encrypted_part,
            )

        # Compare final decrypted content checksum with the user-reported value
        await self._compare_unencrypted_checksums(
            file_upload=file_upload,
            new_checksum=checksums.unencrypted_sha256.hexdigest(),
        )

        # Complete upload
        expected_etag = checksums.encrypted_checksum_for_s3()
        actual_etag = await self._s3_client.complete_upload(
            upload_id=upload_id, object_id=object_id, expected_etag=expected_etag
        )
        if expected_etag != actual_etag:
            error = self.ChecksumMismatchError()
            log.error(error, extra={"object_id": object_id})
            raise error

        await self.report_success(
            file_id=file_upload.id,
            encrypted_parts_md5=checksums.encrypted_md5,
            encrypted_parts_sha256=checksums.encrypted_sha256,
        )

    async def report_success(
        self,
        *,
        file_id: UUID4,
        encrypted_parts_md5: list[str],
        encrypted_parts_sha256: list[str],
    ) -> None:
        """Submit an InterrogationReport for a successful interrogation"""
        report = InterrogationReport(
            file_id=file_id,
            storage_alias=self._inbox_storage_alias,
            interrogated_at=now_utc_ms_prec(),
            passed=True,
            encrypted_parts_md5=encrypted_parts_md5,
            encrypted_parts_sha256=encrypted_parts_sha256,
        )
        await self._central_client.submit_interrogation_report(report=report)

    async def report_failure(self, *, file_id: UUID4, reason: str) -> None:
        """Submit an InterrogationReport for an unsuccessful interrogation"""
        report = InterrogationReport(
            file_id=file_id,
            storage_alias=self._inbox_storage_alias,
            interrogated_at=now_utc_ms_prec(),
            passed=False,
            reason=reason,
        )
        await self._central_client.submit_interrogation_report(report=report)
