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

"""Core logic for file re-encryption and interrogation"""

import io
import logging
import os

import crypt4gh.header
from hexkit.utils import now_utc_ms_prec
from nacl.bindings import (
    crypto_aead_chacha20poly1305_ietf_decrypt as decrypt_algo,
)
from nacl.bindings import (
    crypto_aead_chacha20poly1305_ietf_encrypt as encrypt_algo,
)
from pydantic import UUID4, SecretBytes

from dhfs.config import Config
from dhfs.constants import ENCRYPTION_SECRET_LENGTH, NONCE_LENGTH
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

    async def interrogate_new_files(self) -> None:
        """Query the GHGA Central API for new files that need to be re-encrypted"""
        new_files = await self._central_client.fetch_new_uploads()
        for file in new_files:
            try:
                # Verify that the file exists in the inbox before proceeding
                if not await self._s3_client.get_is_file_in_inbox(file_id=file.id):
                    raise self.FileNotFoundError(file_id=file.id)
                await self.interrogate_file(file)
            except self.InterrogationError as err:
                await self.report_failure(file_id=file.id, reason=str(err))

    async def _fetch_original_secret(self, *, file_upload: FileUpload) -> SecretBytes:
        """Fetch the original file encryption secret"""
        envelope = await self._s3_client.fetch_file_part(
            object_id=str(file_upload.id), start=0, stop=file_upload.offset
        )
        return self._extract_secret(envelope=envelope)

    def _extract_secret(self, *, envelope: bytes) -> SecretBytes:
        """Extract file encryption/decryption secret from envelope"""
        envelope_stream = io.BytesIO(envelope)
        keys = [(0, self._data_hub_private_key.get_secret_value(), None)]
        session_keys, _ = crypt4gh.header.deconstruct(
            infile=envelope_stream, keys=keys, sender_pubkey=None
        )
        return SecretBytes(session_keys[0])

    async def _fetch_and_decrypt_part(
        self, *, object_id: str, part_range: PartRange, secret: SecretBytes
    ) -> bytes:
        part = await self._s3_client.fetch_file_part(
            object_id=object_id, start=part_range.start, stop=part_range.stop
        )
        return self._decrypt_part(encrypted_part=part, secret=secret)

    def _decrypt_part(self, *, encrypted_part: bytes, secret: SecretBytes) -> bytes:
        """Decrypt an encrypted file part with the given key, and handle errors"""
        try:
            return decrypt_algo(
                encrypted_part[NONCE_LENGTH:],  # data to decrypt (after nonce)
                None,
                encrypted_part[:NONCE_LENGTH],  # nonce (first 12 bytes)
                secret.get_secret_value(),
            )
        except Exception as err:
            raise self.DecryptionError() from err

    def _reencrypt_part(
        self, *, decrypted_part: bytes, new_secret: SecretBytes
    ) -> bytes:
        """Re-encrypt a decrypted file part using a new secret"""
        try:
            return encrypt_algo(
                decrypted_part,
                None,
                os.urandom(NONCE_LENGTH),
                new_secret.get_secret_value(),
            )
        except Exception as err:
            error = self.ReencryptionError()
            log.error(
                error,
                exc_info=True,
            )
            raise error from err

    async def _compare_unencrypted_checksums(
        self, *, file_upload: FileUpload, new_checksum: str
    ):
        """Compare the decrypted-content SHA-256 with the one calculated by DHFS."""
        if new_checksum != file_upload.decrypted_sha256:
            log.warning(
                "SHA-256 checksum over unencrypted content for file %s does not match"
                + " the value submitted with the file",
                file_upload.id,
            )

            raise self.ChecksumMismatchError(
                "SHA-256 checksum over unencrypted content does not match the"
                + " value submitted with the file"
            )

    async def _process_file_parts(
        self,
        *,
        file_upload: FileUpload,
        upload_id: str,
        old_secret: SecretBytes,
        new_secret: SecretBytes,
    ) -> Checksums:
        """Perform the decrypt/re-encrypt/decrypt/upload cycle on each file part.

        Returns the `Checksums` object containing the checksums calculated during
        the file processing.
        """
        # Establish Checksums object to track decrypted and encrypted content checksums
        checksums = Checksums()
        object_id = str(file_upload.id)

        # Download, re-encrypt, and upload object part-by-part
        for part_no, part_range in enumerate(file_upload.calc_encrypted_part_ranges()):
            # Initial decryption
            try:
                decrypted_part = await self._fetch_and_decrypt_part(
                    object_id=object_id, part_range=part_range, secret=old_secret
                )
                log.debug("Decrypted part number %i for file %s", part_no, object_id)
            except self.DecryptionError:
                log.error(
                    "Failed initial decryption of part number %i of file %s",
                    part_no,
                    object_id,
                )
                raise

            # Re-encrypt
            try:
                reencrypted_part = self._reencrypt_part(
                    decrypted_part=decrypted_part, new_secret=new_secret
                )
                log.debug("Re-encrypted part %i for file %s", part_no, object_id)
            except self.ReencryptionError:
                log.error(
                    "Failed to re-encrypt part number %i of file %s", part_no, object_id
                )

            # Decrypt again to verify encryption process was correct
            try:
                decrypted_part = self._decrypt_part(
                    encrypted_part=reencrypted_part, secret=new_secret
                )
                log.debug(
                    "Successfully performed confirmatory decryption on part number %i"
                    + " of file %s",
                    part_no,
                    object_id,
                )
            except self.DecryptionError:
                log.error(
                    "Failed confirmatory decryption of part number %i of file %s",
                    part_no,
                    object_id,
                )
                raise

            # Calculate part's encrypted md5 & sha256 and update whole-decrypted-file sha256
            checksums.update_encrypted(reencrypted_part)
            checksums.update_unencrypted(decrypted_part)

            await self._s3_client.upload_file_part(
                upload_id=upload_id,
                object_id=object_id,
                part_no=part_no,
                part_md5=checksums.encrypted_md5[-1],
                part=reencrypted_part,
            )
        return checksums

    async def interrogate_file(self, file_upload: FileUpload) -> None:
        """Inspect and re-encrypt an newly uploaded file"""
        # Extract the file encryption secret and content offset
        old_secret = await self._fetch_original_secret(file_upload=file_upload)

        # Initiate multipart upload
        object_id = str(file_upload.id)
        upload_id = await self._s3_client.init_interrogation_bucket_upload(
            object_id=object_id
        )

        # Generate new file encryption secret
        new_secret = SecretBytes(os.urandom(ENCRYPTION_SECRET_LENGTH))

        # Re-encrypt and upload file parts, obtaining the checksums for the decrypted
        #  and re-encrypted content
        checksums = await self._process_file_parts(
            file_upload=file_upload,
            upload_id=upload_id,
            old_secret=old_secret,
            new_secret=new_secret,
        )

        # Compare final decrypted content checksum with the user-reported value
        try:
            await self._compare_unencrypted_checksums(
                file_upload=file_upload,
                new_checksum=checksums.unencrypted_sha256.hexdigest(),
            )
        except self.ChecksumMismatchError:
            log.debug("Removing file %s from interrogation bucket", file_upload.id)
            await self._s3_client.abort_upload(upload_id=upload_id, object_id=object_id)
            raise

        # Complete upload
        actual_etag = await self._s3_client.complete_upload(
            upload_id=upload_id, object_id=object_id
        )

        # Check integrity of final object in S3
        expected_etag = checksums.encrypted_checksum_for_s3()
        if expected_etag != actual_etag:
            error = self.ChecksumMismatchError(
                "S3 ETag didn't match the expected MD5 checksum"
            )
            log.error(error, extra={"object_id": object_id})
            raise error

        # Issue report to Central API containing new encryption secret and checksums
        await self.report_success(
            file_id=file_upload.id,
            secret=new_secret,
            encrypted_parts_md5=checksums.encrypted_md5,
            encrypted_parts_sha256=checksums.encrypted_sha256,
        )

    async def report_success(
        self,
        *,
        file_id: UUID4,
        secret: SecretBytes,
        encrypted_parts_md5: list[str],
        encrypted_parts_sha256: list[str],
    ) -> None:
        """Submit an InterrogationReport for a successful interrogation"""
        report = InterrogationReport(
            file_id=file_id,
            storage_alias=self._inbox_storage_alias,
            interrogated_at=now_utc_ms_prec(),
            passed=True,
            secret=secret,
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
