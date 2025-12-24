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

"""Client class for communicating with the GHGA Central API"""

import base64
import logging
from json import JSONDecodeError

import httpx
from ghga_service_commons.utils.crypt import encrypt
from ghga_service_commons.utils.jwt_helpers import sign_and_serialize_token
from jwcrypto.jwk import JWK
from pydantic import Field, HttpUrl, ValidationError

from dhfs import models
from dhfs.constants import AUTH_TOKEN_VALID_SECONDS, JWT_AUD, JWT_ISS
from dhfs.core.auth import DataHubAuthConfig
from dhfs.ports.outbound.central import CentralClientPort

log = logging.getLogger(__name__)


class CentralClientConfig(DataHubAuthConfig):
    """Configuration required for the CentralClient class"""

    central_api_public_key: str = Field(
        ..., description="The Crypt4GH public key used by the Central API"
    )
    central_api_url: HttpUrl = Field(
        ..., description="The base URL used to connect to to the GHGA Central API"
    )


class CentralClient(CentralClientPort):
    """This class communicates with GHGA Central to learn about new file uploads"""

    def __init__(
        self,
        *,
        config: CentralClientConfig,
        inbox_storage_alias: str,
        httpx_client: httpx.AsyncClient,
    ) -> None:
        """Initialize the CentralClient instance"""
        self._httpx_client = httpx_client
        self._storage_alias = inbox_storage_alias
        self._central_public_key = config.central_api_public_key
        self._base_url = str(config.central_api_url).rstrip("/")
        self._signing_key = JWK.from_json(
            config.data_hub_private_key.get_secret_value()
        )
        if not self._signing_key.has_private:
            key_error = KeyError("No private token-signing key found.")
            log.error(key_error)
            raise key_error

    def _make_jwt(self) -> str:
        claims: dict[str, str] = {
            "iss": JWT_ISS,
            "aud": JWT_AUD,
            "sub": self._storage_alias,
        }
        return sign_and_serialize_token(
            claims=claims, key=self._signing_key, valid_seconds=AUTH_TOKEN_VALID_SECONDS
        )

    def _auth_header(self) -> dict[str, str]:
        """Create an authorization header with a bearer token containing a fresh JWT"""
        headers = {"Authorization": f"Bearer {self._make_jwt()}"}
        return headers

    def _response_to_file_id_list(self, response: httpx.Response) -> list[str]:
        """Extract a boolean value from an httpx Response.

        Raises:
        - ResponseFormatError if response body parsing fails.
        """
        try:
            body = response.json()
            if not isinstance(body, list):
                raise TypeError("Response did not contain a list")
            return body
        except (JSONDecodeError, TypeError) as err:
            error = self.ResponseFormatError(str(response.url))
            log.error(error, exc_info=True)
            raise error from err

    def _response_to_file_upload_list(
        self, response: httpx.Response
    ) -> list[models.FileUpload]:
        """Extract a list of FileUploads from an httpx Response.

        Raises:
        - ResponseFormatError if response body parsing fails.
        """
        try:
            body = response.json()
            return list(map(models.FileUpload.model_validate, body))
        except (JSONDecodeError, ValidationError) as err:
            error = self.ResponseFormatError(str(response.url))
            log.error(error, exc_info=True)
            raise error from err

    async def fetch_new_uploads(self) -> list[models.FileUpload]:
        """Fetches a list of files that need to be interrogated and re-encrypted."""
        url = f"{self._base_url}/storages/{self._storage_alias}/uploads"

        response = await self._httpx_client.get(url=url, headers=self._auth_header())

        if response.status_code == 200:
            return self._response_to_file_upload_list(response)
        else:
            error = self.CentralAPIError(url=url, status_code=response.status_code)
            log.error(error)
            raise error

    async def get_removable_files(self, *, file_ids: list[str]) -> list[str]:
        """Asks the GHGA Central API if the objects corresponding to the given file IDs
        can be removed from `interrogation` bucket.

        Returns a list of file IDs that may be removed from the bucket.
        """
        # TODO: Add an info log here once final shape of request is hammered out
        params = "&".join([f"file_id={file_id}" for file_id in file_ids])
        url = f"{self._base_url}/uploads/can_remove?{params}"
        response = await self._httpx_client.get(url=url, headers=self._auth_header())

        if (status_code := response.status_code) != 200:
            error = self.CentralAPIError(url=url, status_code=status_code)
            log.error(error)
            raise error

        return self._response_to_file_id_list(response)

    async def submit_interrogation_report(
        self, *, report: models.InterrogationReport
    ) -> None:
        """Submit a file interrogation report to GHGA Central"""
        body = report.model_dump(mode="json")
        url = f"{self._base_url}/interrogation_reports"

        # Encrypt secret (core class doesn't know central api public key)
        if report.secret:
            secret = report.secret.get_secret_value()
            encoded_secret = base64.urlsafe_b64encode(secret).decode("utf-8")
            body["secret"] = encrypt(encoded_secret, key=self._central_public_key)

        response = await self._httpx_client.post(
            url=url, headers=self._auth_header(), json=body
        )

        if (status_code := response.status_code) != 201:
            error = self.CentralAPIError(url=url, status_code=status_code)
            log.error(error)
            raise error
