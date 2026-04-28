"""Mock FIS (File Ingest Service / Central API) server for local DHFS development.

This server mimics the FIS API that DHFS talks to, without JWT validation.
On startup it seeds the localstack S3 inbox with one test encrypted file so
DHFS has something to actually process.

Usage:
    python scripts/mock_fis.py

Environment variables (all optional):
    MOCK_FIS_S3_ENDPOINT           S3 endpoint URL                  (default: http://localstack:4566)
    MOCK_FIS_S3_ACCESS_KEY         S3 access key                    (default: test)
    MOCK_FIS_S3_SECRET_KEY         S3 secret key                    (default: test)
    MOCK_FIS_S3_REGION             S3 region                        (default: eu-west-1)
    MOCK_FIS_INBOX_BUCKET          Inbox bucket name                (default: inbox1)
    MOCK_FIS_INTERROGATION_BUCKET  Interrogation bucket name        (default: interrogation)
    MOCK_FIS_STORAGE_ALIAS         Storage alias                    (default: TESTHUB)
    MOCK_FIS_DHFS_PUB_KEY          DHFS crypt4gh .pub path          (default: /workspace/test_keys/key.pub)
    MOCK_FIS_PORT                  Port to listen on                (default: 8000)
    MOCK_FIS_FILE_SIZE_MB          Plaintext file size MB           (default: 6)
    MOCK_FIS_PART_SIZE_MB          Upload part size MB              (default: 5)
    MOCK_FIS_AUTO_ARCHIVE          Auto-archive on success report   (default: false)
    MOCK_FIS_CHAOS                 Inject random API errors         (default: false)
    MOCK_FIS_CHAOS_CORRUPT         Inject bad metadata in uploads   (default: false)
    MOCK_FIS_CHAOS_PROBABILITY     Probability per API error [0-1]  (default: 0.3)
    MOCK_FIS_CORRUPT_PROBABILITY   Probability per corrupt [0-1]    (default: 0.3)

FIS routes (all under the /central prefix, no JWT validation):
    GET  /central/health
    GET  /central/storages/{alias}/uploads
    POST /central/storages/{alias}/uploads/can_remove
    POST /central/storages/{alias}/interrogation-reports

    Report endpoint behaviour matches FIS:
      - 404 if file_id is unknown
      - 409 if a conflicting report was already received for that file_id
      - 201 on success or idempotent re-submission
      - On success: state → "interrogated", can_remove depends on MOCK_FIS_AUTO_ARCHIVE
      - On failure: state → "failed", can_remove = True (file is done, nothing to archive)

    can_remove endpoint behaviour matches FIS:
      - Object IDs known to be can_remove=True  → included
      - Object IDs not tracked (never seen)     → included (safe to remove per FIS spec)
      - Object IDs whose file is still active   → excluded

Admin routes:
    POST   /admin/reseed        - generate and upload another test file
    POST   /admin/archive_all   - mark all interrogated files as removable (simulates IFRS archival)
    DELETE /admin/inbox         - empty the inbox bucket (DHFS lacks write access to do this)
    GET    /admin/state         - show every tracked file and its current state
    GET    /admin/reports       - show all received interrogation reports
"""

import hashlib
import logging
import os
import random
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID, uuid4

import boto3
import crypt4gh.header
import crypt4gh.lib
import uvicorn
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from crypt4gh.keys import get_public_key
from fastapi import APIRouter, FastAPI, HTTPException, Request, status
from ghga_service_commons.utils.crypt import generate_key_pair
from nacl.bindings import crypto_aead_chacha20poly1305_ietf_encrypt as encrypt_algo
from pydantic import UUID4, BaseModel, Field

from dhfs.constants import ENCRYPTION_SECRET_LENGTH, NONCE_LENGTH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
log = logging.getLogger("mock_fis")

# ── Config from env ───────────────────────────────────────────────────────────
S3_ENDPOINT = os.getenv("MOCK_FIS_S3_ENDPOINT", "http://localstack:4566")
S3_ACCESS_KEY = os.getenv("MOCK_FIS_S3_ACCESS_KEY", "test")
S3_SECRET_KEY = os.getenv("MOCK_FIS_S3_SECRET_KEY", "test")
S3_REGION = os.getenv("MOCK_FIS_S3_REGION", "eu-west-1")
INBOX_BUCKET = os.getenv("MOCK_FIS_INBOX_BUCKET", "inbox1")
INTERROGATION_BUCKET = os.getenv("MOCK_FIS_INTERROGATION_BUCKET", "interrogation")
STORAGE_ALIAS = os.getenv("MOCK_FIS_STORAGE_ALIAS", "TESTHUB")
PUB_KEY_PATH = Path(os.getenv("MOCK_FIS_DHFS_PUB_KEY", "/workspace/test_keys/key.pub"))
PORT = int(os.getenv("MOCK_FIS_PORT", "8000"))
FILE_SIZE = int(os.getenv("MOCK_FIS_FILE_SIZE_MB", "6")) * 1024 * 1024
PART_SIZE = int(os.getenv("MOCK_FIS_PART_SIZE_MB", "5")) * 1024 * 1024
AUTO_ARCHIVE = os.getenv("MOCK_FIS_AUTO_ARCHIVE", "false").lower() in (
    "1",
    "true",
    "yes",
)
CHAOS_MODE = os.getenv("MOCK_FIS_CHAOS", "true").lower() in ("1", "true", "yes")
CHAOS_CORRUPT = os.getenv("MOCK_FIS_CHAOS_CORRUPT", "true").lower() in (
    "1",
    "true",
    "yes",
)
CHAOS_PROBABILITY = float(os.getenv("MOCK_FIS_CHAOS_PROBABILITY", "0.5"))
CORRUPT_PROBABILITY = float(os.getenv("MOCK_FIS_CORRUPT_PROBABILITY", "0.5"))

# All error status codes that the real FIS can return across its endpoints,
# plus 426 which DHFS has special handling for.
_CHAOS_ERROR_CODES = [
    status.HTTP_404_NOT_FOUND,
    status.HTTP_409_CONFLICT,
    status.HTTP_426_UPGRADE_REQUIRED,
    status.HTTP_500_INTERNAL_SERVER_ERROR,
]
_CHAOS_DETAILS = {
    status.HTTP_404_NOT_FOUND: "No such file exists",
    status.HTTP_409_CONFLICT: "Report conflicts with database",
    status.HTTP_426_UPGRADE_REQUIRED: "Client version is outdated — please upgrade",
    status.HTTP_500_INTERNAL_SERVER_ERROR: "Something went wrong.",
}


# ── Models ────────────────────────────────────────────────────────────────────
class FileUpload(BaseModel):
    """Mirrors fis.core.models.BaseFileInformation / dhfs.core.models.FileUpload."""

    id: UUID4 = Field(..., description="Unique file identifier")
    storage_alias: str = Field(..., description="Data hub alias")
    bucket_id: str = Field(..., description="Inbox bucket name")
    object_id: UUID4 = Field(..., description="S3 object ID in the inbox bucket")
    decrypted_sha256: str = Field(..., description="SHA-256 of the unencrypted content")
    decrypted_size: int = Field(
        ..., description="Size of the unencrypted content in bytes"
    )
    encrypted_size: int = Field(..., description="Size of the encrypted file in bytes")
    part_size: int = Field(..., description="Part size used when uploading")


class ConfigUpdate(BaseModel):
    """Partial update payload for runtime chaos configuration."""

    chaos: bool | None = None
    chaos_corrupt: bool | None = None
    chaos_probability: float | None = None
    corrupt_probability: float | None = None


def _maybe_inject_chaos(endpoint: str) -> None:
    """Randomly raise an HTTP error to simulate FIS misbehaving.

    Only active when MOCK_FIS_CHAOS=true.  Fires with probability
    MOCK_FIS_CHAOS_PROBABILITY (default 0.3) and picks uniformly from the
    full set of error codes that FIS can return across all its endpoints.
    """
    if not CHAOS_MODE or random.random() >= CHAOS_PROBABILITY:
        return
    error_code = random.choice(_CHAOS_ERROR_CODES)
    log.warning("CHAOS: injecting HTTP %d on %s", error_code, endpoint)
    raise HTTPException(status_code=error_code, detail=_CHAOS_DETAILS[error_code])


def _maybe_corrupt_upload(file_upload: FileUpload) -> FileUpload:
    """Randomly corrupt one metadata field of a FileUpload to simulate bad data from FIS.

    Only active when MOCK_FIS_CHAOS_CORRUPT=true.  Fires with probability
    MOCK_FIS_CORRUPT_PROBABILITY (independent of the API error probability).

    Corruptions and their effect on DHFS:
      decrypted_sha256 — DHFS will decrypt successfully but the final checksum
                         comparison will fail → failure report submitted
      decrypted_size   — throws off the part-range / offset calculations
      encrypted_size   — throws off the part-range / offset calculations
      part_size        — produces misaligned Crypt4GH segment boundaries
    """
    if not CHAOS_CORRUPT or random.random() >= CORRUPT_PROBABILITY:
        return file_upload

    corruption = random.choice(
        ["decrypted_sha256", "decrypted_size", "encrypted_size", "part_size"]
    )

    if corruption == "decrypted_sha256":
        bad_value = hashlib.sha256(os.urandom(16)).hexdigest()
        log.warning(
            "CHAOS CORRUPT: wrong decrypted_sha256 for file %s (%s… → %s…)",
            file_upload.id,
            file_upload.decrypted_sha256[:8],
            bad_value[:8],
        )
        return file_upload.model_copy(update={"decrypted_sha256": bad_value})

    if corruption == "decrypted_size":
        bad_value = file_upload.decrypted_size + random.randint(1, 4096)
        log.warning(
            "CHAOS CORRUPT: wrong decrypted_size for file %s (%d → %d)",
            file_upload.id,
            file_upload.decrypted_size,
            bad_value,
        )
        return file_upload.model_copy(update={"decrypted_size": bad_value})

    if corruption == "encrypted_size":
        bad_value = file_upload.encrypted_size + random.randint(1, 4096)
        log.warning(
            "CHAOS CORRUPT: wrong encrypted_size for file %s (%d → %d)",
            file_upload.id,
            file_upload.encrypted_size,
            bad_value,
        )
        return file_upload.model_copy(update={"encrypted_size": bad_value})

    # part_size — shift it off a Crypt4GH segment boundary
    bad_value = file_upload.part_size + random.randint(1, 512)
    log.warning(
        "CHAOS CORRUPT: wrong part_size for file %s (%d → %d)",
        file_upload.id,
        file_upload.part_size,
        bad_value,
    )
    return file_upload.model_copy(update={"part_size": bad_value})


# ── In-memory state ───────────────────────────────────────────────────────────
# Possible states mirror the FIS FileUnderInterrogation lifecycle:
#   "inbox"        — waiting to be picked up by DHFS
#   "interrogated" — success report received; awaiting archival by IFRS
#   "failed"       — failure report received; can be removed immediately
FILE_STATE_INBOX = "inbox"
FILE_STATE_INTERROGATED = "interrogated"
FILE_STATE_FAILED = "failed"


@dataclass
class TrackedFile:
    """Mirrors fis.core.models.FileUnderInterrogation for our purposes."""

    upload: FileUpload
    state: str = FILE_STATE_INBOX
    can_remove: bool = False
    # Stored after a report is received; used for idempotency checks
    report: dict | None = None
    # The re-encrypted object ID placed in the interrogation bucket by DHFS;
    # set from the success report and used to answer can_remove queries
    interrogation_object_id: str | None = None


# Keyed by str(file_id)
tracked_files: dict[str, TrackedFile] = {}


def _get_tracked_file(file_id: str) -> TrackedFile | None:
    return tracked_files.get(file_id)


def _register_upload(file_upload: FileUpload) -> TrackedFile:
    tracked = TrackedFile(upload=file_upload)
    tracked_files[str(file_upload.id)] = tracked
    return tracked


# ── S3 helpers ────────────────────────────────────────────────────────────────
def _s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
        config=BotoConfig(signature_version="s3v4"),
    )


def _ensure_bucket(s3_client, bucket_id: str) -> None:
    try:
        s3_client.create_bucket(
            Bucket=bucket_id,
            CreateBucketConfiguration={"LocationConstraint": S3_REGION},
        )
        log.info("Created bucket '%s'.", bucket_id)
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            log.debug("Bucket '%s' already exists.", bucket_id)
        else:
            raise


# ── Crypt4GH helpers ──────────────────────────────────────────────────────────
def _make_envelope(file_secret: bytes, pub_key_path: Path) -> bytes:
    """Wrap a symmetric file secret in a Crypt4GH envelope readable by DHFS."""
    submitter_keypair = generate_key_pair()
    dhfs_public_key = get_public_key(pub_key_path)
    keys = [(0, submitter_keypair.private, dhfs_public_key)]
    header_content = crypt4gh.header.make_packet_data_enc(0, file_secret)
    header_packets = crypt4gh.header.encrypt(header_content, keys)
    return crypt4gh.header.serialize(header_packets)


# ── Test-file generation and upload ───────────────────────────────────────────
def seed_one_file(pub_key_path: Path) -> FileUpload:
    """Generate a Crypt4GH-encrypted test file, upload it to the inbox bucket,
    and return the corresponding FileUpload metadata.

    The plaintext SHA-256 is computed here so that DHFS can verify it matches
    after re-encryption.
    """
    file_secret = os.urandom(ENCRYPTION_SECRET_LENGTH)
    plaintext = os.urandom(FILE_SIZE)
    decrypted_sha256 = hashlib.sha256(plaintext).hexdigest()

    envelope = _make_envelope(file_secret, pub_key_path)

    encrypted_body = bytearray()
    for i in range(0, len(plaintext), crypt4gh.lib.SEGMENT_SIZE):
        chunk = plaintext[i : i + crypt4gh.lib.SEGMENT_SIZE]
        nonce = os.urandom(NONCE_LENGTH)
        encrypted_chunk = encrypt_algo(chunk, None, nonce, file_secret)
        encrypted_body += nonce + encrypted_chunk

    full_encrypted = envelope + bytes(encrypted_body)
    encrypted_size = len(full_encrypted)
    object_id = str(uuid4())
    file_id = uuid4()

    s3_client = _s3()
    _ensure_bucket(s3_client, INBOX_BUCKET)

    mpu = s3_client.create_multipart_upload(Bucket=INBOX_BUCKET, Key=object_id)
    upload_id = mpu["UploadId"]
    parts = []

    for part_num, start in enumerate(range(0, encrypted_size, PART_SIZE), start=1):
        chunk = full_encrypted[start : start + PART_SIZE]
        resp = s3_client.upload_part(
            Bucket=INBOX_BUCKET,
            Key=object_id,
            UploadId=upload_id,
            PartNumber=part_num,
            Body=chunk,
        )
        parts.append({"PartNumber": part_num, "ETag": resp["ETag"]})

    s3_client.complete_multipart_upload(
        Bucket=INBOX_BUCKET,
        Key=object_id,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )

    log.info(
        "Seeded s3://%s/%s — %d bytes encrypted / %d bytes plaintext / sha256=%s…",
        INBOX_BUCKET,
        object_id,
        encrypted_size,
        FILE_SIZE,
        decrypted_sha256[:12],
    )

    return FileUpload(
        id=file_id,
        storage_alias=STORAGE_ALIAS,
        bucket_id=INBOX_BUCKET,
        object_id=UUID(object_id),
        decrypted_sha256=decrypted_sha256,
        decrypted_size=len(plaintext),
        encrypted_size=encrypted_size,
        part_size=PART_SIZE,
    )


# ── Report idempotency ────────────────────────────────────────────────────────
def _reports_conflict(existing: dict, incoming: dict) -> bool:
    """Return True if the two reports cover the same file but disagree on outcome.

    Matches the FIS idempotency check: same passed-flag and (on success) same
    bucket/object pair.  Timestamps and encrypted secrets are intentionally
    excluded because they legitimately differ across retries.
    """
    if existing["passed"] != incoming["passed"]:
        return True
    if incoming["passed"]:
        return existing.get("bucket_id") != incoming.get("bucket_id") or existing.get(
            "object_id"
        ) != incoming.get("object_id")
    return False


# ── FastAPI app ───────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    if not PUB_KEY_PATH.exists():
        log.error(
            "DHFS crypt4gh public key not found at '%s'. "
            "Set MOCK_FIS_DHFS_PUB_KEY to the correct path, or put key.pub in "
            "the working directory.",
            PUB_KEY_PATH,
        )
        sys.exit(1)

    s3_client = _s3()
    _ensure_bucket(s3_client, INBOX_BUCKET)
    _ensure_bucket(s3_client, INTERROGATION_BUCKET)

    log.info(
        "Auto-archive on success: %s",
        "ON (MOCK_FIS_AUTO_ARCHIVE)"
        if AUTO_ARCHIVE
        else "OFF — use POST /admin/archive_all",
    )
    log.info(
        "Chaos mode: %s",
        f"ON — {CHAOS_PROBABILITY:.0%} chance of a random error per FIS request (MOCK_FIS_CHAOS)"
        if CHAOS_MODE
        else "OFF (MOCK_FIS_CHAOS)",
    )
    log.info(
        "Chaos corrupt: %s",
        f"ON — {CORRUPT_PROBABILITY:.0%} chance of bad metadata per upload response (MOCK_FIS_CHAOS_CORRUPT)"
        if CHAOS_CORRUPT
        else "OFF (MOCK_FIS_CHAOS_CORRUPT)",
    )
    log.info("Generating and uploading test file to '%s'…", S3_ENDPOINT)
    file_upload = seed_one_file(PUB_KEY_PATH)
    tracked = _register_upload(file_upload)
    log.info(
        "Ready. file_id=%s  object_id=%s  state=%s  Listening on :%d",
        file_upload.id,
        file_upload.object_id,
        tracked.state,
        PORT,
    )
    yield
    log.info("Mock FIS shutting down.")


app = FastAPI(title="Mock FIS", lifespan=lifespan)

# ── FIS routes (under /central to match central_api_url) ─────────────────────
fis_router = APIRouter(prefix="/central")


@fis_router.get("/health", status_code=status.HTTP_200_OK)
async def health():
    _maybe_inject_chaos("GET /health")
    return {"status": "OK"}


@fis_router.get(
    "/storages/{storage_alias}/uploads",
    response_model=list[FileUpload],
    status_code=status.HTTP_200_OK,
)
async def list_uploads(storage_alias: str):
    """Return files in 'inbox' state that haven't been interrogated yet."""
    _maybe_inject_chaos(f"GET /storages/{storage_alias}/uploads")
    uploads = [
        _maybe_corrupt_upload(tracked.upload)
        for tracked in tracked_files.values()
        if tracked.upload.storage_alias == storage_alias
        and tracked.state == FILE_STATE_INBOX
    ]
    log.info(
        "GET /storages/%s/uploads → %d file(s) in inbox state",
        storage_alias,
        len(uploads),
    )
    return uploads


@fis_router.post(
    "/storages/{storage_alias}/uploads/can_remove",
    status_code=status.HTTP_200_OK,
)
async def can_remove(storage_alias: str, request: Request) -> list[str]:
    """Return the subset of provided interrogation-bucket object IDs that can be removed.

    Mirrors FIS behaviour:
      - IDs belonging to a file with can_remove=True  → removable
      - IDs not tracked (never seen by this mock)     → removable (safe per FIS spec)
      - IDs belonging to a file with can_remove=False → not removable
    """
    _maybe_inject_chaos(f"POST /storages/{storage_alias}/uploads/can_remove")
    object_ids: list[str] = await request.json()

    # Build a lookup from interrogation object_id → TrackedFile for fast access
    interrogation_id_index: dict[str, TrackedFile] = {
        tracked.interrogation_object_id: tracked
        for tracked in tracked_files.values()
        if tracked.interrogation_object_id is not None
    }

    removable: list[str] = []
    for object_id in object_ids:
        tracked = interrogation_id_index.get(object_id)
        if tracked is None or tracked.can_remove:
            removable.append(object_id)

    log.info(
        "POST /storages/%s/uploads/can_remove — %d/%d IDs are removable",
        storage_alias,
        len(removable),
        len(object_ids),
    )
    return removable


@fis_router.post(
    "/storages/{storage_alias}/interrogation-reports",
    status_code=status.HTTP_201_CREATED,
)
async def post_interrogation_report(storage_alias: str, request: Request):
    """Accept an interrogation report, mirroring FIS behaviour.

    - 404 if the file_id is not tracked
    - 409 if a conflicting report was already received
    - 201 on success or idempotent re-submission
    """
    _maybe_inject_chaos(f"POST /storages/{storage_alias}/interrogation-reports")
    body: dict = await request.json()
    file_id: str = body.get("file_id", "")
    passed: bool | None = body.get("passed")
    reason: str | None = body.get("reason")

    tracked = _get_tracked_file(file_id)
    if tracked is None:
        log.warning(
            "POST /storages/%s/interrogation-reports — 404 unknown file_id=%s",
            storage_alias,
            file_id,
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"File {file_id} not found.",
        )

    # Idempotency: if we already have a report, check for conflicts
    if tracked.report is not None:
        if _reports_conflict(tracked.report, body):
            log.warning(
                "POST /storages/%s/interrogation-reports — 409 conflicting report "
                "for file_id=%s",
                storage_alias,
                file_id,
            )
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Already received a conflicting report for file {file_id}.",
            )
        # Identical re-submission — idempotent 201
        log.info(
            "POST /storages/%s/interrogation-reports — idempotent re-submission "
            "for file_id=%s",
            storage_alias,
            file_id,
        )
        return

    # First time receiving this report
    tracked.report = body

    if passed:
        tracked.state = FILE_STATE_INTERROGATED
        tracked.interrogation_object_id = body.get("object_id")
        tracked.can_remove = AUTO_ARCHIVE
        log.info(
            "POST /storages/%s/interrogation-reports — SUCCESS file_id=%s  "
            "interrogation_object_id=%s  can_remove=%s",
            storage_alias,
            file_id,
            tracked.interrogation_object_id,
            tracked.can_remove,
        )
    else:
        tracked.state = FILE_STATE_FAILED
        tracked.can_remove = True
        log.info(
            "POST /storages/%s/interrogation-reports — FAILURE file_id=%s  reason=%s",
            storage_alias,
            file_id,
            reason,
        )


app.include_router(fis_router)

# ── Admin routes ──────────────────────────────────────────────────────────────
admin_router = APIRouter(prefix="/admin")


@admin_router.post("/reseed", status_code=status.HTTP_201_CREATED)
async def reseed():
    """Generate and upload another test file, adding it to the inbox queue."""
    file_upload = seed_one_file(PUB_KEY_PATH)
    tracked = _register_upload(file_upload)
    log.info(
        "Reseeded: file_id=%s  object_id=%s  state=%s",
        file_upload.id,
        file_upload.object_id,
        tracked.state,
    )
    return file_upload.model_dump(mode="json")


@admin_router.post("/archive_all", status_code=status.HTTP_200_OK)
async def archive_all():
    """Simulate IFRS archival: mark all interrogated files as removable.

    In the real system, IFRS publishes a Kafka event after copying a file to
    permanent storage, which causes FIS to set can_remove=True.  Since there
    is no Kafka or IFRS here, call this endpoint to unblock the cleaner.
    """
    archived: list[str] = []
    for file_id, tracked in tracked_files.items():
        if tracked.state == FILE_STATE_INTERROGATED and not tracked.can_remove:
            tracked.can_remove = True
            archived.append(file_id)
    log.info(
        "POST /admin/archive_all — marked %d file(s) as removable: %s",
        len(archived),
        archived,
    )
    return {"archived": archived}


@admin_router.delete("/inbox", status_code=status.HTTP_200_OK)
async def clear_inbox():
    """Delete all objects from the inbox bucket.

    DHFS has read-only access to the inbox, so it cannot clean up after itself.
    Call this endpoint to empty the bucket between test runs.
    """
    s3_client = _s3()
    response = s3_client.list_objects_v2(Bucket=INBOX_BUCKET)
    objects = response.get("Contents", [])

    if not objects:
        log.info(
            "DELETE /admin/inbox — inbox bucket '%s' is already empty.", INBOX_BUCKET
        )
        return {"deleted": []}

    deleted_keys = [obj["Key"] for obj in objects]
    s3_client.delete_objects(
        Bucket=INBOX_BUCKET,
        Delete={"Objects": [{"Key": key} for key in deleted_keys]},
    )
    log.info(
        "DELETE /admin/inbox — deleted %d object(s) from '%s': %s",
        len(deleted_keys),
        INBOX_BUCKET,
        deleted_keys,
    )
    return {"deleted": deleted_keys}


@admin_router.get("/state")
async def get_state():
    """Return every tracked file with its current state and can_remove flag."""
    return [
        {
            "file_id": file_id,
            "state": tracked.state,
            "can_remove": tracked.can_remove,
            "reencrypted_object_id": tracked.interrogation_object_id,
            "upload": tracked.upload.model_dump(mode="json"),
        }
        for file_id, tracked in tracked_files.items()
    ]


@admin_router.get("/reports")
async def get_reports():
    """Return all interrogation reports received so far."""
    return [
        tracked.report
        for tracked in tracked_files.values()
        if tracked.report is not None
    ]


@admin_router.get("/config", status_code=status.HTTP_200_OK)
async def get_config():
    """Return current runtime chaos configuration."""
    return {
        "chaos": CHAOS_MODE,
        "chaos_corrupt": CHAOS_CORRUPT,
        "chaos_probability": CHAOS_PROBABILITY,
        "corrupt_probability": CORRUPT_PROBABILITY,
    }


@admin_router.patch("/config", status_code=status.HTTP_200_OK)
async def patch_config(update: ConfigUpdate):
    """Update runtime chaos configuration without restarting."""
    global CHAOS_MODE, CHAOS_CORRUPT, CHAOS_PROBABILITY, CORRUPT_PROBABILITY
    if update.chaos is not None:
        CHAOS_MODE = update.chaos
        log.info("Config: chaos_mode → %s", CHAOS_MODE)
    if update.chaos_corrupt is not None:
        CHAOS_CORRUPT = update.chaos_corrupt
        log.info("Config: chaos_corrupt → %s", CHAOS_CORRUPT)
    if update.chaos_probability is not None:
        CHAOS_PROBABILITY = max(0.0, min(1.0, update.chaos_probability))
        log.info("Config: chaos_probability → %.0f%%", CHAOS_PROBABILITY * 100)
    if update.corrupt_probability is not None:
        CORRUPT_PROBABILITY = max(0.0, min(1.0, update.corrupt_probability))
        log.info("Config: corrupt_probability → %.0f%%", CORRUPT_PROBABILITY * 100)
    return {
        "chaos": CHAOS_MODE,
        "chaos_corrupt": CHAOS_CORRUPT,
        "chaos_probability": CHAOS_PROBABILITY,
        "corrupt_probability": CORRUPT_PROBABILITY,
    }


app.include_router(admin_router)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
