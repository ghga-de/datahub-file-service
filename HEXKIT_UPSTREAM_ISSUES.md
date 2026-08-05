# Upstream hexkit issues affecting DHFS performance

Three S3 round-trip inefficiencies in `hexkit.providers.s3.provider.S3ObjectStorage` that
DHFS cannot work around locally, because they sit inside the `ObjectStorageProtocol`
methods DHFS calls. They are written up here so they can be filed against hexkit.

- **Observed in:** hexkit 8.4.1 (the version pinned in `lock/requirements.txt`)
- **Also present in:** 8.6.0 and 9.0.0 — `objstorage.py` is byte-identical to 8.6.0 in
  9.0.0, so upgrading does not help. See `PERFORMANCE_REPORT.md` §4.3.
- **DHFS requires:** `hexkit[s3] >= 8.2.1` (`pyproject.toml`)

## Measured impact

S3 operations for one interrogated 60 MiB file, counted by patching
`botocore.client.BaseClient._make_api_call`:

| Operation | 11 parts (6 MiB) | 4 parts (16 MiB) |
|---|---|---|
| `ListMultipartUploads` | 13 | 6 |
| `HeadObject` | 4 | 4 |
| `HeadBucket` | 2 | 2 |
| `CreateMultipartUpload` | 1 | 1 |
| `ListParts` | 1 | 1 |
| `CompleteMultipartUpload` | 1 | 1 |
| **Total** | **22** | **15** |

`ListMultipartUploads` is `part_count + 2` and accounts for **59%** of all S3 calls on the
11-part file. Every one of them is a serialized round trip on the critical path of a part.

---

## Issue 1 — `get_part_upload_url()` issues a bucket-wide `ListMultipartUploads` per part

**Severity: high.** This is the dominant cost for large files.

`_get_part_upload_url()` calls `_assert_multipart_upload_exists()`, which calls
`_list_multipart_uploads_for_object()`, which paginates
`list_multipart_uploads` **over the entire bucket** and then filters client-side:

```python
response_iter = await asyncio.to_thread(
    self._client.get_paginator("list_multipart_uploads").paginate,
    Bucket=bucket_id,          # no Prefix, no KeyMarker
)
for response_page in response_iter:
    uploads.extend([
        upload["UploadId"]
        for upload in response_page.get("Uploads", [])
        if upload["Key"] == object_id   # filtered here, after transfer
    ])
```

Two problems compound:

1. **It runs once per part.** Generating a presigned upload URL needs no validation round
   trip at all — presigning is a local, offline signing operation. The upload itself
   already fails loudly if the upload ID is invalid.
2. **It scales with bucket contents, not with the object.** With no `Prefix`, the cost is
   `ceil(active_MPUs_in_bucket / 1000)` round trips *per part*. It stays at one call per
   part only while the bucket has few active uploads. Note that DHFS's cleaner still
   carries a `TODO: Finish MPU cleanup`, so abandoned multipart uploads accumulate in the
   interrogation bucket over time — which makes this degrade in exactly the deployment
   where it is already most expensive.

**Projected cost.** DHFS caps parts at 9,995 (`FileUpload.adjusted_part_size`). A 100 GiB
file therefore issues ~9,995 of these calls. At a 20–50 ms round trip that is **200–500
seconds of pure latency** added to a single file, before any data moves.

**Suggested fixes**, in order of preference:

1. Drop the check from `get_part_upload_url()` entirely. Presigning is offline; the
   validation buys nothing that the subsequent `UploadPart` does not report.
2. Add an opt-out parameter (`validate: bool = True`) so callers uploading many parts
   against one known-good upload ID can pay the check once instead of *N* times.
3. If the check must stay, pass `Prefix=object_id` to `paginate()` so the filtering
   happens server-side and the cost stops scaling with unrelated bucket contents.

Option 1 or 2 alone would remove ~59% of DHFS's S3 calls per file.

---

## Issue 2 — `complete_multipart_upload()` discards the ETag it already received

**Severity: medium.** One superfluous round trip per file, plus Issue 3's amplification.

S3's `CompleteMultipartUpload` response contains the final object's `ETag`. hexkit's
`_complete_multipart_upload()` is annotated `-> None` and drops the boto3 response:

```python
await asyncio.to_thread(
    self._client.complete_multipart_upload,
    Bucket=bucket_id, Key=object_id,
    MultipartUpload={"Parts": part_etags}, UploadId=upload_id,
)   # response discarded
```

DHFS needs that ETag to verify the uploaded object against its locally computed
concatenated-MD5 checksum, so `S3Client.complete_upload()` immediately calls
`get_object_etag()` to fetch what the previous call had already returned.

**Suggested fix:** return the response's `ETag` from `complete_multipart_upload()`. This
is a signature change from `None` to `str`, so it is source-compatible for existing
callers that ignore the return value.

---

## Issue 3 — `get_object_etag()` costs three round trips

**Severity: low**, but it multiplies Issue 2.

```python
async def _get_object_etag(self, *, bucket_id: str, object_id: str) -> str:
    await self._assert_object_exists(...)      # HeadBucket + HeadObject
    object_metadata = await self._get_object_metadata(...)   # HeadObject again
    ...
    return object_metadata["ETag"]
```

`_assert_object_exists()` performs a `HeadBucket` and a `HeadObject`, and
`_get_object_metadata()` then performs the *same* `HeadObject` a second time. A single
`HeadObject` yields both the existence answer and the ETag: a 404 is exactly the
"object does not exist" signal, and its response carries the ETag when it succeeds.

**Suggested fix:** call `_get_object_metadata()` once and translate a `404`/`NoSuchKey`
`ClientError` into `ObjectNotFoundError`, rather than pre-checking. That turns three
round trips into one.

Combined with Issue 2, DHFS's per-file ETag verification currently costs four round trips
where zero additional ones are necessary.

---

## Summary

| Issue | Cost | Fixable in DHFS? |
|---|---|---|
| 1. `ListMultipartUploads` per part, bucket-wide | `part_count + 2` calls/file; 200–500 s on a 100 GiB file | No — inside `get_part_upload_url()` |
| 2. `CompleteMultipartUpload` ETag discarded | 1 extra call/file | No — return type is `None` |
| 3. `get_object_etag()` triple round trip | 2 extra calls/file | No — inside `get_object_etag()` |

Fixing Issue 1 alone would remove the majority of DHFS's S3 round trips per file and is
the single largest remaining performance win available to this service.
