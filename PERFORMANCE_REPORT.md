# DHFS re-encryption path — performance report

Investigation into slow observed re-encryption times (2–4 s per file), the changes made
in response, and what remains open.

**Date:** 2026-08-04
**Baseline commit:** `751800a`
**Dependency versions:** hexkit 8.4.1, ghga-service-commons 7.0.5 (matching `lock/requirements.txt`)

---

## 1. Method

All measurements come from an instrumented probe driving `Interrogator.interrogate_file()`
and `interrogate_new_files()` against localstack (testcontainers) with the Central API
report mocked out, so only the S3 and crypto path is timed. Every S3 API call was counted
and timed by wrapping `botocore.client.BaseClient._make_api_call`.

Localstack runs on the same host, so its network latency is effectively zero. That is the
one condition under which overlapping network and CPU work cannot help, so the headline
runs were repeated with a fixed 20 ms delay injected into each S3 data operation to model
a remote endpoint. Both baseline and optimised numbers were captured back-to-back in the
same session via `git stash`, on the same machine.

The probes are not committed. They live in the session scratchpad as `test_perf_probe.py`,
`test_perf_latency.py`, and `test_perf_batch.py`, and drop into `tests/integration/` if
they are needed again.

---

## 2. Results

### 2.1 Single file, 60 MiB, no injected latency

Wall time for the whole `interrogate_file()` call:

| Part size | Parts | Before | After | Speedup |
|-----------|------:|-------:|------:|--------:|
| 6 MiB     |    11 | 1.486 s | 1.055 s | **1.41×** |
| 16 MiB    |     4 | 1.250 s | 0.882 s | **1.42×** |
| 64 MiB    |     1 | 1.398 s | 1.152 s | 1.21× |

### 2.2 Single file, 60 MiB, 20 ms RTT injected per S3 operation

| Part size | Parts | Before | After | Speedup |
|-----------|------:|-------:|------:|--------:|
| 6 MiB     |    11 | 1.970 s | 1.152 s | **1.71×** |
| 16 MiB    |     4 | 1.477 s | 0.945 s | **1.56×** |

### 2.3 Full batch — `interrogate_new_files()`, 4 files × 24 MiB, 20 ms RTT

| | Before | After | Speedup |
|---|-------:|------:|--------:|
| Wall time | 3.465 s | 1.784 s | **1.94×** |
| Throughput | 27.7 MiB/s | 53.8 MiB/s | |

### 2.4 Stage breakdown, 60 MiB, no injected latency

Before (everything serial, so stage times sum to wall time):

| Part size | download | decrypt | reencrypt | verify | upload | total |
|-----------|---------:|--------:|----------:|-------:|-------:|------:|
| 6 MiB     | 0.228 | 0.163 | 0.144 | 0.138 | 0.402 | 1.075 |
| 16 MiB    | 0.153 | 0.158 | 0.129 | 0.128 | 0.290 | 0.858 |
| 64 MiB    | 0.178 | 0.173 | 0.180 | 0.174 | 0.248 | 0.953 |

After (stages overlap, so the sums exceed wall time — see §3.4):

| Part size | download | decrypt | reencrypt | verify | upload | wall |
|-----------|---------:|--------:|----------:|-------:|-------:|-----:|
| 6 MiB     | 0.837 | 0.301 | 0.396 | 0.392 | 1.011 | 0.924 |
| 16 MiB    | 0.750 | 0.250 | 0.320 | 0.339 | 0.825 | 0.751 |
| 64 MiB    | 0.154 | 0.134 | 0.165 | 0.127 | 0.204 | 1.034 |

The individual stage figures grow because concurrent operations contend; only wall time
reflects how long the file actually took.

### 2.5 Crypto in isolation

The single-part (64 MiB) case has no concurrency to confound it, so it isolates the buffer
work from the pipelining work:

| | Before | After | Speedup |
|---|-------:|------:|--------:|
| decrypt + reencrypt + verify | 0.527 s | 0.426 s | **1.24×** |

A standalone microbenchmark of the same buffer change predicted 1.34×; the gap is
`asyncio.to_thread` dispatch overhead.

### 2.6 Honest downside

The 64 MiB / single-part row is the one case that gains least. With a single part there is
nothing to overlap, so the thread hop is pure cost and only the buffer work pays off. It
still nets 1.21× overall, but files small enough to fit in one part see the least benefit.

---

## 3. Addressed

### 3.1 Preallocated crypto buffers

`_decrypt_part` and `_reencrypt_part` previously grew a `bytearray` with `+=` and returned
`bytes(buffer)`, and sliced their input twice per segment. Both now compute the exact
output size up front — every Crypt4GH segment differs by a fixed 28 bytes of nonce and
auth tag — allocate once, and write through a `memoryview`. Input is sliced through a
`memoryview` too, so only the payload handed to PyNaCl is materialised.

Roughly 14 full passes over every byte became 7.

One constraint shaped the design: httpx treats `bytearray` and `memoryview` as iterables
of integers and would have sent chunked garbage, so `_reencrypt_part` returns `bytes` —
a single conversion at the upload boundary. `_decrypt_part` returns `bytearray`, since its
consumers (hashlib, and `_reencrypt_part` itself) accept any buffer.

### 3.2 Removed the `upload_buffer`

The accumulate-and-flush buffer in `_process_file_parts` was provably a pass-through:
`adjusted_part_size` is a multiple of `CIPHER_SEGMENT_SIZE`, so decryption removes and
re-encryption re-adds exactly 28 bytes per segment, making a re-encrypted part always the
same size as the encrypted part it came from. The buffer therefore filled and drained
exactly each iteration while costing three extra passes over every byte. Parts now go
straight to S3.

This invariant is now covered by `test_reencrypted_size_matches_crypt4gh_framing`.

### 3.3 Crypto off the event loop, parts pipelined

Crypto runs via `asyncio.to_thread`, and parts are processed under
`asyncio.Semaphore(max_concurrent_parts)` inside an `asyncio.TaskGroup`, so the download of
one part overlaps the re-encryption of another and the upload of a third.

The whole-file SHA-256 is order-dependent, so each part waits on an `asyncio.Event` set by
its predecessor before folding its plaintext in. No deadlock is possible because tasks are
created in part order and `asyncio.Semaphore` is FIFO, so part 0 always acquires a slot
first and never waits on a successor. That invariant is load-bearing and is called out in a
comment at the semaphore.

Per-part MD5/SHA-256 digests are written into indexed slots and assembled in part order
afterwards, via the new `Checksums.digest_encrypted_part()` and `set_encrypted_parts()`.

### 3.4 Concurrency across files in a batch

`interrogate_new_files()` now runs files under `Semaphore(max_concurrent_files)` in a
`TaskGroup`. Per-file error handling is unchanged; `CriticalError` still escapes and now
also aborts the remaining files in the batch, which is the intended behaviour.

### 3.5 Error selection under concurrency

`TaskGroup` raises `ExceptionGroup`, so `_most_significant_error()` flattens it (including
nested groups) and picks by severity: `CriticalError`, then `ConclusiveError`, then
`InconclusiveError`. Without that ordering a concurrent `InconclusiveError` could mask a
`ConclusiveError`, turning a definitive failure into an endless retry loop.

### 3.6 Metrics now report wall time

Previously `total_s` was the sum of stage times, which was accurate only because
everything was serial. With overlap that sum is meaningless as a duration, so:

- `total_s` is now true wall-clock time for `_process_file_parts`
- `total_mib_per_s` derives from it
- the old sum is retained as `stage_total_s`
- `part_count` and `max_concurrent_parts` were added for context

This also mitigates the instrumentation blind spot described in §4.1.

### 3.7 New configuration

| Option | Default | Notes |
|--------|--------:|-------|
| `max_concurrent_files` | 2 | Files from one batch processed simultaneously |
| `max_concurrent_parts` | 4 | Parts of one file processed simultaneously |

Both are deliberately conservative. Peak memory is roughly
`max_concurrent_files × max_concurrent_parts × 3 × part_size`; with the ~51 MiB adjusted
parts that very large files receive, the 2×4 default already implies ~1.2 GB worst case.
Both descriptions state this. Tune to the deployment's memory budget.

`config_schema.json`, `example_config.yaml`, and `README.md` were regenerated via
`scripts/update_config_docs.py` and `scripts/update_readme.py`.

### 3.8 Tests

84 pass (60 before, 24 added). Ruff and mypy clean.

- `tests/unit/test_crypto_buffers.py` — preallocation size maths across empty,
  sub-segment, exactly-aligned, and partial-trailing-segment inputs; round-trip fidelity;
  buffer-type acceptance; wrong-secret failure; per-segment nonce uniqueness.
- `tests/integration/test_interrogator.py::test_parts_completing_out_of_order` — inverts
  download completion order with staggered delays. It passes only if both order-sensitive
  integrity checks still hold: the whole-file SHA-256 over decrypted content, and the ETag
  derived from concatenated per-part MD5s.

**One existing assertion was changed.** `test_interrogate_new_files` compared reports
positionally against the input file list using `zip(..., strict=True)`. With file-level
concurrency, reports arrive in completion order, so reports are now looked up by
`file_id`. Nothing in the report contract depends on ordering — each report is
self-contained — but this is a real behavioural change and warrants review.

---

## 4. Unaddressed

### 4.1 Confirm where the observed 2–4 s actually goes — needs a production check

`report_success` runs outside `_process_file_parts` and is therefore absent from the
logged metrics. During this investigation a Central API call that escaped to the network
added ~3.1 s per file (1.3 s → 4.4 s wall) while the logged stage total stayed at ~1.0 s.
The retry transport defaults to `client_num_retries=3` with exponential backoff capped at
60 s.

That gap is suspiciously close to the reported 2–4 s symptom. **Compare logged `total_s`
against real per-file wall time in production.** If they diverge, the time is in the
Central API round-trip and none of the work in §3 targets it.

### 4.2 Per-part `ListMultipartUploads` — blocked on hexkit

The largest single win, and untouched. `upload_file_part` → `_get_part_upload_url` →
hexkit's `get_part_upload_url` → `_assert_multipart_upload_exists` issues a paginated
`list_multipart_uploads` API call before every part upload. Presigning itself is local
HMAC and needs no network.

Measured at 6.4 ms per call against a local container; the probe still counts 13 / 6 / 3
calls for 11 / 4 / 1 parts, unchanged by this work. Projected against real S3:

| File | Submitter part | Adjusted part | Parts | Overhead @30 ms | @50 ms |
|------|---------------:|--------------:|------:|----------------:|-------:|
| 1 GiB   | 6 MiB | 6.00 MiB | 171 | 5.1 s | 8.6 s |
| 10 GiB  | 6 MiB | 6.00 MiB | 1707 | 51 s | 85 s |
| 100 GiB | 6 MiB | 10.25 MiB | 9991 | 300 s | 500 s |

Note the asymmetry: the download path already caches its presigned URL via `@alru_cache`
(`s3.py:114`). The upload path cannot simply cache, because each part signs a distinct
part number and `Content-MD5` — but the *assert* is what costs, not the signing.

**Required:** an upstream hexkit change adding a flag to skip the existence assertion.
Confirmed still present and byte-identical in hexkit 9.0.0.

### 4.3 Dependency upgrade — no performance benefit, real migration cost

hexkit 9.0.0 and ghga-service-commons 8.0.0 were evaluated and measured.

- `hexkit/providers/s3/provider/objstorage.py` is **byte-identical** between 8.6.0 and
  9.0.0 (`diff` returns zero lines). All 9.0.0 changes are in OpenTelemetry instrumentation
  and test utilities.
- `ListMultipartUploads` counts identical (13 / 6 / 3).
- Only real change: 3 fewer S3 calls per file (2 `HeadBucket` + 1 `HeadObject`), landing in
  hexkit 8.6.0. Once per file, ~18 ms.
- Timing differences within run-to-run noise.

Upgrade blockers, none performance-related:

1. **gsc 8.0.0 migrates `httpx` → `httpx2`** — a different PyPI package
   (`httpx2>=2.9.1`). DHFS imports `httpx` directly in `http.py`, `s3.py`, and `central.py`.
2. **TLS trust store** — httpx2 drops certifi in favour of the OS trust store via
   `truststore`. The gsc source notes minimal images need a populated system CA store;
   relevant to `Dockerfile.dhi`.
3. **Test mocking** — `pytest_httpx` mocks `httpx`, not `httpx2`, so Central API mocking
   across the suite needs reworking.

**Recommendation:** upgrade on its own schedule, not for performance.

### 4.4 Dead cache configuration options

`HttpClientConfig` extends `CompositeCacheConfig`, but `get_configured_httpx_client` calls
`create_ratelimiting_retry_transport` — the *non*-caching factory. `client_cache_capacity`,
`client_cache_ttl`, and `client_cacheable_methods` are exposed in `config_schema.json` and
do nothing. gsc 8.0.0 removes them entirely. Cleanup only; no performance impact.

### 4.5 Environment vs lock file drift

The devcontainer had hexkit 8.6.0 + gsc 8.0.0 installed while `lock/requirements.txt` pins
8.4.1 + 7.0.5, and `HEAD` imports `CompositeCacheConfig`, which 8.0.0 removed — the test
suite could not even be collected. Stale `dhfs 4.0.0` metadata in the environment also
disagrees with `pyproject.toml`'s 3.1.0.

Restored to the locked versions for this work, but it will drift again for the next person.

### 4.6 S3 minimum part size — pre-existing risk, not introduced here

`adjusted_part_size` derives from the submitter's part size. S3 requires every part except
the last to be at least 5 MiB, so a submitter using a smaller part size would produce a
multipart upload S3 rejects. Behaviour is unchanged by this work — the old `upload_buffer`
flushed at exactly `adjusted_part_size` too — but it is worth an explicit floor.

### 4.7 Verify-decrypt pass — deliberately retained

The third crypto pass costs about a third of crypto time, but it is not redundant: the
SHA-256 compared against `decrypted_sha256` is computed from the *re-decrypted* bytes, so
it proves the re-encryption round-trips rather than merely that the first decryption
worked. Dropping it would trade a real integrity property for less than the pipelining
already delivered.

---

## 4a. Follow-up: is the in-order hash fold a bottleneck?

The pipeline keeps one ordering constraint: the whole-file SHA-256 is order-dependent, so
each part waits on `hashed[index-1]` before folding its plaintext in. The question was
whether dropping that — running parts fully unordered and consolidating at the end —
would buy anything, and whether reordering within a part would.

**Measured first.** Instrumenting the wait showed `stall_s` of 0.145–0.178 s and
`hash_s` ~0.06 s, both accrued while holding a semaphore slot. That looked like headroom,
so three variants were built and benchmarked (best of 3 runs, 60 MiB file):

| config | A: baseline | B: fold before upload | C: verify ∥ upload | D: fold at end |
|---|---|---|---|---|
| 6 MiB parts, 0 ms | 0.924 | 0.951 | **0.842** | 0.886 |
| 6 MiB parts, 20 ms | 0.999 | 0.997 | **0.940** | 0.927 |
| 16 MiB parts, 0 ms | 0.749 | 0.728 | **0.677** | 0.730 |
| 16 MiB parts, 20 ms | 0.826 | 0.819 | **0.749** | 0.782 |

**Consolidating order at the end does not help.** Variant D removes the constraint
outright — `stall_s` is 0 by construction — and is still slower than C, which keeps it.
The reason is that the in-order fold currently overlaps with other parts' I/O and is
therefore nearly free, whereas hoisting all the hashing to the end serializes it on the
critical path with nothing left to overlap against. It would also cost O(file size) of
resident plaintext: for a 100 GiB file, the entire decrypted file. Rejected on both counts.

Likewise B, moving the fold ahead of the upload, is within noise. The ordering chain is
pipeline slack, not critical path.

**The reordering that does pay is C**, now implemented. Within a part, the verify-decrypt
and the upload are independent — the verify pass feeds only the whole-file checksum, and
the upload needs only the re-encrypted bytes — so they run concurrently in a per-part
`TaskGroup` instead of the upload waiting on the verify. This removes a whole crypto pass
from each part's critical path for **~1.10× on single files and 1.08× on a batch**
(1.790 s → 1.663 s, 53.6 → 57.7 MiB/s), on top of the gains in §3.

Guarantees held constant:

- The in-order fold is kept, so the SHA-256 is still computed over the re-decrypted bytes
  in part order (§4.7 unchanged).
- The fold moved inside the verify task, so the chain now advances at crypto speed rather
  than S3 speed. The FIFO-semaphore argument against deadlock still holds: tasks acquire
  slots in creation order, so a part never waits on a successor.
- Memory is unchanged. Both concurrent sub-tasks stay inside the same semaphore slot, so
  the `max_concurrent_files × max_concurrent_parts × 3 × part_size` bound in the config
  documentation still holds.
- Error severity survives the extra nesting level. `_flatten_exception_group` was already
  recursive; `tests/unit/test_error_severity.py` now pins that a `CriticalError` raised
  inside the per-part group still outranks an `InconclusiveError` in the per-file group.

On failure the part is now uploaded even when its verify pass fails, where previously the
verify short-circuited it. This is harmless — the multipart upload is aborted and never
completed — and it trades a little wasted bandwidth on the rare failure path for the
speedup on every success.

---

## 5. Summary

| Item | Status |
|------|--------|
| Preallocated crypto buffers | Done — 1.24× on crypto |
| Removed `upload_buffer` copies | Done |
| Crypto off event loop, parts pipelined | Done — 1.4–1.7× per file |
| Concurrency across files | Done — 1.94× per batch |
| Verify pass overlapped with upload | Done — a further 1.10× per file |
| Unordered parts, fold at end | Rejected — measurably slower, O(file) memory |
| Wall-clock metrics | Done |
| Confirm Central API report cost | **Open — needs production check** |
| Per-part `ListMultipartUploads` | **Open — blocked on hexkit** |
| Dependency upgrade | Evaluated — no perf benefit, deferred |
| Dead cache config options | Open — cleanup |
| Environment/lock drift | Open — hygiene |
| S3 minimum part size floor | Open — pre-existing |
| Verify-decrypt pass | Retained by decision |

Net measured improvement: **1.5–2.1× wall time**, largest for multi-part files and full
batches against a latent endpoint. The single largest remaining win (§4.2) requires an
upstream change, and §4.1 may yet prove to be the actual cause of the reported symptom.
