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
| `max_concurrent_parts` | 8 | Parts in flight at once, across all files |

Both are deliberately conservative. Peak memory is roughly
`max_concurrent_parts × 3 × part_size` — see §4c, which made the parts budget
process-wide so it no longer multiplies with the file count. Tune it to the
deployment's memory budget.

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

Written up for filing against hexkit in **`HEXKIT_UPSTREAM_ISSUES.md`** (Issue 1), along
with a detail not noted above: `_list_multipart_uploads_for_object()` paginates
`list_multipart_uploads` over the *whole bucket* with no `Prefix` and filters client-side,
so the per-part cost also scales with unrelated bucket contents. The cleaner's outstanding
`TODO: Finish MPU cleanup` lets abandoned uploads accumulate, which makes this worse over
time in exactly the deployments where it already hurts most.

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
  the per-slot bound documented in the config still holds (§4c later restated that bound
  in terms of one setting instead of two).
- Error severity survives the extra nesting level. `_flatten_exception_group` was already
  recursive; `tests/unit/test_error_severity.py` now pins that a `CriticalError` raised
  inside the per-part group still outranks an `InconclusiveError` in the per-file group.

On failure the part is now uploaded even when its verify pass fails, where previously the
verify short-circuited it. This is harmless — the multipart upload is aborted and never
completed — and it trades a little wasted bandwidth on the rare failure path for the
speedup on every success.

---

## 4b. Follow-up: issues found outside the crypto path

A sweep of the rest of the service — S3 adapter, Central client, cleaner, models, run
loop — turned up two real problems and several candidates that measurement rejected.

### 4b.1 Part hashing was blocking the event loop (fixed — the largest single win)

The crypto was moved to worker threads in §3.3, but the *hashing* was not, and it is not
cheap. Measured on this machine:

| | 6 MiB part | 16 MiB part |
|---|---|---|
| MD5 (~515 MiB/s) | 11.8 ms | 30.9 ms |
| SHA-256 (~1155 MiB/s) | 5.2 ms | 13.9 ms |
| **`digest_encrypted_part` per part** | **17.4 ms** | **44.9 ms** |

Plus `update_unencrypted` (another SHA-256 over the plaintext) at 5.2 / 13.9 ms. So every
part froze the event loop for roughly 22 ms at 6 MiB parts and 59 ms at 16 MiB parts. While
frozen, no other part's download or upload can make progress — it was quietly cancelling
out much of the pipelining from §3.3.

Both now run via `asyncio.to_thread`, like the crypto. `hashlib` releases the GIL for
buffers over 2 KiB, so the threads genuinely run in parallel. Ordering is unaffected: the
successor stays blocked on `hashed[index]`, which is only set after the threaded fold
returns. A `digest` stage was added to the logged metrics.

This is worth **1.18–1.35× on its own**, more than any other single change in this round,
and it scales with part size — the bigger the parts, the longer the loop was frozen.

### 4b.2 Cleanup deleted objects one at a time (fixed)

`S3Cleaner.scan_and_clean()` looped over removable objects with a sequential `await` per
deletion. Each is its own S3 round trip, so cleanup cost object-count × latency — a 500
object cleanup at 20 ms RTT spent 10 seconds doing nothing but waiting.

The deletions are independent, so they now run through a `TaskGroup` bounded by a new
`max_concurrent_deletions` option (default 16). Failure handling is unchanged: the helper
records failures instead of raising, so the group still runs every deletion to completion
and the partial-failure log is identical. `hexkit`'s `ObjectStorageProtocol` exposes only
single-object `delete_object`, so S3's batch `DeleteObjects` API (up to 1000 keys per
call) is not reachable from here — that would be a further upstream win.

`tests/integration/test_cleaner.py::test_deletions_run_concurrently` pins this; it fails
at 0.99 s against the old serial code and passes at ~0.1 s now.

### 4b.3 Candidates measured and rejected

- **JWT signing per request.** `_auth_headers()` mints a fresh token on every Central
  call, and `AUTH_TOKEN_VALID_SECONDS` is 60, so caching looked plausible. Measured at
  **0.175 ms per signature** — about 9 ms across a 50-file batch. Not worth the staleness
  risk. Left alone.
- **`does_object_exist` before `init_multipart_upload`.** This looked like a guaranteed-dead
  HeadObject per file, since the interrogator passes a freshly generated `uuid4()` that
  cannot collide. It is *not* dead: `dhfs verify` patches `uuid4` to a fixed
  `INTERROGATION_OBJECT_ID`, and the check is what makes repeated verification runs
  idempotent. Removing it would break that for one round trip per file. Left alone.

### 4b.4 `timedelta.seconds` misuse in the run loop (fixed)

`main.py` computed elapsed time as `(stop - start).seconds` in two places — the
interrogation run-interval calculation and the `dhfs verify` duration log.
`timedelta.seconds` is the seconds *component* of the delta, not its total: it silently
drops whole days and the sub-second remainder.

| batch duration | `.seconds` | `.total_seconds()` | resulting sleep (60 s interval) |
|---|---|---|---|
| 0.4 s | 0 | 0.4 | 60.0 s → 59.6 s |
| 12.7 s | 12 | 12.7 | 48.0 s → 47.3 s |
| exactly 24 h | 0 | 86400.0 | 60.0 s → 0 s |
| 24 h + 5 s | 5 | 86405.0 | 55.0 s → 0 s |

The sub-second truncation was cosmetic, but the wraparound is not: a batch that ran past
24 hours would report a near-zero elapsed time and then wait out a full interval it had
already vastly exceeded. Both sites now use `.total_seconds()`, and the two log lines use
`%.1f` since the value is no longer an integer.

### 4b.5 Upstream S3 round trips — documented for filing

Three hexkit inefficiencies are now written up in **`HEXKIT_UPSTREAM_ISSUES.md`**, with
measured call counts, source excerpts, and suggested fixes:

1. `get_part_upload_url()` issues a bucket-wide `ListMultipartUploads` per part (§4.2).
2. `complete_multipart_upload()` discards the ETag the S3 response already contains,
   forcing DHFS to fetch it separately.
3. `get_object_etag()` costs three round trips — `_assert_object_exists()` does a
   `HeadBucket` plus a `HeadObject`, then `_get_object_metadata()` repeats the same
   `HeadObject`.

Measured S3 operations for one 60 MiB file, by patching
`botocore.client.BaseClient._make_api_call`:

| Operation | 11 parts (6 MiB) | 4 parts (16 MiB) |
|---|---|---|
| `ListMultipartUploads` | 13 | 6 |
| `HeadObject` | 4 | 4 |
| `HeadBucket` | 2 | 2 |
| `CreateMultipartUpload` / `ListParts` / `CompleteMultipartUpload` | 1 each | 1 each |
| **Total** | **22** | **15** |

`ListMultipartUploads` is `part_count + 2` and is **59%** of all S3 calls on the 11-part
file. None of the three is fixable from DHFS — each sits inside the
`ObjectStorageProtocol` method DHFS has to call.

---

## 4c. One memory budget instead of two multiplying knobs

`max_concurrent_files` and `max_concurrent_parts` were not two independent settings:
they multiplied. The parts semaphore was created per file, so in-flight parts were
`files × parts` and peak memory was
`max_concurrent_files × max_concurrent_parts × 3 × part_size`. The quantity an operator
actually needs to bound — memory — was not the quantity exposed, the factor of three was
documented in prose and enforced nowhere, and raising either knob alone was unsafe.

The parts semaphore is now process-wide (`Interrogator._part_slots`), shared by every
file. Peak memory is `max_concurrent_parts × 3 × part_size`, a function of one setting,
whatever `max_concurrent_files` is set to. `max_concurrent_files` still bounds how many
multipart uploads are open at once, but no longer affects memory.

**The default had to be rebased.** The old 2 × 4 allowed 8 parts in flight; a naive move
to a shared budget of 4 would have silently halved concurrency. `max_concurrent_parts`
therefore defaults to 8, preserving the measured throughput exactly:

| config | before | after |
|---|---|---|
| 60 MiB, 6 MiB parts, 0 ms | 0.735 s | 0.722 s |
| 60 MiB, 6 MiB parts, 20 ms | 0.787 s | 0.835 s |
| 60 MiB, 16 MiB parts, 0 ms | 0.538 s | 0.515 s |
| 60 MiB, 16 MiB parts, 20 ms | 0.611 s | 0.653 s |
| Batch, 4 × 24 MiB, 20 ms | 1.366 s | 1.398 s |

All within run-to-run noise, in both directions.

A shared budget also improves utilisation: a lone large file previously got only
`max_concurrent_parts` slots while the rest of the allowance sat idle, and can now use
the whole budget.

**Deadlock-freedom still holds**, and the argument is unchanged in shape. Semaphore
acquisition is FIFO and a file's part tasks are created in part order, so a part is never
admitted ahead of its predecessor. A part's fold waits only on its immediate predecessor
in the *same* file, and part 0 never waits — so the lowest unfolded part of every file is
always admitted and never blocked. Parts of other files interleave in the queue but
participate in no other file's fold chain, so they can delay it but never block it.

`tests/integration/test_interrogator.py::test_part_budget_is_shared_across_files` pins
this: with the budget forced to 2 and two multi-part files in one batch, it observes 8
concurrent parts against the old per-file semaphore and at most 2 now.

`max_concurrent_deletions` was left as its own option. It is a genuinely separate
concern — a request-rate dial for the cleanup routine with no memory cost and no
interaction with the part budget.

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
| Part hashing off the event loop | Done — a further 1.18–1.35× per file |
| Unified in-flight-parts memory budget | Done — one setting, not two multiplying ones |
| Concurrent cleanup deletions | Done — was 1 round trip per object, serial |
| JWT signing per request | Measured at 0.175 ms — not worth caching |
| Pre-upload existence check | Kept — load-bearing for `dhfs verify` idempotency |
| `complete_upload` extra HeadObject | **Open — filed in `HEXKIT_UPSTREAM_ISSUES.md`** |
| `get_object_etag` triple round trip | **Open — filed in `HEXKIT_UPSTREAM_ISSUES.md`** |
| `.seconds` vs `.total_seconds()` in `main.py` | Done — both sites |
| Wall-clock metrics | Done |
| Confirm Central API report cost | **Open — needs production check** |
| Per-part `ListMultipartUploads` | **Open — filed in `HEXKIT_UPSTREAM_ISSUES.md`** |
| Dependency upgrade | Evaluated — no perf benefit, deferred |
| Dead cache config options | Open — cleanup |
| Environment/lock drift | Open — hygiene |
| S3 minimum part size floor | Open — pre-existing |
| Verify-decrypt pass | Retained by decision |

Net measured improvement: **1.9–2.8× wall time**, largest for multi-part files and full
batches against a latent endpoint. Against the state at the start of this round, the
§4a and §4b changes together give:

| config | before this round | after | gain |
|---|---|---|---|
| 60 MiB, 6 MiB parts, 0 ms | 0.913 s | 0.704 s | 1.30× |
| 60 MiB, 6 MiB parts, 20 ms | 1.023 s | 0.800 s | 1.28× |
| 60 MiB, 16 MiB parts, 0 ms | 0.773 s | 0.497 s | 1.56× |
| 60 MiB, 16 MiB parts, 20 ms | 0.819 s | 0.637 s | 1.29× |
| Batch, 4 × 24 MiB, 20 ms | 1.790 s | 1.247 s | 1.44× |

Batch throughput went from 53.6 to 77.0 MiB/s. The single largest remaining win (§4.2)
requires an upstream change, and §4.1 may yet prove to be the actual cause of the
reported symptom.
