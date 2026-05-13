# ENTERPRISE_FEATURES.md — clean-room reimplementation spec

This file is a **behavioral specification** of the features that were removed
in the prior commit (`chore: remove Feldera Enterprise code`). It exists
solely to drive the clean-room reimplementation in the commits that follow.

**Audit trail:** the original Feldera Enterprise implementation was deleted
*before* this file was written. Each section below describes the **public
interface and behavior contract** that a new implementation must satisfy,
without referencing the original source code. Each reimplementation commit
must:

1. Implement one section from this file.
2. Use only the spec — no inspection of the upstream Feldera Enterprise
   source tree, no copy-pasting of upstream implementation details beyond
   what is already exposed in the open-source interface.
3. Include tests that verify the contract independently (not by matching
   bit-for-bit against the original).

When every section has a corresponding implementation, this file is deleted
in a final commit.

---

## Inventory

The features removed in commit 1 and slated for reimplementation:

1. **Object-store `StorageBackend`** — concrete `opendera_storage::StorageBackend` implementation backed by S3 / GCS / Azure via the `object_store` crate. The trait, factory, and `inventory::collect!()` registration site already exist in OSS; only the concrete backend was proprietary.
2. **Checkpoint synchronization** — the `CheckpointSynchronizer` trait (in `opendera_storage::checkpoint_synchronizer`) had its sole registered implementation behind the feature flag. Provides push/pull of pipeline checkpoints to/from object storage, GC of older checkpoints, and standby-mode continuous pull.
3. **`/checkpoint`, `/checkpoint/sync`, `/checkpoint/sync_status`, `/activate` endpoints** — manager-side HTTP forwarding endpoints. Currently restored as unconditional forwarders, but the pipeline-side handlers (which receive the forwarded requests) need to actually do something. The pipeline-side handler logic is what was gated; reimplementation makes it real.
4. **Graceful stop (`/stop?force=false`)** — pipeline-manager endpoint that initiates a checkpointed suspend before shutting down. Currently restored as an unconditional forwarder; pipeline-side suspend logic is what gated it.
5. **Fault tolerance** — `RuntimeConfig.fault_tolerance` and the supporting circuit-level replay/recovery infrastructure. Today the manager rejects fault-tolerance configs at validation time.
6. **`--enterprise` flag to the SQL compiler** — passed to the Java `sql-to-dbsp` compiler when feature was enabled; selected enterprise-aware code generation paths in the compiler.
7. **License module and `feldera-cloud1-client` dependency** — was the license-validation hook. Currently neutered with a hardcoded "10-year valid" fork override; needs proper removal in a follow-up commit. Not specified here because the OpenDera direction is to remove license validation entirely, not reimplement it.

---

## 1. Object-store `StorageBackend`

### Purpose

Allow pipelines to persist their working state (B-tree shards, write-ahead
logs, checkpoint manifests) on a cloud-native object store rather than a
local POSIX filesystem. The streaming engine writes and reads through the
`StorageBackend` trait; switching from local disk to S3/GCS/Azure changes
the deployment story (multi-replica, durable, geo-replicated) without
touching the engine code.

### Public interface (already in OSS)

- Trait: `opendera_storage::StorageBackend` (see `crates/storage/src/lib.rs`,
  around line 52). Methods: `create_named`, `create`, `create_with_prefix`,
  `open`, `delete`, `list`, `exists`, `read_json`, `write_json`, and the
  trait's helper methods.
- Factory: `opendera_storage::StorageBackendFactory` (line 40).
- Registration: `inventory::collect!(&'static dyn StorageBackendFactory)`.
  Each backend submits a `&'static` instance via `inventory::submit!`.
- Configuration: `opendera_types::config::StorageBackendConfig` already has
  the necessary variants. The object-store-backed variant is selected when
  the user supplies an `object_store` URL such as `s3://bucket/prefix`.

### Behavior contract

- **Atomicity of file creation.** `create_named` and `create` produce a
  `FileWriter` that buffers writes locally and on `commit()` either makes
  the file fully visible at the named path or leaves no trace. For S3 this
  is a multipart upload completed with one `CompleteMultipartUpload`.
  Partial visibility (a half-written object) must never be observable by a
  concurrent `open` or `exists`.
- **Read-after-write.** Once `commit()` returns success on a writer, any
  subsequent `open`, `read_json`, or `exists` against the same path
  observes the new file. (S3 provides this natively as of 2020-12; GCS and
  Azure provide it too.)
- **List ordering.** `list` returns paths in lexicographic order. Pagination
  is hidden from the caller; the implementation may stream pages internally
  but must return a complete result by the time the iterator drains.
- **Deletes are idempotent.** `delete` of a missing path returns `Ok(())`,
  not an error.
- **Concurrent writers to the same path are racy** — last-commit-wins.
  Callers higher up are expected to serialize via the checkpoint UUID
  naming scheme (UUIDv7, monotonic per pipeline).
- **Errors map to `StorageError`** with enough context that a caller can
  distinguish: not-found, permission-denied, network-transient,
  precondition-failed (when conditional puts are used by higher layers).

### Configuration

- The `object_store` backend is selected when the storage config's URL
  begins with `s3://`, `gs://`, `az://`, or `http(s)://` (for S3-compatible
  endpoints like Tigris or MinIO).
- Credentials: pulled from the standard environment of the deployment
  (AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY/AWS_SESSION_TOKEN for S3,
  GOOGLE_APPLICATION_CREDENTIALS for GCS, AZURE_STORAGE_ACCOUNT/KEY for
  Azure). No proprietary credential schema.
- Optional: bucket-side SSE/KMS settings, customer-supplied keys, prefix
  override, retry/timeout knobs — all exposed via the
  `StorageBackendConfig` variants already in `opendera-types`.

### Integration points

- `crates/storage/Cargo.toml` already depends on `object_store` with
  `aws`/`gcp`/`azure`/`http` features.
- `crates/storage/src/lib.rs` already re-exports the object-store path
  types and defines the trait + factory.
- A new file `crates/storage/src/object_store.rs` (or similar) holds the
  implementation. It registers itself with `inventory::submit!` so callers
  (controller, checkpointer) automatically pick it up when the URL scheme
  matches.

### Test approach

- Round-trip property tests: write N records, list them, read each back,
  delete each, list again. Run against a local MinIO container in CI.
- Crash/atomicity tests: kill the writer mid-upload, verify `open` returns
  not-found and a subsequent retry produces a clean file.
- Concurrency: two writers racing on the same path — exactly one commit
  becomes visible, no partial state.
- Error mapping: simulate 403/404/503 from the backend, assert correct
  `StorageError` variants.

### Estimated effort

~1 week per backend (S3, GCS, Azure). S3 first because the cloud uses it.

---

## 2. Checkpoint synchronization

### Purpose

When a pipeline runs on a node with ephemeral local disk (cloud Fly Machine,
K8s pod), durable checkpoints must live in object storage so the pipeline
can recover after a crash or migrate to a new host. The checkpoint
synchronizer is the bridge between the local working state (managed by
`Checkpointer`) and the durable copy on object storage.

### Public interface (already in OSS)

- Trait: `opendera_storage::checkpoint_synchronizer::CheckpointSynchronizer`
  (see `crates/storage/src/checkpoint_synchronizer.rs`). Methods (inferred
  from call sites preserved in commit 1):
  - `pull(storage: Arc<dyn StorageBackend>, sync: SyncConfig) -> Result<(CheckpointMetadata, Option<TransferMetrics>), Error>` — fetch the most recent checkpoint named by the sync config and place it in local storage.
  - `push(uuid: Uuid, storage: Arc<dyn StorageBackend>, sync: SyncConfig) -> Result<Option<TransferMetrics>, Error>` — upload the local checkpoint identified by `uuid` to the durable location.
- Registration: `inventory::collect!(&'static dyn CheckpointSynchronizer)`.
- Config: `opendera_types::config::SyncConfig` (`pull_interval`, `standby`,
  `start_from_checkpoint`, `fail_if_no_checkpoint`, `validate()`).
- Metrics: pull/push success / failure / duration / transferred-bytes /
  transfer-speed counters live in `crates/adapters/src/controller/sync.rs`
  (preserved by commit 1).

### Behavior contract

- **Idempotency.** Pulling the same checkpoint UUID twice produces
  identical local state; pushing the same UUID twice is a no-op if the
  remote already has it complete.
- **Atomic visibility.** A `push` makes the new checkpoint visible only
  after every shard + manifest is uploaded. Partial uploads are never
  observable to a concurrent `pull`.
- **GC.** `pull_and_gc` (the wrapper) garbage-collects local checkpoints
  older than the one just pulled, but only after the pull commits. A failed
  pull never deletes existing local state.
- **Standby mode.** With `standby: true` + `start_from_checkpoint:
  Some(...)`, `continuous_pull` polls the remote on `pull_interval` until
  an activation signal arrives, then performs one final pull and writes
  the activation marker file (`ACTIVATION_MARKER_FILE` constant) before
  returning. The marker prevents a re-activation on restart.
- **Activation marker is durable.** Once written, restarting the pipeline
  detects the marker and short-circuits standby (no further pulls).
- **Failure modes.** Any non-transient pull error fails fast in standby
  mode after the activation signal. Transient errors (network, 5xx) retry
  with backoff inside `pull`/`push`.
- **Metrics.** Each pull/push updates the corresponding atomic counter +
  histogram in `crates/adapters/src/controller/sync.rs`.

### Configuration

- `SyncConfig` in `opendera-types`. Fields already exist (URL, credentials,
  pull interval, standby flag, start_from_checkpoint, fail_if_no_checkpoint).
- Wired into pipeline runtime config under
  `storage.backend.File(FileBackendConfig { sync: Some(...) })`.

### Integration points

- `crates/adapters/src/controller.rs`:
  - `ControllerInit::is_pull_necessary` — currently `None`; reimplement to
    call into the synchronizer when sync config is set.
  - `ControllerInit::pull_once` — currently `Ok(())`; reimplement to call
    `synchronizer.pull(...)` once.
  - `ControllerInit::continuous_pull` — currently returns
    `InvalidStandby`; reimplement to drive the standby loop.
  - `CheckpointSyncThread::run` — currently returns
    `checkpoint_push_error("...unavailable until reimplemented")`;
    reimplement to call `synchronizer.push(...)` and update metrics.
- The reimplementation registers itself via `inventory::submit!` in the
  new module (e.g., `crates/storage/src/object_store_synchronizer.rs`).

### Test approach

- Push then pull round-trip: pipeline checkpoint → S3 → fresh pipeline →
  resume from that checkpoint, verify materialized-view state hash
  equality.
- Crash-mid-push: kill the syncer during multipart upload, verify the
  partial object isn't visible and a retry succeeds.
- Standby + activation: start two pipelines (primary + standby), drive
  primary forward, send activation signal to standby, verify standby picks
  up at the last-committed checkpoint and never re-activates on restart.
- GC: pull a newer checkpoint and verify older local checkpoints are
  removed, but only after the new one commits.

### Estimated effort

~2-3 weeks.

---

## 3. Checkpoint / sync / sync_status / activate endpoints (pipeline-side handlers)

### Purpose

The manager-side endpoints are restored in commit 1 as unconditional HTTP
forwarders to the pipeline. The reimplementation work is on the **pipeline
side** — the actual handler that receives the forwarded request and does
something meaningful.

### Public interface

Pipeline HTTP server, served by `crates/adapters/src/server.rs`:

- `POST /checkpoint` — initiates a checkpoint; returns
  `CheckpointResponse { checkpoint_sequence_number: u64, ... }` (already
  defined in `opendera-types`). Caller polls `/checkpoint_status` to see
  when complete.
- `POST /checkpoint/sync` — triggers a one-shot push of the latest
  checkpoint to the configured object-store sync target.
- `GET /checkpoint/sync_status` — returns the state of the most recent
  push: `Idle | InProgress | Done(uuid) | Error(message)`.
- `POST /activate` — for a standby pipeline, signals activation; the
  pipeline performs its final pull-and-commit and transitions from
  `Standby` to `Running`.

### Behavior contract

- `/checkpoint` is async — returns the sequence number immediately,
  performs the actual checkpoint write in the background.
- `/checkpoint/sync` is idempotent — calling twice in rapid succession
  with the same latest-checkpoint UUID is a no-op for the second call.
- `/checkpoint/sync_status` reflects only the *last* push attempt; older
  history is not retained.
- `/activate` is idempotent — calling on an already-activated pipeline
  returns success without re-triggering the sync.

### Integration points

- `crates/adapters/src/server.rs` — the actix-web server inside the
  pipeline. Handlers register themselves with the actix `web::scope`.
- Calls into the controller (`CheckpointSyncThread`,
  `ControllerInit::continuous_pull`, the in-process checkpointer).

### Test approach

- End-to-end against a manager+pipeline+MinIO test harness: drive a
  pipeline forward, call `POST /checkpoint`, poll
  `/checkpoint/sync_status`, verify the manifest appears in object
  storage.
- Activation: start primary + standby, push from primary, call
  `/activate` on standby, verify standby resumes from the last
  primary-pushed checkpoint.

### Estimated effort

~1 week (after sections 1 and 2 are in place — this is mostly wiring).

---

## 4. Graceful stop (`/v0/pipelines/{name}/stop?force=false`)

### Purpose

Stop a running pipeline cleanly by taking a final checkpoint, flushing
in-flight transactions, and writing the activation marker — then shut
down. Versus `force=true` which terminates immediately and discards
in-flight work.

### Public interface

Manager endpoint (restored in commit 1 as unconditional forwarder):

- `POST /v0/pipelines/{pipeline_name}/stop?force=false` →
  `POST /suspend` on the pipeline.

Pipeline endpoint:

- `POST /suspend` — initiates the graceful-stop sequence. Returns
  `202 Accepted` immediately; actual shutdown happens asynchronously and
  the manager polls deployment status to see it complete.

### Behavior contract

- The pipeline must finish currently-running step transactions before
  shutting down.
- A final checkpoint is taken and (if sync is configured) pushed to object
  storage before the process exits.
- The pipeline's `can_suspend` invariant is consulted first; if it returns
  `Err(SuspendError::Permanent(...))`, the request fails with the listed
  reasons and the pipeline keeps running.
- `SuspendError::Temporary(...)` causes the manager to retry the request
  later (transient conditions like in-flight transaction, replaying,
  bootstrapping).

### Integration points

- `crates/adapters/src/controller.rs:can_suspend` — the eligibility check
  (the `PermanentSuspendError::EnterpriseFeature` push was deleted in
  commit 1; the other checks remain).
- Pipeline-side `/suspend` handler in `crates/adapters/src/server.rs`.
- Manager-side endpoint already restored in commit 1.

### Test approach

- Drive a pipeline through `Running → /suspend → Paused → re-`Running``
  cycle; assert state-hash equality before and after.
- Try to suspend during an in-flight transaction; assert temporary error
  and successful retry once the transaction commits.

### Estimated effort

~1 week (depends on sync from §2 being available for the optional final
push).

---

## 5. Fault tolerance

### Purpose

Allow a pipeline to survive a worker crash without losing in-flight
records. Combines: (a) durable input journal (so unconsumed records can
replay), (b) per-step checkpoint (so the in-memory state can be
reconstructed at a clean boundary), (c) deterministic replay from the
last checkpoint up to the failure point.

### Public interface

- Config: `opendera_types::config::RuntimeConfig.fault_tolerance` —
  enabling this is currently rejected by validation (commit 1 left the
  rejection in place; it must continue to reject until this section is
  implemented).
- Configuration field shape: a `FaultToleranceConfig` with at minimum
  `model: FaultToleranceModel` where the model is one of
  `{ AtLeastOnce, ExactlyOnce }` plus retention / replay-window knobs.

### Behavior contract

- **At-least-once mode**: on worker crash, the new worker resumes from
  the last committed checkpoint and re-consumes any input records that
  arrived after that checkpoint. Output sinks may see duplicates;
  consumers must be idempotent.
- **Exactly-once mode**: requires transactional output sinks (Kafka with
  transactional producer, Postgres with 2PC, etc.). The pipeline
  coordinates step boundaries with the sink's transaction commit such
  that on resume, partial outputs are rolled back and re-emitted
  deterministically.
- **Checkpoint cadence**: configured; default once per N steps or M
  seconds, whichever comes first. A configurable maximum-lag bound
  triggers an immediate checkpoint to keep replay time bounded.
- **Recovery time SLO**: the new worker must restore the most recent
  successful checkpoint plus replay records within the configured
  recovery budget.
- **Input endpoint requirements**: each input must be replayable to an
  earlier offset (Kafka with retention; an input journal for non-replayable
  sources). The check is `endpoint_stats.fault_tolerance.is_some()` (the
  per-endpoint FT capability flag in commit 1 is preserved at
  `controller.rs:can_suspend`).

### Integration points

- `crates/adapters/src/controller.rs`:
  - The fault-tolerance enforcement in `start_controller`
    (`server.rs:652`) currently rejects FT configs; replace with the real
    bring-up of FT machinery.
  - `validate_runtime_config` in
    `crates/pipeline-manager/src/db/types/utils.rs` similarly rejects FT;
    replace.
- `crates/dbsp/src/circuit/` — step / checkpoint boundary semantics.
- `crates/adapters/src/transport/kafka/ft.rs` — already supports
  transactional offset management; depends on the controller driving it.

### Test approach

- Kill -9 a running pipeline mid-step; restart from checkpoint + journal;
  assert exactly-once output (no duplicate or lost records in the sink).
- Property-based test: random input streams + random injected failures →
  the materialized output equals the failure-free baseline.

### Estimated effort

~6-8 weeks (the heaviest item).

---

## 6. `--enterprise` flag to the SQL compiler

### Purpose

Selected enterprise-aware code generation in the Java `sql-to-dbsp`
compiler. Specifics depend on what the upstream compiler does
differently when the flag is set; from this side of the boundary we see
only the flag was passed.

### Behavior contract

- The compiler emits Rust code that links against `feldera-enterprise`
  Cargo features (mostly the optional fault-tolerance hooks per §5).
- Without the flag, generated code uses the OSS code paths only.

### Integration points

- `crates/pipeline-manager/src/compiler/sql_compiler.rs` — commit 1
  removed the `command.arg("--enterprise")` site. To re-enable, reimpl
  passes the flag only when an OpenDera-specific feature is enabled at
  the manager level (TBD; possibly tied to having fault tolerance §5
  available).
- `sql-to-dbsp-compiler/` (Java) — needs corresponding changes to handle
  the renamed flag if we rebrand it to e.g. `--with-fault-tolerance`.

### Test approach

- Compile a SQL program that uses fault-tolerance-only constructs with
  the flag on/off; assert generated code differs in the expected way.

### Estimated effort

~3-5 days, mostly on the Java side; depends on §5.

---

## 7. License module (out of scope for clean-room reimplementation)

The pre-existing `crates/pipeline-manager/src/license.rs` module and the
`feldera-cloud1-client` workspace dependency it depends on are not
behind the `feldera-enterprise` cargo feature, but they are conceptually
enterprise-only (validating a paid license against a cloud service).

The OpenDera direction is to **delete this module outright** rather than
reimplement it. There is no license to validate in an open-source
deployment; the API's `edition` field becomes a constant "Open source"
and the `license_validity` field is dropped from the configuration
response.

This deletion is a separate follow-up commit, tracked outside this spec.

---

## Reimplementation order

Sequenced to unblock cloud deployment first:

1. **§1 Object-store StorageBackend (S3)** — unblocks cloud checkpoint
   storage. ~1 week.
2. **§2 Checkpoint synchronization** — unblocks distributed checkpoint
   flow. ~2-3 weeks.
3. **§3 Pipeline-side endpoint handlers** — wires the manager endpoints
   to a real implementation. ~1 week.
4. **§4 Graceful stop** — depends on §2 for the final push. ~1 week.
5. **§5 Fault tolerance** — heaviest. Can ship after cloud MVP since
   cloud can start with single-replica pipelines. ~6-8 weeks.
6. **§6 SQL compiler `--enterprise` flag** — depends on §5. ~3-5 days.

Total: ~3 months of one senior-dev effort, parallelizable to ~6-8 weeks
with two engineers.

After every section above has a corresponding implementation merged,
delete this file in a final commit.
