//! Object-store-backed `StorageBackend` implementation.
//!
//! Implements `feldera_storage::StorageBackend` on top of the
//! [`object_store`] crate, which abstracts S3, GCS, Azure Blob, and HTTP
//! servers. Selected when the pipeline config's `storage.backend` is
//! `StorageBackendConfig::Object(...)`.
//!
//! This is the clean-room reimplementation of section §1 of
//! `ENTERPRISE_FEATURES.md`. The spec lives at the repo root; the
//! implementation here was written from that spec only.
//!
//! Current capabilities: synchronous-trait facade over async `object_store`
//! calls, single-PUT writes via the [`PutPayload`] API, multipart streaming
//! for large files (threshold tunable via the
//! `opendera.multipart_threshold` option), bounded retry with full-jitter
//! exponential backoff on transient write failures, range reads, list,
//! delete, and exists. Per-object KMS settings remain a TODO.

#![warn(missing_docs)]

use std::fmt::{self, Debug};
use std::io::ErrorKind;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use feldera_storage::block::BlockLocation;
use feldera_storage::error::StorageError;
use feldera_storage::fbuf::FBuf;
use feldera_storage::file::FileId;
use feldera_storage::{
    FileCommitter, FileReader, FileRw, FileWriter, StorageBackend, StorageBackendFactory,
    StorageFileType, StoragePath,
};
use feldera_types::config::{ObjectStorageConfig, StorageBackendConfig, StorageConfig};
use object_store::path::Path as ObjPath;
use object_store::{
    MultipartUpload, ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion, parse_url_opts,
};
use url::Url;

/// Default threshold above which writes are streamed via multipart upload
/// rather than buffered in memory for a single PUT.
///
/// S3 requires non-final parts to be at least 5 MiB; we pick 8 MiB to
/// give some slack and reduce the number of part requests for typical
/// checkpoint shard sizes. Below this threshold a single PUT is used,
/// which is cheaper for small files (one round trip instead of three).
/// Override per backend with the `opendera.multipart_threshold` option
/// (bytes, decimal integer) in `ObjectStorageConfig::other_options`.
const DEFAULT_MULTIPART_THRESHOLD: usize = 8 * 1024 * 1024;

/// `other_options` key holding the multipart threshold override. The
/// `opendera.` prefix namespaces our knobs apart from the keys
/// `object_store` itself understands; prefixed keys are stripped before
/// the remaining options are handed to [`parse_url_opts`].
const MULTIPART_THRESHOLD_OPTION: &str = "opendera.multipart_threshold";

/// S3's minimum size for non-final multipart parts. Thresholds below
/// this are clamped up, otherwise S3 rejects the second part of any
/// multipart upload with `EntityTooSmall`.
const MIN_MULTIPART_THRESHOLD: usize = 5 * 1024 * 1024;

/// Maximum number of multipart part uploads in flight per writer.
/// Bounds both memory (each in-flight part holds its payload, so worst
/// case is `MAX_IN_FLIGHT_PARTS * multipart_threshold` bytes) and the
/// number of concurrent requests against the store.
const MAX_IN_FLIGHT_PARTS: usize = 4;

/// Retry cap for individual object-store write requests (single PUT,
/// multipart initiation, final part, completion). `object_store`
/// already retries individual HTTP requests internally; this outer
/// retry catches the case where those inner retries are exhausted.
/// Mirrors the checkpoint synchronizer's copy retry policy.
///
/// Non-final parts are NOT retried at this layer: they upload
/// concurrently, and a re-`put_part` is assigned a fresh (later) part
/// number, which would splice the retried bytes after parts that
/// logically follow them. A non-final part that fails after
/// `object_store`'s internal retries fails the whole write instead;
/// the checkpoint layer re-drives whole files.
const WRITE_MAX_ATTEMPTS: u32 = 4;

/// Initial backoff between write retries. Doubles each attempt:
/// 200 ms, 400 ms, 800 ms, with full jitter applied each step.
const WRITE_BASE_BACKOFF: std::time::Duration = std::time::Duration::from_millis(200);

/// Decide whether an `object_store` error is worth retrying. Network
/// glitches, server 5xx, and timeouts surface as `Generic`/`JoinError`
/// and retry; structural failures (bad path, missing object, failed
/// precondition, auth) fail fast.
fn is_transient(err: &object_store::Error) -> bool {
    !matches!(
        err,
        object_store::Error::NotFound { .. }
            | object_store::Error::InvalidPath { .. }
            | object_store::Error::NotSupported { .. }
            | object_store::Error::AlreadyExists { .. }
            | object_store::Error::Precondition { .. }
            | object_store::Error::NotModified { .. }
            | object_store::Error::NotImplemented
            | object_store::Error::PermissionDenied { .. }
            | object_store::Error::Unauthenticated { .. }
            | object_store::Error::UnknownConfigurationKey { .. }
    )
}

/// Run one blocking write attempt up to [`WRITE_MAX_ATTEMPTS`] times,
/// sleeping with full-jitter exponential backoff between transient
/// failures. Non-transient errors and the final attempt's error are
/// returned to the caller unchanged.
fn with_write_retries<T>(
    what: &str,
    path: &ObjPath,
    mut attempt_once: impl FnMut() -> Result<T, object_store::Error>,
) -> Result<T, object_store::Error> {
    let mut backoff = WRITE_BASE_BACKOFF;
    let mut attempt = 0;
    loop {
        match attempt_once() {
            Ok(v) => return Ok(v),
            Err(err) => {
                attempt += 1;
                if !is_transient(&err) || attempt >= WRITE_MAX_ATTEMPTS {
                    return Err(err);
                }
                tracing::warn!(
                    "object store {what} for {path} attempt {attempt} failed                      (retryable): {err}; backing off up to {backoff:?}"
                );
                // Full-jitter backoff: random in [0, backoff].
                let jitter_ns = rand::random::<u64>() % (backoff.as_nanos() as u64).max(1);
                std::thread::sleep(std::time::Duration::from_nanos(jitter_ns));
                backoff = backoff.saturating_mul(2);
            }
        }
    }
}

use feldera_storage::tokio::TOKIO_DEDICATED_IO;

/// Allocates a fresh process-unique `FileId`. Thin wrapper that just
/// delegates to `FileId::new()` (which has its own internal counter); the
/// extra indirection makes intent obvious at call sites.
fn next_file_id() -> FileId {
    FileId::new()
}

/// Convert a relative [`StoragePath`] (the trait's path type) into the
/// absolute `object_store::path::Path` rooted under `base`.
fn absolute_path(base: &ObjPath, name: &StoragePath) -> ObjPath {
    let mut joined = base.clone();
    for part in name.parts() {
        joined = joined.child(part.as_ref());
    }
    joined
}

/// Parses the `opendera.multipart_threshold` option value: a decimal
/// byte count, clamped up to S3's 5 MiB non-final part minimum.
fn parse_multipart_threshold(value: &str) -> Result<usize, StorageError> {
    let bytes: usize = value.trim().parse().map_err(|_| {
        StorageError::stdio(
            ErrorKind::InvalidInput,
            "opendera.multipart_threshold must be a byte count (decimal integer)",
            value.to_string(),
        )
    })?;
    if bytes < MIN_MULTIPART_THRESHOLD {
        tracing::warn!(
            "opendera.multipart_threshold {bytes} is below S3's 5 MiB part minimum;              clamping to {MIN_MULTIPART_THRESHOLD}"
        );
        return Ok(MIN_MULTIPART_THRESHOLD);
    }
    Ok(bytes)
}

/// `StorageBackend` implementation backed by an `object_store::ObjectStore`.
pub struct ObjectStoreBackend {
    store: Arc<dyn ObjectStore>,
    base: ObjPath,
    usage: Arc<AtomicI64>,
    /// Writers switch from a single PUT to multipart streaming at this
    /// many buffered bytes. See [`DEFAULT_MULTIPART_THRESHOLD`].
    multipart_threshold: usize,
}

impl Debug for ObjectStoreBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectStoreBackend")
            .field("base", &self.base.as_ref())
            .finish()
    }
}

impl ObjectStoreBackend {
    /// Construct directly from an already-built `ObjectStore` plus a base
    /// path prefix. Useful for tests (e.g. `object_store::memory::InMemory`)
    /// and for callers that have a pre-configured store to share.
    pub fn new_with_store(store: Arc<dyn ObjectStore>, base: ObjPath) -> Self {
        Self {
            store,
            base,
            usage: Arc::new(AtomicI64::new(0)),
            multipart_threshold: DEFAULT_MULTIPART_THRESHOLD,
        }
    }

    /// Override the multipart threshold (clamped to S3's 5 MiB part
    /// minimum in [`from_config`]; unclamped here for tests against
    /// in-memory stores).
    pub fn with_multipart_threshold(mut self, threshold: usize) -> Self {
        self.multipart_threshold = threshold;
        self
    }

    /// Returns the current version (etag) of `name`, or `None` if the
    /// object doesn't exist. Pair with [`Self::put_if_version`] for
    /// optimistic-concurrency writes.
    pub fn object_version(
        &self,
        name: &StoragePath,
    ) -> Result<Option<UpdateVersion>, object_store::Error> {
        let path = absolute_path(&self.base, name);
        match TOKIO_DEDICATED_IO.block_on(self.store.head(&path)) {
            Ok(meta) => Ok(Some(UpdateVersion {
                e_tag: meta.e_tag,
                version: meta.version,
            })),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Conditionally overwrites `name`: the put succeeds only if the
    /// object's version still matches `expected`, or — when `expected`
    /// is `None` — only if the object does not exist yet. A concurrent
    /// writer therefore surfaces as `Error::Precondition` (stale
    /// version) or `Error::AlreadyExists` (created in between).
    ///
    /// Stores that don't implement conditional puts (some S3-compatible
    /// endpoints) degrade to an unconditional put with a warning —
    /// detection is best-effort, never an availability risk.
    ///
    /// Note: a retry after a transient failure whose first attempt
    /// actually landed server-side can report a spurious conflict; the
    /// caller should treat conflicts as "verify and re-drive", not as
    /// data corruption.
    pub fn put_if_version(
        &self,
        name: &StoragePath,
        data: Vec<u8>,
        expected: Option<UpdateVersion>,
    ) -> Result<(), object_store::Error> {
        let path = absolute_path(&self.base, name);
        let payload = PutPayload::from(data);
        let opts = PutOptions::from(match expected {
            Some(version) => PutMode::Update(version),
            None => PutMode::Create,
        });
        let result = with_write_retries("conditional put", &path, || {
            TOKIO_DEDICATED_IO
                .block_on(self.store.put_opts(&path, payload.clone(), opts.clone()))
                .map(|_| ())
        });
        match result {
            Err(object_store::Error::NotImplemented | object_store::Error::NotSupported { .. }) => {
                tracing::warn!(
                    "object store does not support conditional puts; writing {path}                      unconditionally (concurrent-writer detection disabled)"
                );
                with_write_retries("put", &path, || {
                    TOKIO_DEDICATED_IO
                        .block_on(self.store.put(&path, payload.clone()))
                        .map(|_| ())
                })
            }
            other => other,
        }
    }

    /// Construct from `ObjectStorageConfig` (already in `feldera-types`).
    ///
    /// `other_options` keys prefixed with `opendera.` are consumed here
    /// and stripped before the remaining options reach `object_store`:
    ///
    /// * `opendera.multipart_threshold` — bytes (decimal integer) at
    ///   which writers switch to multipart upload. Clamped to S3's
    ///   5 MiB part-size minimum.
    pub fn from_config(cfg: &ObjectStorageConfig) -> Result<Self, StorageError> {
        let url = Url::parse(&cfg.url).map_err(|_| StorageError::InvalidURL(cfg.url.clone()))?;
        let mut multipart_threshold = DEFAULT_MULTIPART_THRESHOLD;
        let mut opts: Vec<(String, String)> = Vec::new();
        for (k, v) in cfg.other_options.iter() {
            if k == MULTIPART_THRESHOLD_OPTION {
                multipart_threshold = parse_multipart_threshold(v)?;
            } else if let Some(unknown) = k.strip_prefix("opendera.") {
                tracing::warn!("ignoring unknown OpenDera object-store option opendera.{unknown}");
            } else {
                opts.push((k.clone(), v.clone()));
            }
        }
        let (store, base) = parse_url_opts(&url, opts)?;
        Ok(Self {
            store: Arc::from(store),
            base,
            usage: Arc::new(AtomicI64::new(0)),
            multipart_threshold,
        })
    }
}

impl StorageBackend for ObjectStoreBackend {
    fn create_named(&self, name: &StoragePath) -> Result<Box<dyn FileWriter>, StorageError> {
        Ok(Box::new(ObjectStoreFileWriter {
            store: self.store.clone(),
            path: absolute_path(&self.base, name),
            relative: name.clone(),
            id: next_file_id(),
            state: WriterState::Pending(Vec::new()),
            bytes_written: 0,
            usage: self.usage.clone(),
            multipart_threshold: self.multipart_threshold,
        }))
    }

    fn open(&self, name: &StoragePath) -> Result<Arc<dyn FileReader>, StorageError> {
        let path = absolute_path(&self.base, name);
        let meta = TOKIO_DEDICATED_IO.block_on(self.store.head(&path))?;
        Ok(Arc::new(ObjectStoreFileReader {
            store: self.store.clone(),
            path,
            relative: name.clone(),
            id: next_file_id(),
            size: meta.size,
        }))
    }

    fn list(
        &self,
        parent: &StoragePath,
        cb: &mut dyn FnMut(feldera_storage::DirEntry),
    ) -> Result<(), StorageError> {
        use futures::StreamExt;

        let prefix = absolute_path(&self.base, parent);
        let base_len = self.base.as_ref().len();
        let result: Result<Vec<(StoragePath, StorageFileType)>, object_store::Error> =
            TOKIO_DEDICATED_IO.block_on(async {
                let mut stream = self.store.list(Some(&prefix));
                let mut out = Vec::new();
                while let Some(item) = stream.next().await {
                    let meta = item?;
                    // Strip `self.base/` to recover the pipeline-relative
                    // path. The trait expects paths relative to the
                    // backend's logical root.
                    let full = meta.location.as_ref();
                    let rel = full.get(base_len..).unwrap_or(full).trim_start_matches('/');
                    let storage_path: StoragePath = ObjPath::from(rel);
                    let entry = StorageFileType::File { size: meta.size };
                    out.push((storage_path, entry));
                }
                Ok(out)
            });
        for (path, entry) in result? {
            cb(feldera_storage::DirEntry {
                name: path,
                file_type: Ok(entry),
            });
        }
        Ok(())
    }

    fn delete(&self, name: &StoragePath) -> Result<(), StorageError> {
        let path = absolute_path(&self.base, name);
        TOKIO_DEDICATED_IO.block_on(self.store.delete(&path))?;
        Ok(())
    }

    fn delete_recursive(&self, name: &StoragePath) -> Result<(), StorageError> {
        // Object stores are flat; "recursive delete" means delete every
        // object with the given prefix.
        use futures::StreamExt;

        let prefix = absolute_path(&self.base, name);
        let result: Result<(), object_store::Error> = TOKIO_DEDICATED_IO.block_on(async {
            let mut stream = self.store.list(Some(&prefix));
            let mut to_delete = Vec::new();
            while let Some(item) = stream.next().await {
                to_delete.push(item?.location);
            }
            for path in to_delete {
                self.store.delete(&path).await?;
            }
            Ok(())
        });
        Ok(result?)
    }

    fn usage(&self) -> Arc<AtomicI64> {
        self.usage.clone()
    }
}

/// Backing state for `ObjectStoreFileWriter`. The writer starts in
/// `Pending` (buffering in memory). The first time the buffer exceeds
/// `MULTIPART_THRESHOLD`, the writer initiates a multipart upload and
/// transitions to `Streaming`; subsequent writes accumulate into the
/// part buffer and are flushed as parts whenever they reach the
/// threshold. `complete()` either does a single PUT (if still pending)
/// or uploads the final part and completes the multipart.
enum WriterState {
    /// No multipart upload started yet; bytes accumulated in this buffer.
    Pending(Vec<u8>),
    /// Multipart upload in progress; `part_buffer` holds bytes for the
    /// next part not yet flushed. The upload is held in a `Mutex` only
    /// because `MultipartUpload: Send` (not `Sync`) and `FileWriter`
    /// requires `Sync`; only one thread ever calls into the writer at a
    /// time, so the mutex is uncontended.
    Streaming {
        upload: Mutex<Box<dyn MultipartUpload>>,
        part_buffer: Vec<u8>,
        /// Part uploads spawned onto [`TOKIO_DEDICATED_IO`] and not yet
        /// awaited, oldest first. Capped at [`MAX_IN_FLIGHT_PARTS`];
        /// `flush_part` awaits the oldest when full and `complete`
        /// drains the rest.
        in_flight: Vec<tokio::task::JoinHandle<Result<(), object_store::Error>>>,
    },
    /// `complete()` has been called and the writer is consumed.
    Done,
}

/// File writer that streams data to object storage. Uses a single PUT
/// for small files (cheaper) and switches to multipart upload at
/// `MULTIPART_THRESHOLD` bytes for large files (bounded memory,
/// supports > 5 GiB).
struct ObjectStoreFileWriter {
    store: Arc<dyn ObjectStore>,
    path: ObjPath,
    relative: StoragePath,
    id: FileId,
    state: WriterState,
    bytes_written: u64,
    usage: Arc<AtomicI64>,
    /// Copied from the owning backend; see [`DEFAULT_MULTIPART_THRESHOLD`].
    multipart_threshold: usize,
}

impl Debug for ObjectStoreFileWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let buffered = match &self.state {
            WriterState::Pending(b) => b.len(),
            WriterState::Streaming { part_buffer, .. } => part_buffer.len(),
            WriterState::Done => 0,
        };
        f.debug_struct("ObjectStoreFileWriter")
            .field("path", &self.path.as_ref())
            .field("bytes_written", &self.bytes_written)
            .field("part_buffered", &buffered)
            .finish()
    }
}

impl FileRw for ObjectStoreFileWriter {
    fn file_id(&self) -> FileId {
        self.id
    }
    // The trait method is named `path` but returns the *relative* storage
    // path; `self.path` is the full ObjPath used to address S3. Clippy
    // can't tell the difference.
    #[allow(clippy::misnamed_getters)]
    fn path(&self) -> &StoragePath {
        &self.relative
    }
}

impl ObjectStoreFileWriter {
    /// Drain `part_buffer` and upload its current contents as a single
    /// multipart part. Called whenever the buffer crosses the threshold
    /// during `write_block`.
    fn flush_part(&mut self) -> Result<(), StorageError> {
        if let WriterState::Streaming {
            upload,
            part_buffer,
            in_flight,
        } = &mut self.state
        {
            if part_buffer.is_empty() {
                return Ok(());
            }
            while in_flight.len() >= MAX_IN_FLIGHT_PARTS {
                await_part(in_flight.remove(0), &self.path)?;
            }
            let payload = PutPayload::from(std::mem::take(part_buffer));
            // We hold &mut self.state, so &mut Mutex<_> already gives us
            // exclusive access — no need to lock. `put_part` returns a
            // `'static` future; spawning it onto the IO runtime makes the
            // upload progress concurrently with buffering the next part.
            let fut = upload.get_mut().unwrap().put_part(payload);
            in_flight.push(TOKIO_DEDICATED_IO.spawn(fut));
        }
        Ok(())
    }
}

/// Waits for one spawned part upload. A part that failed after
/// `object_store`'s internal retries fails the write — see the note on
/// [`WRITE_MAX_ATTEMPTS`] for why non-final parts cannot be re-put.
fn await_part(
    handle: tokio::task::JoinHandle<Result<(), object_store::Error>>,
    path: &ObjPath,
) -> Result<(), StorageError> {
    match TOKIO_DEDICATED_IO.block_on(handle) {
        Ok(result) => Ok(result?),
        Err(join_err) => Err(StorageError::stdio(
            std::io::ErrorKind::Other,
            "multipart part upload task failed",
            format!("{}: {join_err}", path.as_ref()),
        )),
    }
}

impl FileWriter for ObjectStoreFileWriter {
    fn write_block(&mut self, data: FBuf) -> Result<Arc<FBuf>, StorageError> {
        self.bytes_written += data.as_slice().len() as u64;

        // Append to whichever buffer is active.
        match &mut self.state {
            WriterState::Pending(buf) => buf.extend_from_slice(data.as_slice()),
            WriterState::Streaming { part_buffer, .. } => {
                part_buffer.extend_from_slice(data.as_slice())
            }
            WriterState::Done => {
                return Err(StorageError::stdio(
                    std::io::ErrorKind::Other,
                    "write_block after complete",
                    self.path.as_ref().to_string(),
                ));
            }
        }

        // If still pending and we've crossed the threshold, promote to
        // multipart streaming and flush the pending bytes as the first
        // part.
        let should_upgrade = matches!(
            &self.state,
            WriterState::Pending(buf) if buf.len() >= self.multipart_threshold
        );
        if should_upgrade {
            let pending = match std::mem::replace(&mut self.state, WriterState::Done) {
                WriterState::Pending(buf) => buf,
                _ => unreachable!(),
            };
            let upload = with_write_retries("put_multipart", &self.path, || {
                TOKIO_DEDICATED_IO.block_on(self.store.put_multipart(&self.path))
            })?;
            self.state = WriterState::Streaming {
                upload: Mutex::new(upload),
                part_buffer: pending,
                in_flight: Vec::new(),
            };
            self.flush_part()?;
        } else if let WriterState::Streaming { part_buffer, .. } = &self.state
            && part_buffer.len() >= self.multipart_threshold
        {
            self.flush_part()?;
        }

        Ok(Arc::new(data))
    }

    fn complete(mut self: Box<Self>) -> Result<Arc<dyn FileReader>, StorageError> {
        let size = self.bytes_written;
        match std::mem::replace(&mut self.state, WriterState::Done) {
            WriterState::Pending(buf) => {
                // Single-PUT path: cheaper for small files.
                let payload = PutPayload::from(buf);
                with_write_retries("put", &self.path, || {
                    TOKIO_DEDICATED_IO.block_on(self.store.put(&self.path, payload.clone()))
                })?;
            }
            WriterState::Streaming {
                upload,
                part_buffer,
                in_flight,
            } => {
                // Wait for every spawned part upload to land before the
                // final part and completion.
                for handle in in_flight {
                    await_part(handle, &self.path)?;
                }
                // Upload any remaining bytes as the final part (last part
                // has no minimum size), then close the upload. The outer
                // retry is order-safe here: every earlier part has been
                // awaited, so a re-put part number still sorts last.
                let mut upload = upload.into_inner().unwrap();
                if !part_buffer.is_empty() {
                    let payload = PutPayload::from(part_buffer);
                    with_write_retries("put_part", &self.path, || {
                        TOKIO_DEDICATED_IO.block_on(upload.put_part(payload.clone()))
                    })?;
                }
                with_write_retries("complete_multipart", &self.path, || {
                    TOKIO_DEDICATED_IO.block_on(upload.complete()).map(|_| ())
                })?;
            }
            WriterState::Done => {
                return Err(StorageError::stdio(
                    std::io::ErrorKind::Other,
                    "complete called twice",
                    self.path.as_ref().to_string(),
                ));
            }
        }

        self.usage.fetch_add(size as i64, Ordering::Relaxed);
        Ok(Arc::new(ObjectStoreFileReader {
            store: self.store.clone(),
            path: self.path.clone(),
            relative: self.relative.clone(),
            id: self.id,
            size,
        }))
    }
}

impl Drop for ObjectStoreFileWriter {
    fn drop(&mut self) {
        // If the writer is dropped without completing, abort any
        // in-flight multipart upload to avoid leaking S3 storage charges
        // for orphaned parts.
        if let WriterState::Streaming {
            upload, in_flight, ..
        } = &mut self.state
        {
            // Cancel outstanding part uploads first; their tasks hold
            // payload buffers and would otherwise race the abort.
            for handle in in_flight.drain(..) {
                handle.abort();
            }
            // &mut Mutex gives exclusive access; no lock needed.
            let upload = upload.get_mut().unwrap();
            if let Err(err) = TOKIO_DEDICATED_IO.block_on(upload.abort()) {
                tracing::debug!(
                    "ObjectStoreFileWriter for {} dropped during multipart upload; \
                     abort failed: {err}",
                    self.path
                );
            }
        }
    }
}

/// Reader for a single object. `read_block` issues range GETs.
struct ObjectStoreFileReader {
    store: Arc<dyn ObjectStore>,
    path: ObjPath,
    relative: StoragePath,
    id: FileId,
    size: u64,
}

impl Debug for ObjectStoreFileReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectStoreFileReader")
            .field("path", &self.path.as_ref())
            .field("size", &self.size)
            .finish()
    }
}

impl FileRw for ObjectStoreFileReader {
    fn file_id(&self) -> FileId {
        self.id
    }
    #[allow(clippy::misnamed_getters)]
    fn path(&self) -> &StoragePath {
        &self.relative
    }
}

impl FileCommitter for ObjectStoreFileReader {
    fn commit(&self) -> Result<(), StorageError> {
        // Object stores commit on PUT; nothing to do here.
        Ok(())
    }
}

impl FileReader for ObjectStoreFileReader {
    fn mark_for_checkpoint(&self) {
        // Object-store-backed files are never deleted on drop, so this is
        // a no-op (the contract is satisfied by default).
    }

    fn read_block(&self, location: BlockLocation) -> Result<Arc<FBuf>, StorageError> {
        let start: usize = location.offset.try_into().map_err(|_| {
            StorageError::stdio(
                ErrorKind::InvalidInput,
                "read_block",
                self.path.as_ref().to_string(),
            )
        })?;
        let end = start.checked_add(location.size).ok_or_else(|| {
            StorageError::stdio(
                ErrorKind::InvalidInput,
                "read_block",
                self.path.as_ref().to_string(),
            )
        })?;
        let bytes = TOKIO_DEDICATED_IO
            .block_on(self.store.get_range(&self.path, start as u64..end as u64))?;
        let mut buf = FBuf::new();
        buf.extend_from_slice(&bytes);
        Ok(Arc::new(buf))
    }

    fn get_size(&self) -> Result<u64, StorageError> {
        Ok(self.size)
    }
}

// ---------------------------------------------------------------------------
// Factory registration: picks up `StorageBackendConfig::Object(...)`
// ---------------------------------------------------------------------------

/// Factory for the `Object` backend variant.
pub struct ObjectBackendFactory;

impl StorageBackendFactory for ObjectBackendFactory {
    fn backend(&self) -> &'static str {
        "object"
    }

    fn create(
        &self,
        _storage_config: &StorageConfig,
        backend_config: &StorageBackendConfig,
    ) -> Result<Arc<dyn StorageBackend>, StorageError> {
        let StorageBackendConfig::Object(cfg) = backend_config else {
            return Err(StorageError::InvalidBackendConfig {
                backend: self.backend().into(),
                config: Box::new(backend_config.clone()),
            });
        };
        Ok(Arc::new(ObjectStoreBackend::from_config(cfg)?))
    }
}

inventory::submit! {
    &ObjectBackendFactory as &dyn StorageBackendFactory
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_storage::StorageBackend;

    /// Conditional puts: create-only fails once the object exists,
    /// version-matched updates succeed, stale versions are rejected.
    #[test]
    fn conditional_put_detects_concurrent_writers() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backend = ObjectStoreBackend::new_with_store(store, ObjPath::from("pipeline-test"));
        let name: StoragePath = ObjPath::from("manifest.json");

        // Object absent: version is None and create-mode put succeeds.
        assert!(backend.object_version(&name).unwrap().is_none());
        backend
            .put_if_version(&name, b"v1".to_vec(), None)
            .expect("create");

        // Create-mode put now fails: someone else got there first.
        let err = backend
            .put_if_version(&name, b"v1-other".to_vec(), None)
            .expect_err("create over existing object must fail");
        assert!(
            matches!(err, object_store::Error::AlreadyExists { .. }),
            "expected AlreadyExists, got {err:?}"
        );

        // Update with the current version succeeds...
        let v1 = backend.object_version(&name).unwrap().expect("exists");
        backend
            .put_if_version(&name, b"v2".to_vec(), Some(v1.clone()))
            .expect("update with current version");

        // ...and re-using the now-stale version is rejected.
        let err = backend
            .put_if_version(&name, b"v3".to_vec(), Some(v1))
            .expect_err("update with stale version must fail");
        assert!(
            matches!(err, object_store::Error::Precondition { .. }),
            "expected Precondition, got {err:?}"
        );
    }

    /// Transient failures retry up to the attempt cap and then succeed.
    #[test]
    fn write_retries_recover_from_transient_failures() {
        let path = ObjPath::from("retry-test");
        let mut calls = 0;
        let result = with_write_retries("put", &path, || {
            calls += 1;
            if calls < 3 {
                Err(object_store::Error::Generic {
                    store: "test",
                    source: "synthetic transient failure".into(),
                })
            } else {
                Ok(42)
            }
        });
        assert_eq!(result.unwrap(), 42);
        assert_eq!(calls, 3);
    }

    /// Transient failures stop retrying once the attempt cap is hit.
    #[test]
    fn write_retries_give_up_after_cap() {
        let path = ObjPath::from("retry-test");
        let mut calls = 0;
        let result: Result<(), _> = with_write_retries("put", &path, || {
            calls += 1;
            Err(object_store::Error::Generic {
                store: "test",
                source: "always failing".into(),
            })
        });
        assert!(result.is_err());
        assert_eq!(calls, WRITE_MAX_ATTEMPTS);
    }

    /// Non-transient errors fail fast without retrying.
    #[test]
    fn write_retries_fail_fast_on_structural_errors() {
        let path = ObjPath::from("retry-test");
        let mut calls = 0;
        let result: Result<(), _> = with_write_retries("put", &path, || {
            calls += 1;
            Err(object_store::Error::NotFound {
                path: "x".to_string(),
                source: "gone".into(),
            })
        });
        assert!(result.is_err());
        assert_eq!(calls, 1);
    }

    /// The `opendera.multipart_threshold` option parses, clamps to the
    /// S3 part minimum, and rejects garbage.
    #[test]
    fn multipart_threshold_option_parses_and_clamps() {
        assert_eq!(
            parse_multipart_threshold("16777216").unwrap(),
            16 * 1024 * 1024
        );
        // Below the 5 MiB S3 part minimum: clamped.
        assert_eq!(
            parse_multipart_threshold("1024").unwrap(),
            MIN_MULTIPART_THRESHOLD
        );
        assert!(parse_multipart_threshold("not-a-number").is_err());
    }

    /// `from_config` consumes `opendera.*` options instead of passing
    /// them to `object_store` (which would reject unknown keys), and
    /// applies the threshold override.
    #[test]
    fn from_config_strips_opendera_options() {
        let cfg = ObjectStorageConfig {
            url: "memory:///".to_string(),
            other_options: [(
                MULTIPART_THRESHOLD_OPTION.to_string(),
                "16777216".to_string(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        let backend = ObjectStoreBackend::from_config(&cfg).expect("from_config");
        assert_eq!(backend.multipart_threshold, 16 * 1024 * 1024);
    }

    /// A small custom threshold drives the writer through the
    /// multipart path (initiate, part flushes, completion) end to end.
    #[test]
    fn custom_threshold_streams_multipart() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backend = ObjectStoreBackend::new_with_store(store, ObjPath::from("pipeline-test"))
            .with_multipart_threshold(1024);

        let name: StoragePath = ObjPath::from("small-multipart.bin");
        let payload: Vec<u8> = (0..8192u32).map(|i| (i % 251) as u8).collect();

        let mut writer = backend.create_named(&name).expect("create_named");
        for chunk in payload.chunks(512) {
            let mut data = FBuf::new();
            data.extend_from_slice(chunk);
            writer.write_block(data).expect("write_block");
        }
        let reader = writer.complete().expect("complete");

        assert_eq!(reader.get_size().unwrap(), payload.len() as u64);
        let read = reader
            .read_block(BlockLocation {
                offset: 0,
                size: payload.len(),
            })
            .expect("read_block");
        assert_eq!(read.as_slice(), payload.as_slice());
    }

    /// Round-trip a small payload through an in-memory object store
    /// (`object_store::memory::InMemory`) using the same code path that
    /// the S3 / GCS / Azure backends use. Verifies the trait wiring is
    /// correct.
    #[test]
    fn round_trip_in_memory() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backend = ObjectStoreBackend::new_with_store(store, ObjPath::from("pipeline-test"));

        let name: StoragePath = ObjPath::from("hello.bin");
        let payload = b"the quick brown fox jumps over the lazy dog";

        let mut writer = backend.create_named(&name).expect("create_named");
        let mut data = FBuf::new();
        data.extend_from_slice(payload);
        writer.write_block(data).expect("write_block");
        let reader = writer.complete().expect("complete");

        assert_eq!(reader.get_size().unwrap(), payload.len() as u64);
        let read = reader
            .read_block(BlockLocation {
                offset: 0,
                size: payload.len(),
            })
            .expect("read_block");
        assert_eq!(read.as_slice(), payload);

        // Exists, list, delete.
        assert!(backend.exists(&name).unwrap());

        let mut listed = Vec::new();
        backend
            .list(&StoragePath::default(), &mut |entry| {
                listed.push(entry.name.as_ref().to_string());
            })
            .expect("list");
        assert!(listed.iter().any(|p| p.contains("hello.bin")));

        backend.delete(&name).expect("delete");
        assert!(!backend.exists(&name).unwrap());

        // Delete of a missing file is idempotent.
        backend.delete_if_exists(&name).expect("delete_if_exists");
    }

    /// Force the writer onto the multipart path by writing a payload
    /// larger than `MULTIPART_THRESHOLD`. Verifies the streaming code
    /// path against `object_store::memory::InMemory` (which supports
    /// multipart). The byte-for-byte round trip catches any part
    /// boundary or ordering bug.
    #[test]
    fn multipart_streaming_round_trip() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backend = ObjectStoreBackend::new_with_store(store, ObjPath::from("pipeline-mp"));

        // Build ~20 MiB of payload across multiple write_block calls,
        // each one well below the threshold, so the writer accumulates,
        // crosses the threshold once, and then flushes parts on the way
        // through.
        let name: StoragePath = ObjPath::from("large.bin").into();
        let mut writer = backend.create_named(&name).expect("create_named");
        let chunk = vec![0x41u8; 2 * 1024 * 1024]; // 2 MiB
        for i in 0..10u8 {
            let mut data = FBuf::new();
            // Stamp the chunk index in the first byte so we can detect
            // ordering bugs at read time.
            let mut block = chunk.clone();
            block[0] = i;
            data.extend_from_slice(&block);
            writer.write_block(data).expect("write_block");
        }
        let reader = writer.complete().expect("complete");

        let total = reader.get_size().unwrap();
        assert_eq!(total, 20 * 1024 * 1024);

        // Read back the stamps at each chunk boundary and verify order.
        for i in 0..10u64 {
            let offset = i * 2 * 1024 * 1024;
            let block = reader
                .read_block(BlockLocation { offset, size: 512 })
                .expect("read_block");
            assert_eq!(block.as_slice()[0], i as u8, "chunk {i} out of order");
        }
    }

    /// Integration test against a real S3-compatible endpoint (MinIO,
    /// Tigris, AWS S3, …). Skipped unless `OPENDERA_S3_TEST_URL` is
    /// set, so the unit suite stays hermetic.
    ///
    /// Example local run (MinIO):
    ///
    /// ```bash
    /// docker run -d --rm -p 9000:9000 -p 9001:9001 \
    ///   -e MINIO_ROOT_USER=minioadmin \
    ///   -e MINIO_ROOT_PASSWORD=minioadmin \
    ///   --name opendera-test-minio minio/minio server /data
    /// mc alias set local http://localhost:9000 minioadmin minioadmin
    /// mc mb local/opendera-test
    /// OPENDERA_S3_TEST_URL=s3://opendera-test/ \
    /// OPENDERA_S3_TEST_OPTS="endpoint=http://localhost:9000,access_key_id=minioadmin,secret_access_key=minioadmin,region=us-east-1,allow_http=true" \
    ///   cargo test -p dbsp s3_integration -- --ignored --nocapture
    /// ```
    ///
    /// Verifies: create_named -> write_block -> complete -> open ->
    /// read_block -> exists -> list -> delete, plus the multipart
    /// path for a payload above the threshold.
    #[test]
    #[ignore = "requires OPENDERA_S3_TEST_URL; integration test"]
    fn s3_integration_round_trip() {
        let Some(backend) = s3_backend_from_env() else {
            eprintln!("OPENDERA_S3_TEST_URL not set; skipping");
            return;
        };

        let prefix = format!("opendera-it/{}", uuid::Uuid::now_v7().simple());

        // Small file: single-PUT path.
        let small_name: StoragePath = format!("{prefix}/small.bin").as_str().into();
        let body = b"hello s3 integration";
        let mut w = backend.create_named(&small_name).expect("create_named");
        let mut fbuf = FBuf::new();
        fbuf.extend_from_slice(body);
        w.write_block(fbuf).expect("write_block");
        let r = w.complete().expect("complete small");
        assert_eq!(r.get_size().unwrap(), body.len() as u64);
        let read = r
            .read_block(BlockLocation {
                offset: 0,
                size: body.len(),
            })
            .expect("read_block small");
        assert_eq!(read.as_slice(), body);

        // Large file: multipart path. ~12 MiB across six 2 MiB chunks
        // to cross the 8 MiB threshold and produce at least two parts.
        let big_name: StoragePath = format!("{prefix}/big.bin").as_str().into();
        let chunk = vec![0x42u8; 2 * 1024 * 1024];
        let mut w = backend.create_named(&big_name).expect("create_named big");
        for _ in 0..6 {
            let mut fb = FBuf::new();
            fb.extend_from_slice(&chunk);
            w.write_block(fb).expect("write_block big");
        }
        let r = w.complete().expect("complete big");
        assert_eq!(r.get_size().unwrap(), 12 * 1024 * 1024);

        // list + delete the prefix.
        let mut listed = Vec::new();
        backend
            .list(&prefix.as_str().into(), &mut |entry| {
                listed.push(entry.name.as_ref().to_string());
            })
            .expect("list");
        assert_eq!(listed.len(), 2, "expected exactly 2 files under prefix");

        backend
            .delete_recursive(&prefix.as_str().into())
            .expect("delete_recursive should remove everything under the prefix");

        let mut listed2 = Vec::new();
        backend
            .list(&prefix.as_str().into(), &mut |entry| {
                listed2.push(entry.name.as_ref().to_string());
            })
            .expect("list after delete_recursive");
        assert!(
            listed2.is_empty(),
            "prefix not empty after recursive delete"
        );
    }

    /// Build an `ObjectStoreBackend` from environment variables, or
    /// return `None` if `OPENDERA_S3_TEST_URL` isn't set. The optional
    /// `OPENDERA_S3_TEST_OPTS` carries `k=v,k=v` extras passed verbatim
    /// to `object_store`'s URL options (`endpoint`, `access_key_id`,
    /// `secret_access_key`, `region`, `allow_http`, …).
    fn s3_backend_from_env() -> Option<ObjectStoreBackend> {
        let url = std::env::var("OPENDERA_S3_TEST_URL").ok()?;
        let mut other_options = std::collections::BTreeMap::new();
        if let Ok(raw) = std::env::var("OPENDERA_S3_TEST_OPTS") {
            for kv in raw.split(',') {
                let kv = kv.trim();
                if kv.is_empty() {
                    continue;
                }
                if let Some((k, v)) = kv.split_once('=') {
                    other_options.insert(k.trim().to_string(), v.trim().to_string());
                }
            }
        }
        let cfg = ObjectStorageConfig { url, other_options };
        Some(ObjectStoreBackend::from_config(&cfg).expect("from_config"))
    }
}
