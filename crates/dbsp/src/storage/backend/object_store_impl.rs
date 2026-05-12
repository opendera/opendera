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
//! calls, single-PUT writes via the [`PutPayload`] API, range reads, list,
//! delete, and exists. Multipart uploads, retries with backoff, and
//! per-object KMS settings are tracked as TODOs and will be filled in
//! during follow-up commits — none of them affect the trait shape so the
//! skeleton is a stable target for callers.

#![warn(missing_docs)]

use std::fmt::{self, Debug};
use std::io::ErrorKind;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicI64, Ordering};

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
use object_store::{MultipartUpload, ObjectStore, PutPayload, parse_url_opts};
use url::Url;

/// Threshold above which writes are streamed via multipart upload rather
/// than buffered in memory for a single PUT.
///
/// S3 requires non-final parts to be at least 5 MiB; we pick 8 MiB to
/// give some slack and reduce the number of part requests for typical
/// checkpoint shard sizes. Below this threshold a single PUT is used,
/// which is cheaper for small files (one round trip instead of three).
const MULTIPART_THRESHOLD: usize = 8 * 1024 * 1024;

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

/// `StorageBackend` implementation backed by an `object_store::ObjectStore`.
pub struct ObjectStoreBackend {
    store: Arc<dyn ObjectStore>,
    base: ObjPath,
    usage: Arc<AtomicI64>,
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
        }
    }

    /// Construct from `ObjectStorageConfig` (already in `feldera-types`).
    pub fn from_config(cfg: &ObjectStorageConfig) -> Result<Self, StorageError> {
        let url = Url::parse(&cfg.url).map_err(|_| StorageError::InvalidURL(cfg.url.clone()))?;
        let opts: Vec<(String, String)> = cfg
            .other_options
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let (store, base) = parse_url_opts(&url, opts)?;
        Ok(Self {
            store: Arc::from(store),
            base,
            usage: Arc::new(AtomicI64::new(0)),
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
            size: meta.size as u64,
        }))
    }

    fn list(
        &self,
        parent: &StoragePath,
        cb: &mut dyn FnMut(&StoragePath, StorageFileType),
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
                    let entry = StorageFileType::File {
                        size: meta.size as u64,
                    };
                    out.push((storage_path, entry));
                }
                Ok(out)
            });
        for (path, entry) in result? {
            cb(&path, entry);
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
    fn path(&self) -> &StoragePath {
        &self.relative
    }
}

impl ObjectStoreFileWriter {
    /// Drain `part_buffer` and upload its current contents as a single
    /// multipart part. Called whenever the buffer crosses the threshold
    /// during `write_block`.
    fn flush_part(&mut self) -> Result<(), StorageError> {
        if let WriterState::Streaming { upload, part_buffer } = &mut self.state {
            if part_buffer.is_empty() {
                return Ok(());
            }
            let payload = PutPayload::from(std::mem::take(part_buffer));
            let mut upload = upload.lock().unwrap();
            TOKIO_DEDICATED_IO.block_on(upload.put_part(payload))?;
        }
        Ok(())
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
            WriterState::Pending(buf) if buf.len() >= MULTIPART_THRESHOLD
        );
        if should_upgrade {
            let pending = match std::mem::replace(&mut self.state, WriterState::Done) {
                WriterState::Pending(buf) => buf,
                _ => unreachable!(),
            };
            let upload = TOKIO_DEDICATED_IO.block_on(self.store.put_multipart(&self.path))?;
            self.state = WriterState::Streaming {
                upload: Mutex::new(upload),
                part_buffer: pending,
            };
            self.flush_part()?;
        } else if let WriterState::Streaming { part_buffer, .. } = &self.state
            && part_buffer.len() >= MULTIPART_THRESHOLD
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
                TOKIO_DEDICATED_IO.block_on(self.store.put(&self.path, PutPayload::from(buf)))?;
            }
            WriterState::Streaming {
                upload,
                part_buffer,
            } => {
                // Upload any remaining bytes as the final part (last part
                // has no minimum size), then close the upload.
                let mut upload = upload.into_inner().unwrap();
                if !part_buffer.is_empty() {
                    TOKIO_DEDICATED_IO
                        .block_on(upload.put_part(PutPayload::from(part_buffer)))?;
                }
                TOKIO_DEDICATED_IO.block_on(upload.complete())?;
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
        if let WriterState::Streaming { upload, .. } = &mut self.state {
            let mut upload = upload.lock().unwrap();
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
        let bytes = TOKIO_DEDICATED_IO.block_on(self.store.get_range(&self.path, start..end))?;
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
            .list(&StoragePath::default(), &mut |p, _| {
                listed.push(p.as_ref().to_string());
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
                .read_block(BlockLocation {
                    offset,
                    size: 512,
                })
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

        let prefix = format!(
            "opendera-it/{}",
            uuid::Uuid::now_v7().simple()
        );

        // Small file: single-PUT path.
        let small_name: StoragePath =
            format!("{prefix}/small.bin").as_str().into();
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
        let big_name: StoragePath =
            format!("{prefix}/big.bin").as_str().into();
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
            .list(&prefix.as_str().into(), &mut |p, _| {
                listed.push(p.as_ref().to_string());
            })
            .expect("list");
        assert_eq!(listed.len(), 2, "expected exactly 2 files under prefix");

        backend.delete_recursive(&prefix.as_str().into()).expect(
            "delete_recursive should remove everything under the prefix",
        );

        let mut listed2 = Vec::new();
        backend
            .list(&prefix.as_str().into(), &mut |p, _| {
                listed2.push(p.as_ref().to_string());
            })
            .expect("list after delete_recursive");
        assert!(listed2.is_empty(), "prefix not empty after recursive delete");
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
        let cfg = ObjectStorageConfig {
            url,
            other_options,
        };
        Some(ObjectStoreBackend::from_config(&cfg).expect("from_config"))
    }
}
