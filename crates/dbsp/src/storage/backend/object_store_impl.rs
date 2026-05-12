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
use std::sync::Arc;
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
use object_store::{ObjectStore, PutPayload, parse_url_opts};
use url::Url;

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
            buffer: Vec::new(),
            usage: self.usage.clone(),
            completed: false,
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

/// File writer that buffers all writes in memory and uploads as a single
/// PUT on `complete()`. Multipart uploads for large files are a TODO.
struct ObjectStoreFileWriter {
    store: Arc<dyn ObjectStore>,
    path: ObjPath,
    relative: StoragePath,
    id: FileId,
    buffer: Vec<u8>,
    usage: Arc<AtomicI64>,
    completed: bool,
}

impl Debug for ObjectStoreFileWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectStoreFileWriter")
            .field("path", &self.path.as_ref())
            .field("buffered_bytes", &self.buffer.len())
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

impl FileWriter for ObjectStoreFileWriter {
    fn write_block(&mut self, data: FBuf) -> Result<Arc<FBuf>, StorageError> {
        self.buffer.extend_from_slice(data.as_slice());
        Ok(Arc::new(data))
    }

    fn complete(mut self: Box<Self>) -> Result<Arc<dyn FileReader>, StorageError> {
        let buf = std::mem::take(&mut self.buffer);
        let size = buf.len() as u64;
        let payload = PutPayload::from(buf);
        TOKIO_DEDICATED_IO.block_on(self.store.put(&self.path, payload))?;
        self.completed = true;
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
        // If the writer was dropped without completing, no remote object
        // exists yet, so nothing to delete. The buffered bytes will be
        // freed normally.
        if !self.completed {
            tracing::debug!(
                "ObjectStoreFileWriter for {} dropped without complete; \
                 nothing was uploaded",
                self.path
            );
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
}
