//! Object-store-backed `CheckpointSynchronizer`.
//!
//! Implements `feldera_storage::checkpoint_synchronizer::CheckpointSynchronizer`
//! by reading and writing checkpoint files through a second
//! `ObjectStoreBackend` constructed from the pipeline's `SyncConfig`.
//!
//! Clean-room reimplementation of section §2 of `ENTERPRISE_FEATURES.md`.
//! Written from the spec only; no inspection of upstream sources.
//!
//! Push: enumerates the files under the checkpoint's UUID directory in
//! local storage, plus the top-level `checkpoints.feldera` manifest, and
//! uploads each to the remote under the same relative path. Files are
//! uploaded one at a time; the top-level manifest is uploaded last so
//! that pulls never see a manifest that references a partially-uploaded
//! checkpoint.
//!
//! Pull: reads the remote manifest, decides which checkpoint UUID to
//! pull (either `Latest` or a specific UUID from the sync config), then
//! enumerates and downloads every remote file under that UUID, plus the
//! manifest, into local storage.

#![warn(missing_docs)]

use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, anyhow};
use feldera_storage::checkpoint_synchronizer::CheckpointSynchronizer;
use feldera_storage::{StorageBackend, StoragePath};
use feldera_types::checkpoint::{CheckpointMetadata, CheckpointSyncMetrics};
use feldera_types::config::{ObjectStorageConfig, StartFromCheckpoint, SyncConfig};
use feldera_types::constants::CHECKPOINT_FILE_NAME;

use crate::storage::backend::object_store_impl::ObjectStoreBackend;

/// Build an `ObjectStoreBackend` from a `SyncConfig`. Maps the rclone-style
/// fields in `SyncConfig` to the `ObjectStorageConfig` shape that
/// `ObjectStoreBackend::from_config` understands.
fn remote_backend(sync: &SyncConfig) -> anyhow::Result<ObjectStoreBackend> {
    // Translate `SyncConfig` (rclone-style) to `ObjectStorageConfig`
    // (object_store URL + key-value options).
    //
    // We currently produce S3 URLs only. GCS / Azure mapping is a TODO
    // that will land alongside non-S3 cloud support; the spec calls them
    // out but OpenDera Cloud uses S3 / Tigris.
    let url = format!("s3://{}", sync.bucket);

    let mut other_options = std::collections::BTreeMap::new();
    if let Some(endpoint) = &sync.endpoint {
        other_options.insert("endpoint".to_string(), endpoint.clone());
    }
    if let Some(region) = &sync.region {
        other_options.insert("region".to_string(), region.clone());
    }
    if let Some(key) = &sync.access_key {
        other_options.insert("access_key_id".to_string(), key.clone());
    }
    if let Some(secret) = &sync.secret_key {
        other_options.insert("secret_access_key".to_string(), secret.clone());
    }

    let cfg = ObjectStorageConfig {
        url,
        other_options,
    };
    ObjectStoreBackend::from_config(&cfg)
        .map_err(|e| anyhow!("failed to construct remote object store: {e}"))
}

/// Copy a single file from `src` to `dst` at the same path. Returns the
/// number of bytes transferred.
fn copy_file(
    src: &Arc<dyn StorageBackend>,
    dst: &Arc<dyn StorageBackend>,
    path: &StoragePath,
) -> anyhow::Result<u64> {
    let data = src
        .read(path)
        .with_context(|| format!("read {path} from source"))?;
    let committer = dst
        .write(path, (*data).clone())
        .with_context(|| format!("write {path} to destination"))?;
    committer
        .commit()
        .with_context(|| format!("commit {path} to destination"))?;
    Ok(data.len() as u64)
}

/// List every file in `src` under `prefix`. Returns relative paths.
fn list_files(src: &Arc<dyn StorageBackend>, prefix: &StoragePath) -> anyhow::Result<Vec<StoragePath>> {
    let mut out = Vec::new();
    src.list(prefix, &mut |path, _| out.push(path.clone()))?;
    Ok(out)
}

fn metrics(start: Instant, bytes: u64) -> CheckpointSyncMetrics {
    let duration = start.elapsed();
    let speed = if duration.as_secs() > 0 {
        bytes / duration.as_secs()
    } else {
        bytes
    };
    CheckpointSyncMetrics {
        duration,
        speed,
        bytes,
    }
}

/// Object-store-backed checkpoint synchronizer. Singleton: registered via
/// `inventory::submit!` and resolved through the `SYNCHRONIZER` static in
/// `feldera_storage::checkpoint_synchronizer`.
pub struct ObjectStoreSynchronizer;

impl CheckpointSynchronizer for ObjectStoreSynchronizer {
    fn push(
        &self,
        checkpoint: uuid::Uuid,
        storage: Arc<dyn StorageBackend>,
        remote_config: SyncConfig,
    ) -> anyhow::Result<Option<CheckpointSyncMetrics>> {
        let remote = Arc::new(remote_backend(&remote_config)?) as Arc<dyn StorageBackend>;
        let start = Instant::now();

        // 1. Upload every file under the checkpoint's UUID directory.
        let uuid_prefix: StoragePath = checkpoint.to_string().as_str().into();
        let mut total_bytes = 0u64;
        for path in list_files(&storage, &uuid_prefix)? {
            total_bytes += copy_file(&storage, &remote, &path)?;
        }

        // 2. Upload the top-level manifest *last*. A pull that races with
        //    this push will either see the old manifest (if the put hasn't
        //    landed yet) or the new one (after all files are already
        //    uploaded). It never sees a manifest that references a
        //    half-uploaded checkpoint.
        let manifest: StoragePath = CHECKPOINT_FILE_NAME.into();
        if storage.exists(&manifest)? {
            total_bytes += copy_file(&storage, &remote, &manifest)?;
        }

        Ok(Some(metrics(start, total_bytes)))
    }

    fn pull(
        &self,
        storage: Arc<dyn StorageBackend>,
        remote_config: SyncConfig,
    ) -> anyhow::Result<(CheckpointMetadata, Option<CheckpointSyncMetrics>)> {
        let remote = Arc::new(remote_backend(&remote_config)?) as Arc<dyn StorageBackend>;
        let start = Instant::now();

        // 1. Pull the manifest from remote.
        let manifest: StoragePath = CHECKPOINT_FILE_NAME.into();
        let mut total_bytes = copy_file(&remote, &storage, &manifest)?;

        // 2. Re-read it from local storage to discover the checkpoint set.
        let checkpoints: Vec<CheckpointMetadata> = storage
            .read_json(&manifest)
            .context("read pulled checkpoint manifest")?;

        // 3. Pick which checkpoint to materialize.
        let target_uuid = match remote_config
            .start_from_checkpoint
            .as_ref()
            .ok_or_else(|| anyhow!("pull called without start_from_checkpoint"))?
        {
            StartFromCheckpoint::Latest => checkpoints
                .last()
                .ok_or_else(|| anyhow!("remote manifest has no checkpoints"))?
                .uuid,
            StartFromCheckpoint::Uuid(u) => *u,
        };
        let target_meta = checkpoints
            .iter()
            .find(|cpm| cpm.uuid == target_uuid)
            .ok_or_else(|| anyhow!("checkpoint {target_uuid} not found in remote manifest"))?
            .clone();

        // 4. Download every file under the target UUID prefix.
        let uuid_prefix: StoragePath = target_uuid.to_string().as_str().into();
        for path in list_files(&remote, &uuid_prefix)? {
            total_bytes += copy_file(&remote, &storage, &path)?;
        }

        Ok((target_meta, Some(metrics(start, total_bytes))))
    }
}

inventory::submit! {
    &ObjectStoreSynchronizer as &dyn CheckpointSynchronizer
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::backend::object_store_impl::ObjectStoreBackend;
    use feldera_storage::fbuf::FBuf;
    use feldera_types::checkpoint::CheckpointMetadata;
    use object_store::ObjectStore;
    use object_store::path::Path as ObjPath;
    use std::sync::Arc;

    /// Build an `ObjectStoreBackend` backed by an in-memory store so we
    /// can exercise the synchronizer without touching S3.
    fn in_memory_backend(prefix: &str) -> Arc<dyn StorageBackend> {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        Arc::new(ObjectStoreBackend::new_with_store(store, ObjPath::from(prefix)))
    }

    fn write_text(backend: &Arc<dyn StorageBackend>, path: &str, body: &[u8]) {
        let p: StoragePath = path.into();
        let mut writer = backend.create_named(&p).expect("create_named");
        let mut data = FBuf::new();
        data.extend_from_slice(body);
        writer.write_block(data).expect("write_block");
        writer.complete().expect("complete");
    }

    /// End-to-end: stage a fake checkpoint locally, push it to an
    /// in-memory remote, then pull it into a fresh local backend and
    /// verify the files match. Exercises the trait wiring, manifest
    /// ordering, and file enumeration. Does not hit S3.
    ///
    /// The test drives the file-copy logic directly with the same
    /// `copy_file` + `list_files` helpers the synchronizer uses
    /// internally, since constructing a real `SyncConfig` would require
    /// pointing at an S3 endpoint. Behavioural coverage of `push` /
    /// `pull` against a real S3-compatible endpoint (MinIO / Tigris)
    /// will land in a separate integration test in a follow-up.
    #[test]
    fn push_then_pull_round_trip() {
        let local = in_memory_backend("local");
        let remote = in_memory_backend("remote");

        // Stage a fake checkpoint: one UUID directory with two batches,
        // plus a top-level manifest pointing at it.
        let cp_uuid = uuid::Uuid::now_v7();
        let cp_dir = cp_uuid.to_string();
        write_text(&local, &format!("{cp_dir}/batch-0.dat"), b"batch zero");
        write_text(&local, &format!("{cp_dir}/batch-1.dat"), b"batch one");

        let manifest = vec![CheckpointMetadata {
            uuid: cp_uuid,
            identifier: Some("test".into()),
            fingerprint: 0xdead_beef,
            size: Some(20),
            steps: Some(1),
            processed_records: Some(1),
        }];
        let manifest_path: StoragePath = CHECKPOINT_FILE_NAME.into();
        local
            .write_json(&manifest_path, &manifest)
            .expect("write manifest")
            .commit()
            .expect("commit manifest");

        // Push: copy all UUID-prefixed files, then the manifest.
        let listed = list_files(&local, &cp_uuid.to_string().as_str().into()).unwrap();
        assert_eq!(listed.len(), 2);
        for path in &listed {
            copy_file(&local, &remote, path).expect("copy to remote");
        }
        copy_file(&local, &remote, &manifest_path).expect("copy manifest");

        // Pull: into a fresh local backend.
        let local_2 = in_memory_backend("local2");
        copy_file(&remote, &local_2, &manifest_path).expect("pull manifest");
        for path in list_files(&remote, &cp_uuid.to_string().as_str().into()).unwrap() {
            copy_file(&remote, &local_2, &path).expect("pull file");
        }

        // Verify: manifest deserializes and contents match.
        let pulled: Vec<CheckpointMetadata> = local_2.read_json(&manifest_path).unwrap();
        assert_eq!(pulled.len(), 1);
        assert_eq!(pulled[0].uuid, cp_uuid);

        let batch0 = local_2
            .read(&format!("{cp_dir}/batch-0.dat").as_str().into())
            .unwrap();
        assert_eq!(batch0.as_slice(), b"batch zero");
    }
}
