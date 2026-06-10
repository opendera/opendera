//! Checkpoint synchronization wiring for the I/O controller.
//!
//! Drives the `CheckpointSynchronizer` (registered via `inventory` in
//! `dbsp::storage::backend::object_store_synchronizer`) on the pull side:
//! one-shot pull at startup, continuous pull while in standby, plus the
//! activation marker logic. The push side lives in the controller itself
//! (`CheckpointSyncThread::run`).
//!
//! Clean-room reimplementation of §2 of `ENTERPRISE_FEATURES.md` (the
//! call-site half; the trait implementation lives in
//! `crates/dbsp/src/storage/backend/object_store_synchronizer.rs`).

use anyhow::Context;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dbsp::circuit::CircuitStorageConfig;
use dbsp::circuit::checkpointer::Checkpointer;
use feldera_adapterlib::errors::journal::ControllerError;
use feldera_storage::checkpoint_synchronizer::SYNCHRONIZER;
use feldera_storage::histogram::ExponentialHistogram;
use feldera_storage::{StorageBackend, StoragePath};
use feldera_types::checkpoint::CheckpointMetadata;
use feldera_types::config::{FileBackendConfig, StorageBackendConfig, SyncConfig};
use feldera_types::constants::ACTIVATION_MARKER_FILE;

// ---------------------------------------------------------------------------
// Metrics (preserved across the clean-room reimplementation)
// ---------------------------------------------------------------------------

/// Bytes transferred when pulling a checkpoint.
pub static CHECKPOINT_SYNC_PULL_TRANSFERRED_BYTES: ExponentialHistogram =
    ExponentialHistogram::new();

/// Transfer speed when pulling a checkpoint, in bytes per second.
pub static CHECKPOINT_SYNC_PULL_TRANSFER_SPEED: ExponentialHistogram = ExponentialHistogram::new();

/// Number of checkpoints pulled successfully.
pub static CHECKPOINT_SYNC_PULL_SUCCESS: AtomicU64 = AtomicU64::new(0);

/// Number of failures when pulling a checkpoint.
pub static CHECKPOINT_SYNC_PULL_FAILURES: AtomicU64 = AtomicU64::new(0);

/// Time taken to pull a checkpoint from object store in seconds.
pub static CHECKPOINT_SYNC_PULL_DURATION_SECONDS: ExponentialHistogram =
    ExponentialHistogram::new();

/// Bytes transferred when pushing a checkpoint.
pub static CHECKPOINT_SYNC_PUSH_TRANSFERRED_BYTES: ExponentialHistogram =
    ExponentialHistogram::new();

/// Transfer speed when pushing a checkpoint, in bytes per second.
pub static CHECKPOINT_SYNC_PUSH_TRANSFER_SPEED: ExponentialHistogram = ExponentialHistogram::new();

/// Number of checkpoints pushed successfully.
pub static CHECKPOINT_SYNC_PUSH_SUCCESS: AtomicU64 = AtomicU64::new(0);

/// Number of failures when pushing a checkpoint.
pub static CHECKPOINT_SYNC_PUSH_FAILURES: AtomicU64 = AtomicU64::new(0);

/// Time taken to push a checkpoint to object store in seconds.
pub static CHECKPOINT_SYNC_PUSH_DURATION_SECONDS: ExponentialHistogram =
    ExponentialHistogram::new();

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns `Some(sync_config)` if the storage's backend config carries a
/// `sync` block with a `start_from_checkpoint`. Otherwise returns `None`.
pub fn is_pull_necessary(storage: &CircuitStorageConfig) -> Option<&SyncConfig> {
    let StorageBackendConfig::File(ref file_cfg) = storage.options.backend else {
        return None;
    };
    let FileBackendConfig {
        sync: Some(ref sync),
        ..
    } = **file_cfg
    else {
        return None;
    };
    sync.start_from_checkpoint.as_ref()?;
    Some(sync)
}

/// Pull the configured checkpoint from object storage once, GC older
/// local checkpoints, and update metrics. Used at pipeline startup and
/// once per standby iteration.
fn pull_and_gc(
    storage: Arc<dyn StorageBackend>,
    sync: &SyncConfig,
    prev: &mut uuid::Uuid,
) -> Result<CheckpointMetadata, ControllerError> {
    let (cpm, sync_metrics) = SYNCHRONIZER
        .pull(storage.clone(), sync.clone())
        .map_err(|e| {
            CHECKPOINT_SYNC_PULL_FAILURES.fetch_add(1, Ordering::Relaxed);
            ControllerError::checkpoint_fetch_error(format!("{e:?}"))
        })?;

    if cpm.uuid != *prev {
        CHECKPOINT_SYNC_PULL_SUCCESS.fetch_add(1, Ordering::Relaxed);
        *prev = cpm.uuid;
        if let Some(m) = sync_metrics {
            CHECKPOINT_SYNC_PULL_TRANSFER_SPEED.record(m.speed);
            CHECKPOINT_SYNC_PULL_DURATION_SECONDS.record(m.duration.as_secs());
            CHECKPOINT_SYNC_PULL_TRANSFERRED_BYTES.record(m.bytes);
        }
    }

    // GC old local checkpoints. A failed GC isn't fatal — the pull
    // already succeeded and the new state is intact.
    if let Err(e) = Checkpointer::new(storage.clone()).and_then(|cp| cp.gc_startup()) {
        tracing::warn!(
            "checkpoint pulled successfully but local GC failed: {e:?}; older local checkpoints may linger"
        );
    }

    Ok(cpm)
}

/// One-shot pull. Calls `pull_and_gc` once and returns.
pub fn pull_once(
    storage: &CircuitStorageConfig,
    sync: &SyncConfig,
) -> Result<(), ControllerError> {
    pull_and_gc(storage.backend.clone(), sync, &mut uuid::Uuid::nil())?;
    Ok(())
}

/// Continuous pull for standby mode. Polls the remote at the configured
/// `pull_interval`. When `is_activated()` first returns true, performs
/// one final pull (so the activated pipeline starts from the most
/// recent primary checkpoint), writes the activation marker, and
/// returns.
///
/// If a previous activation marker already exists, returns immediately
/// (idempotency on restart).
pub fn continuous_pull<F>(
    storage: &CircuitStorageConfig,
    is_activated: F,
) -> Result<(), ControllerError>
where
    F: Fn() -> bool,
{
    let StorageBackendConfig::File(ref file_cfg) = storage.options.backend else {
        return Err(ControllerError::InvalidStandby(
            "standby mode requires file storage backend",
        ));
    };
    let FileBackendConfig {
        sync: Some(ref sync),
        ..
    } = **file_cfg
    else {
        return Err(ControllerError::InvalidStandby(
            "standby mode requires file storage backend to have synchronization configured",
        ));
    };

    sync.validate()
        .map_err(ControllerError::checkpoint_fetch_error)?;

    if sync.start_from_checkpoint.is_none() {
        return Err(ControllerError::InvalidStandby(
            "standby mode requires file storage backend to have synchronization configured to start from a checkpoint",
        ));
    }

    // Idempotency: if the activation marker is already on disk, this is
    // a restart after a previous activation — skip standby entirely.
    let activation_file = StoragePath::from(ACTIVATION_MARKER_FILE);
    if storage.backend.exists(&activation_file).unwrap_or(false) {
        let previous_activation = storage
            .backend
            .read_json::<Option<CheckpointMetadata>>(&activation_file)
            .ok()
            .flatten();
        tracing::info!(
            "this pipeline was previously activated from {}, skipping standby mode",
            previous_activation
                .map(|p| format!("checkpoint '{}'", p.uuid))
                .unwrap_or_else(|| "scratch".to_owned())
        );
        return Ok(());
    }

    // Pull-loop. The post-activation pass ensures the activated pipeline
    // starts from the freshest possible checkpoint.
    let mut prev = uuid::Uuid::nil();
    let mut latest: Option<CheckpointMetadata> = None;
    let mut post_activation_pass = false;

    loop {
        match pull_and_gc(storage.backend.clone(), sync, &mut prev) {
            Err(err) => {
                if post_activation_pass {
                    // Final pull after activation must succeed, else
                    // the activated pipeline might be missing the
                    // latest data.
                    return Err(err);
                }
                tracing::warn!("standby pull failed (will retry): {err:?}");
            }
            Ok(cpm) => latest = Some(cpm),
        }

        if !sync.standby {
            return Ok(());
        }

        if is_activated() {
            if post_activation_pass {
                break;
            }
            post_activation_pass = true;
            // Skip the sleep on this iteration so we activate quickly.
            continue;
        }

        std::thread::sleep(Duration::from_secs(sync.pull_interval));
    }

    // Write the activation marker. A failure here is logged but only
    // hard-errors when `fail_if_no_checkpoint` is set, matching the
    // documented behaviour in the original SyncConfig.
    tracing::debug!("creating activation marker file: {}", activation_file);
    if let Err(marker_err) = storage
        .backend
        .write_json(&activation_file, &latest)
        .and_then(|reader| reader.commit())
        .context("failed to write activation marker file")
    {
        tracing::error!("{marker_err:?}");
        if sync.fail_if_no_checkpoint {
            return Err(ControllerError::checkpoint_fetch_error(format!(
                "{marker_err:?}"
            )));
        }
    }

    tracing::info!("pipeline activated");
    Ok(())
}
