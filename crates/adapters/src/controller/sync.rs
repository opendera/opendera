//! Metric counters for checkpoint synchronization to/from object storage.
//!
//! The checkpoint synchronizer implementation has been removed pending
//! clean-room reimplementation. Only the metric definitions remain so the
//! controller can continue to reference them; sites that previously called
//! into the synchronizer are stubbed at the call site to return errors or
//! no-ops until the new implementation lands.

use std::sync::atomic::AtomicU64;

use feldera_storage::histogram::ExponentialHistogram;

// Pull metrics
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

// Push metrics
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
