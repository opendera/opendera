//! Offline GC sweep for orphaned pipeline storage prefixes on Tigris.
//!
//! `FlyRunner::clear()` deletes `s3://<bucket>/pipelines/<id>/` when a
//! pipeline's storage is cleared, but that deletion is best-effort: if
//! the manager crashes mid-clear or Tigris hiccups, the prefix is
//! orphaned and accrues storage cost forever. This module is the
//! "separate cron" the runner's TODO called for: an hourly sweep that
//! lists the per-pipeline prefixes, drops the ones whose pipeline no
//! longer exists in the database, and leaves anything recently written
//! alone (grace period) so it can never race an in-flight provision.
//!
//! Spawned from `bin/pipeline-manager.rs` only when the Fly executor is
//! selected.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use object_store::{parse_url_opts, path::Path as ObjPath, ObjectStore};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::config::FlyRunnerConfig;
use crate::db::storage::Storage;
use crate::db::storage_postgres::StoragePostgres;

/// How often the sweep runs. Orphans only accumulate via failed
/// best-effort clears, so hourly is plenty.
const SWEEP_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Prefixes whose newest object is younger than this are never
/// collected, even when no matching pipeline exists. Covers the window
/// where a pipeline is being provisioned (storage written before the
/// pipeline row is visible to this process) and clock skew between the
/// manager and Tigris.
const GRACE: chrono::Duration = chrono::Duration::hours(24);

/// What one sweep did; logged at info level when anything was deleted.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// Orphaned prefixes fully deleted.
    pub prefixes_deleted: usize,
    /// Total objects deleted across those prefixes.
    pub objects_deleted: usize,
    /// Orphaned prefixes left alone because their newest object is
    /// within the grace period.
    pub skipped_recent: usize,
}

/// Periodic sweep entry point; never returns. Failures are logged and
/// retried on the next tick — GC must never take the manager down.
pub async fn tigris_gc(db: Arc<Mutex<StoragePostgres>>, config: FlyRunnerConfig) {
    info!(
        "Tigris GC: sweeping s3://{}/pipelines/ every {}s (grace {}h)",
        config.tigris_bucket,
        SWEEP_INTERVAL.as_secs(),
        GRACE.num_hours()
    );
    loop {
        match sweep_once(&db, &config).await {
            Ok(report) if report.prefixes_deleted > 0 => {
                info!(
                    "Tigris GC: deleted {} orphaned pipeline prefix(es) ({} objects); \
                     {} recent prefix(es) skipped",
                    report.prefixes_deleted, report.objects_deleted, report.skipped_recent
                );
            }
            Ok(report) => {
                debug!("Tigris GC: nothing to collect ({report:?})");
            }
            Err(e) => {
                warn!("Tigris GC sweep failed (will retry next tick): {e:#}");
            }
        }
        tokio::time::sleep(SWEEP_INTERVAL).await;
    }
}

/// One sweep: snapshot the live pipeline-id set, then reconcile the
/// bucket's `pipelines/` prefixes against it.
async fn sweep_once(
    db: &Arc<Mutex<StoragePostgres>>,
    config: &FlyRunnerConfig,
) -> anyhow::Result<SweepReport> {
    // The live set is read BEFORE listing the bucket: a pipeline created
    // mid-sweep is then either in the set (kept) or its objects are
    // newer than the grace period (kept). Both races are safe.
    let live: HashSet<Uuid> = db
        .lock()
        .await
        .list_pipelines_across_all_tenants_for_monitoring()
        .await
        .context("list pipelines for GC")?
        .into_iter()
        .map(|(_, p)| p.id.0)
        .collect();

    let url_str = format!("s3://{}/pipelines", config.tigris_bucket);
    let url = url::Url::parse(&url_str).context("parse tigris url")?;
    let mut opts = vec![
        ("endpoint".to_string(), config.tigris_endpoint.clone()),
        ("region".to_string(), "auto".to_string()),
    ];
    if config.tigris_endpoint.starts_with("http://") {
        opts.push(("allow_http".to_string(), "true".to_string()));
    }
    let (store, base) = parse_url_opts(&url, opts).context("open tigris")?;

    sweep_store(&*store, &base, &live, Utc::now()).await
}

/// Database-free sweep core, separated for testability against
/// `object_store::memory::InMemory`.
///
/// A prefix `<base>/<uuid>/...` is deleted iff the uuid is not in
/// `live` AND its newest object is older than [`GRACE`] relative to
/// `now`. Non-UUID prefixes are never touched.
pub(crate) async fn sweep_store(
    store: &dyn ObjectStore,
    base: &ObjPath,
    live: &HashSet<Uuid>,
    now: DateTime<Utc>,
) -> anyhow::Result<SweepReport> {
    let mut report = SweepReport::default();

    let listing = store
        .list_with_delimiter(Some(base))
        .await
        .context("list pipeline prefixes")?;

    for prefix in listing.common_prefixes {
        let Some(id) = prefix
            .parts()
            .last()
            .and_then(|part| Uuid::parse_str(part.as_ref()).ok())
        else {
            debug!("Tigris GC: skipping non-UUID prefix {prefix}");
            continue;
        };
        if live.contains(&id) {
            continue;
        }

        // Collect the orphan's objects and its most recent write.
        let mut newest: Option<DateTime<Utc>> = None;
        let mut objects: Vec<ObjPath> = Vec::new();
        let mut stream = store.list(Some(&prefix));
        while let Some(item) = stream.next().await {
            let meta = item.context("list orphaned prefix")?;
            newest = Some(newest.map_or(meta.last_modified, |n| n.max(meta.last_modified)));
            objects.push(meta.location);
        }

        match newest {
            None => continue, // raced an external delete; nothing left
            Some(newest) if now - newest < GRACE => {
                debug!(
                    "Tigris GC: orphaned prefix {prefix} written {newest}; \
                     within grace period, skipping"
                );
                report.skipped_recent += 1;
                continue;
            }
            Some(_) => {}
        }

        for path in &objects {
            // Idempotent: a missing object is fine.
            let _ = store.delete(path).await;
        }
        info!(
            "Tigris GC: deleted orphaned pipeline prefix {prefix} ({} objects)",
            objects.len()
        );
        report.prefixes_deleted += 1;
        report.objects_deleted += objects.len();
    }

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::PutPayload;

    async fn put(store: &dyn ObjectStore, path: &str) {
        store
            .put(&ObjPath::from(path), PutPayload::from_static(b"x"))
            .await
            .unwrap();
    }

    async fn exists(store: &dyn ObjectStore, path: &str) -> bool {
        store.head(&ObjPath::from(path)).await.is_ok()
    }

    /// Orphaned prefixes older than the grace period are deleted; live
    /// pipelines and recent orphans are kept; non-UUID prefixes are
    /// ignored.
    #[tokio::test]
    async fn sweep_deletes_only_aged_orphans() {
        let store = object_store::memory::InMemory::new();
        let live_id = Uuid::now_v7();
        let orphan_id = Uuid::now_v7();

        put(&store, &format!("pipelines/{live_id}/storage/a.bin")).await;
        put(&store, &format!("pipelines/{orphan_id}/storage/b.bin")).await;
        put(&store, &format!("pipelines/{orphan_id}/storage/c.bin")).await;
        put(&store, "pipelines/not-a-uuid/d.bin").await;

        let live: HashSet<Uuid> = [live_id].into_iter().collect();
        let base = ObjPath::from("pipelines");

        // Sweep "now": everything was written moments ago, so even the
        // orphan is within the grace period.
        let report = sweep_store(&store, &base, &live, Utc::now()).await.unwrap();
        assert_eq!(
            report,
            SweepReport {
                prefixes_deleted: 0,
                objects_deleted: 0,
                skipped_recent: 1,
            }
        );
        assert!(exists(&store, &format!("pipelines/{orphan_id}/storage/b.bin")).await);

        // Sweep "two days later": the orphan ages out and is deleted;
        // the live pipeline and the non-UUID prefix survive.
        let later = Utc::now() + chrono::Duration::hours(48);
        let report = sweep_store(&store, &base, &live, later).await.unwrap();
        assert_eq!(
            report,
            SweepReport {
                prefixes_deleted: 1,
                objects_deleted: 2,
                skipped_recent: 0,
            }
        );
        assert!(exists(&store, &format!("pipelines/{live_id}/storage/a.bin")).await);
        assert!(!exists(&store, &format!("pipelines/{orphan_id}/storage/b.bin")).await);
        assert!(!exists(&store, &format!("pipelines/{orphan_id}/storage/c.bin")).await);
        assert!(exists(&store, "pipelines/not-a-uuid/d.bin").await);
    }
}
