//! Periodic collector that turns cumulative pipeline-side counters
//! (`GET /stats`) into closed billing-grade usage buckets in the
//! `usage_bucket` table. The Stripe metering daemon
//! (opendera-cloud/stripe/) reads those buckets back out via
//! `GET /internal/v0/usage`.
//!
//! ## Why bucket on the manager side?
//!
//! The pipeline process exposes only *cumulative* counters; it does
//! not know about wall-clock minute boundaries or about the manager's
//! billing cadence. Computing deltas across minute boundaries is the
//! manager's job. Doing it here also keeps usage telemetry alive
//! across pipeline restarts: the pipeline's counters reset on resume,
//! but `last_sample` lives in this collector and the persisted
//! `usage_bucket` rows live in the database.
//!
//! ## Cadence and alignment
//!
//! The collector wakes once per `BUCKET_INTERVAL` (60s by default).
//! Buckets close at `bucket_end_ts = floor(now() / 60s)` so all
//! managers — present and any future shards — agree on bucket edges
//! and the same idempotency key (`pipeline:dim:bucket_end_ts`) is
//! produced regardless of which manager processed which tick.
//!
//! ## Per-pipeline state
//!
//! For each running pipeline we keep one `Sample` of the last
//! cumulative counters we observed and the wall-clock instant at
//! which we observed them. The amount we record for a closed bucket
//! is `current - last`. When a pipeline restarts the cumulative
//! counters can go backwards — we detect that, drop the bucket for
//! that tick, and seed `last_sample` with the new lower values so
//! the next tick is healthy again.
//!
//! ## What we record today
//!
//! - `ingestion_gb`: `total_input_bytes` / 1e9, computed from the
//!   pipeline's `global_metrics.total_input_bytes` delta.
//! - `storage_gb_month`: derived from the pipeline's
//!   `storage_mb_secs` counter (already an integral over time —
//!   converted to GB·month by `mb·s / (1024 * 60 * 60 * 24 * 30)`).
//! - `fcu_hour`: `runtime_elapsed_msecs` delta / 3_600_000.
//!
//! `query_tb` is not yet emitted: the ad-hoc DataFusion query path
//! tracks bytes scanned per-query but doesn't surface a per-pipeline
//! cumulative counter on `/stats`. When that lands, add it here.

use crate::config::CommonConfig;
use crate::db::storage::Storage;
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::pipeline::PipelineId;
use crate::db::types::tenant::TenantId;
use crate::db::types::usage::UsageDimension;
use chrono::{DateTime, DurationRound, TimeDelta, Utc};
use opendera_types::adapter_stats::ExternalControllerStatus;
use opendera_types::runtime_status::RuntimeStatus;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

/// How often to close a bucket and write rows for every running
/// pipeline. 60s gives Stripe per-minute resolution; tightening it
/// inflates the row count and the daemon's per-poll work, while
/// loosening it slows down the rate at which usage shows up in Stripe
/// dashboards.
const BUCKET_INTERVAL: Duration = Duration::from_secs(60);

/// Timeout for the per-pipeline `GET /stats` request. The request is
/// fired and forgotten if it doesn't return in time: we'd rather miss
/// one minute of usage on a slow pipeline than block other pipelines
/// behind it.
const STATS_TIMEOUT: Duration = Duration::from_secs(5);

/// Bytes per gigabyte for the wire dimension. Decimal-base GB to
/// match what cloud bills present to customers (1 GB = 1e9 bytes),
/// not GiB.
const BYTES_PER_GB: f64 = 1_000_000_000.0;

/// Milliseconds per FCU-hour. `runtime_elapsed_msecs` is already
/// thread-weighted on the pipeline side, so dividing by 3_600_000
/// produces FCU-hours directly.
const MSECS_PER_FCU_HOUR: f64 = 3_600_000.0;

/// Megabyte-seconds per gigabyte-month. Used to scale the pipeline's
/// `storage_mb_secs` integral into the billing dimension Stripe
/// expects (GB·month, average storage held over one billing month).
///
/// `1 GB·month = 1024 MB * 60 s/min * 60 min/hr * 24 hr/day * 30 day`.
const MB_SECS_PER_GB_MONTH: f64 = 1024.0 * 60.0 * 60.0 * 24.0 * 30.0;

#[derive(Clone, Copy, Debug)]
struct Sample {
    total_input_bytes: u64,
    runtime_elapsed_msecs: u64,
    storage_mb_secs: u64,
}

/// Indefinite loop. Logs and swallows recoverable errors; never
/// returns under normal operation.
pub async fn usage_collector(db: Arc<Mutex<StoragePostgres>>, common_config: CommonConfig) {
    info!(
        "Usage collector: closing one bucket every {}s",
        BUCKET_INTERVAL.as_secs()
    );

    let protocol = if common_config.enable_https {
        "https"
    } else {
        "http"
    };
    // reqwest, not awc: this runs from `tokio::spawn` and must be
    // `Send`; awc::Client is `!Send` (Rc-backed). The pipeline-manager
    // already brings reqwest in for the same reason — see the comment
    // in its Cargo.toml.
    let client = match reqwest::Client::builder()
        .timeout(STATS_TIMEOUT)
        // Disable HTTPS-only mode so we work on a self-hosted manager
        // talking to a pipeline on the loopback (the protocol is
        // selected per-request from `common_config.enable_https`).
        .https_only(false)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            warn!("Usage collector: failed to construct HTTP client: {e}; collector disabled");
            return;
        }
    };
    let mut last_samples: BTreeMap<PipelineId, Sample> = BTreeMap::new();

    let mut ticker = tokio::time::interval(BUCKET_INTERVAL);
    // If we fall behind (busy DB, paused process), don't try to catch
    // up by running back-to-back ticks — each tick samples the live
    // counters, so we'd just write the same delta twice.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;
        if let Err(e) = collect_once(&db, &client, protocol, &mut last_samples).await {
            warn!("Usage collector tick failed: {e}");
        }
    }
}

async fn collect_once(
    db: &Arc<Mutex<StoragePostgres>>,
    client: &reqwest::Client,
    protocol: &str,
    last_samples: &mut BTreeMap<PipelineId, Sample>,
) -> Result<(), String> {
    let bucket_end = floor_to_minute(Utc::now());

    let pipelines = db
        .lock()
        .await
        .list_pipelines_across_all_tenants_for_monitoring()
        .await
        .map_err(|e| format!("listing pipelines: {e}"))?;

    // Drop state for pipelines that are no longer in the list (deleted
    // or just not running) so the HashMap doesn't grow unbounded.
    let live_ids: BTreeSet<PipelineId> = pipelines.iter().map(|(_, p)| p.id).collect();
    last_samples.retain(|id, _| live_ids.contains(id));

    for (tenant_id, descr) in pipelines {
        if descr.deployment_runtime_status != Some(RuntimeStatus::Running) {
            // Reset the sample so we don't compute a delta across a
            // pause/restart edge — that would either undercount
            // (counters reset to 0) or double-count.
            last_samples.remove(&descr.id);
            continue;
        }
        let Some(location) = descr.deployment_location.as_deref() else {
            continue;
        };
        let status = match fetch_stats(client, protocol, location).await {
            Ok(s) => s,
            Err(e) => {
                debug!(
                    "Usage collector: GET /stats for pipeline {} failed: {e}",
                    descr.id
                );
                continue;
            }
        };
        let now_sample = Sample {
            total_input_bytes: status.global_metrics.total_input_bytes,
            runtime_elapsed_msecs: status.global_metrics.runtime_elapsed_msecs,
            storage_mb_secs: status.global_metrics.storage_mb_secs,
        };
        match last_samples.get(&descr.id).copied() {
            None => {
                // First sample for this pipeline — seed and skip; we
                // can't compute a meaningful delta yet.
            }
            Some(prev) => {
                if let Some(amount) = delta_gb(prev.total_input_bytes, now_sample.total_input_bytes)
                {
                    write_bucket(
                        db,
                        tenant_id,
                        descr.id,
                        UsageDimension::IngestionGb,
                        bucket_end,
                        amount,
                    )
                    .await;
                }
                if let Some(amount) =
                    delta_fcu_hour(prev.runtime_elapsed_msecs, now_sample.runtime_elapsed_msecs)
                {
                    write_bucket(
                        db,
                        tenant_id,
                        descr.id,
                        UsageDimension::FcuHour,
                        bucket_end,
                        amount,
                    )
                    .await;
                }
                if let Some(amount) =
                    delta_gb_month(prev.storage_mb_secs, now_sample.storage_mb_secs)
                {
                    write_bucket(
                        db,
                        tenant_id,
                        descr.id,
                        UsageDimension::StorageGbMonth,
                        bucket_end,
                        amount,
                    )
                    .await;
                }
            }
        }
        last_samples.insert(descr.id, now_sample);
    }

    Ok(())
}

async fn write_bucket(
    db: &Arc<Mutex<StoragePostgres>>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    dim: UsageDimension,
    bucket_end_ts: DateTime<Utc>,
    amount: f64,
) {
    if let Err(e) = db
        .lock()
        .await
        .insert_usage_bucket(tenant_id, pipeline_id, dim, bucket_end_ts, amount)
        .await
    {
        warn!(
            "Usage collector: failed to insert {dim:?} bucket for {pipeline_id} at \
             {bucket_end_ts}: {e}"
        );
    }
}

async fn fetch_stats(
    client: &reqwest::Client,
    protocol: &str,
    location: &str,
) -> Result<ExternalControllerStatus, String> {
    let url = format!("{protocol}://{location}/stats");
    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("send: {e}"))?;
    if !response.status().is_success() {
        return Err(format!("non-2xx response: {}", response.status()));
    }
    response
        .json::<ExternalControllerStatus>()
        .await
        .map_err(|e| format!("parse: {e}"))
}

/// Round `ts` down to the start of the next minute boundary at or
/// after it. Treats a failure to round (chrono returns an error for
/// out-of-range timestamps that can't represent the rounded form) as
/// best-effort: we return `ts` unchanged. In practice this branch is
/// unreachable for any timestamp the manager will ever see.
fn floor_to_minute(ts: DateTime<Utc>) -> DateTime<Utc> {
    ts.duration_trunc(TimeDelta::minutes(1)).unwrap_or(ts)
}

/// Compute `(current - prev)` interpreted as bytes and convert to GB.
/// Returns `None` if the counter went backwards (a pipeline restart).
fn delta_gb(prev: u64, current: u64) -> Option<f64> {
    let raw = current.checked_sub(prev)?;
    Some(raw as f64 / BYTES_PER_GB)
}

fn delta_fcu_hour(prev_msecs: u64, current_msecs: u64) -> Option<f64> {
    let raw = current_msecs.checked_sub(prev_msecs)?;
    Some(raw as f64 / MSECS_PER_FCU_HOUR)
}

fn delta_gb_month(prev_mb_secs: u64, current_mb_secs: u64) -> Option<f64> {
    let raw = current_mb_secs.checked_sub(prev_mb_secs)?;
    Some(raw as f64 / MB_SECS_PER_GB_MONTH)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_gb_normal_and_backwards() {
        // Normal forward delta.
        let amount = delta_gb(1_000_000_000, 3_000_000_000).unwrap();
        assert!((amount - 2.0).abs() < 1e-9);

        // Counter went backwards (pipeline restart) — we drop the
        // bucket rather than report a phantom negative number.
        assert!(delta_gb(5_000_000_000, 1_000_000_000).is_none());

        // Identity: zero delta is still a valid 0.0 bucket.
        let amount = delta_gb(42, 42).unwrap();
        assert_eq!(amount, 0.0);
    }

    #[test]
    fn delta_fcu_hour_units() {
        // One hour of elapsed time = 1 FCU-hour.
        let amount = delta_fcu_hour(0, 3_600_000).unwrap();
        assert!((amount - 1.0).abs() < 1e-9);
    }

    #[test]
    fn delta_gb_month_units() {
        // Holding 1024 MB for 30 days = 1 GB·month.
        let one_gb_month_in_mb_secs = (1024.0 * 60.0 * 60.0 * 24.0 * 30.0) as u64;
        let amount = delta_gb_month(0, one_gb_month_in_mb_secs).unwrap();
        assert!((amount - 1.0).abs() < 1e-9);
    }

    #[test]
    fn floor_to_minute_drops_subsecond() {
        let ts = "2026-05-12T14:23:47.123456Z"
            .parse::<DateTime<Utc>>()
            .unwrap();
        let floored = floor_to_minute(ts);
        assert_eq!(floored.to_rfc3339(), "2026-05-12T14:23:00+00:00");
    }
}
