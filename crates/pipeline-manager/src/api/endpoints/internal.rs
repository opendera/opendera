//! Internal API consumed by the cloud-side activity controller (see
//! ../../../opendera-cloud/activity-controller/). Not part of the
//! public `/v0` surface; not included in the published OpenAPI spec.
//!
//! Endpoints:
//!
//!   GET /internal/v0/pipelines   — lightweight list of every pipeline
//!                                  across all tenants, with the cloud-
//!                                  side projection (Fly machine id,
//!                                  tier, RAM, last_activity_at).
//!   GET /internal/v0/activity    — SSE stream of activity events
//!                                  (ingested, queried, woke,
//!                                  state_changed, always_on).
//!
//! Both endpoints authenticate via a shared bearer token in the
//! `OPENDERA_INTERNAL_API_KEY` env var. When the env var is unset,
//! both endpoints respond `503 Service Unavailable` so accidental
//! exposure on a non-cloud deployment doesn't leak cross-tenant data.
//!
//! Status: skeleton. The list endpoint currently returns an empty
//! array because the admin-scoped, cross-tenant `list_all_pipelines`
//! storage method is not yet in `crate::db::storage`. The activity
//! stream emits heartbeats only; real `ingested` / `queried` events
//! are emitted from the controller's hot path in a follow-up that
//! plumbs a broadcast channel through `ServerState`.

use std::time::Duration;

use actix_web::{
    get, http::header, put, web, web::Data as WebData, HttpRequest, HttpResponse, Responder,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::api::main::ServerState;
use crate::db::storage::Storage;
use crate::db::types::tenant::TenantId;
use crate::db::types::usage::UsageDimension;
use crate::error::ManagerError;
use opendera_types::runtime_status::RuntimeStatus;

const INTERNAL_API_KEY_ENV: &str = "OPENDERA_INTERNAL_API_KEY";

/// Verify the request carries `Authorization: Bearer <expected>` where
/// `<expected>` matches `OPENDERA_INTERNAL_API_KEY`. If the env var is
/// unset, fail with 503 (the internal API is opt-in).
fn check_internal_auth(req: &HttpRequest) -> Result<(), HttpResponse> {
    let expected = match std::env::var(INTERNAL_API_KEY_ENV) {
        Ok(v) if !v.is_empty() => v,
        _ => {
            return Err(HttpResponse::ServiceUnavailable()
                .body("internal API disabled (set OPENDERA_INTERNAL_API_KEY)"));
        }
    };
    let presented = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "));
    if presented == Some(expected.as_str()) {
        Ok(())
    } else {
        Err(HttpResponse::Unauthorized().body("missing or invalid internal API key"))
    }
}

/// Projection of a pipeline tailored for the activity controller. Matches
/// the `PipelineSummary` interface in
/// opendera-cloud/activity-controller/src/manager-client.ts.
#[derive(Serialize, Debug)]
pub struct PipelineSummary {
    pub pipeline_id: String,
    pub tenant_id: String,
    /// Pipeline lifecycle state per `crates/opendera-types/src/runtime_status.rs`.
    pub observed_status: String,
    pub created_at: DateTime<Utc>,
    /// Time of the most recent ingested batch / ad-hoc query. `None` if
    /// no traffic has hit the pipeline since startup.
    pub last_activity_at: Option<DateTime<Utc>>,
    // Fields below are cloud-mode-only; `None` on a self-hosted
    // deployment that doesn't run the Fly executor.
    pub fly_app: Option<String>,
    pub fly_machine_id: Option<String>,
    pub tier: Option<String>,
    pub ram_mb: Option<u64>,
}

#[get("/pipelines")]
pub async fn list_internal_pipelines(
    state: WebData<ServerState>,
    req: HttpRequest,
) -> Result<HttpResponse, ManagerError> {
    if let Err(resp) = check_internal_auth(&req) {
        return Ok(resp);
    }

    let db = state.db.lock().await;
    let rows = db
        .list_pipelines_across_all_tenants_for_monitoring()
        .await?;
    let pipelines: Vec<PipelineSummary> = rows
        .into_iter()
        .map(|(tenant_id, descr)| PipelineSummary {
            pipeline_id: descr.id.to_string(),
            tenant_id: tenant_id.0.to_string(),
            observed_status: descr
                .deployment_runtime_status
                .map(runtime_status_to_str)
                .unwrap_or_else(|| "Unknown".to_string()),
            created_at: descr.created_at,
            // Activity timestamps are emitted on the SSE stream; the DB
            // doesn't (yet) snapshot them so we surface `None` and let
            // the controller derive last-seen from the event stream.
            last_activity_at: None,
            fly_app: descr.fly_app,
            fly_machine_id: descr.fly_machine_id,
            tier: descr.tier,
            // ram_mb is stored as int4 (i32) — widen to u64 for the
            // wire type, which matches Fly's `guest.memory_mb` shape.
            // Saturate at 0 defensively; the column is only written by
            // the Fly executor with valid Fly machine sizes.
            ram_mb: descr.ram_mb.map(|v| v.max(0) as u64),
        })
        .collect();
    Ok(HttpResponse::Ok().json(pipelines))
}

/// Usage record matching the shape the Stripe metering daemon expects
/// (opendera-cloud/stripe/src/manager-client.ts). One record per
/// (tenant, pipeline, dimension, bucket) tuple; `idempotency_key` is
/// stable across retries so the daemon can safely re-submit.
#[derive(Serialize, Clone, Debug)]
struct UsageRecord {
    idempotency_key: String,
    tenant_id: String,
    pipeline_id: String,
    dim: UsageDimension,
    amount: f64,
    bucket_end_ts: DateTime<Utc>,
}

#[derive(Serialize)]
struct UsagePage {
    records: Vec<UsageRecord>,
    /// Cursor (RFC 3339 timestamp) — pass back as `since` to fetch the
    /// next page. `None` when the daemon has caught up; it should poll
    /// again later.
    next: Option<String>,
}

#[derive(Deserialize)]
struct UsageQuery {
    /// RFC 3339 timestamp. Only buckets whose `bucket_end_ts >` this
    /// value are returned. Omit on first call to start from the oldest
    /// available bucket.
    #[serde(default)]
    since: Option<String>,
    #[serde(default = "default_usage_limit")]
    limit: u32,
}

fn default_usage_limit() -> u32 {
    100
}

/// Hard ceiling on a single page. Prevents a misbehaving daemon from
/// scanning the whole table in one request and starving other readers.
const MAX_USAGE_LIMIT: u32 = 1000;

#[get("/usage")]
pub async fn list_usage(
    state: WebData<ServerState>,
    req: HttpRequest,
    query: web::Query<UsageQuery>,
) -> Result<HttpResponse, ManagerError> {
    if let Err(resp) = check_internal_auth(&req) {
        return Ok(resp);
    }

    let since = match query.since.as_deref() {
        Some(s) => match DateTime::parse_from_rfc3339(s) {
            Ok(ts) => Some(ts.with_timezone(&Utc)),
            Err(_) => {
                return Ok(HttpResponse::BadRequest().body("`since` must be an RFC 3339 timestamp"));
            }
        },
        None => None,
    };
    let limit = query.limit.clamp(1, MAX_USAGE_LIMIT) as i64;

    let db = state.db.lock().await;
    let buckets = db.list_usage_buckets(since, limit).await?;

    // Cursor: the bucket_end_ts of the last record. The daemon passes
    // it back as `since` and `WHERE bucket_end_ts > since` excludes it
    // on the next page. When the page is short of `limit`, we still
    // return a cursor so the daemon can resume polling from there
    // when new buckets close.
    let next = buckets.last().map(|b| b.bucket_end_ts.to_rfc3339());

    let records: Vec<UsageRecord> = buckets
        .into_iter()
        .map(|b| {
            let dim_str = b.dim.as_str();
            UsageRecord {
                idempotency_key: format!(
                    "{}:{}:{}",
                    b.pipeline_id.0,
                    dim_str,
                    b.bucket_end_ts.timestamp()
                ),
                tenant_id: b.tenant_id.0.to_string(),
                pipeline_id: b.pipeline_id.0.to_string(),
                dim: b.dim,
                amount: b.amount,
                bucket_end_ts: b.bucket_end_ts,
            }
        })
        .collect();

    Ok(HttpResponse::Ok().json(UsagePage { records, next }))
}

/// Cloud-internal: read a tenant's Stripe customer id. Consumed by the
/// Stripe metering daemon to resolve tenant -> customer before
/// emitting meter events. Returns `null` JSON when the tenant has no
/// linked Stripe customer.
#[derive(Serialize)]
struct TenantBilling {
    stripe_customer_id: Option<String>,
}

#[get("/tenants/{tenant_id}/billing")]
pub async fn get_tenant_billing(
    state: WebData<ServerState>,
    req: HttpRequest,
    path: web::Path<uuid::Uuid>,
) -> Result<HttpResponse, ManagerError> {
    if let Err(resp) = check_internal_auth(&req) {
        return Ok(resp);
    }
    let tenant_id = TenantId(path.into_inner());
    let db = state.db.lock().await;
    let stripe_customer_id = db.get_tenant_stripe_customer_id(tenant_id).await?;
    Ok(HttpResponse::Ok().json(TenantBilling { stripe_customer_id }))
}

#[derive(Deserialize)]
struct SetTenantBilling {
    stripe_customer_id: Option<String>,
}

#[put("/tenants/{tenant_id}/billing")]
pub async fn put_tenant_billing(
    state: WebData<ServerState>,
    req: HttpRequest,
    path: web::Path<uuid::Uuid>,
    body: web::Json<SetTenantBilling>,
) -> Result<HttpResponse, ManagerError> {
    if let Err(resp) = check_internal_auth(&req) {
        return Ok(resp);
    }
    let tenant_id = TenantId(path.into_inner());
    let db = state.db.lock().await;
    db.set_tenant_stripe_customer_id(tenant_id, body.stripe_customer_id.as_deref())
        .await?;
    Ok(HttpResponse::NoContent().finish())
}

/// Stringify a `RuntimeStatus` for the cloud-side controller. Kept
/// inline rather than relying on `Display` so the wire format is
/// stable independent of any future `Display` change.
fn runtime_status_to_str(s: RuntimeStatus) -> String {
    match s {
        RuntimeStatus::Unavailable => "Unavailable",
        RuntimeStatus::Coordination => "Coordination",
        RuntimeStatus::Standby => "Standby",
        RuntimeStatus::Initializing => "Initializing",
        RuntimeStatus::AwaitingApproval => "AwaitingApproval",
        RuntimeStatus::Bootstrapping => "Bootstrapping",
        RuntimeStatus::Replaying => "Replaying",
        RuntimeStatus::Paused => "Paused",
        RuntimeStatus::Running => "Running",
        RuntimeStatus::Suspended => "Suspended",
    }
    .to_string()
}

#[get("/activity")]
pub async fn activity_stream(state: WebData<ServerState>, req: HttpRequest) -> impl Responder {
    if let Err(resp) = check_internal_auth(&req) {
        return resp;
    }

    let mut rx = state.activity_bus.subscribe();

    // The stream interleaves real activity events with a periodic
    // heartbeat. The heartbeat is essential — proxies between the
    // controller and the manager (CloudFront, Fly Proxy, etc.) will
    // close idle HTTP/1.1 connections after 30-60s, so we must emit
    // at least one byte on that cadence to keep the SSE alive.
    let heartbeat = tokio::time::interval(Duration::from_secs(30));
    let stream = async_stream::stream! {
        let mut heartbeat = heartbeat;
        loop {
            tokio::select! {
                evt = rx.recv() => {
                    match evt {
                        Ok(event) => {
                            let body = serde_json::to_string(&event)
                                .unwrap_or_else(|_| "{}".to_string());
                            yield Ok::<_, actix_web::Error>(web::Bytes::from(
                                format!("event: activity\ndata: {body}\n\n")
                            ));
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            // Slow subscriber lagged out. Emit a marker
                            // so the controller knows it missed events
                            // and can re-reconcile via /pipelines.
                            yield Ok(web::Bytes::from(format!(
                                "event: lag\ndata: {{\"missed\":{n}}}\n\n"
                            )));
                        }
                        Err(_) => {
                            // Sender dropped — channel closed for good.
                            return;
                        }
                    }
                }
                _ = heartbeat.tick() => {
                    yield Ok(web::Bytes::from(format!(
                        "event: heartbeat\ndata: {{\"ts\":\"{}\"}}\n\n",
                        Utc::now().to_rfc3339()
                    )));
                }
            }
        }
    };

    HttpResponse::Ok()
        .insert_header((header::CONTENT_TYPE, "text/event-stream"))
        .insert_header((header::CACHE_CONTROL, "no-cache"))
        .streaming(stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::test;

    /// Exercises every case of `check_internal_auth` in a single test.
    /// Merged from two separate tests because cargo runs unit tests in
    /// parallel and they race on the process-wide env var. Could split
    /// again with `serial_test` if the file grows more env-coupled
    /// cases, but for now one test that covers the truth table is
    /// simpler.
    #[actix_web::test]
    async fn internal_api_auth_truth_table() {
        // 1. Env var unset: fail-closed with 503 regardless of header.
        // SAFETY: env mutation is single-threaded here because all the
        // tests in this module live in one #[test] function.
        unsafe {
            std::env::remove_var(INTERNAL_API_KEY_ENV);
        }
        let req = test::TestRequest::default()
            .insert_header((header::AUTHORIZATION, "Bearer anything"))
            .to_http_request();
        let resp = check_internal_auth(&req).expect_err("must reject when env unset");
        assert_eq!(resp.status().as_u16(), 503);

        // 2. Env var set: matching bearer succeeds.
        unsafe {
            std::env::set_var(INTERNAL_API_KEY_ENV, "test-secret");
        }
        let req = test::TestRequest::default()
            .insert_header((header::AUTHORIZATION, "Bearer test-secret"))
            .to_http_request();
        assert!(check_internal_auth(&req).is_ok());

        // 3. Wrong bearer => 401.
        let bad = test::TestRequest::default()
            .insert_header((header::AUTHORIZATION, "Bearer wrong"))
            .to_http_request();
        let resp = check_internal_auth(&bad).expect_err("must reject wrong key");
        assert_eq!(resp.status().as_u16(), 401);

        // 4. Missing header => 401.
        let missing = test::TestRequest::default().to_http_request();
        let resp = check_internal_auth(&missing).expect_err("must reject missing header");
        assert_eq!(resp.status().as_u16(), 401);

        // 5. Non-bearer scheme => 401.
        let basic = test::TestRequest::default()
            .insert_header((header::AUTHORIZATION, "Basic dXNlcjpwYXNz"))
            .to_http_request();
        let resp = check_internal_auth(&basic).expect_err("must reject non-bearer");
        assert_eq!(resp.status().as_u16(), 401);

        // Clean up so we don't poison other tests in the same process.
        unsafe {
            std::env::remove_var(INTERNAL_API_KEY_ENV);
        }
    }
}
