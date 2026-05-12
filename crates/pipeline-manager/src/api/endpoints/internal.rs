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
    HttpRequest, HttpResponse, Responder, get,
    http::header,
    web,
    web::Data as WebData,
};
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::api::main::ServerState;
use crate::db::storage::Storage;
use crate::error::ManagerError;
use feldera_types::runtime_status::RuntimeStatus;

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
    /// Pipeline lifecycle state per `crates/feldera-types/src/runtime_status.rs`.
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
    let rows = db.list_pipelines_across_all_tenants_for_monitoring().await?;
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
            // The cloud-only fields below land once the Fly executor
            // persists its handle in the DB. Until then the activity
            // controller skips pipelines whose `fly_machine_id` is None.
            fly_app: None,
            fly_machine_id: None,
            tier: None,
            ram_mb: None,
        })
        .collect();
    Ok(HttpResponse::Ok().json(pipelines))
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
    let _ = &state;

    let stream = async_stream::stream! {
        loop {
            let payload = format!(
                "event: heartbeat\ndata: {{\"ts\":\"{}\"}}\n\n",
                Utc::now().to_rfc3339()
            );
            yield Ok::<_, actix_web::Error>(web::Bytes::from(payload));
            tokio::time::sleep(Duration::from_secs(30)).await;
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
        unsafe { std::env::remove_var(INTERNAL_API_KEY_ENV); }
        let req = test::TestRequest::default()
            .insert_header((header::AUTHORIZATION, "Bearer anything"))
            .to_http_request();
        let resp = check_internal_auth(&req).expect_err("must reject when env unset");
        assert_eq!(resp.status().as_u16(), 503);

        // 2. Env var set: matching bearer succeeds.
        unsafe { std::env::set_var(INTERNAL_API_KEY_ENV, "test-secret"); }
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
        unsafe { std::env::remove_var(INTERNAL_API_KEY_ENV); }
    }
}
