//! Cloud-only `/v0` endpoints consumed by the opendera-cloud
//! console-plugin (`opendera-cloud/console-plugin/src/lib/cloud-client.ts`).
//!
//! These endpoints are gated by the `cloud_mode` config flag. On a
//! self-hosted deployment (`cloud_mode = false`, the default) none of
//! them are registered, so requests fall through to the SPA's 404. On
//! a cloud deployment they're registered alongside the rest of the
//! `/v0/*` API.
//!
//! Wire shapes match `cloud-client.ts` exactly. The bodies are
//! intentionally thin: `signup` creates a tenant via the existing
//! storage trait; the billing/usage endpoints return empty/zero pages
//! against a contract the cloud daemons can render today, and fill in
//! once the engine-side usage aggregation (REMAINING_WORK.md §3) and
//! Stripe integration (REMAINING_WORK.md §10) land.
//!
//! `POST /signup` is the only unauthenticated endpoint here; it lives
//! in `public_scope` so the signup form can call it without a JWT.
//! Everything else is registered inside `api_scope` and inherits the
//! tenant-auth middleware.

use actix_web::{
    get, post,
    web::{Data as WebData, Json, Path, ReqData},
    HttpResponse,
};
use rand::{distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api::main::ServerState;
use crate::db::storage::Storage;
use crate::db::types::tenant::TenantId;
use crate::error::ManagerError;

// ---------------------------------------------------------------------------
// POST /v0/signup  (unauthenticated, registered in public_scope)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct SignupRequest {
    pub email: String,
    pub password: String,
    pub tenant_name: String,
}

#[derive(Serialize)]
pub struct SignupResponse {
    pub tenant_id: String,
    pub email_verification_sent: bool,
}

/// Cloud signup. Provisions a tenant via `get_or_create_tenant_id` and
/// returns its id. The password is accepted for wire compatibility but
/// not yet stored — real user auth lands when the cloud signup flow
/// (OIDC + email-verification) is wired up; until then the daemon-side
/// signup form is exercising the contract only.
#[post("/signup")]
pub async fn signup(
    state: WebData<ServerState>,
    body: Json<SignupRequest>,
) -> Result<HttpResponse, ManagerError> {
    // We deliberately don't return an error for missing fields here —
    // serde already 400s on a malformed body, and we want to keep the
    // success path self-contained while real auth is unimplemented.
    let SignupRequest {
        email: _,
        password: _,
        tenant_name,
    } = body.into_inner();

    let new_id = Uuid::now_v7();
    let db = state.db.lock().await;
    // Provider "cloud-signup" distinguishes self-service signups from
    // tenants created via the OIDC issuer-tenant path.
    let tenant_id = db
        .get_or_create_tenant_id(new_id, tenant_name, "cloud-signup".to_string())
        .await?;

    Ok(HttpResponse::Ok().json(SignupResponse {
        tenant_id: tenant_id.0.to_string(),
        // Email verification is not implemented yet (the engine has no
        // mailer); the cloud signup UI treats `false` as "skip the
        // confirm-your-email screen and drop the user straight into
        // onboarding".
        email_verification_sent: false,
    }))
}

// ---------------------------------------------------------------------------
// GET /v0/account/tenants  (authenticated)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct Tenant {
    pub id: String,
    pub name: String,
    pub plan: &'static str,
    pub stripe_customer_id: Option<String>,
}

/// Returns the tenants the authenticated principal is a member of.
/// The OSS engine has no membership concept — every JWT maps to one
/// tenant — so today this always returns a single-element list. When
/// per-user multi-tenant membership lands, this becomes a real query.
#[get("/account/tenants")]
pub async fn list_tenants(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
) -> Result<HttpResponse, ManagerError> {
    let db = state.db.lock().await;
    let name = db.get_tenant_name(*tenant_id).await?;
    let stripe_customer_id = db.get_tenant_stripe_customer_id(*tenant_id).await?;
    Ok(HttpResponse::Ok().json(vec![Tenant {
        id: tenant_id.0.to_string(),
        name,
        // Plan tier isn't persisted yet — the on_demand default matches
        // the marketing site's free-trial copy. Real plans land with
        // the billing rework.
        plan: "on_demand",
        stripe_customer_id,
    }]))
}

// ---------------------------------------------------------------------------
// GET /v0/billing/snapshot  (authenticated)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct PerDimensionAmount {
    pub gb: f64,
    pub usd: f64,
}
#[derive(Serialize)]
pub struct PerDimensionStorage {
    pub gb_month: f64,
    pub usd: f64,
}
#[derive(Serialize)]
pub struct PerDimensionCompute {
    pub fcu_hours: f64,
    pub usd: f64,
}
#[derive(Serialize)]
pub struct PerDimensionQuery {
    pub tb_scanned: f64,
    pub usd: f64,
}

#[derive(Serialize)]
pub struct PerDimension {
    pub ingestion: PerDimensionAmount,
    pub storage: PerDimensionStorage,
    pub compute: PerDimensionCompute,
    pub query: PerDimensionQuery,
}

#[derive(Serialize)]
pub struct BillingSnapshot {
    pub month_to_date_usd: f64,
    pub projected_usd: f64,
    pub per_dimension: PerDimension,
}

#[get("/billing/snapshot")]
pub async fn billing_snapshot(_tenant_id: ReqData<TenantId>) -> Result<HttpResponse, ManagerError> {
    // Zero across the board until usage aggregation
    // (REMAINING_WORK.md §3) lands. The cloud billing page renders
    // these as "$0.00" + "no usage yet" copy, which is the correct
    // pre-launch state.
    Ok(HttpResponse::Ok().json(BillingSnapshot {
        month_to_date_usd: 0.0,
        projected_usd: 0.0,
        per_dimension: PerDimension {
            ingestion: PerDimensionAmount { gb: 0.0, usd: 0.0 },
            storage: PerDimensionStorage {
                gb_month: 0.0,
                usd: 0.0,
            },
            compute: PerDimensionCompute {
                fcu_hours: 0.0,
                usd: 0.0,
            },
            query: PerDimensionQuery {
                tb_scanned: 0.0,
                usd: 0.0,
            },
        },
    }))
}

// ---------------------------------------------------------------------------
// GET /v0/billing/invoices  (authenticated)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct Invoice {
    pub id: String,
    pub period_start: String,
    pub period_end: String,
    pub total_usd: f64,
    pub status: &'static str,
    pub hosted_url: Option<String>,
}

#[get("/billing/invoices")]
pub async fn list_invoices(_tenant_id: ReqData<TenantId>) -> Result<HttpResponse, ManagerError> {
    // Empty until the Stripe integration (REMAINING_WORK.md §10) is
    // backed by a real account; once it is, this endpoint proxies to
    // `Stripe.invoices.list({customer: stripe_customer_id})`.
    Ok(HttpResponse::Ok().json(Vec::<Invoice>::new()))
}

// ---------------------------------------------------------------------------
// POST /v0/billing/portal  (authenticated)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
#[allow(dead_code)] // billing_portal endpoint is wired up later (cloud-only)
struct PortalResponse {
    url: String,
}

#[post("/billing/portal")]
pub async fn billing_portal(
    state: WebData<ServerState>,
    tenant_id: ReqData<TenantId>,
) -> Result<HttpResponse, ManagerError> {
    // The cloud-client expects either a 200 with a Stripe-hosted URL
    // or an error. We 503 when the tenant has no linked Stripe
    // customer (which today is every tenant — the link is set by the
    // onboarding flow via PUT /internal/v0/tenants/{id}/billing). The
    // console-plugin renders the 503 body as a "billing not yet set
    // up" empty state. Once the Stripe daemon is live, replace the
    // 503 with a real `Stripe.billingPortal.sessions.create()` call.
    let db = state.db.lock().await;
    let stripe_customer_id = db.get_tenant_stripe_customer_id(*tenant_id).await?;
    drop(db);
    match stripe_customer_id {
        Some(_) => Ok(HttpResponse::ServiceUnavailable()
            .body("billing portal not configured (Stripe integration pending)")),
        None => Ok(HttpResponse::ServiceUnavailable().body("tenant has no linked Stripe customer")),
    }
}

// ---------------------------------------------------------------------------
// GET /v0/usage/current  (authenticated)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct UsageBucket {
    pub pipeline_id: String,
    pub pipeline_name: String,
    pub ingestion_gb: f64,
    pub storage_gb_month: f64,
    pub fcu_hours: f64,
    pub query_tb: f64,
    pub cost_usd: f64,
}

#[get("/usage/current")]
pub async fn current_usage(_tenant_id: ReqData<TenantId>) -> Result<HttpResponse, ManagerError> {
    // Empty page until the manager's usage aggregation
    // (REMAINING_WORK.md §3) lands. The console-plugin's usage page
    // renders this as a "no data yet — start a pipeline to see usage"
    // empty state, which is the correct pre-aggregation behavior.
    Ok(HttpResponse::Ok().json(Vec::<UsageBucket>::new()))
}

// ---------------------------------------------------------------------------
// OAuth signup helpers (unauthenticated, registered in public_scope)
// ---------------------------------------------------------------------------
//
// The cloud signup page offers one-click signup via Google and GitHub
// alongside email/password. These endpoints generate the provider's
// authorization URL from server-side config (so client secrets never
// touch the browser) and return it for the client to navigate to.
//
// The callback handler is stubbed today: real OAuth integration needs
// app registration with each provider (REMAINING_WORK.md §18), and the
// flow then exchanges the auth code for an access token, fetches the
// user profile, and calls `get_or_create_tenant_id` the same way
// `POST /signup` does. The 501 body lays out the exact remaining
// wiring so a follow-up PR can implement it without re-deriving the
// protocol.

#[derive(Serialize)]
pub struct OAuthStartResponse {
    pub authorization_url: String,
    pub state: String,
}

fn random_state() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(32)
        .map(char::from)
        .collect()
}

/// `POST /v0/signup/oauth/{provider}/start`
///
/// Builds the provider's authorization URL from env-configured
/// credentials and a fresh CSRF state token. The client stores the
/// state in a cookie before redirecting; the callback validates it.
///
/// Providers: "google" | "github". Anything else → 404.
#[post("/signup/oauth/{provider}/start")]
pub async fn oauth_start(
    state: WebData<ServerState>,
    provider: Path<String>,
) -> Result<HttpResponse, ManagerError> {
    let provider = provider.into_inner();
    let cfg = &state.config;
    let csrf = random_state();

    let (auth_url, client_id, redirect_uri, scope, base) = match provider.as_str() {
        "google" => (
            "https://accounts.google.com/o/oauth2/v2/auth",
            cfg.oauth_google_client_id.as_deref(),
            cfg.oauth_google_redirect_uri.as_deref(),
            "openid email profile",
            "google",
        ),
        "github" => (
            "https://github.com/login/oauth/authorize",
            cfg.oauth_github_client_id.as_deref(),
            cfg.oauth_github_redirect_uri.as_deref(),
            "read:user user:email",
            "github",
        ),
        _ => return Ok(HttpResponse::NotFound().body("unknown oauth provider")),
    };

    let (Some(client_id), Some(redirect_uri)) = (client_id, redirect_uri) else {
        // The provider isn't configured on this deployment. Surface
        // 503 so the client can hide the button rather than redirect
        // into an obviously-broken provider flow.
        return Ok(HttpResponse::ServiceUnavailable()
            .body(format!("oauth provider {base} not configured")));
    };

    let url = format!(
        "{auth_url}?client_id={client_id}\
         &redirect_uri={redirect_uri}\
         &response_type=code\
         &scope={scope}\
         &state={csrf}",
        // urlencoding the scope keeps the spaces well-formed.
        scope = urlencoding::encode(scope),
        redirect_uri = urlencoding::encode(redirect_uri),
    );

    Ok(HttpResponse::Ok().json(OAuthStartResponse {
        authorization_url: url,
        state: csrf,
    }))
}

/// `GET /v0/signup/oauth/{provider}/callback`
///
/// Stub. Once an OAuth app is registered with each provider and the
/// client_secret is wired into the manager (REMAINING_WORK.md §18),
/// this endpoint will:
///
///   1. Read `code` + `state` from the query string.
///   2. Validate `state` against the cookie set by `/start`.
///   3. Exchange `code` for an access token at the provider's token
///      endpoint (Google: oauth2.googleapis.com/token; GitHub:
///      github.com/login/oauth/access_token).
///   4. Fetch the user profile (Google: openidconnect.googleapis.com
///      /v1/userinfo; GitHub: api.github.com/user).
///   5. Call `db.get_or_create_tenant_id(...)` with the email-derived
///      tenant name and provider `"oauth-google"` / `"oauth-github"`.
///   6. Issue a session JWT (or set an OIDC-style session cookie) and
///      302 to `/onboarding/new-pipeline`.
///
/// Until then, return 501 with that plan so the client renders a
/// helpful message instead of a blank screen.
#[get("/signup/oauth/{provider}/callback")]
pub async fn oauth_callback(provider: Path<String>) -> Result<HttpResponse, ManagerError> {
    Ok(HttpResponse::NotImplemented().body(format!(
        "OAuth callback for provider '{}' is not yet implemented. \
         Real OAuth signup requires app registration with the provider \
         (REMAINING_WORK.md §18).",
        provider.into_inner()
    )))
}

// ---------------------------------------------------------------------------
// Public exports for scope wiring (see api/main.rs).
// ---------------------------------------------------------------------------

/// Register the cloud endpoints that need tenant auth. Called from
/// `api_scope` only when `cloud_mode = true`; on a self-hosted manager
/// none of these are mounted and requests fall through to the SPA 404.
pub(crate) fn register_authenticated(scope: actix_web::Scope) -> actix_web::Scope {
    scope
        .service(list_tenants)
        .service(billing_snapshot)
        .service(list_invoices)
        .service(billing_portal)
        .service(current_usage)
}

/// Register the cloud endpoints that DON'T need tenant auth (signup
/// only). Called from `public_scope` under the same `cloud_mode` gate.
pub(crate) fn register_public(scope: actix_web::Scope) -> actix_web::Scope {
    scope
        .service(signup)
        .service(oauth_start)
        .service(oauth_callback)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pure shape test: the BillingSnapshot we hand to the cloud
    /// console-plugin must serialize with the exact field names the
    /// TypeScript interface expects. If you rename a field here, the
    /// console-plugin breaks silently — this test catches that.
    #[test]
    fn billing_snapshot_wire_shape() {
        let snap = BillingSnapshot {
            month_to_date_usd: 0.0,
            projected_usd: 0.0,
            per_dimension: PerDimension {
                ingestion: PerDimensionAmount { gb: 0.0, usd: 0.0 },
                storage: PerDimensionStorage {
                    gb_month: 0.0,
                    usd: 0.0,
                },
                compute: PerDimensionCompute {
                    fcu_hours: 0.0,
                    usd: 0.0,
                },
                query: PerDimensionQuery {
                    tb_scanned: 0.0,
                    usd: 0.0,
                },
            },
        };
        let json = serde_json::to_value(&snap).unwrap();
        assert!(json.get("month_to_date_usd").is_some());
        assert!(json.get("projected_usd").is_some());
        let pd = json.get("per_dimension").unwrap();
        assert!(pd.get("ingestion").unwrap().get("gb").is_some());
        assert!(pd.get("storage").unwrap().get("gb_month").is_some());
        assert!(pd.get("compute").unwrap().get("fcu_hours").is_some());
        assert!(pd.get("query").unwrap().get("tb_scanned").is_some());
    }

    /// Same idea for the other shapes that the console-plugin
    /// deserializes by structural typing — if any field name drifts
    /// from `cloud-client.ts`, the daemon silently breaks.
    #[test]
    fn other_wire_shapes() {
        let tenant = Tenant {
            id: "t".into(),
            name: "n".into(),
            plan: "on_demand",
            stripe_customer_id: None,
        };
        let v = serde_json::to_value(&tenant).unwrap();
        for k in ["id", "name", "plan", "stripe_customer_id"] {
            assert!(v.get(k).is_some(), "Tenant missing field {k}");
        }

        let signup_resp = SignupResponse {
            tenant_id: "t".into(),
            email_verification_sent: false,
        };
        let v = serde_json::to_value(&signup_resp).unwrap();
        for k in ["tenant_id", "email_verification_sent"] {
            assert!(v.get(k).is_some(), "SignupResponse missing field {k}");
        }

        let invoice = Invoice {
            id: "i".into(),
            period_start: "2026-05-01T00:00:00Z".into(),
            period_end: "2026-06-01T00:00:00Z".into(),
            total_usd: 0.0,
            status: "draft",
            hosted_url: None,
        };
        let v = serde_json::to_value(&invoice).unwrap();
        for k in [
            "id",
            "period_start",
            "period_end",
            "total_usd",
            "status",
            "hosted_url",
        ] {
            assert!(v.get(k).is_some(), "Invoice missing field {k}");
        }

        let bucket = UsageBucket {
            pipeline_id: "p".into(),
            pipeline_name: "n".into(),
            ingestion_gb: 0.0,
            storage_gb_month: 0.0,
            fcu_hours: 0.0,
            query_tb: 0.0,
            cost_usd: 0.0,
        };
        let v = serde_json::to_value(&bucket).unwrap();
        for k in [
            "pipeline_id",
            "pipeline_name",
            "ingestion_gb",
            "storage_gb_month",
            "fcu_hours",
            "query_tb",
            "cost_usd",
        ] {
            assert!(v.get(k).is_some(), "UsageBucket missing field {k}");
        }
    }
}
