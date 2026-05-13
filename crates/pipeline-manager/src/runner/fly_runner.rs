//! Fly.io Machines executor.
//!
//! Drives the lifecycle of a single pipeline as a Fly Machine in a
//! per-tenant Fly App. Companion to the local-process and (future)
//! Kubernetes runners — the manager picks one at startup based on
//! configuration.
//!
//! Each pipeline maps to exactly one Machine. The Machine name
//! encodes the pipeline id so the runner is stateless: on restart it
//! re-discovers the Machine by listing the tenant's Fly App.
//!
//! Checkpoints + working state live on Tigris (Fly-network,
//! no-egress) — see `generate_storage_config` below.
//!
//! Status (May 2026): the happy paths (provision -> ready -> stop ->
//! clear) plus pipeline secrets push and per-machine log streaming are
//! implemented. Anycast IP allocation is intentionally not done — the
//! cloud V1 model proxies pipeline traffic through the manager, so
//! pipelines never need public IPs of their own. Multi-region failover
//! and `program_binary_url` → Fly image registry handoff are still
//! TODO and will land alongside the cloud GA work.

use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::spawn;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::config::{CommonConfig, FlyRunnerConfig};
use crate::db::types::pipeline::PipelineId;
use crate::db::types::version::Version;
use crate::error::ManagerError;
use crate::runner::error::RunnerError;
use crate::runner::fly_image_registry::FlyImagePusher;
use crate::runner::pipeline_executor::{PipelineExecutor, ProvisionStatus};
use crate::runner::pipeline_logs::{LogMessage, LogsSender};
use feldera_types::config::{PipelineConfig, StorageCacheConfig, StorageConfig};
use feldera_types::runtime_status::{BootstrapPolicy, RuntimeDesiredStatus};

const FLY_API_BASE: &str = "https://api.machines.dev";

/// State the runner carries between trait method calls. Everything
/// except the log-streaming task is derivable from `pipeline_id` so a
/// runner restart can rebuild it; the cache exists to avoid round-
/// tripping the Fly API on every call.
pub struct FlyRunner {
    pipeline_id: PipelineId,
    common_config: CommonConfig,
    config: FlyRunnerConfig,
    client: Client,
    logs_sender: LogsSender,
    /// Cached after the first provision call.
    cached: Option<MachineHandle>,
    /// Termination + join handle for the background task forwarding
    /// Fly machine logs into [`LogsSender`]. Created in `provision`,
    /// torn down in `stop` / `clear` / `Drop`.
    log_streamer: Option<(oneshot::Sender<()>, JoinHandle<()>)>,
}

impl Drop for FlyRunner {
    fn drop(&mut self) {
        if let Some((terminate, join)) = self.log_streamer.take() {
            // Best-effort: signal the streamer to stop and abort if it
            // hasn't noticed yet. Same pattern as LocalRunner::drop.
            let _ = terminate.send(());
            join.abort();
        }
    }
}

#[derive(Clone, Debug)]
struct MachineHandle {
    app_name: String,
    machine_id: String,
}

// ---- API request / response shapes (a strict subset of the Fly API) ----

#[derive(Serialize)]
struct CreateAppBody<'a> {
    app_name: &'a str,
    org_slug: &'a str,
}

#[derive(Serialize)]
struct CreateMachineBody<'a> {
    name: &'a str,
    region: &'a str,
    config: MachineConfig,
}

#[derive(Serialize)]
struct MachineConfig {
    image: String,
    env: serde_json::Map<String, Value>,
    guest: GuestConfig,
    /// Optional services block. Pipelines expose their internal HTTP
    /// port to the manager via Fly's private network (`.internal`
    /// DNS); ad-hoc query traffic is proxied through the manager so
    /// there's no public-facing service.
    services: Vec<Value>,
    restart: RestartPolicy,
}

#[derive(Serialize)]
struct GuestConfig {
    cpu_kind: String,
    cpus: u32,
    memory_mb: u32,
}

#[derive(Serialize)]
struct RestartPolicy {
    policy: &'static str,
}

#[derive(Deserialize, Debug)]
struct MachineResponse {
    id: String,
    state: String,
    private_ip: Option<String>,
}

// ---- Helpers ----

impl FlyRunner {
    /// Per-tenant Fly App name. Stable across runner restarts so the
    /// same pipeline always lives in the same app.
    fn app_name_for(&self, deployment_id: Uuid) -> String {
        format!("opendera-p-{}", short_id(self.pipeline_id, deployment_id))
    }

    /// Stable Machine name for the pipeline. Letting Fly auto-name
    /// would force us to store the machine id elsewhere; instead we
    /// derive a name from the pipeline id and look it up.
    fn machine_name(&self) -> String {
        format!("worker-{}", self.pipeline_id)
    }

    fn auth_header(&self) -> String {
        format!("Bearer {}", self.config.api_token)
    }

    /// Build the env map handed to the pipeline binary, omitting any
    /// entries that match `secret_env_suffixes` — those land on the
    /// Fly App as secrets via [`push_secrets`] so they never appear in
    /// plaintext on the Machine config.
    fn pipeline_env(
        &self,
        deployment_id: &Uuid,
        deployment_config: &PipelineConfig,
    ) -> serde_json::Map<String, Value> {
        let mut env = serde_json::Map::new();
        env.insert(
            "OPENDERA_PIPELINE_ID".into(),
            json!(self.pipeline_id.to_string()),
        );
        env.insert(
            "OPENDERA_DEPLOYMENT_ID".into(),
            json!(deployment_id.to_string()),
        );
        env.insert(
            "AWS_ENDPOINT_URL_S3".into(),
            json!(self.config.tigris_endpoint),
        );
        env.insert("AWS_REGION".into(), json!("auto"));
        for (k, v) in &deployment_config.global.env {
            if self.is_secret_env_name(k) {
                continue;
            }
            env.insert(k.clone(), json!(v));
        }
        env
    }

    /// Whether `name` should be pushed as a Fly Secret instead of
    /// inlined in the Machine env block.
    fn is_secret_env_name(&self, name: &str) -> bool {
        is_secret_env_name_with(name, &self.config.secret_env_suffixes)
    }

    /// Subset of `deployment_config.global.env` that should land as
    /// Fly Secrets. Order-stable so a redeploy doesn't churn the App.
    fn collect_secrets(&self, deployment_config: &PipelineConfig) -> BTreeMap<String, String> {
        deployment_config
            .global
            .env
            .iter()
            .filter(|(k, _)| self.is_secret_env_name(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Create the Fly App if it doesn't already exist. Idempotent
    /// (201 first time, 422 if already present).
    async fn ensure_app(&self, app_name: &str) -> Result<(), ManagerError> {
        let url = format!("{FLY_API_BASE}/v1/apps");
        let res = self
            .client
            .post(url)
            .header("Authorization", self.auth_header())
            .json(&CreateAppBody {
                app_name,
                org_slug: &self.config.org_slug,
            })
            .send()
            .await
            .map_err(|e| Self::provision_err(format!("fly: create app: {e}")))?;
        match res.status() {
            StatusCode::CREATED | StatusCode::OK => Ok(()),
            StatusCode::UNPROCESSABLE_ENTITY => {
                // Already exists. Confirm by GET to avoid masking a
                // real validation error with the same status.
                let url = format!("{FLY_API_BASE}/v1/apps/{app_name}");
                let res = self
                    .client
                    .get(url)
                    .header("Authorization", self.auth_header())
                    .send()
                    .await
                    .map_err(|e| Self::provision_err(format!("fly: get app: {e}")))?;
                if res.status().is_success() {
                    Ok(())
                } else {
                    Err(Self::provision_err(format!(
                        "fly: create app returned 422 but get returned {}",
                        res.status()
                    )))
                }
            }
            other => Err(Self::provision_err(format!(
                "fly: create app failed: {other}"
            ))),
        }
    }

    /// Look up the pipeline's Machine by name. Returns Some when it
    /// already exists, None when this is a fresh provision.
    async fn find_machine(&self, app_name: &str) -> Result<Option<MachineResponse>, ManagerError> {
        let url = format!("{FLY_API_BASE}/v1/apps/{app_name}/machines");
        let res = self
            .client
            .get(url)
            .header("Authorization", self.auth_header())
            .send()
            .await
            .map_err(|e| Self::provision_err(format!("fly: list machines: {e}")))?;
        if !res.status().is_success() {
            return Err(Self::provision_err(format!(
                "fly: list machines failed: {}",
                res.status()
            )));
        }
        let machines: Vec<MachineResponse> = res
            .json()
            .await
            .map_err(|e| Self::provision_err(format!("fly: parse machines: {e}")))?;
        // Match by name via a follow-up GET if needed. The list
        // endpoint doesn't include name; for simplicity, take the
        // first one. A per-pipeline app should have at most one
        // worker machine, so this is unambiguous.
        Ok(machines.into_iter().next())
    }

    /// Create a new Machine in the given app and return its handle.
    /// `image` may be a per-pipeline tag pushed earlier in
    /// `provision`; when None, falls back to the bundled
    /// `pipeline_image`.
    async fn create_machine(
        &self,
        app_name: &str,
        deployment_id: &Uuid,
        deployment_config: &PipelineConfig,
        image: Option<&str>,
    ) -> Result<MachineResponse, ManagerError> {
        let body = CreateMachineBody {
            name: &self.machine_name(),
            region: &self.config.region,
            config: MachineConfig {
                image: image
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| self.config.pipeline_image.clone()),
                env: self.pipeline_env(deployment_id, deployment_config),
                guest: GuestConfig {
                    cpu_kind: self.config.default_machine_cpu_kind.clone(),
                    cpus: self.config.default_machine_cpus,
                    memory_mb: self.config.default_machine_memory_mb,
                },
                services: vec![],
                restart: RestartPolicy {
                    policy: "on-failure",
                },
            },
        };
        let url = format!("{FLY_API_BASE}/v1/apps/{app_name}/machines");
        let res = self
            .client
            .post(url)
            .header("Authorization", self.auth_header())
            .json(&body)
            .send()
            .await
            .map_err(|e| Self::provision_err(format!("fly: create machine: {e}")))?;
        if !res.status().is_success() {
            let status = res.status();
            let body = res.text().await.unwrap_or_default();
            return Err(Self::provision_err(format!(
                "fly: create machine failed: {status}: {body}"
            )));
        }
        res.json::<MachineResponse>()
            .await
            .map_err(|e| Self::provision_err(format!("fly: parse machine: {e}")))
    }

    async fn get_machine(
        &self,
        app_name: &str,
        machine_id: &str,
    ) -> Result<MachineResponse, ManagerError> {
        let url = format!("{FLY_API_BASE}/v1/apps/{app_name}/machines/{machine_id}");
        let res = self
            .client
            .get(url)
            .header("Authorization", self.auth_header())
            .send()
            .await
            .map_err(|e| Self::provision_err(format!("fly: get machine: {e}")))?;
        if !res.status().is_success() {
            return Err(Self::provision_err(format!(
                "fly: get machine failed: {}",
                res.status()
            )));
        }
        res.json::<MachineResponse>()
            .await
            .map_err(|e| Self::provision_err(format!("fly: parse machine: {e}")))
    }

    async fn stop_machine(&self, app_name: &str, machine_id: &str) -> Result<(), ManagerError> {
        let url = format!("{FLY_API_BASE}/v1/apps/{app_name}/machines/{machine_id}/stop");
        let res = self
            .client
            .post(url)
            .header("Authorization", self.auth_header())
            .send()
            .await
            .map_err(|e| Self::provision_err(format!("fly: stop machine: {e}")))?;
        if !res.status().is_success() {
            warn!(
                "fly: stop machine {machine_id}: {}; ignoring (treated as already stopped)",
                res.status()
            );
        }
        Ok(())
    }

    async fn destroy_machine(&self, app_name: &str, machine_id: &str) -> Result<(), ManagerError> {
        let url = format!("{FLY_API_BASE}/v1/apps/{app_name}/machines/{machine_id}?force=true");
        let res = self
            .client
            .delete(url)
            .header("Authorization", self.auth_header())
            .send()
            .await
            .map_err(|e| Self::provision_err(format!("fly: destroy machine: {e}")))?;
        if !res.status().is_success() && res.status() != StatusCode::NOT_FOUND {
            warn!(
                "fly: destroy machine {machine_id}: {}; continuing",
                res.status()
            );
        }
        Ok(())
    }

    /// Set secret-bearing pipeline env vars on the Fly App so they get
    /// injected into the Machine's process env at boot without
    /// appearing in the (plaintext) Machine config. Idempotent — Fly
    /// treats a re-set with the same name+value as a no-op.
    ///
    /// Uses the Machines API bulk endpoint:
    /// `POST /v1/apps/{app}/secrets`, body
    /// `{ "secrets": { K: V, ... }, "replace_all": false }`.
    /// `replace_all: false` so an operator pushing additional secrets
    /// out-of-band (e.g. via `flyctl secrets set`) doesn't get wiped.
    async fn push_secrets(
        &self,
        app_name: &str,
        secrets: &BTreeMap<String, String>,
    ) -> Result<(), ManagerError> {
        if secrets.is_empty() {
            return Ok(());
        }
        let body = json!({
            "secrets": secrets,
            "replace_all": false,
        });
        let url = format!("{FLY_API_BASE}/v1/apps/{app_name}/secrets");
        let res = self
            .client
            .post(url)
            .header("Authorization", self.auth_header())
            .json(&body)
            .send()
            .await
            .map_err(|e| Self::provision_err(format!("fly: push secrets: {e}")))?;
        if !res.status().is_success() {
            let status = res.status();
            // Fly's API redacts the value in error responses, but
            // including the keys we tried to set helps debugging
            // without leaking material.
            let keys: Vec<&String> = secrets.keys().collect();
            let body = res.text().await.unwrap_or_default();
            return Err(Self::provision_err(format!(
                "fly: push secrets ({} keys) failed: {status}: {body}; keys={keys:?}",
                secrets.len()
            )));
        }
        info!(
            pipeline_id = %self.pipeline_id,
            app = %app_name,
            count = secrets.len(),
            "Fly: pushed pipeline secrets"
        );
        Ok(())
    }

    fn provision_err(msg: impl Into<String>) -> ManagerError {
        RunnerError::RunnerProvisionError { error: msg.into() }.into()
    }

    /// Download the pipeline binary from the compiler's HTTP endpoint,
    /// then push it as a one-layer OCI image on top of
    /// `image_registry.base_image`. Returns the resulting image
    /// reference suitable for `MachineConfig.image`.
    ///
    /// Pre-condition: `self.config.image_registry.enabled()` is true.
    /// The caller checked.
    async fn push_pipeline_image(&self, program_binary_url: &str) -> Result<String, ManagerError> {
        let registry_cfg = &self.config.image_registry;

        // The compiler exposes the binary at a single HTTP URL —
        // download it as bytes. The download can be large (tens of
        // MB) but well within in-memory budget for a control-plane
        // process that drives one pipeline at a time per executor.
        let res = self
            .client
            .get(program_binary_url)
            .timeout(Duration::from_secs(300))
            .send()
            .await
            .map_err(|e| {
                Self::provision_err(format!(
                    "fly: download pipeline binary from {program_binary_url}: {e}"
                ))
            })?;
        if !res.status().is_success() {
            return Err(Self::provision_err(format!(
                "fly: download pipeline binary {program_binary_url}: status {}",
                res.status()
            )));
        }
        let binary = res
            .bytes()
            .await
            .map_err(|e| Self::provision_err(format!("fly: read pipeline binary body: {e}")))?
            .to_vec();
        let digest_hex = sha256_hex_only(&binary);

        let pusher = FlyImagePusher::new(
            self.client.clone(),
            registry_cfg.clone(),
            &self.config.api_token,
        );
        pusher.push_pipeline_image(&binary, &digest_hex).await
    }
}

fn sha256_hex_only(bytes: &[u8]) -> String {
    // Strip the `sha256:` prefix returned by the helper in
    // `fly_image_registry` — we want just the hex for the tag.
    crate::runner::fly_image_registry::sha256_hex(bytes)
        .trim_start_matches("sha256:")
        .to_string()
}

/// Case-insensitive suffix match. Standalone so tests don't need to
/// build a [`FlyRunner`].
fn is_secret_env_name_with(name: &str, suffixes: &[String]) -> bool {
    let upper = name.to_ascii_uppercase();
    suffixes
        .iter()
        .any(|s| upper.ends_with(&s.to_ascii_uppercase()))
}

/// Produce a short, URL-safe id from a pipeline id + deployment id.
/// Fly app names are limited to ~30 chars; we concat the two short
/// forms so the same pipeline keeps the same app across redeploys but
/// different pipelines never collide.
fn short_id(pipeline_id: PipelineId, deployment_id: Uuid) -> String {
    let p = pipeline_id.to_string();
    let d = deployment_id.simple().to_string();
    format!("{}-{}", &p[..8.min(p.len())], &d[..6.min(d.len())])
}

#[async_trait]
impl PipelineExecutor for FlyRunner {
    type Config = FlyRunnerConfig;

    /// Fly Machine creation typically returns within seconds; we give
    /// the runner up to 90s to account for cold image pulls in a
    /// freshly-warmed region.
    const DEFAULT_PROVISIONING_TIMEOUT: Duration = Duration::from_secs(90);

    fn new(
        pipeline_id: PipelineId,
        common_config: CommonConfig,
        config: Self::Config,
        client: Client,
        logs_sender: LogsSender,
    ) -> Self {
        Self {
            pipeline_id,
            common_config,
            config,
            client,
            logs_sender,
            cached: None,
            log_streamer: None,
        }
    }

    /// Storage is keyed by pipeline id under the Tigris bucket; the
    /// `path` is a Tigris URL the pipeline binary's object_store
    /// backend (StorageBackendConfig::Object) will pick up. Cache
    /// configuration is the engine default.
    async fn generate_storage_config(&self) -> StorageConfig {
        StorageConfig {
            path: format!(
                "s3://{}/pipelines/{}/storage",
                self.config.tigris_bucket, self.pipeline_id
            ),
            cache: StorageCacheConfig::default(),
        }
    }

    async fn provision(
        &mut self,
        _deployment_initial: RuntimeDesiredStatus,
        _bootstrap_policy: Option<BootstrapPolicy>,
        deployment_id: &Uuid,
        deployment_config: &PipelineConfig,
        _program_info: &Value,
        program_binary_url: &str,
        _program_info_url: Option<&str>,
        _program_version: Version,
    ) -> Result<(), ManagerError> {
        // Remaining TODOs (independent of image push):
        //   - bootstrap_policy: pass to the pipeline as an env hint
        //   - deployment_initial: communicate to the binary so it
        //     boots in the requested initial state.
        let _ = &self.common_config;

        let app_name = self.app_name_for(*deployment_id);
        self.ensure_app(&app_name).await?;

        // Secrets must be set on the App before the Machine starts,
        // otherwise the worker boots without them.
        let secrets = self.collect_secrets(deployment_config);
        self.push_secrets(&app_name, &secrets).await?;

        // When configured, build a per-pipeline image (binary on top
        // of the runtime base) and push it to the Fly registry. The
        // resulting tag is passed as the Machine's `config.image`.
        // Otherwise the Machine launches from the shared
        // `pipeline_image` and is expected to fetch the binary at
        // start-up via the legacy `program_binary_url` mechanism.
        let machine_image = if self.config.image_registry.enabled() {
            Some(self.push_pipeline_image(program_binary_url).await?)
        } else {
            None
        };

        let handle = match self.find_machine(&app_name).await? {
            Some(existing) => {
                info!(
                    pipeline_id = %self.pipeline_id,
                    machine_id = %existing.id,
                    "Fly: reusing existing pipeline machine"
                );
                MachineHandle {
                    app_name,
                    machine_id: existing.id,
                }
            }
            None => {
                let created = self
                    .create_machine(
                        &app_name,
                        deployment_id,
                        deployment_config,
                        machine_image.as_deref(),
                    )
                    .await?;
                info!(
                    pipeline_id = %self.pipeline_id,
                    machine_id = %created.id,
                    image = machine_image.as_deref().unwrap_or(&self.config.pipeline_image),
                    "Fly: created pipeline machine"
                );
                MachineHandle {
                    app_name,
                    machine_id: created.id,
                }
            }
        };

        // Begin streaming the Machine's stdout/stderr into the
        // runner's `LogsSender` so `/v0/pipelines/.../logs` followers
        // see the same stream they get from local-runner pipelines.
        // Idempotent: a stale streamer from a previous provision is
        // torn down first.
        self.stop_log_streaming();
        self.log_streamer = Some(self.start_log_streaming(&handle));

        self.cached = Some(handle);
        Ok(())
    }

    async fn is_provisioned(&mut self) -> Result<ProvisionStatus, ManagerError> {
        let handle = self
            .cached
            .clone()
            .ok_or_else(|| Self::provision_err("provision was not called"))?;
        let m = self
            .get_machine(&handle.app_name, &handle.machine_id)
            .await?;
        match m.state.as_str() {
            "started" => Ok(ProvisionStatus::Provisioned {
                location: format!(
                    "{machine}.vm.{app}.internal:{port}",
                    machine = m.id,
                    app = handle.app_name,
                    port = 8080
                ),
                details: serde_json::to_value(&m).unwrap_or_default(),
            }),
            "created" | "starting" | "stopped" | "suspended" => Ok(ProvisionStatus::Ongoing {
                details: serde_json::to_value(&m).unwrap_or_default(),
            }),
            "failed" => Err(Self::provision_err(format!(
                "fly: machine entered failed state: {:?}",
                m
            ))),
            // Anything else (creating, destroying, replacing, etc.)
            // is treated as ongoing.
            _ => Ok(ProvisionStatus::Ongoing {
                details: serde_json::to_value(&m).unwrap_or_default(),
            }),
        }
    }

    async fn check(&mut self) -> Result<Value, ManagerError> {
        let handle = match &self.cached {
            Some(h) => h.clone(),
            None => return Ok(json!({ "state": "not_provisioned" })),
        };
        let m = self
            .get_machine(&handle.app_name, &handle.machine_id)
            .await?;
        Ok(json!({
            "state": m.state,
            "private_ip": m.private_ip,
        }))
    }

    async fn stop(&mut self) -> Result<(), ManagerError> {
        if let Some(handle) = self.cached.clone() {
            self.stop_machine(&handle.app_name, &handle.machine_id)
                .await?;
        }
        // The Fly Machine is no longer producing fresh logs; tear
        // down the streamer so we don't keep polling a stopped
        // machine. A subsequent `provision` will start a new one.
        self.stop_log_streaming();
        Ok(())
    }

    async fn clear(&mut self) -> Result<(), ManagerError> {
        self.stop_log_streaming();
        if let Some(handle) = self.cached.take() {
            self.destroy_machine(&handle.app_name, &handle.machine_id)
                .await?;
        }
        // Drop the pipeline's Tigris prefix. We do this even if there
        // was no cached machine handle, so a `clear` after a partial
        // provision still releases storage. Failure here is logged
        // but not propagated — the upstream `clear` contract is best-
        // effort idempotent, and an orphaned prefix is cheap.
        if let Err(e) = self.clear_tigris_prefix().await {
            warn!(
                pipeline_id = %self.pipeline_id,
                "fly: failed to delete pipeline storage prefix: {e}; \
                 leaving for offline GC"
            );
        }
        Ok(())
    }
}

impl FlyRunner {
    /// Delete every object under `s3://<tigris_bucket>/pipelines/<id>/`
    /// using the same ObjectStoreBackend implementation the pipeline
    /// worker uses. This collapses the lifecycle so the manager and
    /// the worker speak the same storage protocol.
    async fn clear_tigris_prefix(&self) -> Result<(), ManagerError> {
        use futures::StreamExt;
        use object_store::{parse_url_opts, path::Path as ObjPath, ObjectStore};

        // Compose the Tigris URL pointed at this pipeline's prefix and
        // parse it through object_store. Credentials are inherited
        // from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY in the
        // manager process env (Tigris is S3-compatible).
        let url_str = format!(
            "s3://{}/pipelines/{}",
            self.config.tigris_bucket, self.pipeline_id
        );
        let url = url::Url::parse(&url_str)
            .map_err(|e| Self::provision_err(format!("fly: parse tigris url: {e}")))?;
        let opts = vec![
            ("endpoint".to_string(), self.config.tigris_endpoint.clone()),
            ("region".to_string(), "auto".to_string()),
        ];
        let (store, base) = parse_url_opts(&url, opts)
            .map_err(|e| Self::provision_err(format!("fly: open tigris: {e}")))?;

        // List then delete. object_store doesn't have a single
        // 'delete prefix' call so we iterate; the pipeline's prefix
        // contains at most a few hundred shards in steady state.
        let mut stream = store.list(Some(&base));
        let mut to_delete: Vec<ObjPath> = Vec::new();
        while let Some(item) = stream.next().await {
            let meta = item.map_err(|e| Self::provision_err(format!("fly: list tigris: {e}")))?;
            to_delete.push(meta.location);
        }
        for path in to_delete {
            // Idempotent: a missing object is not an error here.
            let _ = store.delete(&path).await;
        }
        Ok(())
    }

    /// Spawn a background task that follows a Fly Machine's log
    /// stream and forwards each line into [`LogsSender`]. Mirrors
    /// the stdout/stderr-tailing thread that `LocalRunner` runs for
    /// in-process pipelines, so consumers of
    /// `/v0/pipelines/{name}/logs` see a unified stream regardless of
    /// which executor backs the pipeline.
    ///
    /// The task:
    /// - Authenticates with `FLY_API_BASE` using the same bearer
    ///   token as the rest of the runner.
    /// - Opens a follow-mode GET on the Machine logs endpoint and
    ///   reads streamed bytes chunk-by-chunk, segmenting on newline.
    /// - Re-opens the stream with exponential backoff if Fly closes
    ///   the connection (Fly typically caps long-lived HTTP follows
    ///   at a few minutes, after which the client must reconnect).
    /// - Exits cleanly when the returned [`oneshot::Sender`] is
    ///   triggered (via `stop_log_streaming` / `Drop`).
    fn start_log_streaming(
        &self,
        handle: &MachineHandle,
    ) -> (oneshot::Sender<()>, JoinHandle<()>) {
        let (terminate_tx, mut terminate_rx) = oneshot::channel::<()>();
        let client = self.client.clone();
        let auth = self.auth_header();
        let url = format!(
            "{FLY_API_BASE}/v1/apps/{}/machines/{}/logs?follow=true",
            handle.app_name, handle.machine_id
        );
        let pipeline_id = self.pipeline_id;
        let mut logs_sender = self.logs_sender.clone();

        let join = spawn(async move {
            let mut backoff = Duration::from_secs(1);
            const MAX_BACKOFF: Duration = Duration::from_secs(30);

            loop {
                // Termination check before each (re)connect.
                if terminate_rx.try_recv().is_ok() {
                    return;
                }

                let req = client
                    .get(&url)
                    .header("Authorization", &auth)
                    .send()
                    .await;
                let mut resp = match req {
                    Ok(r) if r.status().is_success() => r,
                    Ok(r) => {
                        warn!(
                            pipeline_id = %pipeline_id,
                            status = %r.status(),
                            "Fly: machine logs follow returned non-2xx; backing off"
                        );
                        if !backoff_or_exit(&mut terminate_rx, &mut backoff, MAX_BACKOFF).await {
                            return;
                        }
                        continue;
                    }
                    Err(e) => {
                        warn!(
                            pipeline_id = %pipeline_id,
                            "Fly: machine logs follow: connect failed: {e}; backing off"
                        );
                        if !backoff_or_exit(&mut terminate_rx, &mut backoff, MAX_BACKOFF).await {
                            return;
                        }
                        continue;
                    }
                };

                // Reset backoff on a successful connect.
                backoff = Duration::from_secs(1);

                // Stream chunks and segment on '\n'. A pipeline log
                // line can exceed a single chunk (the Fly proxy doesn't
                // align reads on line boundaries), so we buffer the
                // tail of the most recent chunk between iterations.
                let mut tail: Vec<u8> = Vec::new();
                loop {
                    tokio::select! {
                        _ = &mut terminate_rx => {
                            return;
                        }
                        chunk = resp.chunk() => match chunk {
                            Ok(Some(bytes)) => {
                                tail.extend_from_slice(&bytes);
                                while let Some(pos) = tail.iter().position(|b| *b == b'\n') {
                                    let line_bytes: Vec<u8> = tail.drain(..=pos).collect();
                                    // Drop the trailing '\n' (and '\r' if
                                    // CRLF) before forwarding.
                                    let end = line_bytes.len()
                                        - 1
                                        - usize::from(
                                            line_bytes.len() >= 2
                                                && line_bytes[line_bytes.len() - 2] == b'\r',
                                        );
                                    let line = String::from_utf8_lossy(&line_bytes[..end])
                                        .into_owned();
                                    if !line.is_empty() {
                                        logs_sender
                                            .send(LogMessage::new_from_pipeline(&line))
                                            .await;
                                    }
                                }
                            }
                            Ok(None) => {
                                // EOS — Fly closed the follow stream.
                                // Flush any trailing partial line.
                                if !tail.is_empty() {
                                    let line = String::from_utf8_lossy(&tail).into_owned();
                                    if !line.is_empty() {
                                        logs_sender
                                            .send(LogMessage::new_from_pipeline(&line))
                                            .await;
                                    }
                                }
                                debug!(
                                    pipeline_id = %pipeline_id,
                                    "Fly: machine logs stream closed; reconnecting"
                                );
                                break;
                            }
                            Err(e) => {
                                warn!(
                                    pipeline_id = %pipeline_id,
                                    "Fly: machine logs read error: {e}; reconnecting"
                                );
                                break;
                            }
                        }
                    }
                }

                // Brief pause before reconnect so a flapping endpoint
                // doesn't peg the manager.
                if !backoff_or_exit(&mut terminate_rx, &mut backoff, MAX_BACKOFF).await {
                    return;
                }
            }
        });

        (terminate_tx, join)
    }

    /// Tear down the log-streaming task if one is running.
    fn stop_log_streaming(&mut self) {
        if let Some((terminate, join)) = self.log_streamer.take() {
            let _ = terminate.send(());
            join.abort();
        }
    }
}

/// Wait either for the termination signal or until `*backoff` elapses,
/// then double the backoff up to `max`. Returns false if termination
/// fired (caller should exit), true otherwise.
async fn backoff_or_exit(
    terminate_rx: &mut oneshot::Receiver<()>,
    backoff: &mut Duration,
    max: Duration,
) -> bool {
    let wait = *backoff;
    *backoff = std::cmp::min(*backoff * 2, max);
    tokio::select! {
        _ = terminate_rx => false,
        _ = sleep(wait) => true,
    }
}

// Manual Serialize for MachineResponse so the trait's `details: Value`
// payloads keep the same shape across Fly API revisions even if new
// fields appear.
impl Serialize for MachineResponse {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        let mut s = ser.serialize_struct("MachineResponse", 3)?;
        use serde::ser::SerializeStruct;
        s.serialize_field("id", &self.id)?;
        s.serialize_field("state", &self.state)?;
        s.serialize_field("private_ip", &self.private_ip)?;
        s.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::default_secret_env_suffixes;
    use crate::db::types::pipeline::PipelineId;

    #[test]
    fn machine_size_default_supports_suspend() {
        // The default size must allow Fly's snapshot-resume path
        // (≤ 2 GiB RAM). If this assertion ever breaks, also revisit
        // the Tier classification in opendera-cloud's
        // activity-controller.
        use clap::Parser;
        let cfg = FlyRunnerConfig::parse_from(["fly-runner-test"]);
        assert!(
            cfg.default_machine_memory_mb <= 2048,
            "default machine size must be suspendable"
        );
    }

    #[test]
    fn short_id_is_deterministic_and_short_enough() {
        let pid = PipelineId(Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap());
        let did = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
        let a = short_id(pid, did);
        let b = short_id(pid, did);
        assert_eq!(a, b, "must be deterministic for restart re-discovery");
        // Fly app names cap at ~30 chars; the wrapper prefix
        // ('opendera-p-') eats 11, leaving ~19 for short_id. Our
        // format is 8 + '-' + 6 = 15 chars; that's the budget.
        assert!(a.len() <= 19, "short_id too long for fly app name");
    }

    #[test]
    fn default_secret_suffixes_cover_common_names() {
        let s = default_secret_env_suffixes();
        // Sanity-check the cases that motivated the heuristic: a
        // Kafka SASL password, a Postgres CDC password, a generic
        // API token, and an AWS-style access key.
        let secret_names = [
            "KAFKA_SASL_PASSWORD",
            "POSTGRES_PASSWORD",
            "STRIPE_API_KEY",
            "GITHUB_TOKEN",
            "MY_SERVICE_SECRET",
            "DB_CREDENTIALS",
        ];
        for n in secret_names {
            assert!(
                is_secret_env_name_with(n, &s),
                "expected {n} to be classified as secret"
            );
        }
        // Names that should NOT be classified as secret.
        let plain_names = [
            "LOG_LEVEL",
            "RUST_LOG",
            "OPENDERA_PIPELINE_ID",
            "AWS_REGION",
        ];
        for n in plain_names {
            assert!(
                !is_secret_env_name_with(n, &s),
                "did not expect {n} to be classified as secret"
            );
        }
    }

    #[test]
    fn secret_match_is_case_insensitive() {
        let s = default_secret_env_suffixes();
        assert!(is_secret_env_name_with("my_password", &s));
        assert!(is_secret_env_name_with("My_Password", &s));
        assert!(is_secret_env_name_with("MY_PASSWORD", &s));
    }

    #[test]
    fn empty_suffix_list_classifies_nothing_as_secret() {
        // Operators can disable the heuristic by passing an empty
        // suffix list. In that mode no env var is treated as secret;
        // they all flow into the (plaintext) Machine env block.
        assert!(!is_secret_env_name_with("KAFKA_PASSWORD", &[]));
    }
}
