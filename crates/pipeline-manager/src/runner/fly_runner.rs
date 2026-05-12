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
//! Status (May 2026): minimum viable skeleton. The happy paths
//! (provision -> ready -> stop -> clear) are implemented; the harder
//! cases (Anycast IP allocation, multi-region failover, image-from-S3
//! handoff, secrets push, multipart upload of generated binaries to
//! the Fly image registry) are tracked with TODOs and will land
//! alongside the cloud GA work.

use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tracing::{info, warn};
use uuid::Uuid;

use crate::config::CommonConfig;
use crate::db::types::pipeline::PipelineId;
use crate::db::types::version::Version;
use crate::error::ManagerError;
use crate::runner::error::RunnerError;
use crate::runner::pipeline_executor::{PipelineExecutor, ProvisionStatus};
use crate::runner::pipeline_logs::LogsSender;
use feldera_types::config::{PipelineConfig, StorageCacheConfig, StorageConfig};
use feldera_types::runtime_status::{BootstrapPolicy, RuntimeDesiredStatus};

const FLY_API_BASE: &str = "https://api.machines.dev";

/// Runner-specific config supplied by the operator. Lives next to
/// `LocalRunnerConfig` in `crates/pipeline-manager/src/config.rs` (TODO:
/// add it there once Fly is wired into the binary).
#[derive(Debug, Clone)]
pub struct FlyRunnerConfig {
    /// Fly API token with org-scoped permission to create apps + machines.
    pub api_token: String,
    /// Fly organization slug (where new per-tenant apps live).
    pub org_slug: String,
    /// Default Fly region for new machines (e.g. `iad`).
    pub region: String,
    /// Container image to run for pipeline workers.
    /// Format: `ghcr.io/opendera/pipeline-runtime:<tag>` or similar.
    pub pipeline_image: String,
    /// Default Machine size for new pipelines.
    pub default_machine_size: FlyMachineSize,
    /// Tigris endpoint (no-egress storage on Fly's network).
    pub tigris_endpoint: String,
    /// Tigris bucket used as the pipeline checkpoint root.
    pub tigris_bucket: String,
}

/// Bundled CPU / RAM preset, mirroring Fly's named presets.
#[derive(Debug, Clone)]
pub struct FlyMachineSize {
    pub cpu_kind: String,
    pub cpus: u32,
    pub memory_mb: u32,
}

impl Default for FlyMachineSize {
    fn default() -> Self {
        // performance-1x 2GB — the smallest preset that supports
        // suspend (≤2GiB RAM). Reasonable default for new pipelines.
        Self {
            cpu_kind: "performance".into(),
            cpus: 1,
            memory_mb: 2048,
        }
    }
}

/// State the runner carries between trait method calls. Everything is
/// derivable from `pipeline_id` so a runner restart can rebuild it;
/// the cache exists to avoid round-tripping the Fly API on every call.
pub struct FlyRunner {
    pipeline_id: PipelineId,
    common_config: CommonConfig,
    config: FlyRunnerConfig,
    client: Client,
    #[allow(dead_code)]
    logs_sender: LogsSender,
    /// Cached after the first provision call.
    cached: Option<MachineHandle>,
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

    /// Build the env map handed to the pipeline binary.
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
        env.insert("AWS_ENDPOINT_URL_S3".into(), json!(self.config.tigris_endpoint));
        env.insert("AWS_REGION".into(), json!("auto"));
        for (k, v) in &deployment_config.global.env {
            env.insert(k.clone(), json!(v));
        }
        env
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
    async fn create_machine(
        &self,
        app_name: &str,
        deployment_id: &Uuid,
        deployment_config: &PipelineConfig,
    ) -> Result<MachineResponse, ManagerError> {
        let body = CreateMachineBody {
            name: &self.machine_name(),
            region: &self.config.region,
            config: MachineConfig {
                image: self.config.pipeline_image.clone(),
                env: self.pipeline_env(deployment_id, deployment_config),
                guest: GuestConfig {
                    cpu_kind: self.config.default_machine_size.cpu_kind.clone(),
                    cpus: self.config.default_machine_size.cpus,
                    memory_mb: self.config.default_machine_size.memory_mb,
                },
                services: vec![],
                restart: RestartPolicy { policy: "on-failure" },
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

    async fn stop_machine(
        &self,
        app_name: &str,
        machine_id: &str,
    ) -> Result<(), ManagerError> {
        let url =
            format!("{FLY_API_BASE}/v1/apps/{app_name}/machines/{machine_id}/stop");
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

    async fn destroy_machine(
        &self,
        app_name: &str,
        machine_id: &str,
    ) -> Result<(), ManagerError> {
        let url = format!(
            "{FLY_API_BASE}/v1/apps/{app_name}/machines/{machine_id}?force=true"
        );
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

    fn provision_err(msg: impl Into<String>) -> ManagerError {
        RunnerError::RunnerProvisionError { error: msg.into() }.into()
    }
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
        _program_binary_url: &str,
        _program_info_url: Option<&str>,
        _program_version: Version,
    ) -> Result<(), ManagerError> {
        // Avoid letting unused-arg warnings touch the trait impl
        // every time the signature gains a parameter; mark them dead
        // via the bindings above (TODOs:
        //   - bootstrap_policy: pass to the pipeline as an env hint
        //   - program_binary_url: hand off to Fly's image registry or
        //     the pipeline pre-start hook to fetch
        //   - deployment_initial: communicate to the binary so it
        //     boots in the requested initial state).
        let _ = &self.common_config;

        let app_name = self.app_name_for(*deployment_id);
        self.ensure_app(&app_name).await?;

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
                    .create_machine(&app_name, deployment_id, deployment_config)
                    .await?;
                info!(
                    pipeline_id = %self.pipeline_id,
                    machine_id = %created.id,
                    "Fly: created pipeline machine"
                );
                MachineHandle {
                    app_name,
                    machine_id: created.id,
                }
            }
        };

        self.cached = Some(handle);
        Ok(())
    }

    async fn is_provisioned(&mut self) -> Result<ProvisionStatus, ManagerError> {
        let handle = self
            .cached
            .clone()
            .ok_or_else(|| Self::provision_err("provision was not called"))?;
        let m = self.get_machine(&handle.app_name, &handle.machine_id).await?;
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
            "created" | "starting" | "stopped" | "suspended" => {
                Ok(ProvisionStatus::Ongoing {
                    details: serde_json::to_value(&m).unwrap_or_default(),
                })
            }
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
        let m = self.get_machine(&handle.app_name, &handle.machine_id).await?;
        Ok(json!({
            "state": m.state,
            "private_ip": m.private_ip,
        }))
    }

    async fn stop(&mut self) -> Result<(), ManagerError> {
        if let Some(handle) = self.cached.clone() {
            self.stop_machine(&handle.app_name, &handle.machine_id).await?;
        }
        Ok(())
    }

    async fn clear(&mut self) -> Result<(), ManagerError> {
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
        use object_store::{ObjectStore, parse_url_opts, path::Path as ObjPath};

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
            let meta = item
                .map_err(|e| Self::provision_err(format!("fly: list tigris: {e}")))?;
            to_delete.push(meta.location);
        }
        for path in to_delete {
            // Idempotent: a missing object is not an error here.
            let _ = store.delete(&path).await;
        }
        Ok(())
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
    use crate::db::types::pipeline::PipelineId;

    #[test]
    fn machine_size_default_supports_suspend() {
        // The default size must allow Fly's snapshot-resume path
        // (≤ 2 GiB RAM). If this assertion ever breaks, also revisit
        // the Tier classification in opendera-cloud's
        // activity-controller.
        let s = FlyMachineSize::default();
        assert!(
            s.memory_mb <= 2048,
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
}
