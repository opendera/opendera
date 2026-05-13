//! OCI image push for per-pipeline Fly Machine images.
//!
//! Closes the loop between the Rust compiler and the Fly runner: the
//! compiler produces a single Rust binary; this module packages that
//! binary as the top layer of an OCI image on top of a shared base
//! image and pushes the result to `registry.fly.io/<app>:<tag>`. The
//! Fly runner then references that tag as the Machine's `config.image`.
//!
//! Why a "rebase" instead of a from-scratch image?
//!
//! Pipeline binaries are dynamically linked against glibc + libssl +
//! libstdc++. Shipping a from-scratch image would force a static
//! musl-glibc build of the whole DBSP runtime, which is not the build
//! we ship today. Cheaper: take a known base image (`pipeline_image`
//! in `FlyRunnerConfig`) that already carries the runtime ABI, add one
//! tiny layer containing just `/usr/local/bin/opendera-pipeline`, and
//! push a fresh tag. Fly's registry deduplicates blobs across tags so
//! the base layers are uploaded exactly once across the whole tenant.
//!
//! Registry protocol: OCI Distribution Spec v1.1 (formerly Docker
//! Registry HTTP API v2). The interesting endpoints used here:
//!
//! - `GET  /v2/<repo>/manifests/<ref>`                 — pull manifest
//! - `GET  /v2/<repo>/blobs/<digest>`                  — pull config blob
//! - `HEAD /v2/<repo>/blobs/<digest>`                  — blob-exists check
//! - `POST /v2/<repo>/blobs/uploads/?mount=<digest>&from=<repo>` — cross-repo mount
//! - `POST /v2/<repo>/blobs/uploads/`                  — start blob upload
//! - `PUT  <location>?digest=<digest>`                 — finish blob upload (monolithic)
//! - `PUT  /v2/<repo>/manifests/<tag>`                 — push manifest
//!
//! Cross-repo mounting matters: when the base image lives in the same
//! Fly registry under a different repo, the registry can move the
//! blob by reference instead of accepting a fresh upload. The push
//! flow tries that first and falls back to a real upload.
//!
//! Auth: Fly's registry accepts Basic auth where username can be any
//! non-empty string and password is the org-scoped Fly API token —
//! the same token the rest of `fly_runner.rs` already uses.

use std::io::Write;
use std::time::Duration as StdDuration;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use flate2::write::GzEncoder;
use flate2::Compression;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tracing::{debug, info, warn};

use crate::config::FlyImageRegistryConfig;
use crate::error::ManagerError;
use crate::runner::error::RunnerError;

/// HTTP client wrapper that knows how to push images to the Fly
/// registry. Stateless across calls; safe to share via `Arc`/`Clone`
/// or to instantiate per-push.
#[derive(Clone)]
pub struct FlyImagePusher {
    client: Client,
    cfg: FlyImageRegistryConfig,
    auth_header: String,
}

impl FlyImagePusher {
    pub fn new(client: Client, cfg: FlyImageRegistryConfig, fly_api_token: &str) -> Self {
        // Fly's registry accepts Basic auth with username=`x` and
        // password=<fly_api_token>. Anything works for the username —
        // we use `x` to match Fly's documented login string.
        let auth_header = format!("Basic {}", B64.encode(format!("x:{fly_api_token}")));
        Self {
            client,
            cfg,
            auth_header,
        }
    }

    /// Build a per-pipeline image, push it, and return the resulting
    /// image reference suitable for `MachineConfig.image`.
    ///
    /// The tag is fully determined by the integrity checksum of the
    /// binary, so two pushes of the same binary collide on the same
    /// tag (and the second one is essentially a no-op thanks to the
    /// blob-exists short-circuit in `upload_blob`).
    pub async fn push_pipeline_image(
        &self,
        binary: &[u8],
        binary_digest_hex: &str,
    ) -> Result<String, ManagerError> {
        // 1. Parse base image reference.
        let base = ImageRef::parse(&self.cfg.base_image)?;

        // 2. Pull base manifest + config so we can extend them.
        let base_manifest = self.pull_manifest(&base).await?;
        let base_config_bytes = self.pull_blob(&base, &base_manifest.config.digest).await?;
        let mut base_config: ImageConfigBlob = serde_json::from_slice(&base_config_bytes)
            .map_err(|e| push_err(format!("registry: parse base config: {e}")))?;

        // 3. Build our one extra layer (tar.gz of /<install_path>).
        let layer = build_binary_layer(binary, self.cfg.binary_install_path.as_str())?;

        // 4. Build the new config blob — base + appended diff_id + history.
        base_config
            .rootfs
            .diff_ids
            .push(layer.diff_id_digest.clone());
        base_config.history.push(json!({
            "created": chrono::Utc::now().to_rfc3339(),
            "created_by": format!("opendera: add pipeline binary at {}", self.cfg.binary_install_path.as_str()),
        }));
        let new_config_bytes = serde_json::to_vec(&base_config)
            .map_err(|e| push_err(format!("registry: serialize new config: {e}")))?;
        let new_config_digest = sha256_hex(&new_config_bytes);

        // 5. Push blobs. Each base layer is mounted from the base
        //    repo (cross-repo mount, no upload); the new layer and
        //    the new config are uploaded.
        let target_repo = &self.cfg.target_repo;
        for descriptor in &base_manifest.layers {
            self.cross_mount_or_check(target_repo, &descriptor.digest, &base.repo)
                .await?;
        }
        self.upload_blob(target_repo, &layer.tar_gz, &layer.tar_gz_digest)
            .await?;
        self.upload_blob(target_repo, &new_config_bytes, &new_config_digest)
            .await?;

        // 6. Assemble + push the new manifest.
        let mut new_layers = base_manifest.layers.clone();
        new_layers.push(LayerDescriptor {
            media_type: layer_media_type(&base_manifest.layers).into(),
            digest: layer.tar_gz_digest.clone(),
            size: layer.tar_gz.len() as u64,
        });
        let new_manifest = ImageManifest {
            schema_version: 2,
            media_type: base_manifest.media_type.clone(),
            config: ConfigDescriptor {
                media_type: base_manifest.config.media_type.clone(),
                digest: new_config_digest,
                size: new_config_bytes.len() as u64,
            },
            layers: new_layers,
        };

        let tag = format!(
            "p-{}",
            &binary_digest_hex[..16.min(binary_digest_hex.len())]
        );
        let image_ref = format!(
            "{}/{}:{}",
            self.cfg.registry_host, self.cfg.target_repo, tag
        );
        self.push_manifest(target_repo, &tag, &new_manifest).await?;

        info!(
            base_image = %self.cfg.base_image,
            pushed = %image_ref,
            layer_size = layer.tar_gz.len(),
            "registry: pushed pipeline image"
        );
        Ok(image_ref)
    }

    /// `GET /v2/<repo>/manifests/<ref>`.
    async fn pull_manifest(&self, image: &ImageRef) -> Result<ImageManifest, ManagerError> {
        let url = format!(
            "https://{}/v2/{}/manifests/{}",
            image.registry, image.repo, image.reference
        );
        let res = self
            .client
            .get(&url)
            .header("Authorization", &self.auth_header)
            // Accept both Docker v2 and OCI manifest media types. We
            // ignore manifest *lists* (multi-arch indexes) today;
            // pipeline workers are linux/amd64 only.
            .header(
                "Accept",
                "application/vnd.docker.distribution.manifest.v2+json, \
                 application/vnd.oci.image.manifest.v1+json",
            )
            .timeout(StdDuration::from_secs(30))
            .send()
            .await
            .map_err(|e| push_err(format!("registry: pull manifest {url}: {e}")))?;
        if !res.status().is_success() {
            return Err(push_err(format!(
                "registry: pull manifest {url}: status {}",
                res.status()
            )));
        }
        let body = res
            .bytes()
            .await
            .map_err(|e| push_err(format!("registry: pull manifest body: {e}")))?;
        serde_json::from_slice(&body)
            .map_err(|e| push_err(format!("registry: parse manifest: {e}")))
    }

    /// `GET /v2/<repo>/blobs/<digest>`.
    async fn pull_blob(&self, image: &ImageRef, digest: &str) -> Result<Vec<u8>, ManagerError> {
        let url = format!(
            "https://{}/v2/{}/blobs/{}",
            image.registry, image.repo, digest
        );
        let res = self
            .client
            .get(&url)
            .header("Authorization", &self.auth_header)
            .timeout(StdDuration::from_secs(60))
            .send()
            .await
            .map_err(|e| push_err(format!("registry: pull blob {url}: {e}")))?;
        if !res.status().is_success() {
            return Err(push_err(format!(
                "registry: pull blob {url}: status {}",
                res.status()
            )));
        }
        let bytes = res
            .bytes()
            .await
            .map_err(|e| push_err(format!("registry: pull blob body: {e}")))?;
        Ok(bytes.to_vec())
    }

    /// Try a cross-repo mount; if the blob isn't movable that way,
    /// fall back to a HEAD check. The registry returns 201 on a
    /// successful mount and 202 when it wants a real upload. We
    /// treat 202 + 404 as "fallback to upload" and let the caller
    /// proceed with `upload_blob` if needed.
    async fn cross_mount_or_check(
        &self,
        target_repo: &str,
        digest: &str,
        from_repo: &str,
    ) -> Result<(), ManagerError> {
        // Fast path: HEAD — if the blob is already in target, nothing
        // to do. Saves a round-trip in repeated pushes.
        let head_url = format!(
            "https://{}/v2/{}/blobs/{}",
            self.cfg.registry_host, target_repo, digest
        );
        let head = self
            .client
            .head(&head_url)
            .header("Authorization", &self.auth_header)
            .timeout(StdDuration::from_secs(15))
            .send()
            .await
            .map_err(|e| push_err(format!("registry: HEAD blob: {e}")))?;
        if head.status().is_success() {
            debug!(digest, "registry: blob already present, skip mount");
            return Ok(());
        }

        let mount_url = format!(
            "https://{}/v2/{}/blobs/uploads/?mount={}&from={}",
            self.cfg.registry_host, target_repo, digest, from_repo
        );
        let res = self
            .client
            .post(&mount_url)
            .header("Authorization", &self.auth_header)
            .timeout(StdDuration::from_secs(30))
            .send()
            .await
            .map_err(|e| push_err(format!("registry: mount blob: {e}")))?;
        match res.status() {
            StatusCode::CREATED => Ok(()),
            // 202 means the registry didn't mount and is prepared to
            // accept a real upload at the returned `Location`. We
            // can't usefully complete that without the source blob
            // bytes — which we don't have without round-tripping
            // through the base registry. For now, warn and continue;
            // the manifest push will fail later if the blob really
            // isn't reachable, and the operator will see a clear
            // error pointing at this specific layer.
            StatusCode::ACCEPTED => {
                warn!(
                    digest,
                    from_repo,
                    target_repo,
                    "registry: cross-repo mount declined; downstream manifest push \
                     may fail if the blob is not otherwise present"
                );
                Ok(())
            }
            other => Err(push_err(format!(
                "registry: mount blob {digest}: status {other}"
            ))),
        }
    }

    /// Upload a blob using the monolithic-PUT flow:
    ///   POST /v2/<repo>/blobs/uploads/   → 202 with Location
    ///   PUT  <Location>?digest=<digest>  → 201
    async fn upload_blob(
        &self,
        target_repo: &str,
        body: &[u8],
        digest: &str,
    ) -> Result<(), ManagerError> {
        // Short-circuit if the blob already exists. This is the path
        // hit by re-pushing the same binary or the same base config.
        let head_url = format!(
            "https://{}/v2/{}/blobs/{}",
            self.cfg.registry_host, target_repo, digest
        );
        let head = self
            .client
            .head(&head_url)
            .header("Authorization", &self.auth_header)
            .timeout(StdDuration::from_secs(15))
            .send()
            .await
            .map_err(|e| push_err(format!("registry: HEAD blob: {e}")))?;
        if head.status().is_success() {
            debug!(digest, "registry: blob already present, skip upload");
            return Ok(());
        }

        let start_url = format!(
            "https://{}/v2/{}/blobs/uploads/",
            self.cfg.registry_host, target_repo
        );
        let start = self
            .client
            .post(&start_url)
            .header("Authorization", &self.auth_header)
            .timeout(StdDuration::from_secs(30))
            .send()
            .await
            .map_err(|e| push_err(format!("registry: start upload: {e}")))?;
        if start.status() != StatusCode::ACCEPTED {
            return Err(push_err(format!(
                "registry: start upload: status {}",
                start.status()
            )));
        }
        let location = start
            .headers()
            .get("Location")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| push_err("registry: start upload: no Location header"))?
            .to_string();
        // The Location may be relative or absolute. Normalize to absolute.
        let put_base = if location.starts_with("http") {
            location
        } else {
            format!("https://{}{}", self.cfg.registry_host, location)
        };
        let put_url = if put_base.contains('?') {
            format!("{put_base}&digest={digest}")
        } else {
            format!("{put_base}?digest={digest}")
        };

        let res = self
            .client
            .put(&put_url)
            .header("Authorization", &self.auth_header)
            .header("Content-Type", "application/octet-stream")
            .header("Content-Length", body.len().to_string())
            .body(body.to_vec())
            .timeout(StdDuration::from_secs(300))
            .send()
            .await
            .map_err(|e| push_err(format!("registry: finish upload: {e}")))?;
        if !res.status().is_success() {
            let status = res.status();
            let body = res.text().await.unwrap_or_default();
            return Err(push_err(format!(
                "registry: finish upload: status {status}: {body}"
            )));
        }
        Ok(())
    }

    /// `PUT /v2/<repo>/manifests/<tag>` with the manifest JSON.
    async fn push_manifest(
        &self,
        target_repo: &str,
        tag: &str,
        manifest: &ImageManifest,
    ) -> Result<(), ManagerError> {
        let body = serde_json::to_vec(manifest)
            .map_err(|e| push_err(format!("registry: serialize manifest: {e}")))?;
        let url = format!(
            "https://{}/v2/{}/manifests/{}",
            self.cfg.registry_host, target_repo, tag
        );
        let res = self
            .client
            .put(&url)
            .header("Authorization", &self.auth_header)
            .header("Content-Type", &manifest.media_type)
            .header("Content-Length", body.len().to_string())
            .body(body)
            .timeout(StdDuration::from_secs(60))
            .send()
            .await
            .map_err(|e| push_err(format!("registry: push manifest: {e}")))?;
        if !res.status().is_success() {
            let status = res.status();
            let body = res.text().await.unwrap_or_default();
            return Err(push_err(format!(
                "registry: push manifest: status {status}: {body}"
            )));
        }
        Ok(())
    }
}

/// Parsed image reference: `host[/path]/repo:tag` or `host/repo@sha256:...`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ImageRef {
    registry: String,
    repo: String,
    reference: String,
}

impl ImageRef {
    fn parse(s: &str) -> Result<Self, ManagerError> {
        // The reference grammar is non-trivial (registry inferred when
        // omitted, etc.). We require a fully qualified ref here —
        // operator config supplies it — so the parse is simple.
        let (head, reference) = if let Some((h, t)) = s.rsplit_once('@') {
            (h, t.to_string())
        } else if let Some((h, t)) = s.rsplit_once(':') {
            // Be careful: a `:` inside the registry host (port) is
            // not the tag separator. The tag separator is the LAST
            // `:` that comes AFTER the last `/`.
            if t.contains('/') {
                (s, "latest".to_string())
            } else {
                (h, t.to_string())
            }
        } else {
            (s, "latest".to_string())
        };
        let (registry, repo) = head.split_once('/').ok_or_else(|| {
            push_err(format!(
                "registry: image ref {s:?} is missing a registry host"
            ))
        })?;
        if registry.is_empty() || repo.is_empty() {
            return Err(push_err(format!("registry: malformed image ref {s:?}")));
        }
        Ok(Self {
            registry: registry.to_string(),
            repo: repo.to_string(),
            reference,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageManifest {
    #[serde(rename = "schemaVersion")]
    schema_version: u32,
    #[serde(rename = "mediaType")]
    media_type: String,
    config: ConfigDescriptor,
    layers: Vec<LayerDescriptor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigDescriptor {
    #[serde(rename = "mediaType")]
    media_type: String,
    digest: String,
    size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayerDescriptor {
    #[serde(rename = "mediaType")]
    media_type: String,
    digest: String,
    size: u64,
}

/// Minimal subset of the OCI image config blob we need to extend. The
/// real blob has many more fields; `#[serde(flatten)]` + a `Value`
/// catch-all preserves them on round-trip.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ImageConfigBlob {
    rootfs: RootFs,
    #[serde(default)]
    history: Vec<Value>,
    #[serde(flatten)]
    other: serde_json::Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RootFs {
    #[serde(rename = "type")]
    fs_type: String,
    diff_ids: Vec<String>,
}

/// Result of building the binary's tar.gz layer.
#[derive(Debug)]
pub struct BuiltLayer {
    /// gzipped tar bytes (the layer payload as it lives in the
    /// registry).
    pub tar_gz: Vec<u8>,
    /// `sha256:...` of `tar_gz` (the layer's blob digest).
    pub tar_gz_digest: String,
    /// `sha256:...` of the *uncompressed* tar (the diff_id used by
    /// rootfs).
    pub diff_id_digest: String,
}

/// Produce a single-entry tar.gz containing `binary` at
/// `install_path`, mode 0755. Both digests (compressed + diff_id) are
/// returned together so callers don't have to recompute either.
pub fn build_binary_layer(binary: &[u8], install_path: &str) -> Result<BuiltLayer, ManagerError> {
    // Strip leading '/' so `tar` writes a relative entry — that's the
    // OCI convention; the layer is unpacked from the root.
    let tar_path = install_path.trim_start_matches('/');
    if tar_path.is_empty() {
        return Err(push_err(format!(
            "registry: install_path {install_path:?} must not be empty"
        )));
    }

    // Build tar in memory.
    let mut tar_bytes = Vec::with_capacity(binary.len() + 1024);
    {
        let mut builder = tar::Builder::new(&mut tar_bytes);
        let mut header = tar::Header::new_gnu();
        header
            .set_path(tar_path)
            .map_err(|e| push_err(format!("registry: tar set_path {tar_path:?}: {e}")))?;
        header.set_size(binary.len() as u64);
        header.set_mode(0o755);
        header.set_mtime(0);
        header.set_cksum();
        builder
            .append(&header, binary)
            .map_err(|e| push_err(format!("registry: tar append: {e}")))?;
        builder
            .finish()
            .map_err(|e| push_err(format!("registry: tar finish: {e}")))?;
    }
    let diff_id_digest = format!("sha256:{}", sha256_hex_raw(&tar_bytes));

    // Gzip the tar.
    let mut gz_bytes = Vec::with_capacity(tar_bytes.len() / 2);
    {
        let mut encoder = GzEncoder::new(&mut gz_bytes, Compression::default());
        encoder
            .write_all(&tar_bytes)
            .map_err(|e| push_err(format!("registry: gzip write: {e}")))?;
        encoder
            .finish()
            .map_err(|e| push_err(format!("registry: gzip finish: {e}")))?;
    }
    let tar_gz_digest = format!("sha256:{}", sha256_hex_raw(&gz_bytes));

    Ok(BuiltLayer {
        tar_gz: gz_bytes,
        tar_gz_digest,
        diff_id_digest,
    })
}

/// `sha256:<hex>` digest in the OCI-canonical form.
pub fn sha256_hex(bytes: &[u8]) -> String {
    format!("sha256:{}", sha256_hex_raw(bytes))
}

fn sha256_hex_raw(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex_encode(&h.finalize())
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}
const HEX: &[u8; 16] = b"0123456789abcdef";

/// Pick the layer media type matching the base manifest's existing
/// layers. Mixing OCI + Docker media types inside one manifest
/// confuses some registries; sticking with the base's convention
/// avoids the issue.
fn layer_media_type(base_layers: &[LayerDescriptor]) -> &str {
    base_layers
        .iter()
        .map(|l| l.media_type.as_str())
        .next()
        .unwrap_or("application/vnd.oci.image.layer.v1.tar+gzip")
}

fn push_err(msg: impl Into<String>) -> ManagerError {
    RunnerError::RunnerProvisionError { error: msg.into() }.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_image_ref_with_tag() {
        let r = ImageRef::parse("registry.fly.io/opendera/base:v1").unwrap();
        assert_eq!(r.registry, "registry.fly.io");
        assert_eq!(r.repo, "opendera/base");
        assert_eq!(r.reference, "v1");
    }

    #[test]
    fn parse_image_ref_with_digest() {
        let r = ImageRef::parse(
            "registry.fly.io/opendera/base@sha256:0011223344556677889900aabbccddeeff",
        )
        .unwrap();
        assert_eq!(r.registry, "registry.fly.io");
        assert_eq!(r.repo, "opendera/base");
        assert_eq!(r.reference, "sha256:0011223344556677889900aabbccddeeff");
    }

    #[test]
    fn parse_image_ref_with_port_in_host_no_tag() {
        // host:5000/repo (no tag) — the `:5000` must NOT be parsed as
        // a tag because it sits before the last '/'.
        let r = ImageRef::parse("localhost:5000/opendera/base").unwrap();
        assert_eq!(r.registry, "localhost:5000");
        assert_eq!(r.repo, "opendera/base");
        assert_eq!(r.reference, "latest");
    }

    #[test]
    fn parse_image_ref_with_port_and_tag() {
        let r = ImageRef::parse("localhost:5000/opendera/base:v2").unwrap();
        assert_eq!(r.registry, "localhost:5000");
        assert_eq!(r.repo, "opendera/base");
        assert_eq!(r.reference, "v2");
    }

    #[test]
    fn sha256_hex_canonical_form() {
        assert_eq!(
            sha256_hex(b""),
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256_hex(b"hello"),
            "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn build_layer_produces_valid_tar_gz_with_binary_at_install_path() {
        let binary = b"\x7fELF...not really an elf but good enough";
        let layer = build_binary_layer(binary, "/usr/local/bin/pipeline").unwrap();
        assert!(layer.tar_gz_digest.starts_with("sha256:"));
        assert!(layer.diff_id_digest.starts_with("sha256:"));
        assert_ne!(layer.tar_gz_digest, layer.diff_id_digest);

        // Decompress + re-read the tar to verify the binary is at the
        // requested path with the right mode and contents.
        use flate2::read::GzDecoder;
        let tar = GzDecoder::new(&layer.tar_gz[..]);
        let mut archive = tar::Archive::new(tar);
        let mut entries: Vec<(String, u32, Vec<u8>)> = Vec::new();
        for entry in archive.entries().unwrap() {
            let mut e = entry.unwrap();
            let path = e.path().unwrap().to_string_lossy().into_owned();
            let mode = e.header().mode().unwrap();
            let mut buf = Vec::new();
            std::io::Read::read_to_end(&mut e, &mut buf).unwrap();
            entries.push((path, mode, buf));
        }
        assert_eq!(entries.len(), 1, "exactly one entry");
        let (path, mode, contents) = &entries[0];
        assert_eq!(path, "usr/local/bin/pipeline");
        assert_eq!(*mode, 0o755);
        assert_eq!(contents, &binary.to_vec());
    }

    #[test]
    fn build_layer_rejects_root_install_path() {
        let err = build_binary_layer(b"x", "/").unwrap_err();
        let msg = format!("{:?}", err);
        assert!(msg.contains("must not be empty"), "got: {msg}");
    }

    #[test]
    fn layer_media_type_matches_base_oci() {
        let base = vec![LayerDescriptor {
            media_type: "application/vnd.oci.image.layer.v1.tar+gzip".into(),
            digest: "sha256:00".into(),
            size: 1,
        }];
        assert_eq!(
            layer_media_type(&base),
            "application/vnd.oci.image.layer.v1.tar+gzip"
        );
    }

    #[test]
    fn layer_media_type_matches_base_docker() {
        let base = vec![LayerDescriptor {
            media_type: "application/vnd.docker.image.rootfs.diff.tar.gzip".into(),
            digest: "sha256:00".into(),
            size: 1,
        }];
        assert_eq!(
            layer_media_type(&base),
            "application/vnd.docker.image.rootfs.diff.tar.gzip"
        );
    }

    #[test]
    fn config_enabled_requires_all_three_required_fields() {
        let full = FlyImageRegistryConfig {
            registry_host: "registry.fly.io".into(),
            base_image: "registry.fly.io/base:v1".into(),
            target_repo: "opendera-pipelines".into(),
            binary_install_path: "/usr/local/bin/opendera-pipeline".into(),
        };
        assert!(full.enabled());

        let missing_host = FlyImageRegistryConfig {
            registry_host: "".into(),
            ..full.clone()
        };
        assert!(!missing_host.enabled());

        let missing_base = FlyImageRegistryConfig {
            base_image: "".into(),
            ..full.clone()
        };
        assert!(!missing_base.enabled());

        let missing_repo = FlyImageRegistryConfig {
            target_repo: "".into(),
            ..full
        };
        assert!(!missing_repo.enabled());
    }
}
