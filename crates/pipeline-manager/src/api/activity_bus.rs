//! Activity event broadcast bus.
//!
//! Hot-path code (pipeline-manager endpoints that proxy ingest, query,
//! and lifecycle traffic) emits `ActivityEvent`s through this bus.
//! The internal API's SSE handler subscribes for the cloud-side
//! activity controller (see opendera-cloud/activity-controller/).
//!
//! Backed by `tokio::sync::broadcast`: multiple subscribers are OK,
//! slow subscribers are lagged out rather than blocking the producer.
//! When the channel has no subscribers the producers' `send()` calls
//! return `Err(NoReceiver)` which we ignore — the bus is best-effort.
//!
//! The event shape mirrors the `ActivityEvent` discriminated union in
//! opendera-cloud/activity-controller/src/manager-client.ts.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::db::storage::Storage;
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::pipeline::PipelineId;
use crate::db::types::tenant::TenantId;
use feldera_types::runtime_status::RuntimeStatus;

/// Bus capacity. Sized for ~10 seconds of activity at 100 events/s
/// per pipeline across hundreds of pipelines; well within memory.
const ACTIVITY_BUS_CAPACITY: usize = 4096;

/// How long a pipeline name -> id resolution stays cached for hot-path
/// emission. Pipeline ids are stable across renames, so a stale entry
/// only matters if a pipeline is deleted and re-created under the same
/// name within the TTL; the activity controller reconciles against
/// `GET /internal/v0/pipelines` on every poll cycle, so a few seconds
/// of events keyed to the old id are harmless.
const NAME_CACHE_TTL: Duration = Duration::from_secs(10);

/// Discriminated union over the lifecycle events the cloud-side
/// activity controller cares about.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ActivityEvent {
    /// A non-empty batch of input records arrived at the pipeline.
    Ingested {
        pipeline_id: String,
        ts: DateTime<Utc>,
    },
    /// An ad-hoc query was served against the pipeline.
    Queried {
        pipeline_id: String,
        ts: DateTime<Utc>,
    },
    /// A pipeline transitioned out of `Suspended` / `Stopped` and is
    /// running again.
    Woke {
        pipeline_id: String,
        ts: DateTime<Utc>,
    },
    /// The pipeline's `observed_status` changed; the controller can use
    /// this to keep its per-pipeline state machine in sync without
    /// polling.
    StateChanged {
        pipeline_id: String,
        ts: DateTime<Utc>,
        observed: String,
    },
    /// One-shot at startup or config-change: the controller should
    /// treat this pipeline as Always-On (never suspend it).
    AlwaysOn {
        pipeline_id: String,
        ts: DateTime<Utc>,
    },
}

impl ActivityEvent {
    pub fn ingested(pipeline_id: PipelineId) -> Self {
        Self::Ingested {
            pipeline_id: pipeline_id.to_string(),
            ts: Utc::now(),
        }
    }
    pub fn queried(pipeline_id: PipelineId) -> Self {
        Self::Queried {
            pipeline_id: pipeline_id.to_string(),
            ts: Utc::now(),
        }
    }
    pub fn woke(pipeline_id: PipelineId) -> Self {
        Self::Woke {
            pipeline_id: pipeline_id.to_string(),
            ts: Utc::now(),
        }
    }
    pub fn state_changed(pipeline_id: PipelineId, observed: impl Into<String>) -> Self {
        Self::StateChanged {
            pipeline_id: pipeline_id.to_string(),
            ts: Utc::now(),
            observed: observed.into(),
        }
    }
    pub fn always_on(pipeline_id: PipelineId) -> Self {
        Self::AlwaysOn {
            pipeline_id: pipeline_id.to_string(),
            ts: Utc::now(),
        }
    }
}

/// Hot-path event kinds that are emitted for a pipeline addressed by
/// its (tenant, name) pair rather than its id.
#[derive(Copy, Clone, Debug)]
pub enum ActivityEventKind {
    Ingested,
    Queried,
    Woke,
}

/// Cache entry map for hot-path pipeline name -> id resolution.
type NameCache = HashMap<(TenantId, String), (PipelineId, Instant)>;

/// Sender half of the activity bus. Cheap to clone (it's an `Arc`
/// internally); the convention is to clone into `ServerState` once
/// and pass references everywhere else.
#[derive(Clone)]
pub struct ActivityBus {
    inner: broadcast::Sender<ActivityEvent>,
    /// TTL cache for hot-path name -> id resolution; see
    /// [`ActivityBus::emit_for_pipeline_name`].
    name_cache: Arc<StdMutex<NameCache>>,
}

impl ActivityBus {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(ACTIVITY_BUS_CAPACITY);
        Self {
            inner: tx,
            name_cache: Arc::new(StdMutex::new(HashMap::new())),
        }
    }

    /// Hot-path emission for handlers that only know the pipeline by
    /// name. Resolves the name to the stable pipeline id (cached for
    /// `NAME_CACHE_TTL` (10s) to avoid a database round-trip per ingest /
    /// query call), then emits the event. Best-effort: resolution
    /// failures are logged at debug and swallowed, because dropping an
    /// activity event must never fail the user's request.
    pub async fn emit_for_pipeline_name(
        &self,
        db: &tokio::sync::Mutex<StoragePostgres>,
        tenant_id: TenantId,
        pipeline_name: &str,
        kind: ActivityEventKind,
    ) {
        let id = match self.resolve_cached(db, tenant_id, pipeline_name).await {
            Some(id) => id,
            None => return,
        };
        let event = match kind {
            ActivityEventKind::Ingested => ActivityEvent::ingested(id),
            ActivityEventKind::Queried => ActivityEvent::queried(id),
            ActivityEventKind::Woke => ActivityEvent::woke(id),
        };
        self.emit(event);
    }

    /// Looks up `(tenant_id, pipeline_name)` in the TTL cache, falling
    /// back to the database and refreshing the entry on miss.
    async fn resolve_cached(
        &self,
        db: &tokio::sync::Mutex<StoragePostgres>,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Option<PipelineId> {
        let key = (tenant_id, pipeline_name.to_string());
        if let Some((id, inserted)) = self.name_cache.lock().unwrap().get(&key).copied() {
            if inserted.elapsed() < NAME_CACHE_TTL {
                return Some(id);
            }
        }
        match db
            .lock()
            .await
            .get_pipeline_for_monitoring(tenant_id, pipeline_name)
            .await
        {
            Ok(p) => {
                self.name_cache
                    .lock()
                    .unwrap()
                    .insert(key, (p.id, Instant::now()));
                Some(p.id)
            }
            Err(e) => {
                tracing::debug!(
                    "activity emit: failed to resolve {pipeline_name} for tenant {tenant_id}: {e}; skipping"
                );
                None
            }
        }
    }

    /// Fire-and-forget event emit. Returns silently if there are no
    /// subscribers, which is the steady state for non-cloud
    /// deployments.
    pub fn emit(&self, event: ActivityEvent) {
        let _ = self.inner.send(event);
    }

    /// Subscribe to the bus. Slow subscribers are lagged out by the
    /// broadcast channel; they receive `RecvError::Lagged(n)` and
    /// must drop the skipped events.
    pub fn subscribe(&self) -> broadcast::Receiver<ActivityEvent> {
        self.inner.subscribe()
    }

    /// Emits `state_changed` iff the externally observable status
    /// string differs between `old` and `new`. The string mirrors the
    /// `observed_status` derivation in `list_internal_pipelines`, so
    /// the cloud activity controller sees consistent values whether it
    /// reconciles from the polling endpoint or the SSE stream.
    pub fn emit_state_change(
        &self,
        pipeline_id: PipelineId,
        old: Option<RuntimeStatus>,
        new: Option<RuntimeStatus>,
    ) {
        let to_observed = |s: Option<RuntimeStatus>| {
            s.map(crate::api::endpoints::internal::runtime_status_to_str)
                .unwrap_or_else(|| "Unknown".to_string())
        };
        let new_observed = to_observed(new);
        if to_observed(old) != new_observed {
            self.emit(ActivityEvent::state_changed(pipeline_id, new_observed));
        }
    }

    /// Internal helper for callers that want to construct an event
    /// with a non-`Utc::now()` timestamp (replay, deterministic tests).
    pub fn emit_with_ts(&self, kind: &str, pipeline_id: Uuid, ts: DateTime<Utc>) {
        let pid = pipeline_id.to_string();
        let event = match kind {
            "ingested" => ActivityEvent::Ingested {
                pipeline_id: pid,
                ts,
            },
            "queried" => ActivityEvent::Queried {
                pipeline_id: pid,
                ts,
            },
            "woke" => ActivityEvent::Woke {
                pipeline_id: pid,
                ts,
            },
            _ => return,
        };
        self.emit(event);
    }
}

impl Default for ActivityBus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn emit_with_no_subscribers_is_silent() {
        let bus = ActivityBus::new();
        // No panic, no error.
        bus.emit(ActivityEvent::Queried {
            pipeline_id: "p".into(),
            ts: Utc::now(),
        });
    }

    #[tokio::test]
    async fn subscriber_receives_emitted_event() {
        let bus = ActivityBus::new();
        let mut rx = bus.subscribe();
        let pid = "00000000-0000-0000-0000-000000000001"
            .parse::<Uuid>()
            .unwrap();
        bus.emit(ActivityEvent::Ingested {
            pipeline_id: pid.to_string(),
            ts: Utc::now(),
        });
        let evt = rx.recv().await.unwrap();
        match evt {
            ActivityEvent::Ingested { pipeline_id, .. } => {
                assert_eq!(pipeline_id, pid.to_string());
            }
            other => panic!("expected Ingested, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn many_subscribers_all_receive() {
        let bus = ActivityBus::new();
        let mut a = bus.subscribe();
        let mut b = bus.subscribe();
        bus.emit(ActivityEvent::Woke {
            pipeline_id: "p".into(),
            ts: Utc::now(),
        });
        let _ = a.recv().await.unwrap();
        let _ = b.recv().await.unwrap();
    }

    #[test]
    fn event_serializes_with_kind_tag() {
        let event = ActivityEvent::Ingested {
            pipeline_id: "abc".into(),
            ts: Utc::now(),
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["kind"], "ingested");
        assert_eq!(json["pipeline_id"], "abc");
    }
}
