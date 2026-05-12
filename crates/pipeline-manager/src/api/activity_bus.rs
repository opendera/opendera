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

use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::db::types::pipeline::PipelineId;

/// Bus capacity. Sized for ~10 seconds of activity at 100 events/s
/// per pipeline across hundreds of pipelines; well within memory.
const ACTIVITY_BUS_CAPACITY: usize = 4096;

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

/// Sender half of the activity bus. Cheap to clone (it's an `Arc`
/// internally); the convention is to clone into `ServerState` once
/// and pass references everywhere else.
#[derive(Clone)]
pub struct ActivityBus {
    inner: broadcast::Sender<ActivityEvent>,
}

impl ActivityBus {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(ACTIVITY_BUS_CAPACITY);
        Self { inner: tx }
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

    /// Internal helper for callers that want to construct an event
    /// with a non-`Utc::now()` timestamp (replay, deterministic tests).
    pub fn emit_with_ts(
        &self,
        kind: &str,
        pipeline_id: Uuid,
        ts: DateTime<Utc>,
    ) {
        let pid = pipeline_id.to_string();
        let event = match kind {
            "ingested" => ActivityEvent::Ingested { pipeline_id: pid, ts },
            "queried" => ActivityEvent::Queried { pipeline_id: pid, ts },
            "woke" => ActivityEvent::Woke { pipeline_id: pid, ts },
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
