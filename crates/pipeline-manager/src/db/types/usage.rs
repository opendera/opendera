use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::db::types::pipeline::PipelineId;
use crate::db::types::tenant::TenantId;

/// One closed usage bucket — the unit of billing-grade usage telemetry
/// the manager records for each running pipeline. Persisted in the
/// `usage_bucket` table by [`Storage::insert_usage_bucket`] when the
/// usage collector closes a wall-clock-aligned bucket; consumed by the
/// cloud-side Stripe metering daemon via GET /internal/v0/usage.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UsageBucket {
    pub pipeline_id: PipelineId,
    pub tenant_id: TenantId,
    pub dim: UsageDimension,
    pub bucket_end_ts: DateTime<Utc>,
    pub amount: f64,
}

/// Billing dimensions tracked per pipeline. Wire format is snake_case
/// — the Stripe meter event names hardcoded in
/// `opendera-cloud/stripe/src/stripe-client.ts` derive from these
/// variants (e.g. `opendera_ingestion_gb`).
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageDimension {
    IngestionGb,
    StorageGbMonth,
    FcuHour,
    QueryTb,
}

impl UsageDimension {
    /// Stable string form persisted in the `usage_bucket.dim` column
    /// and used as the wire format on /internal/v0/usage. Matches
    /// `#[serde(rename_all = "snake_case")]`.
    pub fn as_str(self) -> &'static str {
        match self {
            UsageDimension::IngestionGb => "ingestion_gb",
            UsageDimension::StorageGbMonth => "storage_gb_month",
            UsageDimension::FcuHour => "fcu_hour",
            UsageDimension::QueryTb => "query_tb",
        }
    }

    /// Parse the column value back into a [`UsageDimension`]. Returns
    /// `None` for unknown strings — callers may want to skip
    /// forward-incompatible rows rather than fail the whole query.
    pub fn from_str(s: &str) -> Option<Self> {
        Some(match s {
            "ingestion_gb" => UsageDimension::IngestionGb,
            "storage_gb_month" => UsageDimension::StorageGbMonth,
            "fcu_hour" => UsageDimension::FcuHour,
            "query_tb" => UsageDimension::QueryTb,
            _ => return None,
        })
    }
}
