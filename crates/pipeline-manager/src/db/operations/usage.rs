use chrono::{DateTime, Utc};
use deadpool_postgres::Transaction;

use crate::db::error::DBError;
use crate::db::types::pipeline::PipelineId;
use crate::db::types::tenant::TenantId;
use crate::db::types::usage::{UsageBucket, UsageDimension};

/// Insert (or upsert) a closed usage bucket. The natural key is
/// (pipeline_id, dim, bucket_end_ts); a second insert with the same
/// key replaces the amount in place. This makes the writer side
/// idempotent so the manager can retry a failed write without
/// double-counting.
pub async fn insert_usage_bucket(
    txn: &Transaction<'_>,
    tenant_id: TenantId,
    pipeline_id: PipelineId,
    dim: UsageDimension,
    bucket_end_ts: DateTime<Utc>,
    amount: f64,
) -> Result<(), DBError> {
    let stmt = txn
        .prepare_cached(
            "INSERT INTO usage_bucket (pipeline_id, tenant_id, dim, bucket_end_ts, amount) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (pipeline_id, dim, bucket_end_ts) DO UPDATE SET amount = EXCLUDED.amount",
        )
        .await?;
    txn.execute(
        &stmt,
        &[
            &pipeline_id.0,
            &tenant_id.0,
            &dim.as_str(),
            &bucket_end_ts,
            &amount,
        ],
    )
    .await?;
    Ok(())
}

/// Read up to `limit` usage buckets, oldest first, with
/// `bucket_end_ts > since` (or from the start when `since` is `None`).
/// Order is stable: `(bucket_end_ts, pipeline_id, dim)` ascending so a
/// cursor on `bucket_end_ts` never skips rows that share a timestamp.
///
/// Rows whose `dim` column is not one of the known
/// [`UsageDimension`] variants are silently skipped. This is the
/// forward-compat policy: if a future migration adds a new dimension,
/// old daemon binaries still see well-formed data instead of failing.
pub async fn list_usage_buckets(
    txn: &Transaction<'_>,
    since: Option<DateTime<Utc>>,
    limit: i64,
) -> Result<Vec<UsageBucket>, DBError> {
    let stmt = txn
        .prepare_cached(
            "SELECT pipeline_id, tenant_id, dim, bucket_end_ts, amount \
             FROM usage_bucket \
             WHERE ($1::timestamptz IS NULL OR bucket_end_ts > $1) \
             ORDER BY bucket_end_ts ASC, pipeline_id ASC, dim ASC \
             LIMIT $2",
        )
        .await?;
    let rows = txn.query(&stmt, &[&since, &limit]).await?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let dim_str: String = row.get(2);
        let Some(dim) = UsageDimension::parse(&dim_str) else {
            continue;
        };
        out.push(UsageBucket {
            pipeline_id: PipelineId(row.get(0)),
            tenant_id: TenantId(row.get(1)),
            dim,
            bucket_end_ts: row.get(3),
            amount: row.get(4),
        });
    }
    Ok(out)
}
