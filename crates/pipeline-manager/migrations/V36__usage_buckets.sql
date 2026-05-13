-- Per-pipeline usage buckets that back GET /internal/v0/usage.
--
-- Each row records one bucket-close event: a fixed-width time window
-- during which the manager observed `amount` units of usage in a single
-- billing dimension on a single pipeline. The Stripe metering daemon
-- (opendera-cloud/stripe/) reads these rows in monotonic
-- bucket_end_ts order and forwards them to Stripe Meter Events,
-- de-duplicating on the natural idempotency key.
--
-- Identifying tuple: (pipeline_id, dim, bucket_end_ts). One row per
-- closed bucket per dimension per pipeline; rolling buckets are not
-- persisted until they close.
--
-- The tenant_id is denormalized from the pipeline row so we can serve
-- the endpoint with a single index lookup and so usage data survives a
-- (hypothetical) pipeline rename or move; the FK on tenant guarantees
-- the denormalized value stays valid. There is NO FK on pipeline_id —
-- we want usage history to outlive the pipeline so a customer can be
-- billed for usage right up to the moment they deleted the pipeline.
--
-- dim takes one of: 'ingestion_gb', 'storage_gb_month', 'fcu_hour',
-- 'query_tb' (the JSON snake_case variants of the UsageDimension enum).

CREATE TABLE IF NOT EXISTS usage_bucket (
    pipeline_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    dim varchar NOT NULL,
    bucket_end_ts timestamptz NOT NULL,
    amount double precision NOT NULL,
    PRIMARY KEY (pipeline_id, dim, bucket_end_ts),
    FOREIGN KEY (tenant_id) REFERENCES tenant(id) ON DELETE CASCADE
);

-- Cursor-paginated reads scan ordered by bucket_end_ts. Index covers
-- the common query shape (`WHERE bucket_end_ts > $since ORDER BY
-- bucket_end_ts`) without needing to consult the row data.
CREATE INDEX IF NOT EXISTS idx_usage_bucket_end_ts
    ON usage_bucket (bucket_end_ts);
