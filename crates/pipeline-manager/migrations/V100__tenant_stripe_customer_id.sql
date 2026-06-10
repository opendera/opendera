-- Add stripe_customer_id to the tenant table.
--
-- Used by the cloud-side Stripe metering daemon
-- (opendera-cloud/stripe/) to map an OpenDera tenant onto a Stripe
-- customer. NULL until the tenant signs up for paid billing; the
-- daemon drops usage records whose tenant lacks a Stripe customer id
-- (logged as a warning, no double-billing risk).
--
-- Nullable so existing self-hosted deployments aren't forced into a
-- billing relationship.

--
-- NOTE: originally shipped as V34. Renumbered to V100 (commit on
-- 2026-06-10) to leave the V34+ range free for upstream feldera
-- migrations. `run_migrations` deletes the old (V34, 'tenant_stripe_customer_id')
-- refinery_schema_history row before refinery runs, and the DDL below
-- is idempotent, so databases that applied the old number re-apply
-- this as a no-op.

ALTER TABLE tenant
    ADD COLUMN IF NOT EXISTS stripe_customer_id varchar NULL;
