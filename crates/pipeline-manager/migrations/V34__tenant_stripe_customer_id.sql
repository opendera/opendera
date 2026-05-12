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

ALTER TABLE tenant
    ADD COLUMN stripe_customer_id varchar NULL;
