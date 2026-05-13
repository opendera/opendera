-- Persist the Fly Machine handle and resource tier for pipelines that
-- run under the Fly executor.
--
-- These columns are written by the Fly executor when it provisions a
-- Machine for a pipeline (`crates/pipeline-manager/src/runner/fly_runner.rs`)
-- and surfaced over `GET /internal/v0/pipelines` for the cloud-side
-- activity controller (opendera-cloud/activity-controller/), which
-- needs `fly_machine_id` to call Fly's `machines/start|stop|suspend`
-- API on idle.
--
-- All columns are nullable so:
--   1. Self-hosted deployments using the local runner never write them
--      and the column reads as NULL.
--   2. Cloud-mode pipelines created before the Fly executor lands will
--      backfill on next provision; no migration data step required.
--
-- `tier` is a free-form string (e.g. "shared-cpu-1x", "performance-2x")
-- to avoid coupling the schema to Fly's evolving machine catalog.
-- `ram_mb` mirrors Fly's per-machine guest RAM, surfaced for tier-
-- mismatch detection by the right-sizing recommender.

ALTER TABLE pipeline
    ADD COLUMN fly_app varchar NULL,
    ADD COLUMN fly_machine_id varchar NULL,
    ADD COLUMN tier varchar NULL,
    ADD COLUMN ram_mb integer NULL;
