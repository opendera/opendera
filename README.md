<h1 align="center">OpenDera</h1>

<p align="center">
  <em>An MIT-licensed, fully self-hostable streaming SQL engine.</em>
</p>

<p align="center">
  <a href="https://opensource.org/licenses/MIT">
    <img src="https://img.shields.io/badge/License-MIT-green.svg" alt="MIT License">
  </a>
  <a href="https://github.com/opendera/opendera">
    <img src="https://img.shields.io/badge/repo-opendera/opendera-blue" alt="repo">
  </a>
</p>

---

## What is OpenDera?

OpenDera is a fork of [Feldera](https://github.com/feldera/feldera) — the
fast incremental SQL query engine built on Database Stream Processor
(DBSP). The fork exists to offer a single, fully MIT-licensed
distribution: every enterprise feature Feldera gates behind a commercial
license is reimplemented here, clean-room, in the open.

Upstream Feldera is dual-licensed: MIT for the OSS core, proprietary
"Feldera Enterprise" for features like S3-backed persistence, multi-node
fault tolerance, SSO, and secrets management. OpenDera removes that
split — everything is MIT.

Credit and attribution belong to the original Feldera team for the DBSP
engine, the Calcite-based SQL compiler, the adapters, and the
pipeline-manager scaffolding this project is built on.

## Status

OpenDera is **early**. The clean-room reimplementation of Feldera
Enterprise features is in progress. Current state:

| Feature | OpenDera status |
|---|---|
| Object-store storage backend (S3 / GCS / Azure / HTTP) | done — with multipart upload + provider auto-detection |
| Checkpoint synchronization (push / pull / GC / standby) | done — with bounded retry + exponential backoff |
| Pipeline-side `/checkpoint`, `/checkpoint/sync`, `/sync_status`, `/activate` | functional via existing OSS handlers |
| Graceful stop `/stop?force=false` → `/suspend` | functional via existing OSS handlers |
| Fault tolerance | not yet — config rejected at validation time |
| SQL compiler `--enterprise` flag | not yet (depends on fault tolerance) |
| License module + cloud1 client | deleted |

The spec that drives the remaining work is in
[`ENTERPRISE_FEATURES.md`](./ENTERPRISE_FEATURES.md). It will be
deleted in a final commit once every section is implemented.

## What OpenDera is good for

The same workloads Feldera is good for:

- **Continuously updated materialized views over streaming data.**
  Define a pipeline as a set of SQL tables and views; OpenDera processes
  changes (inserts, updates, deletes) and incrementally maintains every
  view without recomputing over history.
- **Unified batch + streaming compute.** The same SQL works against live
  and historical data.
- **Feature engineering, real-time analytics, ETL.** Millions of events
  per second on a laptop with no tuning.

## Engine

OpenDera ships the DBSP engine, the Calcite-based SQL-to-DBSP compiler,
the adapter framework (Kafka, HTTP, S3, Delta, Postgres CDC, …), the
ad-hoc query layer (DataFusion), and the pipeline-manager — all
MIT-licensed.

## Building from source

The build tree mirrors Feldera's:

```bash
# Java SQL compiler (downloads Apache Calcite)
(cd sql-to-dbsp-compiler && ./build.sh)

# Rust workspace
cargo build

# Run pipeline-manager with embedded Postgres
cargo run --bin=pipeline-manager
# Web console served at http://localhost:8080
```

See [`CONTRIBUTING.md`](./CONTRIBUTING.md) for the full toolchain
inventory (Rust, Java 21, Bun, uv) and the workflow for running tests.

## Repos

- [`opendera/opendera`](https://github.com/opendera/opendera) — this
  repo. The engine + pipeline-manager + console + SDKs.
- [`opendera/opendera-cloud`](https://github.com/opendera/opendera-cloud) — private SaaS plumbing
  for the [`opendera.com`](https://opendera.com) managed offering.

## License

MIT. See [`LICENSE`](./LICENSE).
