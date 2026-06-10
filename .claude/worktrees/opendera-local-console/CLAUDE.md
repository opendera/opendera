# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Work Workflow (worktrees)

When working on *anything* that modifies the repo — features, bug fixes,
refactors, docs, chores, experiments — follow this workflow:

1. **Use a worktree** — isolate the work in a git worktree (e.g. via the
   `EnterWorktree` tool when available, or `git worktree add`). Do not
   make changes directly in the user's main working copy.
2. **Commit as you go** — make incremental commits within the worktree
   as logical units of work complete. Do not batch everything into a
   single end-of-task commit.
3. **When done**, and only when no remaining work is left:
   - Merge the worktree branch back into `main`.
   - **Never rebase.** Use a merge commit (`git merge --no-ff` if a
     fast-forward would otherwise occur) if a merge commit is needed.
   - Clean up the worktree (`git worktree remove ...`) and delete the
     working branch.

## Repository orientation

Feldera is an incremental SQL query engine. The repository combines a Rust runtime, a Java/Calcite SQL compiler that
emits Rust code, a Svelte web console, and a Python SDK in one tree.

- `crates/dbsp` — DBSP, the incremental computation engine (streams, circuits, indexed Z-sets, operators).
- `crates/sqllib`, `crates/fxp`, `crates/sqllib`, `crates/iceberg`, `crates/storage`, `crates/buffer-cache` —
  runtime support libraries used by generated pipeline code.
- `crates/adapters` — I/O controller plus input/output transports (Kafka, HTTP, S3, Delta, Postgres CDC, etc.),
  formats (JSON, CSV, Avro, Parquet), and the ad-hoc query layer (DataFusion).
- `crates/pipeline-manager` — Actix-based control plane. Owns the REST API (`api/`), Postgres-backed metadata
  (`db/`), the compiler driver (`compiler/`), and the pipeline runner (`runner/`). Embeds the built web console as
  static assets and serves OpenAPI/Swagger.
- `crates/rest-api`, `crates/feldera-types` — types shared between the manager, generated clients, and pipeline.
- `crates/fda` — `fda` CLI talking to the manager API.
- `crates/nexmark`, `crates/datagen` — benchmark / synthetic-data tooling.
- `sql-to-dbsp-compiler/` — Java (Maven) SQL compiler built on Apache Calcite. Generates Rust pipeline code that
  links against the crates above.
- `js-packages/web-console` — SvelteKit GUI for the manager. Built static output is embedded into pipeline-manager
  at build time via `web-console/build/`. Other `js-packages/*` (`profiler-*`, `feldera-theme`,
  `vite-plugin-monaco-editor`, `triage-types`) are workspace siblings consumed by web-console.
- `python/` — Python SDK (`python/feldera`) and the **integration test suite** in `python/tests/`.
- `docs.feldera.com/` — Docusaurus docs site (separate sub-project).
- `deploy/` — Dockerfiles and docker-compose for running Feldera as a service.

End-to-end data flow: a user submits a SQL program to `pipeline-manager`; the manager invokes the
`sql-to-dbsp-compiler` to translate SQL into a Rust crate; that crate is compiled, linked with `dbsp` + `adapters` +
`sqllib`, and run as a pipeline process; the manager supervises the pipeline and proxies REST/WS traffic from the
web console and SDKs into it.

## Common commands

### Build / run the platform

```bash
# First-time: build the Java SQL compiler (downloads Calcite)
(cd sql-to-dbsp-compiler && ./build.sh)

# Run the pipeline-manager (uses embedded postgres by default)
cargo run --bin=pipeline-manager
# or via the convenience script (release build, defaults to 127.0.0.1)
./scripts/start_manager.sh
# WITH_POSTGRES=1 forces a real local Postgres instead of postgres-embed

# Build everything (Rust workspace)
cargo build
```

The web console is served at `http://localhost:8080` once the manager is running.

### Rust workspace

```bash
cargo build --workspace
cargo test  -p <crate>            # e.g. -p dbsp, -p pipeline-manager, -p dbsp_adapters
cargo test  -p dbsp <test-name>   # filter by name
cargo bench --bench nexmark       # see CONTRIBUTING.md for options
cargo clippy --locked --no-deps -- -D warnings
cargo fmt --all
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
```

Use the workspace dep table in the top-level `Cargo.toml` (workspace inheritance) — do not pin versions in
individual crate `Cargo.toml` files.

Tracing example (matches CI/dev defaults):

```bash
RUST_BACKTRACE=1 RUST_LOG=warn,pipeline_manager=info,feldera_types=info,project=info,dbsp=info,dbsp_adapters=info \
  cargo run -p pipeline-manager -- --dev-mode
```

### Web console (js-packages/web-console)

Run all commands from `js-packages/web-console/`. Bun 1.3.x and Node 20 are required.

```bash
bun install
bun run dev                    # vite dev server
bun run build                  # static build consumed by pipeline-manager
bun run check                  # svelte-check (type/syntax)
bun run lint / bun run format  # prettier + eslint + biome
bun run test-unit              # vitest
bun run test-e2e               # playwright; needs pipeline-manager on :8080
bun run test-e2e -- -g "name"  # filter by test name
bun run generate-openapi       # regenerate TS client from openapi.json
bun run build-openapi          # dump fresh openapi.json from the Rust crate
```

`bun run clean` (run from repo root) wipes node_modules and stale pipeline-manager build artifacts when the front
end gets into a bad state.

### Python SDK + integration tests

```bash
# Run the integration suite against a manager at http://localhost:8080
cd python && uv run python -m pytest -n 8 tests/

# Single file / test
(cd python && uv run python -m pytest tests/runtime/<file>.py)
(cd python && uv run python -m pytest tests/platform/test_shared_pipeline.py::TestPipeline::test_adhoc_query_hash -v)

# Aggregated SQL test framework
cd python && PYTHONPATH=$(pwd) ./tests/runtime_aggtest/run.sh
```

Override the manager endpoint with `FELDERA_HOST=...`. See `python/tests/README.md` for the `SharedTestPipeline`
pattern (combine DDL via `@sql` annotations to minimize compilation cycles), `@enterprise_only`, and the
`unique_pipeline_name` / `gen_pipeline_name` helpers — use them so tests do not collide in CI.

### SQL compiler (Java)

```bash
(cd sql-to-dbsp-compiler && ./build.sh)                       # full build (includes Calcite)
(cd sql-to-dbsp-compiler/SQL-compiler && mvn package -DskipTests)  # skip Calcite rebuild
```

## Architectural details worth knowing

- **Generated Rust pipelines.** A running pipeline is a separate Rust binary compiled per SQL program by the
  manager. Changes to `adapters`, `dbsp`, `sqllib`, `feldera-types`, `rest-api`, or the SQL compiler can break the
  generated code surface — keep the public APIs consumed by the compiler in mind.
- **OpenAPI is the contract.** `openapi.json` at the repo root is generated by `pipeline-manager --dump-openapi`
  and is consumed by the TS client (`bun run generate-openapi`) and the Rust `feldera-rest-api` build script. If
  you change request/response types in `crates/rest-api` or `crates/pipeline-manager/src/api`, regenerate it. The
  `update-openapi` pre-commit hook does this automatically; if codegen fails with “Token does not exist”, register
  the new type in `crates/pipeline-manager/src/api/main.rs` or annotate type aliases with `#[schema(value_type = …)]`
  (see `js-packages/web-console/README.md`).
- **DB schema migrations.** Pipeline-manager uses [refinery](https://github.com/rust-db/refinery) with files in
  `crates/pipeline-manager/migrations/V{N}__*.sql`. **Never modify an existing migration** — add a new `V{N+1}`
  file that mutates the live schema (`ALTER TABLE`, backfills, etc.) and pair it with a migration test from
  `V{N}` to `V{N+1}` covering pre- and post-state via the manager APIs.
- **Pipeline-manager modules.** `api/` (Actix routes, OpenAPI), `db/` (Postgres model + refinery), `compiler/`
  (drives the Java compiler and `cargo build` of generated crates), `runner/` (supervises pipeline processes,
  proxies HTTP/WS), `auth.rs` + `license.rs` (auth, enterprise gating), `pipeline_env.rs` (env vars passed to
  pipelines).
- **Adapters layering.** `crates/adapterlib` holds traits + types reused by `crates/adapters` and by generated
  pipeline code. `transport/` is the connector framework, `format/` is encoding/decoding, `controller/` runs the
  circuit. Ad-hoc SQL is in `adhoc/` and is evaluated via DataFusion against materialized state.
- **Workspace versioning.** All Feldera crates share `version = "0.x.y"` in the workspace `Cargo.toml` and are
  bumped together by `scripts/bump-versions.sh` during release; the Python SDK version (in
  `python/pyproject.toml`) and OpenAPI version must stay in lockstep.

## Hooks, formatting, and CI expectations

`.pre-commit-config.yaml` runs on every push (`scripts/pre-push` installs the hook):

- `cargo clippy --fix` + `cargo clippy --locked --no-deps -- -D warnings`
- `cargo fmt --all`
- `cargo machete --fix` (unused dependency check, scoped to `crates/`)
- `bun ci && bun run check` for `js-packages/**`
- `cargo run --bin=pipeline-manager -- --dump-openapi` to refresh `openapi.json`
- `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps`
- `ruff` / `ruff format` for `python/**`
- `scripts/fix-rust-module-naming` (run when modifying `mod.rs` files)

If a hook flags something, fix it rather than re-running with `--no-verify`.

## Project-specific conventions

- To gather more context beyond `README.md`:
  - Look at the outstanding changes in the tree.
  - On a branch, check the last 2–3 commits.
  - Read the relevant `README.md` in the sub-folder you are working in (`crates/*/README.md`,
    `python/tests/README.md`, `js-packages/web-console/README.md`, `sql-to-dbsp-compiler/README.md`).
- Write production-quality code; follow the conventions in *Code Complete* (McConnell) and
  *The Art of Readable Code* (Boswell & Foucher).
- Ensure the code compiles and that new code is covered by tests:
  - Unit tests for regular and exceptional inputs.
  - Property-based / model-based testing / fuzzing where appropriate (the repo uses `proptest` widely — see
    `proptest-regressions/` directories).
  - Integration tests for big platform-level features go in `python/tests/` (see the file's own README for the
    `SharedTestPipeline` pattern that batches DDL across tests).
- Update documentation and comments when changing behavior; follow *Bugs in Writing* (Dupre) and
  *The Elements of Style* (Strunk & White) for prose.

## Shared LLM context branch

At the start of every conversation, offer the user to run `scripts/claude.sh` to pull in shared LLM context files
(any `CLAUDE.md` in the repo) as unstaged changes from `origin/claude-context`. `scripts/claude.sh push` pushes
local `CLAUDE.md` edits back to that branch. These files **must not be committed outside the `claude-context`
branch**.
