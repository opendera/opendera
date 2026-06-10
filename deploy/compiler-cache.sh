#!/bin/sh
# Object-store cache for the compile pool's cargo dependency graph.
#
# Why this exists
# ---------------
# The compile pool keeps cargo's `target/` on the machine's ephemeral SSD,
# which Fly wipes whenever an idle machine is auto-stopped. sccache (Tigris)
# already caches per-crate rustc outputs, but rematerialising a warm sccache
# into a fresh `target/` still re-invokes rustc for ~920 dep crates and pulls
# ~920 separate objects from Tigris -- ~10 min even on a 100% cache hit.
#
# This script snapshots the *whole* dependency graph (plus the cargo registry
# and git caches) as a single zstd tarball in the same Tigris bucket sccache
# uses, keyed by hash(Cargo.lock + rustc version). On a cold machine we restore
# that one archive; cargo then sees every fingerprint as fresh and rebuilds
# only the per-pipeline crate (~30 s). See deploy/entrypoint.sh for wiring and
# the project plan for the full three-layer cache (suspend / this / sccache).
#
# Usage:
#   compiler-cache.sh restore         # one-shot, synchronous, run before the engine
#   compiler-cache.sh snapshot-daemon # background loop, uploads once the graph is built
#
# Auth + endpoint reuse the same Tigris credentials sccache uses. Required env:
#   SCCACHE_BUCKET                S3 bucket (e.g. opendera-sccache)
#   SCCACHE_ENDPOINT              S3 endpoint URL (e.g. https://fly.storage.tigris.dev)
#   OPENDERA_TARGET_CACHE_PREFIX  key prefix within the bucket (e.g. target-cache)
#   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY  (already set on the pool for sccache)
# Optional:
#   SCCACHE_REGION                region for s5cmd (default: auto)

set -eu

log() { echo "[compiler-cache] $*" >&2; }

# --- Layout (the engine defaults to ~/.feldera/compiler; HOME=/home/ubuntu) ---
HOME_DIR="${HOME:-/home/ubuntu}"
WORK_DIR="$HOME_DIR/.feldera/compiler/rust-compilation"
TARGET_DIR="$WORK_DIR/target"
CARGO_DIR="$HOME_DIR/.cargo"
SENTINEL="$WORK_DIR/.target-cache-key"
CARGO_LOCK="$HOME_DIR/feldera/Cargo.lock"

# Only snapshot once the dep graph is actually built, so we never upload a
# half-populated target/. ~5 GB warm; 2 GB is comfortably past "deps done".
MIN_DEPS_BYTES=2147483648
SNAPSHOT_POLL_SECONDS=60

require_env() {
    eval "v=\${$1:-}"
    if [ -z "$v" ]; then
        log "disabled: required env $1 is unset"
        return 1
    fi
}

cache_configured() {
    require_env SCCACHE_BUCKET \
        && require_env SCCACHE_ENDPOINT \
        && require_env OPENDERA_TARGET_CACHE_PREFIX \
        && require_env AWS_ACCESS_KEY_ID \
        && require_env AWS_SECRET_ACCESS_KEY
}

# s5cmd needs a region; Tigris accepts "auto". Credentials come from the
# AWS_* env already set on the pool (sigv4 signing is s5cmd's default).
export AWS_REGION="${SCCACHE_REGION:-auto}"
S5="s5cmd --endpoint-url ${SCCACHE_ENDPOINT:-}"

cache_key() {
    # hash(Cargo.lock contents + exact rustc version). Toolchain bumps and
    # lockfile changes rotate the key, so a stale graph is never restored.
    { cat "$CARGO_LOCK"; rustc --version; } | sha256sum | cut -c1-64
}

object_url() {
    echo "s3://$SCCACHE_BUCKET/$OPENDERA_TARGET_CACHE_PREFIX/$1.tar.zst"
}

object_exists() {
    $S5 ls "$(object_url "$1")" >/dev/null 2>&1
}

deps_present() {
    # True once target/ holds a meaningful dep graph (suspend-resume or a prior
    # restore/build leaves it; a fresh SSD does not).
    [ -d "$TARGET_DIR" ] || return 1
    size=$(du -sb "$TARGET_DIR" 2>/dev/null | cut -f1)
    [ -n "$size" ] && [ "$size" -ge "$MIN_DEPS_BYTES" ]
}

build_in_progress() {
    # True while a pipeline compile is writing target/. Scans /proc instead
    # of pgrep -- procps is not in the image, and a missing pgrep would make
    # this guard silently report "no build running".
    for f in /proc/[0-9]*/comm; do
        read -r comm < "$f" 2>/dev/null || continue
        case "$comm" in
            cargo|rustc) return 0 ;;
        esac
    done
    return 1
}

# Exclude per-pipeline artifacts (rebuilt every compile, ~280 MB binaries) and
# incremental state. Keep dep rlibs + .fingerprint + cargo registry/git.
TAR_EXCLUDES="--exclude=*feldera_pipe_* \
--exclude=.feldera/compiler/rust-compilation/pipeline-binaries \
--exclude=.feldera/compiler/rust-compilation/crates \
--exclude=.feldera/compiler/rust-compilation/target/*/incremental"

restore() {
    cache_configured || { log "restore skipped (cache not configured)"; return 0; }

    key=$(cache_key)

    # Suspend-resume (or same-key reboot) leaves a warm target/ and matching
    # sentinel -- nothing to download.
    if [ -f "$SENTINEL" ] && [ "$(cat "$SENTINEL" 2>/dev/null)" = "$key" ] && deps_present; then
        log "restore skipped: dep graph already present for key $(echo "$key" | cut -c1-12)… (warm)"
        return 0
    fi

    if ! object_exists "$key"; then
        log "restore: no snapshot for key $key yet -- cold build will populate it"
        return 0
    fi

    log "restore: downloading $(object_url "$key")"
    mkdir -p "$HOME_DIR"
    # `s5cmd cp` downloads with parallel ranged GETs; the single-stream
    # `s5cmd cat` took 4m24s for a ~1 GB archive in production. The temp
    # file costs transient disk equal to the archive size.
    tmp="$HOME_DIR/.target-cache-restore.$$.tar.zst"
    if $S5 cp "$(object_url "$key")" "$tmp" \
        && zstd -d -T0 -c "$tmp" | tar -x -C "$HOME_DIR"; then
        echo "$key" > "$SENTINEL"
        log "restore: complete ($(du -sh "$TARGET_DIR" 2>/dev/null | cut -f1) in target/)"
    else
        log "restore: FAILED to extract snapshot -- falling back to sccache cold build"
    fi
    rm -f "$tmp"
}

snapshot_once() {
    key=$(cache_key)

    if object_exists "$key"; then
        echo "$key" > "$SENTINEL"   # keep suspend-resume fast for this key
        return 0
    fi
    # Never tar a live target/. MIN_DEPS_BYTES is a size gate, not a
    # completion gate: it passes a few minutes into a cold build (measured:
    # 2.3 GB of an eventual 4.4 GB graph), and the torn snapshot would
    # permanently cache half a dep graph for this key.
    if build_in_progress; then
        return 0
    fi
    if ! deps_present; then
        return 0
    fi

    tmp="$WORK_DIR/.snapshot.$$.tar.zst"
    log "snapshot: building $(object_url "$key") from $(du -sh "$TARGET_DIR" | cut -f1) target/"
    # shellcheck disable=SC2086
    if ! tar -C "$HOME_DIR" $TAR_EXCLUDES -cf - \
            .feldera/compiler/rust-compilation/target \
            .cargo/registry \
            .cargo/git 2>/dev/null \
            | zstd -1 -T0 -o "$tmp" -f; then
        log "snapshot: FAILED to build archive -- will retry next cycle"
        rm -f "$tmp"
        return 0
    fi
    # A compile that started while we tarred makes the archive torn too.
    if build_in_progress; then
        log "snapshot: discarded (compile started mid-tar) -- will retry next cycle"
        rm -f "$tmp"
        return 0
    fi
    if $S5 cp "$tmp" "$(object_url "$key")"; then
        echo "$key" > "$SENTINEL"
        log "snapshot: uploaded ($(du -sh "$tmp" | cut -f1) compressed)"
    else
        log "snapshot: FAILED to upload -- will retry next cycle"
    fi
    rm -f "$tmp"
}

snapshot_daemon() {
    cache_configured || { log "snapshot daemon not started (cache not configured)"; return 0; }
    log "snapshot daemon started (poll ${SNAPSHOT_POLL_SECONDS}s)"
    while true; do
        snapshot_once || true
        sleep "$SNAPSHOT_POLL_SECONDS"
    done
}

cmd="${1:-}"
case "$cmd" in
    restore)          restore ;;
    snapshot-daemon)  snapshot_daemon ;;
    snapshot-once)    snapshot_once ;;   # handy for manual testing
    *)
        echo "usage: $0 {restore|snapshot-daemon|snapshot-once}" >&2
        exit 64
        ;;
esac
