#!/bin/sh
# Entrypoint shim for pipeline-manager.
#
# sccache v0.8+ uses opendal under the hood for its S3 backend, and
# opendal's S3 credential chain reads OPENDAL_S3_ACCESS_KEY_ID /
# OPENDAL_S3_SECRET_ACCESS_KEY (not the AWS_* names). When cargo
# spawns rustc-via-sccache for a per-pipeline compile, the AWS_*
# env that the compile pool has set is *not* mapped through, so
# sccache falls through to EC2 IMDS, times out, and the build aborts.
#
# Mirror the AWS_* names onto OPENDAL_S3_* before exec so the chain
# resolves on the first try. Harmless when AWS_* is unset (the if's
# short-circuit). Also harmless when OPENDAL_S3_* is already set
# (operator override takes priority).

if [ -n "${AWS_ACCESS_KEY_ID:-}" ] && [ -z "${OPENDAL_S3_ACCESS_KEY_ID:-}" ]; then
    export OPENDAL_S3_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID"
fi
if [ -n "${AWS_SECRET_ACCESS_KEY:-}" ] && [ -z "${OPENDAL_S3_SECRET_ACCESS_KEY:-}" ]; then
    export OPENDAL_S3_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY"
fi
if [ -n "${AWS_SESSION_TOKEN:-}" ] && [ -z "${OPENDAL_S3_SESSION_TOKEN:-}" ]; then
    export OPENDAL_S3_SESSION_TOKEN="$AWS_SESSION_TOKEN"
fi

# Compile-pool only: restore the cargo dependency graph from object storage
# before the engine starts accepting jobs, and keep a background daemon that
# snapshots it once built. This is what turns a cold (auto-stopped) machine's
# first compile from ~10 min back into ~30 s. See deploy/compiler-cache.sh.
# Gated on compiler mode so the control-plane (manager) app is untouched.
if [ "${OPENDERA_SERVICE_MODE:-}" = "compiler" ] \
   && [ -x /usr/local/bin/compiler-cache ]; then
    # Synchronous: the first claimed job must not race a half-extracted target/.
    /usr/local/bin/compiler-cache restore || true
    # Background: uploads the snapshot for this Cargo.lock+toolchain key once the
    # dep graph exists. Reparents to the engine (PID 1) after the exec below.
    /usr/local/bin/compiler-cache snapshot-daemon &
fi

exec /home/ubuntu/feldera/build/pipeline-manager "$@"
