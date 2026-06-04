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

exec /home/ubuntu/feldera/build/pipeline-manager "$@"
