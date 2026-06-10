#!/bin/sh
#
# Entrypoint for the pipeline-runtime image. Per-pipeline Fly Machines
# launch with this script as PID 1. We download the compiled pipeline
# binary + rendered deployment config from URLs we get via env, then
# exec the binary with the same argv contract LocalRunner uses
# (crates/pipeline-manager/src/runner/local_runner.rs:585-594).
#
# Required env (set by FlyRunner when creating the machine, see
# crates/pipeline-manager/src/runner/fly_runner.rs:156-202):
#
#   OPENDERA_PIPELINE_ID       UUID identifying the pipeline.
#   OPENDERA_DEPLOYMENT_ID     UUID identifying this specific
#                              provision attempt.
#   OPENDERA_BINARY_URL        Where to fetch the compiled binary.
#                              Served by the compiler service.
#   OPENDERA_MANAGER_URL       Base URL of the pipeline-manager. Used
#                              to fetch the rendered deployment config.
#   OPENDERA_INTERNAL_API_KEY  Bearer token for the manager's
#                              /internal/v0 surface.
#   OPENDERA_INITIAL_STATUS    Runtime desired status to start in
#                              (Standby|Paused|Running). Optional;
#                              defaults to Paused so the runner-side
#                              state machine remains the source of
#                              truth and the worker waits for a
#                              follow-up control message.
#
# Optional:
#   OPENDERA_BOOTSTRAP_POLICY  Allow|Reject|AwaitApproval. Forwarded
#                              as --bootstrap-policy. Default is
#                              AwaitApproval per the engine default.
#   OPENDERA_SILENT_BOOTSTRAP  When non-empty, forwarded as
#                              --silent-bootstrap.

set -eu

require_env() {
    eval "val=\${$1:-}"
    if [ -z "$val" ]; then
        echo "[pipeline-entrypoint] FATAL: required env $1 is unset" >&2
        exit 64
    fi
}

require_env OPENDERA_PIPELINE_ID
require_env OPENDERA_DEPLOYMENT_ID
require_env OPENDERA_BINARY_URL
require_env OPENDERA_MANAGER_URL
require_env OPENDERA_INTERNAL_API_KEY

INITIAL_STATUS="${OPENDERA_INITIAL_STATUS:-Paused}"

BIN_PATH=/tmp/pipeline
CFG_PATH=/tmp/config.yaml

echo "[pipeline-entrypoint] pipeline_id=$OPENDERA_PIPELINE_ID"
echo "[pipeline-entrypoint] deployment_id=$OPENDERA_DEPLOYMENT_ID"
echo "[pipeline-entrypoint] initial_status=$INITIAL_STATUS"

echo "[pipeline-entrypoint] fetching binary from $OPENDERA_BINARY_URL"
curl --fail --location --silent --show-error --retry 5 --retry-delay 2 \
     --output "$BIN_PATH" \
     "$OPENDERA_BINARY_URL"
chmod 0755 "$BIN_PATH"

CFG_URL="$OPENDERA_MANAGER_URL/internal/v0/pipelines/$OPENDERA_PIPELINE_ID/deployment-config.yaml"
echo "[pipeline-entrypoint] fetching config from $CFG_URL"
curl --fail --location --silent --show-error --retry 5 --retry-delay 2 \
     --header "Authorization: Bearer $OPENDERA_INTERNAL_API_KEY" \
     --output "$CFG_PATH" \
     "$CFG_URL"

ARGS="--config-file $CFG_PATH --bind-address 0.0.0.0 --initial $INITIAL_STATUS --deployment-id $OPENDERA_DEPLOYMENT_ID"
if [ -n "${OPENDERA_BOOTSTRAP_POLICY:-}" ]; then
    ARGS="$ARGS --bootstrap-policy $OPENDERA_BOOTSTRAP_POLICY"
fi
if [ -n "${OPENDERA_SILENT_BOOTSTRAP:-}" ]; then
    ARGS="$ARGS --silent-bootstrap"
fi

echo "[pipeline-entrypoint] exec $BIN_PATH $ARGS"
# shellcheck disable=SC2086
exec "$BIN_PATH" $ARGS
