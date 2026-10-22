#!/bin/sh
# Copyright 2026 Workloom Solutions Private Limited
# SPDX-License-Identifier: MIT
# The end-to-end lifecycle test: start Postgres, Redis, Mailpit, the Slack stub,
# the fake SES and ElasticMQ in their own compose project, then run the e2e Go
# test, which runs the service on the host (built from e2e/server) with short
# timings, kills and restarts it, and checks every outcome. Nothing leaves the
# machine: every AWS call goes to the fake SES or ElasticMQ with throwaway
# credentials. On exit it removes the project's containers and volumes, unless
# E2E_KEEP=1.
#
# Usage: scripts/e2e.sh
#   COMPOSE            the compose command (default "docker compose")
#   E2E_PROJECT        the compose project name (default outbox-comms-e2e)
#   E2E_PORT_OFFSET    added to every default host port below (default 0), so
#                      a second run beside the first needs only this and
#                      E2E_PROJECT, for example E2E_PORT_OFFSET=100
#   E2E_KEEP=1         leave the stack up; the next run reuses it
#   OUTBOX_COMMS_IMAGE the image the stand-ins run (default outbox-comms:e2e,
#                      so the local stack's image is left alone)
#   OUTBOX_COMMS_*_PORT  the host ports (PG, REDIS, SMTP, MAILPIT, STUB, SES, SQS,
#                      GRPC, HTTP), defaulting to 55432, 56379, 51025, 58025,
#                      58090, 58095, 59324, 59090 and 58080 so a local stack on
#                      the usual ports keeps running
set -eu
cd "$(dirname "$0")/.."
COMPOSE=${COMPOSE:-docker compose}
project=${E2E_PROJECT:-outbox-comms-e2e}
off=${E2E_PORT_OFFSET:-0}
export OUTBOX_COMMS_PG_PORT="${OUTBOX_COMMS_PG_PORT:-$((55432 + off))}"
export OUTBOX_COMMS_REDIS_PORT="${OUTBOX_COMMS_REDIS_PORT:-$((56379 + off))}"
export OUTBOX_COMMS_SMTP_PORT="${OUTBOX_COMMS_SMTP_PORT:-$((51025 + off))}"
export OUTBOX_COMMS_MAILPIT_PORT="${OUTBOX_COMMS_MAILPIT_PORT:-$((58025 + off))}"
export OUTBOX_COMMS_STUB_PORT="${OUTBOX_COMMS_STUB_PORT:-$((58090 + off))}"
export OUTBOX_COMMS_SES_PORT="${OUTBOX_COMMS_SES_PORT:-$((58095 + off))}"
export OUTBOX_COMMS_SQS_PORT="${OUTBOX_COMMS_SQS_PORT:-$((59324 + off))}"
export OUTBOX_COMMS_GRPC_PORT="${OUTBOX_COMMS_GRPC_PORT:-$((59090 + off))}"
export OUTBOX_COMMS_HTTP_PORT="${OUTBOX_COMMS_HTTP_PORT:-$((58080 + off))}"
export OUTBOX_COMMS_IMAGE="${OUTBOX_COMMS_IMAGE:-outbox-comms:e2e}"
export E2E_COMPOSE="$COMPOSE -p $project -f compose.yaml -f compose.e2e.yaml --profile e2e"
work=$(mktemp -d)
on_exit() {
  rc=$?
  trap - EXIT
  if [ "${E2E_KEEP:-0}" != 1 ]; then
    echo "== remove the e2e stack"
    $E2E_COMPOSE down -v || rc=1
  fi
  rm -rf "$work"
  exit "$rc"
}
trap on_exit EXIT

echo "== build the image and the e2e service"
$E2E_COMPOSE build outbox-comms slack-stub fake-ses
go build -o "$work/outbox-comms-e2e" ./e2e/server

echo "== start the stand-ins (project $project)"
$E2E_COMPOSE up -d --wait postgres redis mailpit slack-stub fake-ses elasticmq

echo "== run the lifecycle test"
mkdir -p "$work/logs"
E2E_SERVER_BIN="$work/outbox-comms-e2e" E2E_LOG_DIR="$work/logs" \
  go test -tags e2e -count=1 -timeout 15m -v ./e2e/
