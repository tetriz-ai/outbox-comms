#!/bin/sh
# Copyright 2026 Workloom Solutions Private Limited
# SPDX-License-Identifier: MIT
# The acceptance test: with the compose stack up and a Slack stub that holds each
# post 10 seconds, post a notification, kill the service with SIGKILL while the
# post is in flight, restart it, and check that the stub received exactly one
# message and the row ended DELIVERED. It deletes every message in the local
# stack, and on exit, pass or fail, puts the normal stack back.
#
# Usage: scripts/acceptance.sh
#   COMPOSE        the compose command (default "docker compose")
#   COMPOSE_EXTRA  space-separated compose files to keep on top of compose.yaml,
#                  such as your own override
set -eu
cd "$(dirname "$0")/.."
COMPOSE=${COMPOSE:-docker compose}
files="-f compose.yaml"
for f in ${COMPOSE_EXTRA:-}; do files="$files -f $f"; done
normal="$COMPOSE $files"
accept="$COMPOSE $files -f compose.acceptance.yaml"
compose() { $accept "$@"; }
on_exit() {
  rc=$?
  trap - EXIT
  echo "== restore the normal stack"
  $normal up -d --wait outbox-comms slack-stub || rc=1
  exit "$rc"
}
trap on_exit EXIT
psql() { compose exec -T postgres psql -U outbox -d outbox_comms -tAc "$1"; }
stub_count() { curl -fsS "http://127.0.0.1:${OUTBOX_COMMS_STUB_PORT:-8090}/messages" | grep -o '"channel"' | wc -l | tr -d ' '; }
row() { psql "SELECT status || ' attempts=' || attempts FROM comms_messages WHERE recipient = 'C0DEPLOYS'"; }
wait_for() { # seconds, description, command...
  t=$1; what=$2; shift 2
  i=0
  while ! "$@" >/dev/null 2>&1; do
    i=$((i + 1))
    if [ "$i" -gt "$t" ]; then echo "FAIL: timed out waiting for $what" >&2; exit 1; fi
    sleep 1
  done
}
sending() { [ "$(psql "SELECT status FROM comms_messages WHERE recipient = 'C0DEPLOYS'")" = MESSAGE_STATUS_SENDING ]; }
delivered() { [ "$(psql "SELECT status FROM comms_messages WHERE recipient = 'C0DEPLOYS'")" = MESSAGE_STATUS_DELIVERED ]; }

echo "== start the stack"
compose up -d --build --wait
curl -fsS -X DELETE "http://127.0.0.1:${OUTBOX_COMMS_STUB_PORT:-8090}/messages"
psql "DELETE FROM comms_delivery_events; DELETE FROM comms_messages; DELETE FROM comms_requests;" >/dev/null

echo "== post a notification"
handle=$(go run ./scripts/acceptance submit "acceptance-$(date +%s)")
echo "handle: $handle"
wait_for 30 "the dispatch to reach the Slack stub" sending
echo "row while the post is in flight: $(row)"

echo "== kill the service mid-delivery"
compose kill -s SIGKILL outbox-comms
sleep 11
echo "posts the stub has after the kill: $(stub_count)"
[ "$(stub_count)" = 0 ] || { echo "FAIL: the killed post reached the stub" >&2; exit 1; }
echo "row after the kill: $(row)"

echo "== restart"
compose up -d --wait outbox-comms
wait_for 120 "the message to be DELIVERED" delivered
sleep 30
echo "row: $(row)"
echo "posts the stub has: $(stub_count)"
echo "status over gRPC: $(go run ./scripts/acceptance status "$handle")"
[ "$(stub_count)" = 1 ] || { echo "FAIL: the stub has $(stub_count) posts, want 1" >&2; exit 1; }
delivered || { echo "FAIL: the row is not DELIVERED" >&2; exit 1; }
echo "PASS: one message, no duplicate, row DELIVERED"
