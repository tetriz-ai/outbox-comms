#!/bin/sh
# Copyright 2026 Workloom Solutions Private Limited
# SPDX-License-Identifier: MIT
# Runs every example in the order the index lists them, from the repo root, with
# the command in its README's sh block, and compares standard output with the
# README's last (text) block. Fails when the index and the folders disagree.
# Needs the compose stack up (docker compose up -d --build --wait). It first
# resets the local stack: it deletes every request, message and delivery event
# in its Postgres, and empties the Slack stub (DELETE /messages) and Mailpit.
# The examples use fixed idempotency keys, so on a reused stack they would
# otherwise send nothing, and check-from-the-shell would list no channel.
#   COMPOSE        the compose command (default "docker compose")
set -eu
cd "$(dirname "$0")/.."
COMPOSE=${COMPOSE:-docker compose}
$COMPOSE exec -T postgres psql -U outbox -d outbox_comms -q -c \
  "DELETE FROM comms_delivery_events; DELETE FROM comms_messages; DELETE FROM comms_requests;"
curl -fsS -X DELETE "http://127.0.0.1:${OUTBOX_COMMS_STUB_PORT:-8090}/messages" > /dev/null
curl -fsS -X DELETE "http://127.0.0.1:${OUTBOX_COMMS_MAILPIT_PORT:-8025}/api/v1/messages" > /dev/null
go vet ./examples/...
listed=$(grep -o '](\([a-z0-9-]*\)/)' examples/README.md | sed 's/](\(.*\)\/)/\1/')
present=$(find examples -mindepth 1 -maxdepth 1 -type d | sed 's#examples/##' | sort)
if [ "$(printf '%s\n' "$listed" | sort)" != "$present" ]; then
  echo "examples/README.md lists [$(printf '%s\n' "$listed" | tr '\n' ' ')] but the folders are [$(printf '%s\n' "$present" | tr '\n' ' ')]" >&2
  exit 1
fi
fail=0
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
for name in $listed; do
  readme=examples/$name/README.md
  cmd=$(awk '/^```sh$/{f=1;next} /^```$/{if(f){exit}} f' "$readme")
  want="$tmp/want"; got="$tmp/got"
  awk '/^```text$/{f=1;buf="";next} /^```$/{if(f){f=0;last=buf}} f{buf=buf $0 "\n"} END{printf "%s", last}' "$readme" > "$want"
  if ! sh -c "$cmd" > "$got"; then
    echo "FAIL $name: \`$cmd\` exited non-zero" >&2; fail=1; continue
  fi
  if diff -u "$want" "$got"; then echo "ok   $name"; else echo "FAIL $name: output differs from $readme" >&2; fail=1; fi
done
exit $fail
