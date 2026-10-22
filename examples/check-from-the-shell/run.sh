#!/bin/sh
# Checks the running stack with curl alone: the health endpoint, the
# sender-reputation gauges, and what the Slack stub has received.
set -eu
curl -fsS "http://127.0.0.1:${OUTBOX_COMMS_HTTP_PORT:-8080}/healthz"
curl -fsS "http://127.0.0.1:${OUTBOX_COMMS_HTTP_PORT:-8080}/metrics" | grep -E '^outbox_comms_(bounce|complaint)_rate '
curl -fsS "http://127.0.0.1:${OUTBOX_COMMS_STUB_PORT:-8090}/messages" | grep -o '"channel":"[^"]*"' | sort -u
