// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Command slack-stub serves a local stand-in for the Slack Web API, for local
// runs and the compose acceptance test. Point OUTBOX_COMMS_SLACK_API_URL at
// http://<host>:<port>/api; GET /messages lists what it received.
package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/tetriz-ai/outbox-comms/internal/slackstub"
)

func main() {
	addr := flag.String("addr", ":8090", "listen address")
	delay := flag.Duration("delay", 0, "hold each chat.postMessage this long before answering")
	flag.Parse()
	s := &slackstub.Stub{Delay: *delay}
	srv := &http.Server{Addr: *addr, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	log.Printf("slack-stub listening on %s (delay %s)", *addr, *delay)
	log.Fatal(srv.ListenAndServe())
}
