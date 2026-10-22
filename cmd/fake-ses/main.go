// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Command fake-ses serves a local stand-in for Amazon SES SendEmail and
// SendRawEmail, for the e2e run. Point AWS_ENDPOINT_URL_SES at
// http://<host>:<port>; GET /calls lists what it received.
package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/tetriz-ai/outbox-comms/internal/fakeses"
)

func main() {
	addr := flag.String("addr", ":8095", "listen address")
	flag.Parse()
	srv := &http.Server{Addr: *addr, Handler: (&fakeses.SES{}).Handler(), ReadHeaderTimeout: 5 * time.Second}
	log.Printf("fake-ses listening on %s", *addr)
	log.Fatal(srv.ListenAndServe())
}
