// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sender_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dispatch"
	"github.com/tetriz-ai/outbox-comms/internal/sender"
	"github.com/tetriz-ai/outbox-comms/internal/slackclient"
)

// sendDeadline is the context deadline these tests give a send, far below the
// clients' own 30-second timeouts, so only the deadline can end the call.
const sendDeadline = 300 * time.Millisecond

// TestSlackFileStepsEndAtTheDeadline pins that the dispatcher's send deadline
// ends a Slack file upload stalled at any of its three steps, and that the
// outcome is the one that step's failure has: transient before the share,
// uncertain at the share.
func TestSlackFileStepsEndAtTheDeadline(t *testing.T) {
	for step, want := range map[string]dispatch.Outcome{
		"files.getUploadURLExternal":   dispatch.OutcomeTransient,
		"upload":                       dispatch.OutcomeTransient,
		"files.completeUploadExternal": dispatch.OutcomeUncertain,
	} {
		t.Run(step, func(t *testing.T) {
			var completes atomic.Int32
			var srv *httptest.Server
			srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				name := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/api"), "/")
				if name == "files.completeUploadExternal" {
					completes.Add(1)
				}
				if name == step {
					select {
					case <-r.Context().Done():
					case <-time.After(10 * time.Second):
					}
					return
				}
				if name == "files.getUploadURLExternal" {
					_, _ = fmt.Fprintf(w, `{"ok":true,"upload_url":%q,"file_id":"F0FILE"}`, srv.URL+"/upload")
					return
				}
				_, _ = io.WriteString(w, `{"ok":true}`)
			}))
			t.Cleanup(srv.Close)
			c, err := slackclient.New(&http.Client{Transport: &http.Transport{Proxy: nil}}, srv.URL+"/api")
			if err != nil {
				t.Fatal(err)
			}
			s := sender.NewSlackSender(sender.StaticTokens{Default: "example-token"}, c, fakeFetcher{data: []byte("%PDF-1.4")})
			in := slackIn("")
			in.SlackDelivery = pb.SlackDelivery_SLACK_DELIVERY_FILE
			in.Params, _ = structpb.NewStruct(map[string]any{sender.AttachmentKeyParam: "reports/r.pdf"})
			ctx, cancel := context.WithTimeout(context.Background(), sendDeadline)
			defer cancel()
			start := time.Now()
			res := s.Send(ctx, in)
			if took := time.Since(start); took > 3*time.Second {
				t.Fatalf("the send took %v; the %v deadline did not end it", took, sendDeadline)
			}
			if res.Outcome != want {
				t.Fatalf("outcome %v (%v), want %v", res.Outcome, res.Err, want)
			}
			if step != "files.completeUploadExternal" && completes.Load() != 0 {
				t.Fatalf("the share ran %d time(s) after an earlier step stalled", completes.Load())
			}
		})
	}
}

// TestSMTPEndsAtTheDeadline pins that the send deadline ends an SMTP
// transaction whose relay never answers the end of DATA, well inside the
// sender's own timeout, and that the outcome is uncertain: the relay had the
// whole message.
func TestSMTPEndsAtTheDeadline(t *testing.T) {
	relay := newScriptedSMTP(t, "250 ok", "stall")
	s := sender.NewSMTPSender(sender.SMTPConfig{Addr: relay.addr, From: "Example <noreply@example.com>", Timeout: 30 * time.Second}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), sendDeadline)
	defer cancel()
	start := time.Now()
	res := s.Send(ctx, smtpInput())
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("the send took %v; the %v deadline did not end it", took, sendDeadline)
	}
	if res.Outcome != dispatch.OutcomeUncertain {
		t.Fatalf("outcome %v (%v), want uncertain", res.Outcome, res.Err)
	}
}
