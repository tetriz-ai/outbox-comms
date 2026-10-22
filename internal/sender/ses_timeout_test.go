// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sender_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dispatch"
	"github.com/tetriz-ai/outbox-comms/internal/fakeses"
	"github.com/tetriz-ai/outbox-comms/internal/sender"
)

// TestSESClientHasAnHTTPTimeout pins that the SES client ends a call SES has not
// answered within its timeout, even when the caller's context has no deadline,
// and that the cut-off call reads as uncertain.
func TestSESClientHasAnHTTPTimeout(t *testing.T) {
	fake := &fakeses.SES{}
	if err := fake.AddFault(fakeses.Fault{Delay: "10s"}); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(fake.Handler())
	defer srv.Close()
	cfg := aws.Config{Region: "us-east-1", BaseEndpoint: aws.String(srv.URL),
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "example-only"}, nil
		})}
	s := sender.NewEmailSender(sender.NewSESClientWithTimeout(cfg, 300*time.Millisecond), nil, "noreply@example.com", "")
	start := time.Now()
	res := s.Send(context.Background(), dispatch.SendInput{Channel: pb.Channel_CHANNEL_EMAIL, Recipient: "jane@example.com",
		Rendered: &pb.RenderedPayload{EmailSubject: "Hi", EmailHtml: "<p>hi</p>"}})
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("the call took %v; the 300ms HTTP timeout did not end it", took)
	}
	if res.Outcome != dispatch.OutcomeUncertain {
		t.Fatalf("outcome %v (%v), want uncertain", res.Outcome, res.Err)
	}
}
