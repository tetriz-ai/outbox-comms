// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package fakeses_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dispatch"
	"github.com/tetriz-ai/outbox-comms/internal/fakeses"
	"github.com/tetriz-ai/outbox-comms/internal/sender"
)

type bytesStore []byte

func (b bytesStore) Get(context.Context, string, string) ([]byte, error) { return b, nil }

// TestFakeSESAnswersTheSender pins the fake against the real SES sender: a
// plain and a raw send succeed with the fake's MessageId and are recorded with
// their recipient, and each fault reads as the sender classifies it.
func TestFakeSESAnswersTheSender(t *testing.T) {
	fake := &fakeses.SES{}
	srv := httptest.NewServer(fake.Handler())
	defer srv.Close()
	cfg := aws.Config{Region: "us-east-1", BaseEndpoint: aws.String(srv.URL),
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "example-only"}, nil
		})}
	s := sender.NewEmailSender(sender.NewSESClient(cfg), bytesStore("%PDF-1.4"), "noreply@example.com", "")
	in := func(attach bool) dispatch.SendInput {
		i := dispatch.SendInput{Channel: pb.Channel_CHANNEL_EMAIL, Recipient: "jane@example.com",
			Rendered: &pb.RenderedPayload{EmailSubject: "Hi", EmailHtml: "<p>hi</p>"}}
		if attach {
			i.Params, _ = structpb.NewStruct(map[string]any{sender.AttachmentKeyParam: "r.pdf"})
		}
		return i
	}
	for _, attach := range []bool{false, true} {
		if res := s.Send(context.Background(), in(attach)); res.Outcome != dispatch.OutcomeSuccess || res.ProviderMessageID == "" {
			t.Fatalf("send (attachment %v): %v %q %v", attach, res.Outcome, res.ProviderMessageID, res.Err)
		}
	}
	calls := fake.Calls()
	if len(calls) != 2 || calls[0].Action != "SendEmail" || calls[1].Action != "SendRawEmail" || calls[0].To != "jane@example.com" || calls[1].To != "jane@example.com" {
		t.Fatalf("calls %+v", calls)
	}
	for _, tc := range []struct {
		fault fakeses.Fault
		want  dispatch.Outcome
	}{
		{fakeses.Fault{Status: 400, Code: "MessageRejected", Times: 1}, dispatch.OutcomePermanent},
		{fakeses.Fault{Status: 400, Code: "Throttling", Times: 1}, dispatch.OutcomeTransient},
		{fakeses.Fault{Status: 503, Times: 1}, dispatch.OutcomeUncertain},
		{fakeses.Fault{EmptyMessageID: true, Times: 1}, dispatch.OutcomeUncertain},
	} {
		if err := fake.AddFault(tc.fault); err != nil {
			t.Fatal(err)
		}
		if res := s.Send(context.Background(), in(false)); res.Outcome != tc.want {
			t.Fatalf("fault %+v: outcome %v (%v), want %v", tc.fault, res.Outcome, res.Err, tc.want)
		}
	}
}

// TestFakeSESDelayEndsOnDisconnect pins the delay fault: a client that gives up
// ends the call, which is still recorded, and a call that arrives while another
// is in flight is marked overlapped.
func TestFakeSESDelayEndsOnDisconnect(t *testing.T) {
	fake := &fakeses.SES{}
	srv := httptest.NewServer(fake.Handler())
	defer srv.Close()
	cfg := aws.Config{Region: "us-east-1", BaseEndpoint: aws.String(srv.URL),
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "example-only"}, nil
		})}
	s := sender.NewEmailSender(sender.NewSESClient(cfg), nil, "noreply@example.com", "")
	in := dispatch.SendInput{Channel: pb.Channel_CHANNEL_EMAIL, Recipient: "jane@example.com",
		Rendered: &pb.RenderedPayload{EmailSubject: "Hi", EmailHtml: "<p>hi</p>"}}
	if err := fake.AddFault(fakeses.Fault{Delay: "10s", Times: 1}); err != nil {
		t.Fatal(err)
	}
	slow := make(chan dispatch.SendResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		slow <- s.Send(ctx, in)
	}()
	time.Sleep(100 * time.Millisecond)
	if res := s.Send(context.Background(), in); res.Outcome != dispatch.OutcomeSuccess {
		t.Fatalf("second send: %v %v", res.Outcome, res.Err)
	}
	start := time.Now()
	if res := <-slow; res.Outcome != dispatch.OutcomeUncertain {
		t.Fatalf("cut-off send: %v %v, want uncertain", res.Outcome, res.Err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the cut-off send took %v more; the delay outlived the client", took)
	}
	time.Sleep(100 * time.Millisecond)
	// A call is recorded when it is answered, so the fast second call comes first.
	calls := fake.Calls()
	if len(calls) != 2 || !calls[0].Overlapped || calls[1].Overlapped {
		t.Fatalf("calls %+v; want 2, the second-arriving one (recorded first) overlapped", calls)
	}
}
