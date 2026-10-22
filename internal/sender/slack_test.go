// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sender_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dispatch"
	"github.com/tetriz-ai/outbox-comms/internal/sender"
)

type fakeBroker struct {
	token string
	err   error
}

func (f fakeBroker) Token(context.Context, string) (string, error) {
	return f.token, f.err
}

type fakePoster struct {
	ts                 string
	postErr, uploadErr error
	posted, uploaded   bool
	joined             bool
	uploadName         string
	text               string
}

func (f *fakePoster) PostMessage(_ context.Context, _, _, text, _ string) (string, error) {
	f.posted = true
	f.text = text
	return f.ts, f.postErr
}

func (f *fakePoster) UploadFile(_ context.Context, _, _ string, _ []byte, filename string) (string, error) {
	f.uploaded = true
	f.uploadName = filename
	return f.ts, f.uploadErr
}

func (f *fakePoster) JoinChannel(context.Context, string, string) error {
	f.joined = true
	return nil
}

func slackInput(delivery pb.SlackDelivery) dispatch.SendInput {
	return dispatch.SendInput{
		OrgID: "org", Channel: pb.Channel_CHANNEL_SLACK, Recipient: "C0AB12CD3",
		Rendered: &pb.RenderedPayload{SlackBlocksJson: `[]`, SlackText: "Report ready"}, SlackDelivery: delivery,
	}
}

// TestSlackLinkSend proves LINK mode posts a Block Kit message and records the ts.
func TestSlackLinkSend(t *testing.T) {
	poster := &fakePoster{ts: "1700000000.0001"}
	s := sender.NewSlackSender(fakeBroker{token: "test-token"}, poster, fakeFetcher{})
	res := s.Send(context.Background(), slackInput(pb.SlackDelivery_SLACK_DELIVERY_LINK))
	if res.Outcome != dispatch.OutcomeSuccess || res.ProviderMessageID != "1700000000.0001" {
		t.Fatalf("want success+ts, got %+v", res)
	}
	if !poster.posted || poster.uploaded {
		t.Errorf("LINK must post a message, not upload a file")
	}
	if poster.text != "Report ready" {
		t.Errorf("the rendered fallback text must reach chat.postMessage, got %q", poster.text)
	}
}

// TestSlackFileSend proves FILE mode fetches the PDF and uploads it natively.
func TestSlackFileSend(t *testing.T) {
	poster := &fakePoster{ts: "1700000000.0002"}
	in := slackInput(pb.SlackDelivery_SLACK_DELIVERY_FILE)
	in.Params = paramsWith(t, "comms/org/report.pdf")
	s := sender.NewSlackSender(fakeBroker{token: "test-token"}, poster, fakeFetcher{data: []byte("%PDF")})
	res := s.Send(context.Background(), in)
	if res.Outcome != dispatch.OutcomeSuccess {
		t.Fatalf("want success, got %+v", res)
	}
	if !poster.uploaded || poster.posted {
		t.Errorf("FILE must upload a file, not post a message")
	}
	if poster.uploadName != "attachment.pdf" {
		t.Errorf("FILE default filename = %q, want attachment.pdf", poster.uploadName)
	}
}

// TestSlackAuthFailure proves invalid_auth or token_revoked is OutcomeAuth (so the
// dispatcher calls the auth-failure hook). A token lookup failure is auth-class
// only when the token is missing; a token-store outage is transient, so an outage
// never marks a healthy connection invalid.
func TestSlackAuthFailure(t *testing.T) {
	tests := []struct {
		name   string
		broker sender.TokenProvider
		poster *fakePoster
		want   dispatch.Outcome
	}{
		{"no token for the org → auth", fakeBroker{err: sender.ErrNoSlackToken}, &fakePoster{}, dispatch.OutcomeAuth},
		{"token store outage → transient, not auth", fakeBroker{err: errors.New("dial tcp: connection refused")}, &fakePoster{}, dispatch.OutcomeTransient},
		{"invalid_auth → auth", fakeBroker{token: "x"}, &fakePoster{postErr: &sender.SlackError{Code: "invalid_auth"}}, dispatch.OutcomeAuth},
		{"channel_not_found → permanent", fakeBroker{token: "x"}, &fakePoster{postErr: &sender.SlackError{Code: "channel_not_found"}}, dispatch.OutcomePermanent},
		{"rate_limited → transient", fakeBroker{token: "x"}, &fakePoster{postErr: &sender.SlackError{Code: "rate_limited"}}, dispatch.OutcomeTransient},
		{"ratelimited (the client's HTTP 429) → transient", fakeBroker{token: "x"}, &fakePoster{postErr: &sender.SlackError{Code: "ratelimited"}}, dispatch.OutcomeTransient},
		{"timeout before the write → transient", fakeBroker{token: "x"}, &fakePoster{postErr: context.DeadlineExceeded}, dispatch.OutcomeTransient},
		{"token lookup deadline → transient", fakeBroker{err: context.DeadlineExceeded}, &fakePoster{}, dispatch.OutcomeTransient},
		{"connection reset before the write → transient", fakeBroker{token: "x"}, &fakePoster{postErr: fmt.Errorf("slack http do: %w", &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)})}, dispatch.OutcomeTransient},
		{"connection reset after the write → uncertain", fakeBroker{token: "x"}, &fakePoster{postErr: fmt.Errorf("slack http do: %w: %w", sender.ErrMaybeSent, &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)})}, dispatch.OutcomeUncertain},
		{"response cut off → uncertain", fakeBroker{token: "x"}, &fakePoster{postErr: fmt.Errorf("slack http do: %w: %w", sender.ErrMaybeSent, io.ErrUnexpectedEOF)}, dispatch.OutcomeUncertain},
		{"fatal_error → uncertain", fakeBroker{token: "x"}, &fakePoster{postErr: &sender.SlackError{Code: "fatal_error"}}, dispatch.OutcomeUncertain},
		{"internal_error → uncertain", fakeBroker{token: "x"}, &fakePoster{postErr: &sender.SlackError{Code: "internal_error"}}, dispatch.OutcomeUncertain},
		{"ok:false with no code → uncertain", fakeBroker{token: "x"}, &fakePoster{postErr: &sender.SlackError{}}, dispatch.OutcomeUncertain},
		{"service_unavailable → transient", fakeBroker{token: "x"}, &fakePoster{postErr: &sender.SlackError{Code: "service_unavailable"}}, dispatch.OutcomeTransient},
		{"request_timeout → transient", fakeBroker{token: "x"}, &fakePoster{postErr: &sender.SlackError{Code: "request_timeout"}}, dispatch.OutcomeTransient},
		{"a code no list names → transient", fakeBroker{token: "x"}, &fakePoster{postErr: &sender.SlackError{Code: "some_future_error"}}, dispatch.OutcomeTransient},
		{"internal_error before the post → transient", fakeBroker{token: "x"}, &fakePoster{postErr: fmt.Errorf("files.getUploadURLExternal: %w: %w", sender.ErrNotSent, &sender.SlackError{Code: "internal_error"})}, dispatch.OutcomeTransient},
		{"invalid_auth before the post is still auth", fakeBroker{token: "x"}, &fakePoster{postErr: fmt.Errorf("files.getUploadURLExternal: %w: %w", sender.ErrNotSent, &sender.SlackError{Code: "invalid_auth"})}, dispatch.OutcomeAuth},
		{"dial refused, nothing written → transient", fakeBroker{token: "x"}, &fakePoster{postErr: fmt.Errorf("slack http do: %w", &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)})}, dispatch.OutcomeTransient},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := sender.NewSlackSender(tt.broker, tt.poster, fakeFetcher{})
			res := s.Send(context.Background(), slackInput(pb.SlackDelivery_SLACK_DELIVERY_LINK))
			if res.Outcome != tt.want {
				t.Fatalf("outcome = %v, want %v", res.Outcome, tt.want)
			}
		})
	}
}

// TestSlackRateLimitCarriesRetryAfter pins that a Slack 429 is transient and
// hands the dispatcher Slack's requested wait.
func TestSlackRateLimitCarriesRetryAfter(t *testing.T) {
	s := sender.NewSlackSender(fakeBroker{token: "x"}, &fakePoster{postErr: &sender.SlackError{Code: "ratelimited", RetryAfter: 30 * time.Second}}, fakeFetcher{})
	res := s.Send(context.Background(), slackInput(pb.SlackDelivery_SLACK_DELIVERY_LINK))
	if res.Outcome != dispatch.OutcomeTransient || res.RetryAfter != 30*time.Second {
		t.Fatalf("outcome %v, retry after %v; want transient, 30s", res.Outcome, res.RetryAfter)
	}
}

// TestStaticTokensCanonicalOrgID pins that the per-org lookup matches an org id
// in any case against the canonical lowercase key.
func TestStaticTokensCanonicalOrgID(t *testing.T) {
	tokens := sender.StaticTokens{PerOrg: map[string]string{"8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f": "tok-a"}}
	for _, org := range []string{"8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f", "8C4F6F0E-6A7B-4C1D-9E2F-3A4B5C6D7E8F"} {
		if tok, err := tokens.Token(context.Background(), org); err != nil || tok != "tok-a" {
			t.Fatalf("Token(%q) = %q, %v; want tok-a", org, tok, err)
		}
	}
	if _, err := tokens.Token(context.Background(), "2b0c7a3e-1f4d-4e5a-8b9c-0d1e2f3a4b5c"); !errors.Is(err, sender.ErrNoSlackToken) {
		t.Fatalf("another org: %v, want ErrNoSlackToken", err)
	}
}
