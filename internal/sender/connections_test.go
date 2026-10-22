// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sender_test

import (
	"context"
	"errors"
	"testing"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/sender"
)

type tokenFunc func(context.Context, string) (string, error)

func (f tokenFunc) Token(ctx context.Context, org string) (string, error) { return f(ctx, org) }

// TestConnectionsReadTheProvider pins how a provider's answer reads as a Slack
// connection: a token is connected, ErrNoSlackToken is not, and any other
// error is a failed lookup. Email needs no connection and asks nothing.
func TestConnectionsReadTheProvider(t *testing.T) {
	outage := errors.New("vault unreachable")
	for _, tc := range []struct {
		name    string
		channel pb.Channel
		tok     string
		err     error
		want    bool
		wantErr error
	}{
		{"a token", pb.Channel_CHANNEL_SLACK, "xoxb-1", nil, true, nil},
		{"no token", pb.Channel_CHANNEL_SLACK, "", sender.ErrNoSlackToken, false, nil},
		{"a lookup failure", pb.Channel_CHANNEL_SLACK, "", outage, false, outage},
		{"email", pb.Channel_CHANNEL_EMAIL, "", outage, true, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := sender.Connections{Tokens: tokenFunc(func(context.Context, string) (string, error) { return tc.tok, tc.err })}
			got, err := c.IsConnected(context.Background(), "8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f", tc.channel)
			if got != tc.want || !errors.Is(err, tc.wantErr) {
				t.Fatalf("IsConnected = %v, %v; want %v, %v", got, err, tc.want, tc.wantErr)
			}
		})
	}
}
