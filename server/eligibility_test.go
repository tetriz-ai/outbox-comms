// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package server_test

import (
	"context"
	"slices"
	"testing"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/server"
)

// orgTokens is a plain TokenProvider: one token per listed organization, and
// ErrNoSlackToken for every other.
type orgTokens map[string]string

func (o orgTokens) Token(_ context.Context, orgID string) (string, error) {
	if tok, ok := o[orgID]; ok {
		return tok, nil
	}
	return "", server.ErrNoSlackToken
}

// TestEligibilityAsksTheSendersTokenProvider pins that CheckSendEligibility
// reads a Slack connection from the same TokenProvider the sender uses, not
// from the environment's tokens, so eligibility and the send agree.
func TestEligibilityAsksTheSendersTokenProvider(t *testing.T) {
	const (
		connected    = "4c1d9e2f-3a4b-4c5d-8e6f-7a8b9c0d1e2f"
		disconnected = "5d2e0f3a-4b5c-4d6e-9f7a-8b9c0d1e2f3a"
	)
	tokens := orgTokens{connected: "xoxb-from-the-provider"}
	for _, tc := range []struct {
		name     string
		envToken string
		org      string
		reasons  []string
	}{
		{"no environment token, the provider has one", "", connected, nil},
		{"an environment token, the provider has none", "test-token", disconnected, []string{"no_connection"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := startWithOptions(t, map[string]string{"OUTBOX_COMMS_SLACK_BOT_TOKEN": tc.envToken}, server.Options{SlackTokens: tokens}, nil)
			resp, err := e.client.CheckSendEligibility(context.Background(), &pb.CheckSendEligibilityRequest{
				OrganizationId: tc.org, Usecase: "notice", Channels: []pb.Channel{pb.Channel_CHANNEL_SLACK},
			})
			if err != nil {
				t.Fatal(err)
			}
			got := resp.GetChannelEligibility()[0]
			if got.GetEligible() != (len(tc.reasons) == 0) || !slices.Equal(got.GetReasons(), tc.reasons) {
				t.Fatalf("eligible %v, reasons %v; want reasons %v", got.GetEligible(), got.GetReasons(), tc.reasons)
			}
		})
	}
}
