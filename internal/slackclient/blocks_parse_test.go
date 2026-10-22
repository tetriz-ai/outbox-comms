// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package slackclient

import (
	"context"
	"errors"
	"testing"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dispatch"
	"github.com/tetriz-ai/outbox-comms/internal/sender"
)

type oneToken struct{}

func (oneToken) Token(context.Context, string) (string, error) { return "xoxb-test", nil }

// TestUnparsableBlocksArePermanent pins that blocks the client cannot parse
// end the send as permanent, with no call to Slack: the same blocks would
// fail on every retry.
func TestUnparsableBlocksArePermanent(t *testing.T) {
	f := &fakeSlack{postBody: `{"ok":true,"ts":"1"}`}
	c, _ := newClient(t, f)
	for _, blocks := range []string{`[{"type":"section"`, `{"type":"divider"}`} {
		_, err := c.PostMessage(context.Background(), "xoxb-test", "C1", "hi", blocks)
		var se *sender.SlackError
		if !errors.Is(err, sender.ErrNotSent) || !errors.As(err, &se) || se.Code != "invalid_blocks" {
			t.Fatalf("blocks %s: error %v, want not sent and invalid_blocks", blocks, err)
		}
		res := sender.NewSlackSender(oneToken{}, c, nil).Send(context.Background(), dispatch.SendInput{
			OrgID: "8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f", Channel: pb.Channel_CHANNEL_SLACK, Recipient: "C1",
			Rendered: &pb.RenderedPayload{SlackText: "hi", SlackBlocksJson: blocks},
		})
		if res.Outcome != dispatch.OutcomePermanent {
			t.Fatalf("blocks %s: outcome %v (%v), want permanent", blocks, res.Outcome, res.Err)
		}
	}
	if n := len(f.get()); n != 0 {
		t.Fatalf("%d call(s) reached Slack, want none", n)
	}
}
