// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package dispatch_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dispatch"
	"github.com/tetriz-ai/outbox-comms/templates"
)

// badBlocks maps a usecase key to the Slack blocks its renderer returns, one
// usecase per malformed shape.
var badBlocks = map[string]string{
	"test.badblocks.json":   `[{"type":"section","text":{"type":"mrkdwn","text":"INV"1"}}]`,
	"test.badblocks.object": `{"type":"section","text":{"type":"mrkdwn","text":"a"}}`,
	"test.badblocks.51":     "[" + strings.TrimSuffix(strings.Repeat(`{"type":"divider"},`, 51), ",") + "]",
}

func init() {
	for key, blocks := range badBlocks {
		templates.MustRegister(key, templates.Contract{
			NewParams:           func() proto.Message { return &structpb.Struct{} },
			Category:            pb.MessageCategory_MESSAGE_CATEGORY_TRANSACTIONAL,
			EnabledChannels:     []pb.Channel{pb.Channel_CHANNEL_SLACK},
			DefaultVerification: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK,
			DefaultGuarantee:    pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
			Renderers: map[templates.ChannelVendor]templates.Renderer{
				{Channel: pb.Channel_CHANNEL_SLACK, Vendor: pb.Vendor_VENDOR_SLACK}: func(proto.Message) (*pb.RenderedPayload, error) {
					return &pb.RenderedPayload{SlackText: "hi", SlackBlocksJson: blocks}, nil
				},
			},
		})
	}
}

// TestDispatchInvalidSlackBlocksIsARenderBug pins that a renderer whose Slack
// blocks are not valid JSON, not an array, or over 50 blocks ends the message
// DEAD(render_bug) without a send, instead of a send that fails and retries.
func TestDispatchInvalidSlackBlocksIsARenderBug(t *testing.T) {
	for key := range badBlocks {
		t.Run(key, func(t *testing.T) {
			ctx := context.Background()
			f := newFixture(t)
			id := f.seedQueued(t, "k-"+key, pb.Channel_CHANNEL_SLACK, pb.Vendor_VENDOR_SLACK, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, key)
			s := &countingSender{res: dispatch.SendResult{Outcome: dispatch.OutcomeSuccess, ProviderMessageID: "ts"}}
			if err := f.dispatcher(s, pb.Channel_CHANNEL_SLACK).Dispatch(ctx, testOrg, id); err != nil {
				t.Fatal(err)
			}
			got := f.status(t, id)
			if got.Status != pb.MessageStatus_MESSAGE_STATUS_DEAD || got.DeadReason == nil || *got.DeadReason != "render_bug" || s.calls != 0 {
				t.Fatalf("status %v, dead %v, %d send(s); want DEAD(render_bug) and none", got.Status, got.DeadReason, s.calls)
			}
		})
	}
}

// cancellingSender answers success and ends the caller's context as it does,
// as a shutdown arriving the moment the provider answers would.
type cancellingSender struct{ cancel context.CancelFunc }

func (c cancellingSender) Send(context.Context, dispatch.SendInput) dispatch.SendResult {
	c.cancel()
	return dispatch.SendResult{Outcome: dispatch.OutcomeSuccess, ProviderMessageID: "ses-1"}
}

// TestOutcomeIsWrittenAfterTheContextEnds pins that the provider's answer is
// recorded even when the dispatch context ends right after the call: the row
// leaves SENDING, so reconcile does not send the message again.
func TestOutcomeIsWrittenAfterTheContextEnds(t *testing.T) {
	f := newFixture(t)
	id := f.seedQueued(t, "k-cancel-after-send", pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, typedUsecase)
	f.tx.Exec(`UPDATE comms_requests SET params = '{"title":"Welcome"}' WHERE id = ?`, f.status(t, id).RequestID)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := f.dispatcher(cancellingSender{cancel: cancel}, pb.Channel_CHANNEL_EMAIL).Dispatch(ctx, testOrg, id); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if got := f.status(t, id); got.Status != pb.MessageStatus_MESSAGE_STATUS_SENT {
		t.Fatalf("status %v after the context ended with the send done; want SENT", got.Status)
	}
}

// TestLadderLastStepRepeats pins the clamp: with an attempt cap past the end of
// the ladder, every later retry waits the last step.
func TestLadderLastStepRepeats(t *testing.T) {
	now := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	for _, attempts := range []int{1, 2, 7} {
		f := newFixture(t)
		id := f.seedQueued(t, fmt.Sprint("k-clamp-", attempts), pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, typedUsecase)
		f.tx.Exec("UPDATE comms_messages SET attempts = ? WHERE id = ?", attempts, id)
		f.tx.Exec(`UPDATE comms_requests SET params = '{"title":"Welcome"}' WHERE id = ?`, f.status(t, id).RequestID)
		d := dispatch.NewDispatcher(f.msgRepo, f.reqRepo, f.supRepo, f.evRepo,
			map[pb.Channel]dispatch.Sender{pb.Channel_CHANNEL_EMAIL: fakeSender{res: dispatch.SendResult{Outcome: dispatch.OutcomeTransient, Err: errors.New("x")}}},
			dispatch.RetryPolicy{Ladder: []time.Duration{time.Minute, 5 * time.Minute}, MaxAttempts: 10}, f.reporter, f.retry,
			func() time.Time { return now }, templates.EmailChromeOptions{})
		if err := d.Dispatch(context.Background(), testOrg, id); err != nil {
			t.Fatal(err)
		}
		got := f.status(t, id)
		want := 5 * time.Minute
		if got.Status != pb.MessageStatus_MESSAGE_STATUS_FAILED || got.NextRetryAt == nil || got.NextRetryAt.Sub(now) != want {
			t.Fatalf("attempts %d: %v, next %v; want FAILED after %v", attempts, got.Status, got.NextRetryAt, want)
		}
	}
}
