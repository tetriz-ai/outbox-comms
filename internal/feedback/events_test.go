// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package feedback_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/feedback"
	"github.com/tetriz-ai/outbox-comms/internal/queue"
)

// TestUntrackedEventTypesAreAcknowledged proves an event type the service does
// not track (SES publishes Send, Open, Click, DeliveryDelay and more) is
// acknowledged and writes nothing, rather than retrying into a dead-letter
// queue.
func TestUntrackedEventTypesAreAcknowledged(t *testing.T) {
	f := newFix(t)
	id := f.seedSent(t, "pm-untracked", "untracked@example.com")
	for _, kind := range []string{"Send", "Open", "Click", "DeliveryDelay", "Reject", "RenderingFailure", "Subscription"} {
		if err := f.cons.ProcessRaw(context.Background(), []byte(`{"eventType":"`+kind+`","mail":{"messageId":"pm-untracked"}}`)); err != nil {
			t.Fatalf("%s: want nil, got %v", kind, err)
		}
	}
	evs, _ := f.evRepo.ListByMessage(context.Background(), testOrg, id)
	if len(evs) != 0 {
		t.Fatalf("untracked events must write nothing, got %d events", len(evs))
	}
}

// TestOccurredAtIsProviderTime proves occurred_at is the event's own time: the
// event timestamp, else the send's, else the receipt time.
func TestOccurredAtIsProviderTime(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want time.Time
	}{
		{"delivery timestamp", `{"eventType":"Delivery","mail":{"messageId":"%s","timestamp":"2026-01-02T03:04:05Z"},"delivery":{"timestamp":"2026-01-02T03:04:06Z"}}`, time.Date(2026, 1, 2, 3, 4, 6, 0, time.UTC)},
		{"bounce timestamp", `{"eventType":"Bounce","mail":{"messageId":"%s"},"bounce":{"bounceType":"Transient","feedbackId":"fb-%s","timestamp":"2026-01-03T00:00:00Z"}}`, time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)},
		{"complaint timestamp", `{"eventType":"Complaint","mail":{"messageId":"%s"},"complaint":{"feedbackId":"cf-%s","timestamp":"2026-01-04T00:00:00Z"}}`, time.Date(2026, 1, 4, 0, 0, 0, 0, time.UTC)},
		{"mail timestamp when the event has none", `{"eventType":"Delivery","mail":{"messageId":"%s","timestamp":"2026-01-05T00:00:00Z"}}`, time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFix(t)
			pmid := "pm-at-" + string(rune('a'+i))
			id := f.seedSent(t, pmid, pmid+"@example.com")
			raw := []byte(strings.ReplaceAll(tt.raw, "%s", pmid))
			if err := f.cons.ProcessRaw(context.Background(), raw); err != nil {
				t.Fatal(err)
			}
			evs, _ := f.evRepo.ListByMessage(context.Background(), testOrg, id)
			if len(evs) != 1 || !evs[0].OccurredAt.Equal(tt.want) {
				t.Fatalf("occurred_at = %v, want %v", evs, tt.want)
			}
		})
	}
	t.Run("receipt time when nothing carries a time", func(t *testing.T) {
		f := newFix(t)
		id := f.seedSent(t, "pm-at-now", "now@example.com")
		before := time.Now().Add(-time.Second)
		if err := f.cons.ProcessRaw(context.Background(), []byte(`{"eventType":"Delivery","mail":{"messageId":"pm-at-now"}}`)); err != nil {
			t.Fatal(err)
		}
		evs, _ := f.evRepo.ListByMessage(context.Background(), testOrg, id)
		if len(evs) != 1 || evs[0].OccurredAt.Before(before) {
			t.Fatalf("occurred_at = %v, want about now", evs)
		}
	})
}

func orphanCount(t *testing.T) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == "outbox_comms_orphan_feedback_total" {
			return mf.GetMetric()[0].GetCounter().GetValue()
		}
	}
	return 0
}

// TestHandleOrphans proves an unmatched event is retried until its fifth
// delivery, then counted in outbox_comms_orphan_feedback_total and dropped.
func TestHandleOrphans(t *testing.T) {
	f := newFix(t)
	raw := []byte(`{"eventType":"Delivery","mail":{"messageId":"pm-orphan"}}`)
	before := orphanCount(t)
	for n := 1; n < feedback.DefaultMaxReceives; n++ {
		if err := f.cons.Handle(context.Background(), raw, n); !errors.Is(err, feedback.ErrNoMatch) || queue.IsPermanent(err) {
			t.Fatalf("receive %d: want a retryable ErrNoMatch, got %v", n, err)
		}
	}
	if got := orphanCount(t); got != before {
		t.Fatalf("orphans counted before the last receive: %v -> %v", before, got)
	}
	if err := f.cons.Handle(context.Background(), raw, feedback.DefaultMaxReceives); err != nil {
		t.Fatalf("last receive: want nil (dropped), got %v", err)
	}
	if got := orphanCount(t); got != before+1 {
		t.Fatalf("orphan counter %v -> %v, want +1", before, got)
	}
	f.cons.MaxReceives = 2
	if err := f.cons.Handle(context.Background(), raw, 2); err != nil {
		t.Fatalf("MaxReceives=2 must drop at the second receive, got %v", err)
	}
}

// TestProviderPayloadStored proves the raw notification is kept on the event,
// so the prune job has a payload to strip.
func TestProviderPayloadStored(t *testing.T) {
	f := newFix(t)
	id := f.seedSent(t, "pm-payload", "payload@example.com")
	raw := `{"eventType":"Delivery","mail":{"messageId":"pm-payload"},"delivery":{"smtpResponse":"250 ok"}}`
	if err := f.cons.ProcessRaw(context.Background(), []byte(raw)); err != nil {
		t.Fatal(err)
	}
	var smtp string
	testDB.Raw("SELECT provider_payload->'delivery'->>'smtpResponse' FROM comms_delivery_events WHERE message_id = ?", id).Scan(&smtp)
	if smtp != "250 ok" {
		t.Fatalf("stored payload smtpResponse = %q, want the raw event", smtp)
	}
}

// TestUnfixableBodiesArePermanent proves a body that cannot be parsed, or names
// no message, is dropped (queue.Permanent), never retried.
func TestUnfixableBodiesArePermanent(t *testing.T) {
	f := newFix(t)
	for _, raw := range []string{`{not json`, `{"eventType":"Delivery","mail":{}}`, `[]`} {
		if err := f.cons.ProcessRaw(context.Background(), []byte(raw)); !queue.IsPermanent(err) {
			t.Errorf("%s: want a permanent error, got %v", raw, err)
		}
	}
}

// TestSNSEnvelopeUnwrapped proves a notification inside the SNS envelope (an SQS
// subscription without raw message delivery) is applied the same as a raw one.
func TestSNSEnvelopeUnwrapped(t *testing.T) {
	f := newFix(t)
	id := f.seedSent(t, "pm-sns", "sns@example.com")
	raw := `{"Type":"Notification","MessageId":"x","Message":"{\"eventType\":\"Delivery\",\"mail\":{\"messageId\":\"pm-sns\"}}"}`
	if err := f.cons.ProcessRaw(context.Background(), []byte(raw)); err != nil {
		t.Fatal(err)
	}
	m, _ := f.msgRepo.GetByID(context.Background(), testOrg, id)
	if m.Status != pb.MessageStatus_MESSAGE_STATUS_DELIVERED {
		t.Fatalf("status = %v, want DELIVERED", m.Status)
	}
}
