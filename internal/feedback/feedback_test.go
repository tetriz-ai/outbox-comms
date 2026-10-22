// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package feedback_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/types/known/structpb"
	"gorm.io/gorm"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
	"github.com/tetriz-ai/outbox-comms/internal/feedback"
	"github.com/tetriz-ai/outbox-comms/internal/testdb"
	"github.com/tetriz-ai/outbox-comms/templates"
)

var testDB *gorm.DB

func TestMain(m *testing.M) { os.Exit(testdb.Main(m, &testDB)) }

const testOrg = "11111111-1111-1111-1111-111111111111"

type fix struct {
	tx      *gorm.DB
	msgRepo *dao.MessageRepo
	evRepo  *dao.DeliveryEventRepo
	supRepo *dao.SuppressionRepo
	cons    *feedback.Consumer
}

// newFix runs on the base testDB (not a tx) because GetByProviderMessageID opens
// its own transaction. Rows are cleaned up explicitly at test end (tests run
// sequentially under one org).
func newFix(t *testing.T) *fix {
	t.Helper()
	t.Cleanup(func() {
		testDB.Exec("DELETE FROM comms_delivery_events WHERE organization_id = ?", testOrg)
		testDB.Exec("DELETE FROM comms_messages WHERE organization_id = ?", testOrg)
		testDB.Exec("DELETE FROM comms_requests WHERE organization_id = ?", testOrg)
	})
	f := &fix{tx: testDB, msgRepo: dao.NewMessageRepo(testDB), evRepo: dao.NewDeliveryEventRepo(testDB), supRepo: dao.NewSuppressionRepo(testDB)}
	f.cons = feedback.NewConsumer(f.msgRepo, f.evRepo, f.supRepo)
	return f
}

// seedSent creates a SENT email message with a provider id + unique recipient.
func (f *fix) seedSent(t *testing.T, providerID, recipient string) string {
	t.Helper()
	ctx := context.Background()
	reqRepo := dao.NewRequestRepo(f.tx)
	_, req, err := reqRepo.CreateIfAbsent(ctx, &model.CommsRequest{
		OrganizationID: testOrg, IdempotencyKey: "fb:" + providerID, Usecase: templates.UsecaseNotice,
		DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, Status: pb.RequestStatus_REQUEST_STATUS_ACCEPTED,
		Params: &structpb.Struct{},
	})
	if err != nil {
		t.Fatalf("seed request: %v", err)
	}
	msg := &model.CommsMessage{
		OrganizationID: testOrg, RequestID: req.ID, Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SES,
		Recipient: recipient, Status: pb.MessageStatus_MESSAGE_STATUS_QUEUED,
		VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_CONFIRMED, DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
	}
	if _, err := f.msgRepo.MaterializeBatch(ctx, testOrg, []*model.CommsMessage{msg}); err != nil {
		t.Fatalf("seed msg: %v", err)
	}
	q, _ := f.msgRepo.ListQueuedForRequest(ctx, testOrg, req.ID)
	id := q[0].ID
	ok, claimed, err := f.msgRepo.ClaimForDispatch(ctx, testOrg, id)
	if !ok || err != nil {
		t.Fatalf("claim: %v %v", ok, err)
	}
	if err := f.msgRepo.MarkSent(ctx, testOrg, id, *claimed.ClaimID, providerID); err != nil {
		t.Fatalf("mark sent: %v", err)
	}
	return id
}

func (f *fix) get(t *testing.T, id string) *model.CommsMessage {
	t.Helper()
	m, err := f.msgRepo.GetByID(context.Background(), testOrg, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	return m
}

func deliveryJSON(pmid string) []byte {
	return []byte(fmt.Sprintf(`{"notificationType":"Delivery","mail":{"messageId":%q,"destination":["x@example.com"]}}`, pmid))
}

// TestFeedbackDelivery proves a Delivery event advances SENT → DELIVERED.
func TestFeedbackDelivery(t *testing.T) {
	f := newFix(t)
	id := f.seedSent(t, "ses-del-1", "del@example.com")
	if err := f.cons.ProcessRaw(context.Background(), deliveryJSON("ses-del-1")); err != nil {
		t.Fatalf("process: %v", err)
	}
	if got := f.get(t, id).Status; got != pb.MessageStatus_MESSAGE_STATUS_DELIVERED {
		t.Fatalf("status = %v, want DELIVERED", got)
	}
}

// TestFeedbackHardBounce proves a permanent bounce → BOUNCED + a suppression.
func TestFeedbackHardBounce(t *testing.T) {
	ctx := context.Background()
	f := newFix(t)
	email := fmt.Sprintf("bnc-%d@example.com", time.Now().UnixNano())
	t.Cleanup(func() { testDB.Exec("DELETE FROM comms_suppressions WHERE email = ?", email) })
	id := f.seedSent(t, "ses-bnc-1", email)
	raw := []byte(fmt.Sprintf(`{"notificationType":"Bounce","mail":{"messageId":"ses-bnc-1"},"bounce":{"bounceType":"Permanent","feedbackId":"fb-1","bouncedRecipients":[{"emailAddress":%q}]}}`, email))
	if err := f.cons.ProcessRaw(ctx, raw); err != nil {
		t.Fatalf("process: %v", err)
	}
	if got := f.get(t, id).Status; got != pb.MessageStatus_MESSAGE_STATUS_BOUNCED {
		t.Fatalf("status = %v, want BOUNCED", got)
	}
	sup, _ := f.supRepo.GetActiveByEmail(ctx, email)
	if sup == nil {
		t.Fatalf("hard bounce must suppress the address")
	}
}

// TestFeedbackComplaint proves a complaint suppresses but leaves the (delivered)
// status forward-only — the row stays DELIVERED, the complaint is an event fact.
func TestFeedbackComplaint(t *testing.T) {
	ctx := context.Background()
	f := newFix(t)
	email := fmt.Sprintf("cmp-%d@example.com", time.Now().UnixNano())
	t.Cleanup(func() { testDB.Exec("DELETE FROM comms_suppressions WHERE email = ?", email) })
	id := f.seedSent(t, "ses-cmp-1", email)
	if err := f.msgRepo.MarkDelivered(ctx, testOrg, id, "", "ses-cmp-1"); err != nil {
		t.Fatalf("pre-deliver: %v", err)
	}
	raw := []byte(fmt.Sprintf(`{"notificationType":"Complaint","mail":{"messageId":"ses-cmp-1"},"complaint":{"feedbackId":"fbc-1","complainedRecipients":[{"emailAddress":%q}]}}`, email))
	if err := f.cons.ProcessRaw(ctx, raw); err != nil {
		t.Fatalf("process: %v", err)
	}
	if got := f.get(t, id).Status; got != pb.MessageStatus_MESSAGE_STATUS_DELIVERED {
		t.Fatalf("complaint must not overwrite DELIVERED, got %v", got)
	}
	sup, _ := f.supRepo.GetActiveByEmail(ctx, email)
	if sup == nil {
		t.Fatalf("complaint must suppress the address")
	}
}

// TestFeedbackDeliveryEventPublishing proves the real wire shape: SES
// configuration-set event publishing uses `eventType` (not `notificationType`),
// and a Delivery in that shape must still advance SENT → DELIVERED. Reading only
// the older field would drop every event of this shape.
func TestFeedbackDeliveryEventPublishing(t *testing.T) {
	f := newFix(t)
	id := f.seedSent(t, "ses-evt-del-1", "evtdel@example.com")
	raw := []byte(`{"eventType":"Delivery","mail":{"messageId":"ses-evt-del-1","destination":["evtdel@example.com"]},"delivery":{"recipients":["evtdel@example.com"]}}`)
	if err := f.cons.ProcessRaw(context.Background(), raw); err != nil {
		t.Fatalf("process: %v", err)
	}
	if got := f.get(t, id).Status; got != pb.MessageStatus_MESSAGE_STATUS_DELIVERED {
		t.Fatalf("status = %v, want DELIVERED", got)
	}
}

// TestFeedbackHardBounceEventPublishing proves a permanent bounce in the
// `eventType` (event-publishing) shape → BOUNCED + suppression.
func TestFeedbackHardBounceEventPublishing(t *testing.T) {
	ctx := context.Background()
	f := newFix(t)
	email := fmt.Sprintf("evtbnc-%d@example.com", time.Now().UnixNano())
	t.Cleanup(func() { testDB.Exec("DELETE FROM comms_suppressions WHERE email = ?", email) })
	id := f.seedSent(t, "ses-evt-bnc-1", email)
	raw := []byte(fmt.Sprintf(`{"eventType":"Bounce","mail":{"messageId":"ses-evt-bnc-1"},"bounce":{"bounceType":"Permanent","feedbackId":"evtfb-1","bouncedRecipients":[{"emailAddress":%q}]}}`, email))
	if err := f.cons.ProcessRaw(ctx, raw); err != nil {
		t.Fatalf("process: %v", err)
	}
	if got := f.get(t, id).Status; got != pb.MessageStatus_MESSAGE_STATUS_BOUNCED {
		t.Fatalf("status = %v, want BOUNCED", got)
	}
	if sup, _ := f.supRepo.GetActiveByEmail(ctx, email); sup == nil {
		t.Fatalf("hard bounce must suppress the address")
	}
}

// TestFeedbackNoMatch proves an event for an unmailed provider id
// writes nothing and returns ErrNoMatch (the consumer NACKs).
func TestFeedbackNoMatch(t *testing.T) {
	f := newFix(t)
	err := f.cons.ProcessRaw(context.Background(), deliveryJSON("ses-never-mailed"))
	if !errors.Is(err, feedback.ErrNoMatch) {
		t.Fatalf("want ErrNoMatch, got %v", err)
	}
}

// TestFeedbackDedup proves a redelivered event with the same provider_event_id is
// a no-op (one event row), not a double-count.
func TestFeedbackDedup(t *testing.T) {
	ctx := context.Background()
	f := newFix(t)
	id := f.seedSent(t, "ses-dup-1", "dup@example.com")
	for i := 0; i < 2; i++ {
		if err := f.cons.ProcessRaw(ctx, deliveryJSON("ses-dup-1")); err != nil {
			t.Fatalf("process %d: %v", i, err)
		}
	}
	var count int64
	f.tx.Model(&model.CommsDeliveryEvent{}).Where("message_id = ?", id).Count(&count)
	if count != 1 {
		t.Fatalf("redelivered event must dedup to 1 row, got %d", count)
	}
}

// TestFeedbackTransientBounce proves a transient (soft) bounce is recorded as an
// event but neither suppresses the address nor moves the row to BOUNCED.
func TestFeedbackTransientBounce(t *testing.T) {
	ctx := context.Background()
	f := newFix(t)
	email := fmt.Sprintf("soft-%d@example.com", time.Now().UnixNano())
	t.Cleanup(func() { testDB.Exec("DELETE FROM comms_suppressions WHERE email = ?", email) })
	id := f.seedSent(t, "ses-soft-1", email)
	raw := []byte(fmt.Sprintf(`{"notificationType":"Bounce","mail":{"messageId":"ses-soft-1"},"bounce":{"bounceType":"Transient","feedbackId":"fb-soft-1","bouncedRecipients":[{"emailAddress":%q}]}}`, email))
	if err := f.cons.ProcessRaw(ctx, raw); err != nil {
		t.Fatalf("process: %v", err)
	}
	if got := f.get(t, id).Status; got != pb.MessageStatus_MESSAGE_STATUS_SENT {
		t.Fatalf("status = %v, want SENT", got)
	}
	if sup, _ := f.supRepo.GetActiveByEmail(ctx, email); sup != nil {
		t.Fatal("a transient bounce must not suppress the address")
	}
	evs, _ := f.evRepo.ListByMessage(ctx, testOrg, id)
	if len(evs) != 1 || evs[0].EventType != pb.DeliveryEventType_DELIVERY_EVENT_TYPE_BOUNCE {
		t.Fatalf("events = %+v, want one BOUNCE", evs)
	}
}

// TestTransitionMetricsBounced pins that a hard bounce is counted in
// outbox_comms_message_transitions_total under the message's usecase.
func TestTransitionMetricsBounced(t *testing.T) {
	ctx := context.Background()
	labels := map[string]string{"usecase": templates.UsecaseNotice, "channel": "CHANNEL_EMAIL", "to_status": "MESSAGE_STATUS_BOUNCED"}
	before := counter(t, "outbox_comms_message_transitions_total", labels)
	f := newFix(t)
	f.cons.Requests = dao.NewRequestRepo(testDB)
	email := fmt.Sprintf("metric-bnc-%d@example.com", time.Now().UnixNano())
	t.Cleanup(func() { testDB.Exec("DELETE FROM comms_suppressions WHERE email = ?", email) })
	f.seedSent(t, "ses-metric-bnc", email)
	raw := []byte(fmt.Sprintf(`{"notificationType":"Bounce","mail":{"messageId":"ses-metric-bnc"},"bounce":{"bounceType":"Permanent","feedbackId":"fb-metric","bouncedRecipients":[{"emailAddress":%q}]}}`, email))
	// The same notification twice, as SQS may redeliver it, counts once.
	for range 2 {
		if err := f.cons.ProcessRaw(ctx, raw); err != nil {
			t.Fatal(err)
		}
	}
	if got := counter(t, "outbox_comms_message_transitions_total", labels); got != before+1 {
		t.Fatalf("BOUNCED transitions went %v -> %v, want +1", before, got)
	}
}

// TestTransitionMetricsDelivered pins that an SES Delivery that moves a row to
// DELIVERED is counted under the message's usecase, once across a redelivery.
func TestTransitionMetricsDelivered(t *testing.T) {
	ctx := context.Background()
	labels := map[string]string{"usecase": templates.UsecaseNotice, "channel": "CHANNEL_EMAIL", "to_status": "MESSAGE_STATUS_DELIVERED"}
	before := counter(t, "outbox_comms_message_transitions_total", labels)
	f := newFix(t)
	f.cons.Requests = dao.NewRequestRepo(testDB)
	f.seedSent(t, "ses-metric-del", "metric-del@example.com")
	for range 2 {
		if err := f.cons.ProcessRaw(ctx, deliveryJSON("ses-metric-del")); err != nil {
			t.Fatal(err)
		}
	}
	if got := counter(t, "outbox_comms_message_transitions_total", labels); got != before+1 {
		t.Fatalf("DELIVERED transitions went %v -> %v, want +1", before, got)
	}
}

// counter reads one series of a counter from the default registry, matching
// every label given, or 0 before its first increment.
func counter(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
	metric:
		for _, m := range mf.GetMetric() {
			got := map[string]string{}
			for _, l := range m.GetLabel() {
				got[l.GetName()] = l.GetValue()
			}
			for k, v := range labels {
				if got[k] != v {
					continue metric
				}
			}
			return m.GetCounter().GetValue()
		}
	}
	return 0
}

// TestPurgeLeavesATombstone pins that purging an organization does not lose the
// suppression a late hard bounce or complaint would add. The purge keeps the
// sent message's provider id and lowercased address. A permanent bounce or a
// complaint for it then suppresses the address and is not an orphan; a
// transient bounce suppresses nothing.
func TestPurgeLeavesATombstone(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name, raw string
		suppress  bool
	}{
		{"permanent bounce", `{"notificationType":"Bounce","mail":{"messageId":"%s"},"bounce":{"bounceType":"Permanent","feedbackId":"fb-%s","bouncedRecipients":[{"emailAddress":"x@example.com"}]}}`, true},
		{"complaint", `{"notificationType":"Complaint","mail":{"messageId":"%s"},"complaint":{"feedbackId":"fc-%s","complainedRecipients":[{"emailAddress":"x@example.com"}]}}`, true},
		{"transient bounce", `{"notificationType":"Bounce","mail":{"messageId":"%s"},"bounce":{"bounceType":"Transient","feedbackId":"ft-%s","bouncedRecipients":[{"emailAddress":"x@example.com"}]}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFix(t)
			pmid := fmt.Sprintf("ses-purged-%d", time.Now().UnixNano())
			addr := fmt.Sprintf("Purged-%d@Example.com", time.Now().UnixNano())
			lower := strings.ToLower(addr)
			t.Cleanup(func() {
				testDB.Exec("DELETE FROM comms_suppressions WHERE email = ?", lower)
				testDB.Exec("DELETE FROM comms_purge_tombstones WHERE provider_message_id = ?", pmid)
			})
			f.seedSent(t, pmid, addr)
			if _, err := dao.NewReconcileRepo(testDB).PurgeOrg(ctx, testOrg); err != nil {
				t.Fatal(err)
			}
			// The tombstone keeps the address lowercased, as suppressions do.
			if got, ok, err := f.supRepo.TombstoneRecipient(ctx, pmid, time.Time{}); err != nil || !ok || got != lower {
				t.Fatalf("tombstone recipient %q (found %v, %v), want %q", got, ok, err, lower)
			}
			orphans := counter(t, "outbox_comms_orphan_feedback_total", nil)
			if err := f.cons.Handle(ctx, []byte(fmt.Sprintf(tc.raw, pmid, pmid)), feedback.DefaultMaxReceives); err != nil {
				t.Fatalf("handle: %v", err)
			}
			sup, err := f.supRepo.GetActiveByEmail(ctx, lower)
			if err != nil {
				t.Fatal(err)
			}
			if (sup != nil) != tc.suppress {
				t.Fatalf("suppressed %v, want %v", sup != nil, tc.suppress)
			}
			if got := counter(t, "outbox_comms_orphan_feedback_total", nil); got != orphans {
				t.Fatalf("orphan count went %v -> %v; a tombstone match is not an orphan", orphans, got)
			}
		})
	}
}

// TestDroppedFeedbackIsCounted pins that feedback the consumer cannot read is
// counted by reason as it is dropped: a body that is not JSON, and one with no
// mail.messageId.
func TestDroppedFeedbackIsCounted(t *testing.T) {
	f := newFix(t)
	for reason, raw := range map[string]string{
		"malformed_json":     `not json`,
		"missing_message_id": `{"notificationType":"Bounce","mail":{}}`,
	} {
		before := counter(t, "outbox_comms_feedback_dropped_total", map[string]string{"reason": reason})
		if err := f.cons.ProcessRaw(context.Background(), []byte(raw)); err == nil {
			t.Fatalf("%s: want an error", reason)
		}
		if after := counter(t, "outbox_comms_feedback_dropped_total", map[string]string{"reason": reason}); after != before+1 {
			t.Fatalf("%s: dropped count went %v -> %v, want +1", reason, before, after)
		}
	}
}

// TestTombstoneMatchesOnlyWithinItsTTL pins that a purge tombstone matches late
// feedback only while it is younger than the TTL, the same window the prune job
// keeps: a minute inside it the bounce suppresses; a minute past it the event is
// unmatched, as if the prune had already run.
func TestTombstoneMatchesOnlyWithinItsTTL(t *testing.T) {
	ctx := context.Background()
	const ttl = 30 * 24 * time.Hour
	f := newFix(t)
	f.cons.TombstoneTTL = ttl
	for _, tc := range []struct {
		name     string
		age      time.Duration
		suppress bool
	}{
		{"a minute inside the TTL", ttl - time.Minute, true},
		{"a minute past the TTL", ttl + time.Minute, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pmid := fmt.Sprintf("ses-ttl-%d", time.Now().UnixNano())
			addr := fmt.Sprintf("ttl-%d@example.com", time.Now().UnixNano())
			t.Cleanup(func() {
				testDB.Exec("DELETE FROM comms_suppressions WHERE email = ?", addr)
				testDB.Exec("DELETE FROM comms_purge_tombstones WHERE provider_message_id = ?", pmid)
			})
			if err := testDB.Exec(`INSERT INTO comms_purge_tombstones (provider_message_id, recipient, created_at) VALUES (?, ?, ?)`,
				pmid, addr, time.Now().Add(-tc.age)).Error; err != nil {
				t.Fatal(err)
			}
			raw := fmt.Sprintf(`{"notificationType":"Bounce","mail":{"messageId":"%s"},"bounce":{"bounceType":"Permanent","feedbackId":"fb-%s"}}`, pmid, pmid)
			err := f.cons.ProcessRaw(ctx, []byte(raw))
			if tc.suppress && err != nil {
				t.Fatalf("process: %v", err)
			}
			if !tc.suppress && !errors.Is(err, feedback.ErrNoMatch) {
				t.Fatalf("process: %v; want ErrNoMatch for an expired tombstone", err)
			}
			sup, err := f.supRepo.GetActiveByEmail(ctx, addr)
			if err != nil {
				t.Fatal(err)
			}
			if (sup != nil) != tc.suppress {
				t.Fatalf("suppressed %v, want %v", sup != nil, tc.suppress)
			}
		})
	}
}
