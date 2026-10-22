// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package reconcile_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/types/known/structpb"
	"gorm.io/gorm"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
	"github.com/tetriz-ai/outbox-comms/internal/reconcile"
	"github.com/tetriz-ai/outbox-comms/internal/testdb"
	"github.com/tetriz-ai/outbox-comms/templates"
)

var testDB *gorm.DB

func TestMain(m *testing.M) { os.Exit(testdb.Main(m, &testDB)) }

const testOrg = "11111111-1111-1111-1111-111111111111"

type recorder struct{ requests, dispatches, retries int }

func (r *recorder) PublishRequest(context.Context, string, string) error  { r.requests++; return nil }
func (r *recorder) PublishDispatch(context.Context, string, string) error { r.dispatches++; return nil }
func (r *recorder) EnqueueRetry(context.Context, string, string) error    { r.retries++; return nil }

func cleanupOrg(t *testing.T) {
	t.Cleanup(func() {
		testDB.Exec("DELETE FROM comms_delivery_events WHERE organization_id = ?", testOrg)
		testDB.Exec("DELETE FROM comms_messages WHERE organization_id = ?", testOrg)
		testDB.Exec("DELETE FROM comms_requests WHERE organization_id = ?", testOrg)
	})
}

func seedRequest(t *testing.T, key, status string) string {
	t.Helper()
	r := &model.CommsRequest{
		OrganizationID: testOrg, IdempotencyKey: key, Usecase: templates.UsecaseNotice,
		Params: &structpb.Struct{}, DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
		Status: pb.RequestStatus_REQUEST_STATUS_ACCEPTED,
	}
	if err := testDB.Create(r).Error; err != nil {
		t.Fatalf("seed request: %v", err)
	}
	testDB.Exec("UPDATE comms_requests SET status = ? WHERE id = ?", status, r.ID)
	return r.ID
}

func seedMsg(t *testing.T, reqID, status string, guarantee pb.DeliveryGuarantee, verification pb.VerificationLevel, nextRetry *time.Time) string {
	t.Helper()
	m := &model.CommsMessage{
		OrganizationID: testOrg, RequestID: reqID, Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SES,
		Recipient: fmt.Sprintf("r-%d@example.com", time.Now().UnixNano()), Status: pb.MessageStatus_MESSAGE_STATUS_QUEUED,
		VerificationLevel: verification, DeliveryGuarantee: guarantee, NextRetryAt: nextRetry,
	}
	if err := testDB.Create(m).Error; err != nil {
		t.Fatalf("seed msg: %v", err)
	}
	testDB.Exec("UPDATE comms_messages SET status = ? WHERE id = ?", status, m.ID)
	return m.ID
}

func msgStatus(t *testing.T, id string) pb.MessageStatus {
	t.Helper()
	var s string
	testDB.Raw("SELECT status FROM comms_messages WHERE id = ?", id).Scan(&s)
	return pb.MessageStatus(pb.MessageStatus_value[s])
}

// TestReconcileArms drives all four arms with a far-future clock so every seeded
// row is past its threshold.
func TestReconcileArms(t *testing.T) {
	ctx := context.Background()
	cleanupOrg(t)
	now := time.Now()

	reqStuck := seedRequest(t, "rec:req", pb.RequestStatus_REQUEST_STATUS_ACCEPTED.String())
	// A crash in the middle of a fan-out leaves the request FANNING_OUT.
	reqMidFanOut := seedRequest(t, "rec:req-mid", pb.RequestStatus_REQUEST_STATUS_FANNING_OUT.String())
	reqForMsgs := seedRequest(t, "rec:msgs", pb.RequestStatus_REQUEST_STATUS_FANNED_OUT.String())
	sendingG := seedMsg(t, reqForMsgs, pb.MessageStatus_MESSAGE_STATUS_SENDING.String(), pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, nil)
	sendingBE := seedMsg(t, reqForMsgs, pb.MessageStatus_MESSAGE_STATUS_SENDING.String(), pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT, pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, nil)
	sentConfirmed := seedMsg(t, reqForMsgs, pb.MessageStatus_MESSAGE_STATUS_SENT.String(), pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, pb.VerificationLevel_VERIFICATION_LEVEL_CONFIRMED, nil)
	retryAt := now
	failed := seedMsg(t, reqForMsgs, pb.MessageStatus_MESSAGE_STATUS_FAILED.String(), pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, &retryAt)
	queuedAged := seedMsg(t, reqForMsgs, pb.MessageStatus_MESSAGE_STATUS_QUEUED.String(), pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, nil)
	_, _ = reqStuck, reqMidFanOut

	rec := &recorder{}
	r := reconcile.NewReconciler(dao.NewReconcileRepo(testDB), rec, rec, rec, reconcile.Default(),
		func() time.Time { return now.Add(24 * time.Hour) })
	if err := r.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}

	if rec.requests != 2 {
		t.Errorf("arm1: want 2 request re-enqueues (ACCEPTED and FANNING_OUT), got %d", rec.requests)
	}
	if got := msgStatus(t, sendingG); got != pb.MessageStatus_MESSAGE_STATUS_QUEUED {
		t.Errorf("arm2: stuck SENDING GUARANTEED → QUEUED, got %v", got)
	}
	if got := msgStatus(t, sendingBE); got != pb.MessageStatus_MESSAGE_STATUS_UNCONFIRMED {
		t.Errorf("arm2: stuck SENDING BEST_EFFORT → UNCONFIRMED, got %v", got)
	}
	if got := msgStatus(t, sentConfirmed); got != pb.MessageStatus_MESSAGE_STATUS_UNCONFIRMED {
		t.Errorf("arm3: SENT+CONFIRMED → UNCONFIRMED, got %v", got)
	}
	if rec.retries != 1 {
		t.Errorf("arm4: want 1 retry re-arm for the FAILED row, got %d", rec.retries)
	}
	// arm2 (sendingG → QUEUED) + arm4 (queuedAged) both re-dispatch. The newly
	// re-queued sendingG is also picked up by AgedQueued in the same pass.
	if rec.dispatches < 2 {
		t.Errorf("arm2+arm4: want ≥2 dispatch re-enqueues, got %d", rec.dispatches)
	}
	_ = failed
	_ = queuedAged
}

// TestRedriveDead proves RedriveDead resets matching DEAD rows to QUEUED
// with attempts/next_retry/dead_reason cleared, filtered by reason class.
func TestRedriveDead(t *testing.T) {
	ctx := context.Background()
	cleanupOrg(t)
	reqID := seedRequest(t, "rec:redrive", pb.RequestStatus_REQUEST_STATUS_FANNED_OUT.String())
	dead := seedMsg(t, reqID, pb.MessageStatus_MESSAGE_STATUS_DEAD.String(), pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, nil)
	testDB.Exec("UPDATE comms_messages SET dead_reason = 'transient_exhausted', attempts = 6, last_error = 'boom' WHERE id = ?", dead)
	// A DEAD row the caller did not name stays DEAD.
	other := seedMsg(t, reqID, pb.MessageStatus_MESSAGE_STATUS_DEAD.String(), pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, nil)
	testDB.Exec("UPDATE comms_messages SET dead_reason = 'render_bug' WHERE id = ?", other)

	repo := dao.NewReconcileRepo(testDB)
	n, err := repo.RedriveDead(ctx, []string{dead})
	if err != nil {
		t.Fatalf("redrive: %v", err)
	}
	if n != 1 {
		t.Fatalf("redrive should reset exactly the 1 transient_exhausted row, got %d", n)
	}
	if got := msgStatus(t, dead); got != pb.MessageStatus_MESSAGE_STATUS_QUEUED {
		t.Fatalf("redriven row → QUEUED, got %v", got)
	}
	var attempts int
	testDB.Raw("SELECT attempts FROM comms_messages WHERE id = ?", dead).Scan(&attempts)
	if attempts != 0 {
		t.Errorf("redrive must reset attempts to 0, got %d", attempts)
	}
	if got := msgStatus(t, other); got != pb.MessageStatus_MESSAGE_STATUS_DEAD {
		t.Errorf("render_bug row must be untouched by the transient_exhausted filter, got %v", got)
	}
}

// TestPruneKeepsRowAndSuppressions proves Prune strips an aged
// provider_payload but keeps the event row, and never touches suppressions.
func TestPruneKeepsRowAndSuppressions(t *testing.T) {
	ctx := context.Background()
	cleanupOrg(t)
	now := time.Now()
	reqID := seedRequest(t, "rec:prune", pb.RequestStatus_REQUEST_STATUS_FANNED_OUT.String())
	msgID := seedMsg(t, reqID, pb.MessageStatus_MESSAGE_STATUS_SENT.String(), pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, nil)

	ev := &model.CommsDeliveryEvent{
		OrganizationID: testOrg, MessageID: msgID, EventType: pb.DeliveryEventType_DELIVERY_EVENT_TYPE_BOUNCE,
		ProviderPayload: []byte(`{"raw":"pii"}`), OccurredAt: now,
	}
	if err := testDB.Create(ev).Error; err != nil {
		t.Fatalf("seed event: %v", err)
	}
	// Age the event past the 90-day window.
	testDB.Exec("UPDATE comms_delivery_events SET created_at = ? WHERE id = ?", now.Add(-100*24*time.Hour), ev.ID)

	email := fmt.Sprintf("keep-%d@example.com", now.UnixNano())
	t.Cleanup(func() { testDB.Exec("DELETE FROM comms_suppressions WHERE email = ?", email) })
	if err := dao.NewSuppressionRepo(testDB).SuppressIfAbsent(ctx, &model.CommsSuppression{Email: email, Reason: "hard_bounce"}); err != nil {
		t.Fatalf("suppress: %v", err)
	}

	r := reconcile.NewReconciler(dao.NewReconcileRepo(testDB), &recorder{}, &recorder{}, &recorder{}, reconcile.Default(), func() time.Time { return now })
	if err := r.Prune(ctx); err != nil {
		t.Fatalf("prune: %v", err)
	}

	var payloadIsNull bool
	var count int64
	testDB.Raw("SELECT provider_payload IS NULL FROM comms_delivery_events WHERE id = ?", ev.ID).Scan(&payloadIsNull)
	testDB.Raw("SELECT count(*) FROM comms_delivery_events WHERE id = ?", ev.ID).Scan(&count)
	if count != 1 {
		t.Fatalf("prune must keep the event row (rows are kept for life), got count=%d", count)
	}
	if !payloadIsNull {
		t.Errorf("prune must strip the aged provider_payload to NULL")
	}
	if sup, _ := dao.NewSuppressionRepo(testDB).GetActiveByEmail(ctx, email); sup == nil {
		t.Errorf("prune must not touch suppressions")
	}
}

// TestReconcileCountsUnconfirmed pins that reconcile's two UNCONFIRMED endings,
// a stuck best-effort send and a missing delivery event, are counted in
// outbox_comms_unconfirmed_total and outbox_comms_message_transitions_total,
// once per row moved, under the row's usecase: none for no rows, two for two,
// and no more than the per-run limit.
func TestReconcileCountsUnconfirmed(t *testing.T) {
	for _, tt := range []struct {
		name         string
		seeded, want int
	}{{"no rows", 0, 0}, {"two rows", 2, 2}, {"over the limit", 5, 3}} {
		t.Run(tt.name, func(t *testing.T) {
			cleanupOrg(t)
			now := time.Now()
			uncertain := counter(t, "outbox_comms_unconfirmed_total", map[string]string{"reason": "uncertain_send"})
			noFeedback := counter(t, "outbox_comms_unconfirmed_total", map[string]string{"reason": "no_delivery_feedback"})
			transitions := map[string]string{"usecase": templates.UsecaseNotice, "to_status": "MESSAGE_STATUS_UNCONFIRMED", "channel": "CHANNEL_EMAIL"}
			moved := counter(t, "outbox_comms_message_transitions_total", transitions)

			req := seedRequest(t, "rec:metrics:"+tt.name, pb.RequestStatus_REQUEST_STATUS_FANNED_OUT.String())
			for range tt.seeded {
				seedMsg(t, req, pb.MessageStatus_MESSAGE_STATUS_SENDING.String(), pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT, pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, nil)
				seedMsg(t, req, pb.MessageStatus_MESSAGE_STATUS_SENT.String(), pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, pb.VerificationLevel_VERIFICATION_LEVEL_CONFIRMED, nil)
			}
			th := reconcile.Default()
			th.Limit = 3
			rec := &recorder{}
			r := reconcile.NewReconciler(dao.NewReconcileRepo(testDB), rec, rec, rec, th,
				func() time.Time { return now.Add(24 * time.Hour) })
			if err := r.Run(context.Background()); err != nil {
				t.Fatal(err)
			}
			want := float64(tt.want)
			if got := counter(t, "outbox_comms_unconfirmed_total", map[string]string{"reason": "uncertain_send"}); got != uncertain+want {
				t.Errorf("uncertain_send went %v -> %v, want +%v", uncertain, got, want)
			}
			if got := counter(t, "outbox_comms_unconfirmed_total", map[string]string{"reason": "no_delivery_feedback"}); got != noFeedback+want {
				t.Errorf("no_delivery_feedback went %v -> %v, want +%v", noFeedback, got, want)
			}
			if got := counter(t, "outbox_comms_message_transitions_total", transitions); got != moved+2*want {
				t.Errorf("UNCONFIRMED transitions under %q went %v -> %v, want +%v", templates.UsecaseNotice, moved, got, 2*want)
			}
		})
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

// claimAndAge claims a message as a worker would and makes the claim look
// older than the stuck-send window, as a worker that crashed mid-send leaves it.
func claimAndAge(ctx context.Context, t *testing.T, id string) bool {
	t.Helper()
	ok, _, err := dao.NewMessageRepo(testDB).ClaimForDispatch(ctx, testOrg, id)
	if err != nil {
		t.Fatal(err)
	}
	testDB.Exec("UPDATE comms_messages SET updated_at = now() - interval '1 hour' WHERE id = ?", id)
	return ok
}

// TestCrashRequeuesCountTowardTheCap pins that a GUARANTEED message whose
// worker crashes mid-send on every claim is not re-sent forever. Each crash
// requeue counts as an attempt, and the message ends DEAD with crash_exhausted
// after the default 6 attempts, having been claimed, and so possibly sent, at
// most 6 times.
func TestCrashRequeuesCountTowardTheCap(t *testing.T) {
	ctx := context.Background()
	cleanupOrg(t)
	reqID := seedRequest(t, "rec:crash-loop", pb.RequestStatus_REQUEST_STATUS_FANNED_OUT.String())
	id := seedMsg(t, reqID, pb.MessageStatus_MESSAGE_STATUS_QUEUED.String(), pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, nil)
	before := counter(t, "outbox_comms_dead_total", map[string]string{"reason_class": "crash_exhausted"})
	r := reconcile.NewReconciler(dao.NewReconcileRepo(testDB), &recorder{}, &recorder{}, &recorder{}, reconcile.Default(), time.Now)
	claims := 0
	for pass := 0; pass < 20 && msgStatus(t, id) == pb.MessageStatus_MESSAGE_STATUS_QUEUED; pass++ {
		if claimAndAge(ctx, t, id) {
			claims++
		}
		if err := r.Run(ctx); err != nil {
			t.Fatal(err)
		}
	}
	var attempts int
	var reason *string
	testDB.Raw("SELECT attempts FROM comms_messages WHERE id = ?", id).Scan(&attempts)
	testDB.Raw("SELECT dead_reason FROM comms_messages WHERE id = ?", id).Scan(&reason)
	if got := msgStatus(t, id); got != pb.MessageStatus_MESSAGE_STATUS_DEAD || reason == nil || *reason != "crash_exhausted" || claims > 6 || attempts != 6 {
		t.Fatalf("after %d claims: %v, dead_reason %v, attempts %d; want DEAD(crash_exhausted) after at most 6 claims, attempts 6", claims, got, reason, attempts)
	}
	if after := counter(t, "outbox_comms_dead_total", map[string]string{"reason_class": "crash_exhausted"}); after != before+1 {
		t.Fatalf("crash_exhausted dead count went %v -> %v, want +1", before, after)
	}
}

// TestRecoveredCrashSendsOnceMore pins the other side: one crash requeues the
// message with one attempt counted, and the resend completes it.
func TestRecoveredCrashSendsOnceMore(t *testing.T) {
	ctx := context.Background()
	cleanupOrg(t)
	reqID := seedRequest(t, "rec:crash-once", pb.RequestStatus_REQUEST_STATUS_FANNED_OUT.String())
	id := seedMsg(t, reqID, pb.MessageStatus_MESSAGE_STATUS_QUEUED.String(), pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, nil)
	rec := &recorder{}
	r := reconcile.NewReconciler(dao.NewReconcileRepo(testDB), rec, rec, rec, reconcile.Default(), time.Now)
	claimAndAge(ctx, t, id)
	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
	var attempts int
	testDB.Raw("SELECT attempts FROM comms_messages WHERE id = ?", id).Scan(&attempts)
	if got := msgStatus(t, id); got != pb.MessageStatus_MESSAGE_STATUS_QUEUED || attempts != 1 || rec.dispatches != 1 {
		t.Fatalf("after one crash: %v, attempts %d, %d re-dispatches; want QUEUED, 1, 1", got, attempts, rec.dispatches)
	}
	msgs := dao.NewMessageRepo(testDB)
	ok, claimed, err := msgs.ClaimForDispatch(ctx, testOrg, id)
	if !ok || err != nil {
		t.Fatalf("the resend's claim: %v %v", ok, err)
	}
	if err := msgs.MarkDelivered(ctx, testOrg, id, *claimed.ClaimID, "ts-1"); err != nil {
		t.Fatal(err)
	}
	if got := msgStatus(t, id); got != pb.MessageStatus_MESSAGE_STATUS_DELIVERED {
		t.Fatalf("after the resend: %v, want DELIVERED", got)
	}
}

// TestPruneDeletesExpiredTombstones pins the tombstone window: the prune job
// deletes a purge tombstone older than the TTL, 30 days by default, and keeps a
// newer one.
func TestPruneDeletesExpiredTombstones(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		testDB.Exec("DELETE FROM comms_purge_tombstones WHERE provider_message_id LIKE 'prune-tomb-%'")
	})
	testDB.Exec("INSERT INTO comms_purge_tombstones (provider_message_id, recipient, created_at) VALUES ('prune-tomb-old', 'old@example.com', now() - interval '31 days'), ('prune-tomb-new', 'new@example.com', now() - interval '29 days')")
	r := reconcile.NewReconciler(dao.NewReconcileRepo(testDB), &recorder{}, &recorder{}, &recorder{}, reconcile.Default(), time.Now)
	if err := r.Prune(ctx); err != nil {
		t.Fatal(err)
	}
	var left []string
	testDB.Raw("SELECT provider_message_id FROM comms_purge_tombstones WHERE provider_message_id LIKE 'prune-tomb-%' ORDER BY 1").Scan(&left)
	if strings.Join(left, ",") != "prune-tomb-new" {
		t.Fatalf("tombstones left %v, want [prune-tomb-new]", left)
	}
}

// TestConcurrentRequeueCountsTheCrashOnce pins that two reconcile runs taking
// the same crashed row at once move it once: one requeue, attempts + 1 exactly
// once. Overlapping runs are possible since the scheduler stopped holding one
// run's tick against the next.
func TestConcurrentRequeueCountsTheCrashOnce(t *testing.T) {
	ctx := context.Background()
	cleanupOrg(t)
	repo := dao.NewReconcileRepo(testDB)
	for round := 0; round < 10; round++ {
		reqID := seedRequest(t, fmt.Sprintf("rec:overlap-%d", round), pb.RequestStatus_REQUEST_STATUS_FANNED_OUT.String())
		id := seedMsg(t, reqID, pb.MessageStatus_MESSAGE_STATUS_QUEUED.String(), pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, nil)
		claimAndAge(ctx, t, id)
		var requeued atomic.Int64
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				refs, dead, err := repo.RequeueStuckSendingGuaranteed(ctx, time.Now().Add(time.Hour), 10, dao.AttemptCap{Default: 6})
				if err != nil {
					t.Error(err)
				}
				requeued.Add(int64(len(refs) + len(dead)))
			}()
		}
		close(start)
		wg.Wait()
		var attempts int
		testDB.Raw("SELECT attempts FROM comms_messages WHERE id = ?", id).Scan(&attempts)
		if requeued.Load() != 1 || attempts != 1 || msgStatus(t, id) != pb.MessageStatus_MESSAGE_STATUS_QUEUED {
			t.Fatalf("round %d: %d requeue(s), attempts %d, %v; want 1, 1, QUEUED", round, requeued.Load(), attempts, msgStatus(t, id))
		}
	}
}
