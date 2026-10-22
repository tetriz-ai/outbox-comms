// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package dispatch_test

import (
	"context"
	"testing"
	"time"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
)

// TestSlowSendEndsAtItsDeadline pins what a provider slower than the deadline
// does: one call, cut at the deadline and classified uncertain, so the
// GUARANTEED row is FAILED and waiting for its retry, not SENDING. Reconcile's
// stuck-send check then finds nothing to send again.
func TestSlowSendEndsAtItsDeadline(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	id := f.seedQueued(t, "k-slow", pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, typedUsecase)
	f.tx.Exec(`UPDATE comms_requests SET params = '{"title":"Welcome"}' WHERE id = ?`, f.status(t, id).RequestID)
	s := &deadlineSender{block: true}
	d := releaseDispatcher(f, f.msgRepo, f.reqRepo, f.supRepo, s)
	d.SetSendTimeout(200 * time.Millisecond)
	start := time.Now()
	if err := d.Dispatch(ctx, testOrg, id); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("the send took %v; the 200ms deadline did not end it", took)
	}
	if got := f.status(t, id); got.Status != pb.MessageStatus_MESSAGE_STATUS_FAILED || s.calls != 1 {
		t.Fatalf("row %v after %d call(s); want FAILED after 1", got.Status, s.calls)
	}
	requeued, dead, err := dao.NewReconcileRepo(f.tx).RequeueStuckSendingGuaranteed(ctx, time.Now().Add(time.Hour), 10, dao.AttemptCap{Default: 6})
	if err != nil || len(requeued)+len(dead) != 0 {
		t.Fatalf("reconcile took %d stuck row(s), %v; want none", len(requeued)+len(dead), err)
	}
}

// TestSlowBestEffortSendEndsUnconfirmed is the BEST_EFFORT side: the call cut
// at the deadline is uncertain, so the row ends UNCONFIRMED(uncertain_send)
// after one call, is never retried, and leaves nothing for reconcile.
func TestSlowBestEffortSendEndsUnconfirmed(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	id := f.seedQueued(t, "k-slow-be", pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT, typedUsecase)
	f.tx.Exec(`UPDATE comms_requests SET params = '{"title":"Welcome"}' WHERE id = ?`, f.status(t, id).RequestID)
	s := &deadlineSender{block: true}
	d := releaseDispatcher(f, f.msgRepo, f.reqRepo, f.supRepo, s)
	d.SetSendTimeout(200 * time.Millisecond)
	start := time.Now()
	if err := d.Dispatch(ctx, testOrg, id); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("the send took %v; the 200ms deadline did not end it", took)
	}
	got := f.status(t, id)
	if got.Status != pb.MessageStatus_MESSAGE_STATUS_UNCONFIRMED || got.UnconfirmedReason == nil || *got.UnconfirmedReason != "uncertain_send" || s.calls != 1 {
		t.Fatalf("row %v (%v) after %d call(s); want UNCONFIRMED(uncertain_send) after 1", got.Status, got.UnconfirmedReason, s.calls)
	}
	refs, err := dao.NewReconcileRepo(f.tx).UnconfirmStuckSendingBestEffort(ctx, time.Now().Add(time.Hour), 10)
	if err != nil || len(refs) != 0 {
		t.Fatalf("reconcile took %d stuck row(s), %v; want none", len(refs), err)
	}
}
