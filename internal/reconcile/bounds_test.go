// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package reconcile_test

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/internal/reconcile"
)

func countStatus(status string) int64 {
	var n int64
	testDB.Raw("SELECT count(*) FROM comms_messages WHERE organization_id = ? AND status = ?", testOrg, status).Scan(&n)
	return n
}

// TestArmsHonourLimit proves every arm that writes takes Thresholds.Limit: a
// backlog bigger than one pass is worked off over several passes.
func TestArmsHonourLimit(t *testing.T) {
	tests := []struct {
		name      string
		seed      string
		guarantee pb.DeliveryGuarantee
		verify    pb.VerificationLevel
		after     string
	}{
		{"stuck GUARANTEED SENDING", "MESSAGE_STATUS_SENDING", pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, "MESSAGE_STATUS_QUEUED"},
		{"stuck BEST_EFFORT SENDING", "MESSAGE_STATUS_SENDING", pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT, pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, "MESSAGE_STATUS_UNCONFIRMED"},
		{"SENT with no confirmation", "MESSAGE_STATUS_SENT", pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, pb.VerificationLevel_VERIFICATION_LEVEL_CONFIRMED, "MESSAGE_STATUS_UNCONFIRMED"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cleanupOrg(t)
			req := seedRequest(t, "bounds-"+tt.name, "REQUEST_STATUS_FANNED_OUT")
			for i := 0; i < 3; i++ {
				seedMsg(t, req, tt.seed, tt.guarantee, tt.verify, nil)
			}
			th := reconcile.Default()
			th.Limit = 2
			r := reconcile.NewReconciler(dao.NewReconcileRepo(testDB), &recorder{}, &recorder{}, &recorder{}, th, func() time.Time { return time.Now().Add(time.Hour) })
			_ = r.Run(context.Background())
			if got := countStatus(tt.seed); got != 1 {
				t.Fatalf("after one pass with Limit=2, %d rows still %s; want 1", got, tt.seed)
			}
			_ = r.Run(context.Background())
			if got := countStatus(tt.seed); got != 0 {
				t.Fatalf("after two passes, %d rows still %s", got, tt.seed)
			}
		})
	}
}

func gauge(t *testing.T, name string) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			return mf.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("metric %s not registered", name)
	return 0
}

// TestReconcileSetsGauges proves a reconcile run writes the overdue-pending
// gauge and the 24h bounce and complaint rates.
func TestReconcileSetsGauges(t *testing.T) {
	cleanupOrg(t)
	ctx := context.Background()
	now := time.Now()
	req := seedRequest(t, "gauges", "REQUEST_STATUS_FANNED_OUT")
	overdue := now.Add(-time.Hour)
	recent := now.Add(-time.Minute)
	seedMsg(t, req, "MESSAGE_STATUS_FAILED", pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, &overdue)
	seedMsg(t, req, "MESSAGE_STATUS_FAILED", pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, &recent)
	var sentIDs []string
	for i := 0; i < 4; i++ {
		sentIDs = append(sentIDs, seedMsg(t, req, "MESSAGE_STATUS_SENT", pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, nil))
	}
	for _, id := range sentIDs {
		testDB.Exec("INSERT INTO comms_delivery_events (organization_id, message_id, event_type, occurred_at) VALUES (?, ?, 'DELIVERY_EVENT_TYPE_SENT', now())", testOrg, id)
	}
	testDB.Exec("INSERT INTO comms_delivery_events (organization_id, message_id, event_type, occurred_at) VALUES (?, ?, 'DELIVERY_EVENT_TYPE_COMPLAINT', now())", testOrg, sentIDs[0])
	testDB.Exec("UPDATE comms_messages SET status = 'MESSAGE_STATUS_BOUNCED' WHERE id = ?", sentIDs[1])

	th := reconcile.Default()
	th.ReputationMinSends = 5
	below := reconcile.NewReconciler(dao.NewReconcileRepo(testDB), &recorder{}, &recorder{}, &recorder{}, th, func() time.Time { return now })
	if err := below.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if b, c, n := gauge(t, "outbox_comms_bounce_rate"), gauge(t, "outbox_comms_complaint_rate"), gauge(t, "outbox_comms_reputation_sample_sends"); b != 0 || c != 0 || n != 4 {
		t.Errorf("below the 5-send minimum: bounce %v, complaint %v, sample %v; want 0, 0, 4", b, c, n)
	}
	th.ReputationMinSends = 4
	r := reconcile.NewReconciler(dao.NewReconcileRepo(testDB), &recorder{}, &recorder{}, &recorder{}, th, func() time.Time { return now })
	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if got := gauge(t, "outbox_comms_overdue_pending"); got != 1 {
		t.Errorf("overdue_pending = %v, want 1 (one row an hour past its retry time; one a minute past is in grace)", got)
	}
	if got := gauge(t, "outbox_comms_bounce_rate"); got != 0.25 {
		t.Errorf("bounce_rate = %v, want 0.25 (1 hard bounce / 4 sends)", got)
	}
	if got := gauge(t, "outbox_comms_complaint_rate"); got != 0.25 {
		t.Errorf("complaint_rate = %v, want 0.25 (1 complaint / 4 sends)", got)
	}
}
