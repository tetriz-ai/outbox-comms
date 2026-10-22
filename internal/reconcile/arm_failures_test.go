// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package reconcile_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/internal/reconcile"
)

var errDown = errors.New("database down")

// failingDAO fails every read and update a reconcile run makes.
type failingDAO struct{ dao.IReconcileDAO }

func (failingDAO) StuckRequests(context.Context, time.Time, int) ([]dao.Ref, error) {
	return nil, errDown
}
func (failingDAO) RequeueStuckSendingGuaranteed(context.Context, time.Time, int, dao.AttemptCap) ([]dao.Ref, []dao.Ref, error) {
	return nil, nil, errDown
}
func (failingDAO) UnconfirmStuckSendingBestEffort(context.Context, time.Time, int) ([]dao.Ref, error) {
	return nil, errDown
}
func (failingDAO) MarkMissingConfirmation(context.Context, time.Time, int) ([]dao.Ref, error) {
	return nil, errDown
}
func (failingDAO) CountOverdueFailed(context.Context, time.Time) (int64, error) { return 0, errDown }
func (failingDAO) ReputationCounts(context.Context, time.Time) (dao.Reputation, error) {
	return dao.Reputation{}, errDown
}
func (failingDAO) RetriableFailed(context.Context, time.Time, int) ([]dao.Ref, error) {
	return nil, errDown
}
func (failingDAO) AgedQueued(context.Context, time.Time, int) ([]dao.Ref, error) { return nil, errDown }

// TestRunReportsEveryFailedArm pins that a run in which every arm fails still
// runs them all, returns each failure, and counts one failure per arm.
func TestRunReportsEveryFailedArm(t *testing.T) {
	arms := []string{"stuck_requests", "stuck_sending", "unconfirm_best_effort", "missing_confirmation", "observe", "retriable_failed", "aged_queued"}
	before := map[string]float64{}
	for _, a := range arms {
		before[a] = counter(t, "outbox_comms_reconcile_failures_total", map[string]string{"arm": a})
	}
	r := reconcile.NewReconciler(failingDAO{}, &recorder{}, &recorder{}, &recorder{}, reconcile.Default(), time.Now)
	err := r.Run(context.Background())
	if err == nil || !errors.Is(err, errDown) {
		t.Fatalf("Run returned %v, want the joined failures", err)
	}
	for _, a := range arms {
		if !strings.Contains(err.Error(), "reconcile "+a+":") {
			t.Errorf("the error does not name arm %s: %v", a, err)
		}
		if got := counter(t, "outbox_comms_reconcile_failures_total", map[string]string{"arm": a}); got != before[a]+1 {
			t.Errorf("failures for %s went %v -> %v, want +1", a, before[a], got)
		}
	}
}

// oneRefDAO finds one row for every row check and reads the gauges cleanly,
// so every arm reaches its publish. Failing, if set, names the one check that
// returns errDown instead.
type oneRefDAO struct {
	dao.IReconcileDAO
	failing string
}

var ref = []dao.Ref{{OrganizationID: "8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f", ID: "m1"}}

func (d oneRefDAO) refs(name string) ([]dao.Ref, error) {
	if d.failing == name {
		return nil, errDown
	}
	return ref, nil
}
func (d oneRefDAO) StuckRequests(context.Context, time.Time, int) ([]dao.Ref, error) {
	return d.refs("stuck_requests")
}
func (d oneRefDAO) RequeueStuckSendingGuaranteed(context.Context, time.Time, int, dao.AttemptCap) ([]dao.Ref, []dao.Ref, error) {
	r, err := d.refs("stuck_sending")
	return r, nil, err
}
func (d oneRefDAO) UnconfirmStuckSendingBestEffort(context.Context, time.Time, int) ([]dao.Ref, error) {
	return nil, nil
}
func (d oneRefDAO) MarkMissingConfirmation(context.Context, time.Time, int) ([]dao.Ref, error) {
	return nil, nil
}
func (d oneRefDAO) CountOverdueFailed(context.Context, time.Time) (int64, error) { return 0, nil }
func (d oneRefDAO) ReputationCounts(context.Context, time.Time) (dao.Reputation, error) {
	return dao.Reputation{}, nil
}
func (d oneRefDAO) RetriableFailed(context.Context, time.Time, int) ([]dao.Ref, error) {
	return d.refs("retriable_failed")
}
func (d oneRefDAO) AgedQueued(context.Context, time.Time, int) ([]dao.Ref, error) {
	return d.refs("aged_queued")
}

// failingPublisher counts every publish and fails the kinds named.
type failingPublisher struct {
	failRequest, failDispatch, failRetry bool
	requests, dispatches, retries        int
}

func (p *failingPublisher) PublishRequest(context.Context, string, string) error {
	p.requests++
	if p.failRequest {
		return errDown
	}
	return nil
}
func (p *failingPublisher) PublishDispatch(context.Context, string, string) error {
	p.dispatches++
	if p.failDispatch {
		return errDown
	}
	return nil
}
func (p *failingPublisher) EnqueueRetry(context.Context, string, string) error {
	p.retries++
	if p.failRetry {
		return errDown
	}
	return nil
}

// TestRunReportsAFailedPublish pins the per-row failures: a publish that fails
// is returned in the joined error and adds one to its arm's failure counter,
// and every other arm still runs and publishes. The last case fails one arm's
// read alone: only that arm is counted.
func TestRunReportsAFailedPublish(t *testing.T) {
	arms := []string{"stuck_requests", "stuck_sending", "unconfirm_best_effort", "missing_confirmation", "observe", "retriable_failed", "aged_queued"}
	for _, tc := range []struct {
		name   string
		dao    oneRefDAO
		pub    *failingPublisher
		failed []string
	}{
		{"request publish fails", oneRefDAO{}, &failingPublisher{failRequest: true}, []string{"stuck_requests"}},
		{"dispatch publish fails", oneRefDAO{}, &failingPublisher{failDispatch: true}, []string{"stuck_sending", "aged_queued"}},
		{"retry enqueue fails", oneRefDAO{}, &failingPublisher{failRetry: true}, []string{"retriable_failed"}},
		{"one read fails", oneRefDAO{failing: "stuck_sending"}, &failingPublisher{}, []string{"stuck_sending"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := map[string]float64{}
			for _, a := range arms {
				before[a] = counter(t, "outbox_comms_reconcile_failures_total", map[string]string{"arm": a})
			}
			err := reconcile.NewReconciler(tc.dao, tc.pub, tc.pub, tc.pub, reconcile.Default(), time.Now).Run(context.Background())
			if !errors.Is(err, errDown) {
				t.Fatalf("Run returned %v, want the failure", err)
			}
			for _, a := range arms {
				want := before[a]
				for _, f := range tc.failed {
					if f == a {
						want++
					}
				}
				if got := counter(t, "outbox_comms_reconcile_failures_total", map[string]string{"arm": a}); got != want {
					t.Errorf("failures for %s went %v -> %v, want %v", a, before[a], got, want)
				}
			}
			for _, f := range tc.failed {
				if !strings.Contains(err.Error(), "reconcile "+f+":") {
					t.Errorf("the error does not name arm %s: %v", f, err)
				}
			}
			// Every arm still ran: one request publish, a dispatch publish for
			// each of the two dispatch arms whose read succeeded, one retry.
			wantDispatches := 2
			if tc.dao.failing == "stuck_sending" {
				wantDispatches = 1
			}
			if tc.pub.requests != 1 || tc.pub.dispatches != wantDispatches || tc.pub.retries != 1 {
				t.Errorf("publishes: %d request, %d dispatch, %d retry; want 1, %d, 1", tc.pub.requests, tc.pub.dispatches, tc.pub.retries, wantDispatches)
			}
		})
	}
}
