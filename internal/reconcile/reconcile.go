// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package reconcile finds work that a crash or a lost trigger left behind and
// drives it on. Each run makes four checks: stuck requests, stuck sends,
// missing delivery events, and lost triggers. The prune job strips old provider
// payloads.
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"time"

	"log/slog"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/internal/metrics"
)

// RequestRequeuer re-enqueues a stuck request onto the requests queue (arm 1).
type RequestRequeuer interface {
	PublishRequest(ctx context.Context, orgID, requestID string) error
}

// DispatchRequeuer re-enqueues a message onto the dispatch queue (arms 2 and 4).
type DispatchRequeuer interface {
	PublishDispatch(ctx context.Context, orgID, messageID string) error
}

// RetryEnqueuer re-arms a delayed retry task (arm 4). The deterministic task id
// keyed on (msg, attempts) makes a duplicate enqueue a no-op.
type RetryEnqueuer interface {
	EnqueueRetry(ctx context.Context, orgID, messageID string) error
}

// Thresholds are the reconcile time windows. Defaults via Default().
type Thresholds struct {
	StuckRequest      time.Duration // ACCEPTED/FANNING_OUT age
	StuckSending      time.Duration // SENDING age (crashed worker)
	MissingConfirm    time.Duration // SENT+CONFIRMED with no delivery event
	AgedQueued        time.Duration // QUEUED age (lost dispatch trigger)
	PrunePayloadAfter time.Duration // provider_payload PII strip age
	// TombstoneTTL is how long a purge's tombstones are kept for late feedback.
	TombstoneTTL time.Duration
	// OverdueAfter is how long past next_retry_at a FAILED row counts as overdue
	// in the outbox_comms_overdue_pending gauge.
	OverdueAfter time.Duration
	// ReputationWindow is the trailing window of the bounce and complaint rates.
	ReputationWindow time.Duration
	// ReputationMinSends is the email-send count in the window below which both
	// rates read 0, so a handful of sends cannot trip a rate alert.
	ReputationMinSends int
	Limit              int
	// MaxAttempts caps the attempts a crash requeue counts toward, the same cap
	// the dispatcher applies to failed sends.
	MaxAttempts dao.AttemptCap
}

// Default returns the default windows: 5 minutes for stuck work, 30 minutes for a
// missing delivery confirmation, 90 days for provider payloads, 1000 rows a scan,
// 24 hours with at least 100 email sends for the reputation rates, and the
// default 6 attempts, and 30 days of purge tombstones.
func Default() Thresholds {
	return Thresholds{
		StuckRequest: 5 * time.Minute, StuckSending: 5 * time.Minute,
		MissingConfirm: 30 * time.Minute, AgedQueued: 5 * time.Minute,
		PrunePayloadAfter: 90 * 24 * time.Hour, OverdueAfter: 5 * time.Minute, TombstoneTTL: 30 * 24 * time.Hour,
		ReputationWindow: 24 * time.Hour, ReputationMinSends: 100, Limit: 1000,
		MaxAttempts: dao.AttemptCap{Default: 6},
	}
}

// Reconciler runs the backstop scans.
type Reconciler struct {
	dao        dao.IReconcileDAO
	requestPub RequestRequeuer
	dispatch   DispatchRequeuer
	retry      RetryEnqueuer
	th         Thresholds
	now        func() time.Time
	logger     *slog.Logger
}

// SetLogger sets where the reconciler logs; nil means slog's default.
func (r *Reconciler) SetLogger(l *slog.Logger) { r.logger = l }

// NewReconciler constructs a Reconciler. now defaults to time.Now when nil.
func NewReconciler(d dao.IReconcileDAO, requestPub RequestRequeuer, dispatch DispatchRequeuer, retry RetryEnqueuer, th Thresholds, now func() time.Time) *Reconciler {
	if now == nil {
		now = time.Now
	}
	return &Reconciler{dao: d, requestPub: requestPub, dispatch: dispatch, retry: retry, th: th, now: now}
}

// Run executes the four reconcile arms and the gauge reads. Each is
// independent: a failure in one is logged and counted in
// outbox_comms_reconcile_failures_total, and the rest still run. Run returns
// every failure, joined, so the task reports a run that did not complete.
func (r *Reconciler) Run(ctx context.Context) error {
	now := r.now()
	var errs []error
	failed := map[string]bool{}
	fail := func(arm string, err error) {
		errs = append(errs, fmt.Errorf("reconcile %s: %w", arm, err))
		if !failed[arm] {
			failed[arm] = true
			metrics.RecordReconcileFailure(arm)
		}
	}

	// Arm 1 — stuck requests → re-enqueue fan-out (idempotent: the fan-out's
	// ON CONFLICT no-ops already-materialized rows).
	if refs, err := r.dao.StuckRequests(ctx, now.Add(-r.th.StuckRequest), r.th.Limit); err != nil {
		r.log().ErrorContext(ctx, "reconcile arm1 stuck-requests failed", "error", err)
		fail("stuck_requests", err)
	} else {
		for _, ref := range refs {
			if err := r.requestPub.PublishRequest(ctx, ref.OrganizationID, ref.ID); err != nil {
				r.log().ErrorContext(ctx, "reconcile arm1 re-enqueue failed", "request_id", ref.ID, "error", err)
				fail("stuck_requests", err)
			}
		}
	}

	// Arm 2 — stuck SENDING. GUARANTEED → QUEUED + re-dispatch (accept a rare dup),
	// with the crash counted as an attempt, or DEAD(crash_exhausted) at the cap;
	// BEST_EFFORT → UNCONFIRMED (no re-send).
	if refs, dead, err := r.dao.RequeueStuckSendingGuaranteed(ctx, now.Add(-r.th.StuckSending), r.th.Limit, r.th.MaxAttempts); err != nil {
		r.log().ErrorContext(ctx, "reconcile arm2 requeue-sending failed", "error", err)
		fail("stuck_sending", err)
	} else {
		for _, ref := range dead {
			metrics.RecordTransition(ref.Usecase, pb.Channel(pb.Channel_value[ref.Channel]), pb.MessageStatus_MESSAGE_STATUS_DEAD)
			metrics.RecordDead(metrics.ReasonCrashExhausted)
		}
		for _, ref := range refs {
			if err := r.dispatch.PublishDispatch(ctx, ref.OrganizationID, ref.ID); err != nil {
				r.log().ErrorContext(ctx, "reconcile arm2 re-enqueue failed", "message_id", ref.ID, "error", err)
				fail("stuck_sending", err)
			}
		}
	}
	if refs, err := r.dao.UnconfirmStuckSendingBestEffort(ctx, now.Add(-r.th.StuckSending), r.th.Limit); err != nil {
		r.log().ErrorContext(ctx, "reconcile arm2 unconfirm-besteffort failed", "error", err)
		fail("unconfirm_best_effort", err)
	} else {
		countUnconfirmed(refs, "uncertain_send")
	}

	// Arm 3 — missing confirmation → UNCONFIRMED(no_delivery_feedback).
	if refs, err := r.dao.MarkMissingConfirmation(ctx, now.Add(-r.th.MissingConfirm), r.th.Limit); err != nil {
		r.log().ErrorContext(ctx, "reconcile arm3 missing-confirmation failed", "error", err)
		fail("missing_confirmation", err)
	} else {
		countUnconfirmed(refs, "no_delivery_feedback")
	}

	// The gauges read before arm 4 re-arms anything, so overdue rows are the ones
	// the retry tasks missed.
	if err := r.observe(ctx, now); err != nil {
		fail("observe", err)
	}

	// Arm 4 — lost dispatch/retry. FAILED past next_retry_at → re-arm the retry
	// task (deterministic id dedups); aged QUEUED → re-dispatch (CAS dedups).
	if refs, err := r.dao.RetriableFailed(ctx, now, r.th.Limit); err != nil {
		r.log().ErrorContext(ctx, "reconcile arm4 retriable-failed failed", "error", err)
		fail("retriable_failed", err)
	} else {
		for _, ref := range refs {
			if err := r.retry.EnqueueRetry(ctx, ref.OrganizationID, ref.ID); err != nil {
				r.log().ErrorContext(ctx, "reconcile arm4 retry-enqueue failed", "message_id", ref.ID, "error", err)
				fail("retriable_failed", err)
			}
		}
	}
	if refs, err := r.dao.AgedQueued(ctx, now.Add(-r.th.AgedQueued), r.th.Limit); err != nil {
		r.log().ErrorContext(ctx, "reconcile arm4 aged-queued failed", "error", err)
		fail("aged_queued", err)
	} else {
		for _, ref := range refs {
			if err := r.dispatch.PublishDispatch(ctx, ref.OrganizationID, ref.ID); err != nil {
				r.log().ErrorContext(ctx, "reconcile arm4 re-dispatch failed", "message_id", ref.ID, "error", err)
				fail("aged_queued", err)
			}
		}
	}
	return errors.Join(errs...)
}

// countUnconfirmed records the rows a reconcile check ended UNCONFIRMED.
func countUnconfirmed(refs []dao.Ref, reason string) {
	for _, ref := range refs {
		metrics.RecordTransition(ref.Usecase, pb.Channel(pb.Channel_value[ref.Channel]), pb.MessageStatus_MESSAGE_STATUS_UNCONFIRMED)
		metrics.RecordUnconfirmed(reason)
	}
}

// observe sets the overdue-pending and sender-reputation gauges. A failed read
// leaves the gauge at its last value, is logged, and is returned.
func (r *Reconciler) observe(ctx context.Context, now time.Time) error {
	var errs []error
	if n, err := r.dao.CountOverdueFailed(ctx, now.Add(-r.th.OverdueAfter)); err != nil {
		r.log().ErrorContext(ctx, "reconcile: count overdue failed", "error", err)
		errs = append(errs, err)
	} else {
		metrics.SetOverduePending(float64(n))
	}
	rep, err := r.dao.ReputationCounts(ctx, now.Add(-r.th.ReputationWindow))
	if err != nil {
		r.log().ErrorContext(ctx, "reconcile: reputation counts failed", "error", err)
		return errors.Join(append(errs, err)...)
	}
	metrics.SetReputationSample(float64(rep.Sent))
	bounce, complaint := 0.0, 0.0
	if rep.Sent > 0 && rep.Sent >= int64(r.th.ReputationMinSends) {
		bounce = float64(rep.HardBounces) / float64(rep.Sent)
		complaint = float64(rep.Complaints) / float64(rep.Sent)
	}
	metrics.SetReputationRates(bounce, complaint)
	return errors.Join(errs...)
}

// Prune strips provider_payload older than the retention window, and deletes
// purge tombstones older than TombstoneTTL (the daily prune job). Message rows
// are kept for life; only the raw payload, which carries addresses, is
// stripped. A zero TombstoneTTL keeps tombstones.
func (r *Reconciler) Prune(ctx context.Context) error {
	cutoff := r.now().Add(-r.th.PrunePayloadAfter)
	n, err := r.dao.PruneProviderPayload(ctx, cutoff)
	if err != nil {
		return fmt.Errorf("prune: %w", err)
	}
	r.log().InfoContext(ctx, "prune stripped provider payloads", "count", n)
	if r.th.TombstoneTTL > 0 {
		t, err := r.dao.PruneTombstones(ctx, r.now().Add(-r.th.TombstoneTTL))
		if err != nil {
			return fmt.Errorf("prune: %w", err)
		}
		r.log().InfoContext(ctx, "prune deleted expired purge tombstones", "count", t)
	}
	return nil
}

func (r *Reconciler) log() *slog.Logger {
	if r.logger != nil {
		return r.logger
	}
	return slog.Default()
}
