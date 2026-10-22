// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package jobs registers three task types with the scheduler.
//
//   - reconcile runs on a cron, every 10 minutes by default, and drives stuck
//     work on.
//   - prune runs on a cron, daily by default, and strips old provider payloads.
//   - retry runs once per failed send, at its next_retry_at. The dispatcher
//     schedules it, and reconcile schedules a lost one again.
//
// reconcile and prune run across organizations and are idempotent.
// RetryScheduler is the single enqueue used by both the dispatcher and
// reconcile. Its deterministic task id dedups re-enqueues within each path; it
// does not dedup across the two (their ids differ), so both can enqueue for one
// message. That is safe because the dispatcher's CAS claim, not the task id, is
// what collapses a duplicate re-dispatch at send time.
package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/hibiken/asynq"

	"github.com/tetriz-ai/outbox-comms/internal/scheduler"
)

// Task types and the queue the jobs ride.
const (
	TaskReconcile = "outbox_comms:reconcile"
	TaskPrune     = "outbox_comms:prune"
	TaskRetry     = "outbox_comms:retry"
)

// reconcileRunner is the slice of *reconcile.Reconciler the handler drives.
type reconcileRunner interface {
	Run(ctx context.Context) error
	Prune(ctx context.Context) error
}

// dispatchPublisher is the dispatch re-publish the retry path uses.
type dispatchPublisher interface {
	PublishDispatch(ctx context.Context, orgID, messageID string) error
}

// Handler is the asynq handler for the three job types.
type Handler struct {
	reconciler reconcileRunner
	dispatch   dispatchPublisher
}

// NewHandler constructs the handler.
func NewHandler(reconciler reconcileRunner, dispatch dispatchPublisher) *Handler {
	return &Handler{reconciler: reconciler, dispatch: dispatch}
}

// Register routes the three job types on mux.
func (h *Handler) Register(mux *asynq.ServeMux) {
	mux.Handle(TaskReconcile, h)
	mux.Handle(TaskPrune, h)
	mux.Handle(TaskRetry, h)
}

// ProcessTask dispatches by task type. reconcile and prune carry no payload;
// retry carries (org, message).
func (h *Handler) ProcessTask(ctx context.Context, t *asynq.Task) error {
	switch t.Type() {
	case TaskReconcile:
		if err := h.reconciler.Run(ctx); err != nil {
			return fmt.Errorf("running reconcile: %w", err)
		}
		return nil
	case TaskPrune:
		if err := h.reconciler.Prune(ctx); err != nil {
			return fmt.Errorf("running prune: %w", err)
		}
		return nil
	case TaskRetry:
		if err := h.runRetry(ctx, t.Payload()); err != nil {
			return fmt.Errorf("running retry: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("jobs: unknown task type %q", t.Type())
	}
}

// runRetry re-enters the dispatch path for one message. The dispatcher
// CAS-claims QUEUED|FAILED → SENDING, so re-publishing a FAILED row is safe (a
// row already advanced is a no-op).
func (h *Handler) runRetry(ctx context.Context, body []byte) error {
	var p retryPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return fmt.Errorf("unmarshal retry payload: %w", err)
	}
	if err := h.dispatch.PublishDispatch(ctx, p.OrgID, p.MessageID); err != nil {
		return fmt.Errorf("re-dispatch (org=%s, msg=%s): %w", p.OrgID, p.MessageID, err)
	}
	return nil
}

// retryPayload is the retry task body: org and message for the re-publish, plus
// attempts for the deterministic task id.
type retryPayload struct {
	OrgID     string `json:"org_id"`
	MessageID string `json:"message_id"`
	Attempts  int    `json:"attempts"`
}

// RetryScheduler enqueues the delayed retry task. It satisfies the
// dispatch.RetryScheduler and reconcile.RetryEnqueuer contracts.
type RetryScheduler struct {
	client *asynq.Client
	queue  string
	now    func() time.Time
	// Logger receives the scheduler's log lines. Nil means slog's default.
	Logger *slog.Logger
}

// NewRetryScheduler builds the scheduler over an asynq client; tasks go to queue.
func NewRetryScheduler(client *asynq.Client, queue string) *RetryScheduler {
	return &RetryScheduler{client: client, queue: queue, now: time.Now}
}

// retryTaskID is the deterministic asynq id "dispatch:<msg>:<attempts>": the same
// (message, attempts) always maps to one id, so a re-enqueue collapses to a
// no-op.
func retryTaskID(messageID string, attempts int) string {
	return fmt.Sprintf("dispatch:%s:%d", messageID, attempts)
}

// Schedule enqueues a retry task to fire at processAt under the deterministic id
// for (messageID, attempts). ErrTaskIDConflict is the expected dedup outcome and
// is swallowed.
func (s *RetryScheduler) Schedule(ctx context.Context, orgID, messageID string, attempts int, processAt time.Time) error {
	body, err := json.Marshal(retryPayload{OrgID: orgID, MessageID: messageID, Attempts: attempts})
	if err != nil {
		return fmt.Errorf("marshal retry payload: %w", err)
	}
	_, err = s.client.EnqueueContext(ctx, asynq.NewTask(TaskRetry, body),
		asynq.Queue(s.queue),
		asynq.TaskID(retryTaskID(messageID, attempts)),
		asynq.ProcessAt(processAt),
	)
	if err != nil {
		if errors.Is(err, asynq.ErrTaskIDConflict) {
			s.log().DebugContext(ctx, "retry enqueue: task already scheduled (dedup)", "message_id", messageID, "attempts", attempts)
			return nil
		}
		return fmt.Errorf("enqueue retry (org=%s, msg=%s, attempts=%d): %w", orgID, messageID, attempts, err)
	}
	return nil
}

// EnqueueRetry re-arms the retry for a FAILED row whose next_retry_at has passed
// (the reconcile lost-retry arm). Reconcile carries neither attempts nor
// next_retry_at, so it fires now under an id from the current minute: repeated
// reconcile ticks within a minute collapse to one enqueue, and the dispatcher's
// CAS makes a rare duplicate safe.
func (s *RetryScheduler) EnqueueRetry(ctx context.Context, orgID, messageID string) error {
	now := s.now()
	bucket := int(now.UTC().Truncate(time.Minute).Unix())
	if err := s.Schedule(ctx, orgID, messageID, bucket, now); err != nil {
		return fmt.Errorf("enqueue retry: %w", err)
	}
	return nil
}

// Source is the ScheduleSource for the two crons.
type Source struct {
	Queue         string
	ReconcileCron string
	PruneCron     string
}

// Name identifies this source in scheduler logs.
func (Source) Name() string { return "outbox-comms" }

// FetchSchedules returns the reconcile and prune cron definitions. Two runs of
// either may overlap (the scheduler does not hold one run's tick against the
// next for longer than half the interval). That is safe: reconcile's status
// changes pick rows with FOR UPDATE SKIP LOCKED and re-check the status in the
// same statement, so a row moves once; its re-publishing arms only send
// triggers, which the fan-out (ON CONFLICT), the dispatch claim and the retry
// task id deduplicate; and prune's updates and deletes are idempotent.
func (s Source) FetchSchedules(context.Context) ([]scheduler.ScheduleDefinition, error) {
	return []scheduler.ScheduleDefinition{
		{ID: "outbox_comms/reconcile", JobType: TaskReconcile, CronExpression: s.ReconcileCron, Payload: []byte("{}"), Queue: s.Queue},
		{ID: "outbox_comms/prune", JobType: TaskPrune, CronExpression: s.PruneCron, Payload: []byte("{}"), Queue: s.Queue},
	}, nil
}

func (s *RetryScheduler) log() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}
