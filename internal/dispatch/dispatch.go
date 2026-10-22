// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package dispatch sends one message. It claims the row, checks the address
// against the suppression list, renders the content, calls the channel's
// sender, and records the new status and a delivery event. Each channel's
// sender sits behind the Sender interface, so tests use fakes.
package dispatch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	"log/slog"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
	"github.com/tetriz-ai/outbox-comms/internal/metrics"
	"github.com/tetriz-ai/outbox-comms/templates"
)

// Outcome is the closed classification of a send attempt. An
// unrecognized signal must map to OutcomeTransient (retries-then-DEADs), never a
// silent drop.
type Outcome int

const (
	// OutcomeSuccess is a provider-accepted send.
	OutcomeSuccess Outcome = iota
	// OutcomeTransient is a retryable failure the provider did not act on:
	// throttling, or a connection that never carried the request.
	OutcomeTransient
	// OutcomePermanent is a clear, non-retryable failure (bad config/recipient).
	OutcomePermanent
	// OutcomeAuth is an auth failure (Slack invalid_auth/token_revoked).
	OutcomeAuth
	// OutcomeUncertain is a send that may have happened: a timeout, a reset or
	// cut-off connection, or a 5xx after the request left. GUARANTEED retries it,
	// BEST_EFFORT stops at UNCONFIRMED.
	OutcomeUncertain
)

// String renders the outcome for structured logs.
func (o Outcome) String() string {
	switch o {
	case OutcomeSuccess:
		return "success"
	case OutcomeTransient:
		return "transient"
	case OutcomePermanent:
		return "permanent"
	case OutcomeAuth:
		return "auth"
	case OutcomeUncertain:
		return "uncertain"
	default:
		return "unknown"
	}
}

// SendInput is the channel-agnostic send request handed to a Sender.
type SendInput struct {
	OrgID     string
	Channel   pb.Channel
	Vendor    pb.Vendor
	Recipient string
	Rendered  *pb.RenderedPayload
	// Params is the merged shared and per-recipient params (attachment_key and
	// the like, which the sender needs beyond the rendered body).
	Params        *structpb.Struct
	SlackDelivery pb.SlackDelivery
}

// SendResult is a Sender's outcome.
type SendResult struct {
	ProviderMessageID string
	Outcome           Outcome
	Err               error
	// ErrCode is a PII-free provider error code (e.g. "MessageRejected") for
	// logging. Err itself is not logged: a raw provider error can echo the
	// recipient address. The full Err lands in the row's last_error.
	ErrCode string
	// RetryAfter is the wait the provider asked for, such as Slack's
	// Retry-After on a 429. A retry waits the later of it and the ladder step.
	RetryAfter time.Duration
}

// Sender egresses one message on its channel. It must never panic and must
// classify its result into an Outcome.
type Sender interface {
	Send(ctx context.Context, in SendInput) SendResult
}

// AuthFailureReporter is the optional hook the dispatcher calls on OutcomeAuth,
// so a deployment can flag the org's Slack connection for reconnect. nil-safe.
type AuthFailureReporter interface {
	ReportGrantInvalid(ctx context.Context, orgID, reason string) error
}

// RetryScheduler enqueues a delayed task to re-enter the dispatch path at
// next_retry_at after a transient or uncertain failure. The deterministic
// (message, attempts) task id makes a duplicate enqueue a no-op. nil-safe: a nil
// scheduler falls back to the reconcile aged-FAILED sweep alone (the row still
// carries next_retry_at), so the dispatcher remains correct without it.
type RetryScheduler interface {
	Schedule(ctx context.Context, orgID, messageID string, attempts int, processAt time.Time) error
}

// RetryPolicy is the backoff ladder and cap. attempts reaching MaxAttempts
// terminates the message DEAD.
type RetryPolicy struct {
	Ladder      []time.Duration
	MaxAttempts int
}

// DefaultRetryPolicy is the default ladder: 1m, 5m, 30m, 2h, 6h, then DEAD on
// the sixth failed send.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		Ladder:      []time.Duration{time.Minute, 5 * time.Minute, 30 * time.Minute, 2 * time.Hour, 6 * time.Hour},
		MaxAttempts: 6,
	}
}

// Dispatcher runs the per-row dispatch path.
type Dispatcher struct {
	messageDAO     dao.IMessageDAO
	requestDAO     dao.IRequestDAO
	suppressionDAO dao.ISuppressionDAO
	eventDAO       dao.IDeliveryEventDAO
	senders        map[pb.Channel]Sender
	retry          RetryPolicy
	authReporter   AuthFailureReporter
	retryScheduler RetryScheduler
	now            func() time.Time
	chrome         templates.EmailChromeOptions
	sendTimeout    time.Duration
	logger         *slog.Logger
}

// DefaultSendTimeout bounds one provider call unless SetSendTimeout changes it.
const DefaultSendTimeout = 60 * time.Second

// NewDispatcher constructs a Dispatcher. authReporter and retryScheduler may be
// nil. now defaults to time.Now when nil. chrome is the email chrome every email
// is wrapped in; its Now is set per send from now.
func NewDispatcher(messageDAO dao.IMessageDAO, requestDAO dao.IRequestDAO, suppressionDAO dao.ISuppressionDAO, eventDAO dao.IDeliveryEventDAO, senders map[pb.Channel]Sender, retry RetryPolicy, authReporter AuthFailureReporter, retryScheduler RetryScheduler, now func() time.Time, chrome templates.EmailChromeOptions) *Dispatcher {
	if now == nil {
		now = time.Now
	}
	return &Dispatcher{messageDAO, requestDAO, suppressionDAO, eventDAO, senders, retry, authReporter, retryScheduler, now, chrome, DefaultSendTimeout, nil}
}

// SetLogger sets where the dispatcher logs; nil means slog's default.
func (d *Dispatcher) SetLogger(l *slog.Logger) { d.logger = l }

func (d *Dispatcher) log() *slog.Logger {
	if d.logger != nil {
		return d.logger
	}
	return slog.Default()
}

// SetSendTimeout sets the deadline of each provider call; zero or less keeps
// the current one. A call still running at its deadline is cut off, and the
// sender classifies it as usual: uncertain once the request was written, so it
// never outlives the stuck-send window and gets sent again while it runs.
func (d *Dispatcher) SetSendTimeout(t time.Duration) {
	if t > 0 {
		d.sendTimeout = t
	}
}

// Dispatch processes one message row. A CAS miss (the row already advanced, or a
// queue redelivery for a SENT row) is a safe no-op, and so is a row that moved on
// mid-attempt (dao.ErrStaleTransition: the reconciler took a stuck SENDING row):
// the state the other actor wrote stands.
func (d *Dispatcher) Dispatch(ctx context.Context, orgID, messageID string) error {
	err := d.dispatch(ctx, orgID, messageID)
	if errors.Is(err, dao.ErrStaleTransition) {
		d.log().WarnContext(ctx, "dispatch: message moved on during the attempt; its current state stands", "message_id", messageID)
		return nil
	}
	return err
}

// releaseTimeout bounds the claim release, which runs after the attempt's
// context may have been cancelled.
const releaseTimeout = 5 * time.Second

func (d *Dispatcher) dispatch(ctx context.Context, orgID, messageID string) error {
	ok, msg, err := d.messageDAO.ClaimForDispatch(ctx, orgID, messageID)
	if err != nil {
		return fmt.Errorf("dispatch: CAS claim: %w", err)
	}
	if !ok {
		return nil // CAS miss — another worker/redelivery; no-op.
	}
	// An error before the provider call leaves nothing sent, so the claim goes
	// back: the row returns to QUEUED and the redelivery sends it once. Once the
	// call has started, the row is never released, because the send may have
	// happened. The release outlives the attempt's context, so a worker
	// cancelled mid-attempt still hands the row back.
	called := false
	err = d.claimed(ctx, orgID, msg, &called)
	if err != nil && !called && !errors.Is(err, dao.ErrStaleTransition) {
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
		defer cancel()
		if rerr := d.messageDAO.ReleaseClaim(rctx, orgID, messageID, claimOf(msg)); rerr != nil && !errors.Is(rerr, dao.ErrStaleTransition) {
			return errors.Join(err, fmt.Errorf("dispatch: release claim: %w", rerr))
		}
	}
	return err
}

// claimed runs one claimed row through the suppression check, render, send and
// outcome. It sets *called just before the provider call.
func (d *Dispatcher) claimed(ctx context.Context, orgID string, msg *model.CommsMessage, called *bool) error {
	messageID := msg.ID
	req, err := d.requestDAO.GetByID(ctx, orgID, msg.RequestID)
	if err != nil {
		return fmt.Errorf("dispatch: load request: %w", err)
	}
	// Suppression is email-only and checked here, at dispatch, so a bounce arriving
	// after fan-out is still caught; transactional usecases never bypass it.
	if msg.Channel == pb.Channel_CHANNEL_EMAIL {
		sup, err := d.suppressionDAO.GetActiveByEmail(ctx, msg.Recipient)
		if err != nil {
			return fmt.Errorf("dispatch: suppression check: %w", err)
		}
		if sup != nil {
			if err := d.messageDAO.MarkSuppressed(ctx, orgID, messageID, claimOf(msg)); err != nil {
				return fmt.Errorf("mark suppressed: %w", err)
			}
			metrics.RecordTransition(req.Usecase, msg.Channel, pb.MessageStatus_MESSAGE_STATUS_SUPPRESSED)
			if err := d.appendEvent(ctx, orgID, messageID, pb.DeliveryEventType_DELIVERY_EVENT_TYPE_FAILED); err != nil {
				return fmt.Errorf("append suppressed event: %w", err)
			}
			return nil
		}
	}

	// Render the body. Block content (resolved per channel at fan-out) is the
	// primary path; a message with no content renders the usecase's typed renderer
	// over the merged params. merged is also handed to the sender (attachment_key).
	merged := mergeStruct(req.Params, msg.RecipientContext)
	var rendered *pb.RenderedPayload
	if rc := msg.Content; rc != nil {
		rendered, err = templates.RenderContent(msg.Channel, rc)
	} else {
		rendered, err = templates.Render(req.Usecase, msg.Channel, msg.Vendor, merged)
	}
	// Wrap email bodies in the shared chrome so the snapshot and the send both
	// carry the final document. The contract lookup can only fail on registry
	// drift, treated as a render bug.
	if err == nil && msg.Channel == pb.Channel_CHANNEL_EMAIL {
		if c, cerr := templates.Get(req.Usecase); cerr != nil {
			err = cerr
		} else {
			chrome := d.chrome
			chrome.Now = d.now()
			rendered = templates.WrapEmail(rendered, c.Category, chrome)
		}
	}
	if err != nil {
		// A render bug fails identically on every retry: DEAD, never FAILED.
		d.log().ErrorContext(ctx, "dispatch: render failed", "message_id", messageID, "error", err)
		if derr := d.messageDAO.MarkDead(ctx, orgID, messageID, claimOf(msg), "render_bug", err.Error()); derr != nil {
			return fmt.Errorf("mark dead (render_bug): %w", derr)
		}
		// Emit the DEAD metric on this early path too: it bypasses applyResult, and
		// the dead-incident alert keys on the render_bug series.
		metrics.RecordTransition(req.Usecase, msg.Channel, pb.MessageStatus_MESSAGE_STATUS_DEAD)
		metrics.RecordDead(metrics.ReasonRenderBug)
		if err := d.appendEvent(ctx, orgID, messageID, pb.DeliveryEventType_DELIVERY_EVENT_TYPE_FAILED); err != nil {
			return fmt.Errorf("append render-bug event: %w", err)
		}
		return nil
	}
	if err := d.messageDAO.SnapshotRendered(ctx, orgID, messageID, claimOf(msg), rendered); err != nil {
		return fmt.Errorf("snapshot rendered: %w", err)
	}

	sender := d.senders[msg.Channel]
	if sender == nil {
		// Config gap: no egress for this channel. Fail terminally, not silently.
		if derr := d.messageDAO.MarkDead(ctx, orgID, messageID, claimOf(msg), "no_sender", "no sender configured for channel"); derr != nil {
			return fmt.Errorf("mark dead (no_sender): %w", derr)
		}
		metrics.RecordTransition(req.Usecase, msg.Channel, pb.MessageStatus_MESSAGE_STATUS_DEAD)
		metrics.RecordDead(metrics.ReasonPermanentConfig)
		if err := d.appendEvent(ctx, orgID, messageID, pb.DeliveryEventType_DELIVERY_EVENT_TYPE_FAILED); err != nil {
			return fmt.Errorf("append no-sender event: %w", err)
		}
		return nil
	}

	start := d.now()
	*called = true
	sendCtx, cancelSend := context.WithTimeout(ctx, d.sendTimeout)
	res := sender.Send(sendCtx, SendInput{
		OrgID: orgID, Channel: msg.Channel, Vendor: msg.Vendor, Recipient: msg.Recipient,
		Rendered: rendered, Params: merged, SlackDelivery: msg.SlackDelivery,
	})
	cancelSend()
	metrics.RecordDispatchDuration(msg.Channel, d.now().Sub(start).Seconds())
	// The row's status + last_error is the system of record, but a failed send is
	// otherwise invisible to a log search (applyResult only writes the DB). Surface
	// it once here so a provider rejection is greppable without reading the row.
	// Warn, not Error: a transient failure is expected to retry. Log the PII-free
	// error code, never the raw error: a provider message can echo the recipient
	// address. The full error stays in the row's last_error.
	if res.Outcome != OutcomeSuccess {
		d.log().WarnContext(ctx, "dispatch: send did not succeed",
			"message_id", messageID,
			"org_id", orgID,
			"channel", msg.Channel.String(),
			"usecase", req.Usecase,
			"outcome", res.Outcome.String(),
			"error_code", res.ErrCode)
	}
	// The provider has answered, so its outcome is written even if the caller's
	// context ends now: a shutdown mid-write would otherwise leave a sent
	// message SENDING, and reconcile would send it again.
	if err := d.applyResult(context.WithoutCancel(ctx), orgID, req.Usecase, msg, res); err != nil {
		return fmt.Errorf("apply send result: %w", err)
	}
	return nil
}

// applyResult maps a SendResult to the row's terminal/retry status + a delivery
// event, applying the delivery_guarantee fork on an uncertain send, and emits the
// transition and dead metrics.
func (d *Dispatcher) applyResult(ctx context.Context, orgID string, usecase string, msg *model.CommsMessage, res SendResult) error {
	errStr := ""
	if res.Err != nil {
		errStr = res.Err.Error()
	}
	switch res.Outcome {
	case OutcomeSuccess:
		if msg.Channel == pb.Channel_CHANNEL_SLACK {
			// A Slack ack is delivery; there is no async feedback.
			if err := d.messageDAO.MarkDelivered(ctx, orgID, msg.ID, claimOf(msg), res.ProviderMessageID); err != nil {
				return fmt.Errorf("mark delivered: %w", err)
			}
			metrics.RecordTransition(usecase, msg.Channel, pb.MessageStatus_MESSAGE_STATUS_DELIVERED)
			metrics.RecordDeliveryLatency(msg.Channel, d.now().Sub(msg.CreatedAt).Seconds())
			if err := d.appendEvent(ctx, orgID, msg.ID, pb.DeliveryEventType_DELIVERY_EVENT_TYPE_DELIVERED); err != nil {
				return fmt.Errorf("append delivered event: %w", err)
			}
			return nil
		}
		if err := d.messageDAO.MarkSent(ctx, orgID, msg.ID, claimOf(msg), res.ProviderMessageID); err != nil {
			return fmt.Errorf("mark sent: %w", err)
		}
		metrics.RecordTransition(usecase, msg.Channel, pb.MessageStatus_MESSAGE_STATUS_SENT)
		metrics.RecordDeliveryLatency(msg.Channel, d.now().Sub(msg.CreatedAt).Seconds())
		if err := d.appendEvent(ctx, orgID, msg.ID, pb.DeliveryEventType_DELIVERY_EVENT_TYPE_SENT); err != nil {
			return fmt.Errorf("append sent event: %w", err)
		}
		return nil

	case OutcomePermanent:
		if err := d.messageDAO.MarkDead(ctx, orgID, msg.ID, claimOf(msg), "permanent_config", errStr); err != nil {
			return fmt.Errorf("mark dead (permanent_config): %w", err)
		}
		metrics.RecordTransition(usecase, msg.Channel, pb.MessageStatus_MESSAGE_STATUS_DEAD)
		metrics.RecordDead(metrics.ReasonPermanentConfig)
		if err := d.appendEvent(ctx, orgID, msg.ID, pb.DeliveryEventType_DELIVERY_EVENT_TYPE_FAILED); err != nil {
			return fmt.Errorf("append permanent-failure event: %w", err)
		}
		return nil

	case OutcomeAuth:
		if err := d.messageDAO.MarkDead(ctx, orgID, msg.ID, claimOf(msg), "invalid_auth", errStr); err != nil {
			return fmt.Errorf("mark dead (invalid_auth): %w", err)
		}
		metrics.RecordTransition(usecase, msg.Channel, pb.MessageStatus_MESSAGE_STATUS_DEAD)
		metrics.RecordDead(metrics.ReasonPermanentConfig)
		if d.authReporter != nil {
			if rerr := d.authReporter.ReportGrantInvalid(ctx, orgID, "invalid_auth"); rerr != nil {
				d.log().ErrorContext(ctx, "dispatch: ReportGrantInvalid failed", "message_id", msg.ID, "error", rerr)
			}
		}
		if err := d.appendEvent(ctx, orgID, msg.ID, pb.DeliveryEventType_DELIVERY_EVENT_TYPE_FAILED); err != nil {
			return fmt.Errorf("append auth-failure event: %w", err)
		}
		return nil

	case OutcomeUncertain:
		// GUARANTEED retries (accepts a rare duplicate); BEST_EFFORT stops at
		// UNCONFIRMED (accepts a rare miss) and is never retried.
		if msg.DeliveryGuarantee == pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT {
			if err := d.messageDAO.MarkUnconfirmed(ctx, orgID, msg.ID, claimOf(msg), "uncertain_send"); err != nil {
				return fmt.Errorf("mark unconfirmed (uncertain_send): %w", err)
			}
			metrics.RecordTransition(usecase, msg.Channel, pb.MessageStatus_MESSAGE_STATUS_UNCONFIRMED)
			metrics.RecordUnconfirmed("uncertain_send")
			if err := d.appendEvent(ctx, orgID, msg.ID, pb.DeliveryEventType_DELIVERY_EVENT_TYPE_FAILED); err != nil {
				return fmt.Errorf("append uncertain-send event: %w", err)
			}
			return nil
		}
		if err := d.retryOrDie(ctx, orgID, usecase, msg, errStr, res.RetryAfter); err != nil {
			return fmt.Errorf("retry or die (uncertain): %w", err)
		}
		return nil

	case OutcomeTransient:
		if err := d.retryOrDie(ctx, orgID, usecase, msg, errStr, res.RetryAfter); err != nil {
			return fmt.Errorf("retry or die (transient): %w", err)
		}
		return nil

	default:
		// Unrecognized: transient (retries, then DEAD; never a silent drop).
		if err := d.retryOrDie(ctx, orgID, usecase, msg, errStr, res.RetryAfter); err != nil {
			return fmt.Errorf("retry or die (unknown outcome): %w", err)
		}
		return nil
	}
}

// policyFor resolves the retry policy for a usecase, most specific first: the
// contract's RetryOverride, field by field, over the dispatcher's global policy.
func (d *Dispatcher) policyFor(usecase string) RetryPolicy {
	p := d.retry
	c, err := templates.Get(usecase)
	if err != nil || c.RetryOverride == nil {
		return p
	}
	if len(c.RetryOverride.Ladder) > 0 {
		p.Ladder = c.RetryOverride.Ladder
	}
	if c.RetryOverride.MaxAttempts > 0 {
		p.MaxAttempts = c.RetryOverride.MaxAttempts
	}
	return p
}

// retryOrDie schedules the next retry, or terminates DEAD once attempts reach the
// cap. attempts is 0-based on the row; this is its (attempts+1)-th send.
func (d *Dispatcher) retryOrDie(ctx context.Context, orgID string, usecase string, msg *model.CommsMessage, errStr string, retryAfter time.Duration) error {
	policy := d.policyFor(usecase)
	nextAttempt := msg.Attempts + 1
	if nextAttempt >= policy.MaxAttempts {
		if err := d.messageDAO.MarkDead(ctx, orgID, msg.ID, claimOf(msg), "transient_exhausted", errStr); err != nil {
			return fmt.Errorf("mark dead (transient_exhausted): %w", err)
		}
		metrics.RecordTransition(usecase, msg.Channel, pb.MessageStatus_MESSAGE_STATUS_DEAD)
		metrics.RecordDead(metrics.ReasonTransientExhausted)
		if err := d.appendEvent(ctx, orgID, msg.ID, pb.DeliveryEventType_DELIVERY_EVENT_TYPE_FAILED); err != nil {
			return fmt.Errorf("append exhausted-failure event: %w", err)
		}
		return nil
	}
	idx := msg.Attempts
	if idx >= len(policy.Ladder) {
		idx = len(policy.Ladder) - 1
	}
	wait := policy.Ladder[idx]
	if retryAfter > wait {
		wait = retryAfter
	}
	nextRetryAt := d.now().Add(wait)
	if err := d.messageDAO.MarkFailed(ctx, orgID, msg.ID, claimOf(msg), nextRetryAt, errStr); err != nil {
		return fmt.Errorf("mark failed: %w", err)
	}
	metrics.RecordTransition(usecase, msg.Channel, pb.MessageStatus_MESSAGE_STATUS_FAILED)
	// Arm the delayed retry to fire at next_retry_at. The deterministic
	// (message, nextAttempt) task id dedups a double-fire. A schedule failure is
	// logged, not propagated: the row is already FAILED with next_retry_at set, so
	// the reconcile lost-retry arm still re-drives it.
	if d.retryScheduler != nil {
		if err := d.retryScheduler.Schedule(ctx, orgID, msg.ID, nextAttempt, nextRetryAt); err != nil {
			d.log().ErrorContext(ctx, "dispatch: schedule retry task failed (reconcile is the backstop)",
				"message_id", msg.ID, "attempts", nextAttempt, "error", err)
		}
	}
	if err := d.appendEvent(ctx, orgID, msg.ID, pb.DeliveryEventType_DELIVERY_EVENT_TYPE_FAILED); err != nil {
		return fmt.Errorf("append retry-failure event: %w", err)
	}
	return nil
}

func (d *Dispatcher) appendEvent(ctx context.Context, orgID, messageID string, t pb.DeliveryEventType) error {
	if err := d.eventDAO.Append(ctx, &model.CommsDeliveryEvent{
		OrganizationID: orgID,
		MessageID:      messageID,
		EventType:      t,
		OccurredAt:     d.now(),
	}); err != nil {
		return fmt.Errorf("append delivery event: %w", err)
	}
	return nil
}

// mergeStruct overlays per-recipient vars on the shared params (a shallow
// top-level override: vars wins; no deep merge).
func mergeStruct(params, vars *structpb.Struct) *structpb.Struct {
	out := &structpb.Struct{Fields: map[string]*structpb.Value{}}
	for k, v := range params.GetFields() {
		out.Fields[k] = v
	}
	for k, v := range vars.GetFields() {
		out.Fields[k] = v
	}
	return out
}

// claimOf is the claim id ClaimForDispatch put on the row; every move this
// attempt makes out of SENDING carries it.
func claimOf(msg *model.CommsMessage) string {
	if msg.ClaimID == nil {
		return ""
	}
	return *msg.ClaimID
}
