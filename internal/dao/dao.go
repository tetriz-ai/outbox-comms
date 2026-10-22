// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package dao holds the GORM repositories for the six pipeline tables. Every
// org-owned read and write filters on organization_id in the query itself; the
// suppression list and the reconcile scans are deliberately cross-org.
package dao

import (
	"context"
	"time"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
)

// IRequestDAO persists and reads the producer's submission rows.
type IRequestDAO interface {
	// CreateIfAbsent inserts the request, returning the existing row on an
	// (org, idempotency_key) conflict (idempotent submit). The returned
	// bool is true when this call created the row.
	CreateIfAbsent(ctx context.Context, req *model.CommsRequest) (created bool, out *model.CommsRequest, err error)
	// GetByHandle returns the request by (org, handle==idempotency_key).
	GetByHandle(ctx context.Context, orgID, handle string) (*model.CommsRequest, error)
	// GetByID returns the request by (org, id): the UUID the request trigger
	// carries for the fan-out.
	GetByID(ctx context.Context, orgID, id string) (*model.CommsRequest, error)
	// UpdateStatus advances the request's fan-out lifecycle.
	UpdateStatus(ctx context.Context, orgID, id string, status pb.RequestStatus) error
	// GetUsecasesByIDs batch-resolves usecase for a page of request ids (the
	// usecase ListMessages shows for each message). Missing/foreign ids are simply
	// absent from the returned map — callers must not assume every id resolves.
	GetUsecasesByIDs(ctx context.Context, orgID string, ids []string) (map[string]string, error)
}

// IMessageDAO persists and reads the per-recipient message rows.
type IMessageDAO interface {
	// MaterializeBatch inserts the fan-out's message rows with
	// ON CONFLICT(request_id, channel, recipient) DO NOTHING, so a re-driven
	// fan-out no-ops the overlap, and returns only the rows it inserted.
	MaterializeBatch(ctx context.Context, orgID string, msgs []*model.CommsMessage) ([]*model.CommsMessage, error)
	// ListQueuedForRequest re-selects the request's QUEUED rows so a re-driven
	// fan-out re-emits the dispatch triggers.
	ListQueuedForRequest(ctx context.Context, orgID, requestID string) ([]*model.CommsMessage, error)
	// ClaimForDispatch CAS-advances a QUEUED row, or a FAILED row past its
	// next_retry_at, to SENDING; ok is false when the row is not claimable (another
	// worker advanced it, or its retry time has not come).
	ClaimForDispatch(ctx context.Context, orgID, id string) (ok bool, msg *model.CommsMessage, err error)
	// GetByID returns the message row.
	GetByID(ctx context.Context, orgID, id string) (*model.CommsMessage, error)
	// ListByRequest returns all message rows of a request (status rollup).
	ListByRequest(ctx context.Context, orgID, requestID string) ([]*model.CommsMessage, error)
	// ListByOrg returns the org's messages newest-first, keyset-paginated on
	// (created_at, id) for ListMessages.
	ListByOrg(ctx context.Context, orgID string, beforeCreatedAt *time.Time, beforeID string, limit int) ([]*model.CommsMessage, error)
	// A move out of SENDING carries the claim id ClaimForDispatch returned on the
	// row; a worker whose claim was taken over gets ErrStaleTransition.
	// The Mark* moves return ErrStaleTransition when the row is no longer in a
	// state the move may start from; the move is then not applied.

	// MarkSent records a provider-acked email send (SENDING → SENT).
	MarkSent(ctx context.Context, orgID, id, claimID, providerMessageID string) error
	// MarkDelivered records a confirmed delivery: a Slack ack (from SENDING) or a
	// delivery event (from SENT, or UNCONFIRMED(no_delivery_feedback)).
	MarkDelivered(ctx context.Context, orgID, id, claimID, providerMessageID string) error
	// MarkFailed records a transient/uncertain-GUARANTEED failure: attempts++,
	// next_retry_at set, last_error recorded (SENDING → FAILED).
	MarkFailed(ctx context.Context, orgID, id, claimID string, nextRetryAt time.Time, lastErr string) error
	// ReleaseClaim moves a claimed row back from SENDING to QUEUED, attempts
	// unchanged, when the attempt failed before the provider was called.
	ReleaseClaim(ctx context.Context, orgID, id, claimID string) error
	// MarkDead records a terminal failure (SENDING → DEAD) with a reason.
	MarkDead(ctx context.Context, orgID, id, claimID, reason, lastErr string) error
	// MarkUnconfirmed records an unconfirmed terminal (SENDING → UNCONFIRMED).
	MarkUnconfirmed(ctx context.Context, orgID, id, claimID, reason string) error
	// MarkSuppressed records a pre-send suppression hit (SENDING → SUPPRESSED).
	MarkSuppressed(ctx context.Context, orgID, id, claimID string) error
	// SnapshotRendered stores the rendered artifact for audit.
	SnapshotRendered(ctx context.Context, orgID, id, claimID string, payload *pb.RenderedPayload) error
	// GetByProviderMessageID resolves a message across orgs by its provider id —
	// the cross-org feedback lookup that discovers the org. Returns
	// nil (no error) when no dispatched row matches.
	GetByProviderMessageID(ctx context.Context, providerMessageID string) (*model.CommsMessage, error)
	// MarkBounced records a hard bounce (from any non-terminal state).
	MarkBounced(ctx context.Context, orgID, id string) error
}

// IDeliveryEventDAO appends provider delivery events (append-only).
type IDeliveryEventDAO interface {
	// Append inserts one delivery event, idempotent on provider_event_id when set
	// (ON CONFLICT DO NOTHING: feedback is delivered at least once).
	Append(ctx context.Context, e *model.CommsDeliveryEvent) error
	// ListByMessage returns a message's delivery events ordered oldest-first on
	// (occurred_at, id) so the caller renders the timeline in order.
	ListByMessage(ctx context.Context, orgID, messageID string) ([]*model.CommsDeliveryEvent, error)
}

// ISuppressionDAO manages the global, system-scoped sender-reputation list.
type ISuppressionDAO interface {
	// GetActiveByEmail returns the live (deleted_at IS NULL) suppression for an
	// address, or nil if none.
	GetActiveByEmail(ctx context.Context, email string) (*model.CommsSuppression, error)
	// SuppressIfAbsent inserts a live suppression for an address; a no-op if one
	// already exists (the unique-live index).
	SuppressIfAbsent(ctx context.Context, s *model.CommsSuppression) error
	// Deactivate soft-deletes the live suppression for an address (un-suppress).
	Deactivate(ctx context.Context, email string) error
	// TombstoneRecipient returns the lowercased address a purge kept for a
	// provider message id, and whether there is one created at or after
	// notBefore. A zero notBefore matches a tombstone of any age.
	TombstoneRecipient(ctx context.Context, providerMessageID string, notBefore time.Time) (string, bool, error)
}
