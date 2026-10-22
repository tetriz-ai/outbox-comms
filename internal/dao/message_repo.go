// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package dao

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
)

// MessageRepo is the GORM-backed IMessageDAO.
type MessageRepo struct {
	db *gorm.DB
}

// NewMessageRepo constructs a MessageRepo.
func NewMessageRepo(db *gorm.DB) *MessageRepo { return &MessageRepo{db: db} }

var _ IMessageDAO = (*MessageRepo)(nil)

// MaterializeBatch inserts the fan-out rows with ON CONFLICT(request_id, channel,
// recipient) DO NOTHING and returns the rows it inserted; a row that conflicted
// with one already stored is left out. Each row gets a fresh id before the
// insert, replacing any it carried, so the ids found afterwards are exactly the
// inserted rows: GORM reads the INSERT's RETURNING itself, and with DO NOTHING
// it cannot say which rows those were.
func (r *MessageRepo) MaterializeBatch(ctx context.Context, orgID string, msgs []*model.CommsMessage) ([]*model.CommsMessage, error) {
	for _, m := range msgs {
		if m.OrganizationID != orgID {
			return nil, fmt.Errorf("materialize comms_messages: row for org %s in a batch for org %s", m.OrganizationID, orgID)
		}
	}
	if len(msgs) == 0 {
		return nil, nil
	}
	ids := make([]string, len(msgs))
	for i, m := range msgs {
		m.ID = uuid.NewString()
		ids[i] = m.ID
	}
	db := r.db.WithContext(ctx)
	res := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "request_id"}, {Name: "channel"}, {Name: "recipient"}},
		DoNothing: true,
	}).Create(&msgs)
	if res.Error != nil {
		return nil, fmt.Errorf("materialize comms_messages: %w", res.Error)
	}
	var stored []string
	if err := db.Model(&model.CommsMessage{}).Where("organization_id = ? AND id IN ?", orgID, ids).Pluck("id", &stored).Error; err != nil {
		return nil, fmt.Errorf("materialize comms_messages: read back inserted ids: %w", err)
	}
	inserted := make(map[string]bool, len(stored))
	for _, id := range stored {
		inserted[id] = true
	}
	var out []*model.CommsMessage
	for _, m := range msgs {
		if inserted[m.ID] {
			out = append(out, m)
		}
	}
	return out, nil
}

// ListQueuedForRequest re-selects the request's QUEUED rows.
func (r *MessageRepo) ListQueuedForRequest(ctx context.Context, orgID, requestID string) ([]*model.CommsMessage, error) {
	db := r.db.WithContext(ctx)
	var out []*model.CommsMessage
	err := db.Where("organization_id = ? AND request_id = ? AND status = ?",
		orgID, requestID, pb.MessageStatus_MESSAGE_STATUS_QUEUED.String()).
		Find(&out).Error
	if err != nil {
		return nil, fmt.Errorf("list queued comms_messages: %w", err)
	}
	return out, nil
}

// ClaimForDispatch CAS-advances a QUEUED row, or a FAILED row whose next_retry_at
// has passed, to SENDING. A FAILED row is not claimable before its retry time, so
// a redelivered trigger cannot skip the backoff. ok is false when
// another worker (or a queue redelivery for an already-advanced row) raced it:
// the at-least-once idempotency guard. The claim runs in its own transaction,
// and its UPDATE ... RETURNING reads the claimed row before that commits. An
// error at any point, the context cancelled while the row is read included,
// rolls the claim back, so either the caller holds the claim or the row is
// unchanged. This holds whatever the handle's SkipDefaultTransaction setting.
func (r *MessageRepo) ClaimForDispatch(ctx context.Context, orgID, id string) (bool, *model.CommsMessage, error) {
	var claimed []*model.CommsMessage
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.Model(&claimed).
			Clauses(clause.Returning{}).
			Where("organization_id = ? AND id = ?", orgID, id).
			Where("status = ? OR (status = ? AND (next_retry_at IS NULL OR next_retry_at <= ?))",
				pb.MessageStatus_MESSAGE_STATUS_QUEUED.String(),
				pb.MessageStatus_MESSAGE_STATUS_FAILED.String(), time.Now()).
			Updates(map[string]any{
				"status":     pb.MessageStatus_MESSAGE_STATUS_SENDING.String(),
				"claim_id":   uuid.NewString(),
				"updated_at": time.Now(),
			}).Error
	})
	if err != nil {
		return false, nil, fmt.Errorf("CAS claim comms_message: %w", err)
	}
	if len(claimed) == 0 {
		return false, nil, nil
	}
	return true, claimed[0], nil
}

// GetByID returns the message row by (org, id).
func (r *MessageRepo) GetByID(ctx context.Context, orgID, id string) (*model.CommsMessage, error) {
	db := r.db.WithContext(ctx)
	var out model.CommsMessage
	if err := db.Where("organization_id = ? AND id = ?", orgID, id).First(&out).Error; err != nil {
		return nil, fmt.Errorf("get comms_message: %w", err)
	}
	return &out, nil
}

// ListByRequest returns all message rows of a request (the status rollup).
func (r *MessageRepo) ListByRequest(ctx context.Context, orgID, requestID string) ([]*model.CommsMessage, error) {
	db := r.db.WithContext(ctx)
	var out []*model.CommsMessage
	err := db.Where("organization_id = ? AND request_id = ?", orgID, requestID).
		Order("created_at, id").Find(&out).Error
	if err != nil {
		return nil, fmt.Errorf("list comms_messages by request: %w", err)
	}
	return out, nil
}

// ListByOrg returns the org's messages newest-first, keyset-paginated on
// (created_at DESC, id DESC). A nil beforeCreatedAt starts at the head.
func (r *MessageRepo) ListByOrg(ctx context.Context, orgID string, beforeCreatedAt *time.Time, beforeID string, limit int) ([]*model.CommsMessage, error) {
	db := r.db.WithContext(ctx)
	q := db.Where("organization_id = ?", orgID)
	if beforeCreatedAt != nil {
		q = q.Where("(created_at, id) < (?, ?)", *beforeCreatedAt, beforeID)
	}
	var out []*model.CommsMessage
	if err := q.Order("created_at DESC, id DESC").Limit(limit).Find(&out).Error; err != nil {
		return nil, fmt.Errorf("list comms_messages by org: %w", err)
	}
	return out, nil
}

// ErrStaleTransition is returned by a Mark* method when the row exists but is no
// longer in a state that move may start from: another worker, the reconciler or
// a feedback event moved it first. The caller's move did not happen, and the
// row's current state stands.
var ErrStaleTransition = errors.New("comms_message moved on; transition not applied")

var (
	sending   = pb.MessageStatus_MESSAGE_STATUS_SENDING.String()
	sent      = pb.MessageStatus_MESSAGE_STATUS_SENT.String()
	terminals = []string{
		pb.MessageStatus_MESSAGE_STATUS_DEAD.String(),
		pb.MessageStatus_MESSAGE_STATUS_BOUNCED.String(),
		pb.MessageStatus_MESSAGE_STATUS_SUPPRESSED.String(),
		pb.MessageStatus_MESSAGE_STATUS_SKIPPED.String(),
	}
)

// transition applies a status update scoped to (org, id) and to the from-state
// condition guard (SQL with its args). updated_at is always bumped. It returns
// an error, never a silent success, when nothing moved: ErrStaleTransition when
// the row is in another state, a plain error when no row has that (org, id).
func (r *MessageRepo) transition(ctx context.Context, orgID, id string, guard string, guardArgs []any, fields map[string]any) error {
	fields["updated_at"] = time.Now()
	db := r.db.WithContext(ctx)
	res := db.Model(&model.CommsMessage{}).
		Where("organization_id = ? AND id = ?", orgID, id).
		Where(guard, guardArgs...).
		Updates(fields)
	if res.Error != nil {
		return fmt.Errorf("transition comms_message: %w", res.Error)
	}
	if res.RowsAffected == 1 {
		return nil
	}
	var n int64
	if err := db.Model(&model.CommsMessage{}).Where("organization_id = ? AND id = ?", orgID, id).Count(&n).Error; err != nil {
		return fmt.Errorf("transition comms_message: check row: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("transition comms_message: no row for (org=%s, id=%s)", orgID, id)
	}
	return ErrStaleTransition
}

// fromSending is the guard for the dispatcher's moves: only the worker holding
// the CAS claim may finish the attempt.
func (r *MessageRepo) fromSending(ctx context.Context, orgID, id, claimID string, fields map[string]any) error {
	return r.transition(ctx, orgID, id, "status = ? AND claim_id::text = ?", []any{sending, claimID}, fields)
}

// MarkSent records a provider-acked email send. From SENDING only.
func (r *MessageRepo) MarkSent(ctx context.Context, orgID, id, claimID, providerMessageID string) error {
	if err := r.fromSending(ctx, orgID, id, claimID, map[string]any{
		"status":              sent,
		"provider_message_id": providerMessageID,
	}); err != nil {
		return fmt.Errorf("mark sent: %w", err)
	}
	return nil
}

// MarkDelivered records a confirmed delivery: a Slack ack (from SENDING) or a
// delivery event (from SENT, or from UNCONFIRMED when the reason was only missing
// feedback). An uncertain_send UNCONFIRMED is never resurrected: that row may
// never have been sent. An empty providerMessageID keeps the stored one.
//
// claimID is the dispatcher's claim for a Slack ack: then a SENDING row must
// carry it. Feedback passes "", holds no claim, and may deliver a SENDING row
// whose SENT write has not landed yet.
func (r *MessageRepo) MarkDelivered(ctx context.Context, orgID, id, claimID, providerMessageID string) error {
	f := map[string]any{"status": pb.MessageStatus_MESSAGE_STATUS_DELIVERED.String()}
	if providerMessageID != "" {
		f["provider_message_id"] = providerMessageID
	}
	sendingGuard, sendingArgs := "status = ?", []any{sending}
	if claimID != "" {
		sendingGuard, sendingArgs = "(status = ? AND claim_id::text = ?)", []any{sending, claimID}
	}
	if err := r.transition(ctx, orgID, id,
		sendingGuard+" OR status = ? OR (status = ? AND unconfirmed_reason = ?)",
		append(sendingArgs, sent, pb.MessageStatus_MESSAGE_STATUS_UNCONFIRMED.String(), "no_delivery_feedback"), f); err != nil {
		return fmt.Errorf("mark delivered: %w", err)
	}
	return nil
}

// MarkFailed records a transient or uncertain-GUARANTEED failure: attempts+1,
// next_retry_at, last_error. From SENDING only.
func (r *MessageRepo) MarkFailed(ctx context.Context, orgID, id, claimID string, nextRetryAt time.Time, lastErr string) error {
	if err := r.fromSending(ctx, orgID, id, claimID, map[string]any{
		"status":        pb.MessageStatus_MESSAGE_STATUS_FAILED.String(),
		"attempts":      gorm.Expr("attempts + 1"),
		"next_retry_at": nextRetryAt,
		"last_error":    lastErr,
	}); err != nil {
		return fmt.Errorf("mark failed: %w", err)
	}
	return nil
}

// ReleaseClaim implements IMessageDAO. From SENDING only, so it never undoes a
// move another path already made.
func (r *MessageRepo) ReleaseClaim(ctx context.Context, orgID, id, claimID string) error {
	if err := r.fromSending(ctx, orgID, id, claimID, map[string]any{
		"status":   pb.MessageStatus_MESSAGE_STATUS_QUEUED.String(),
		"claim_id": gorm.Expr("NULL"),
	}); err != nil {
		return fmt.Errorf("requeue: %w", err)
	}
	return nil
}

// MarkDead records a terminal failure with a reason. From SENDING only.
func (r *MessageRepo) MarkDead(ctx context.Context, orgID, id, claimID, reason, lastErr string) error {
	if err := r.fromSending(ctx, orgID, id, claimID, map[string]any{
		"status":      pb.MessageStatus_MESSAGE_STATUS_DEAD.String(),
		"dead_reason": reason,
		"last_error":  lastErr,
	}); err != nil {
		return fmt.Errorf("mark dead: %w", err)
	}
	return nil
}

// MarkUnconfirmed records an unconfirmed terminal with a reason. From SENDING
// only.
func (r *MessageRepo) MarkUnconfirmed(ctx context.Context, orgID, id, claimID, reason string) error {
	if err := r.fromSending(ctx, orgID, id, claimID, map[string]any{
		"status":             pb.MessageStatus_MESSAGE_STATUS_UNCONFIRMED.String(),
		"unconfirmed_reason": reason,
	}); err != nil {
		return fmt.Errorf("mark unconfirmed: %w", err)
	}
	return nil
}

// MarkSuppressed records a pre-send suppression hit. From SENDING only.
func (r *MessageRepo) MarkSuppressed(ctx context.Context, orgID, id, claimID string) error {
	if err := r.fromSending(ctx, orgID, id, claimID, map[string]any{
		"status":      pb.MessageStatus_MESSAGE_STATUS_SUPPRESSED.String(),
		"skip_reason": "suppressed",
	}); err != nil {
		return fmt.Errorf("mark suppressed: %w", err)
	}
	return nil
}

// SnapshotRendered stores the rendered artifact for audit. Only while SENDING:
// the snapshot belongs to the attempt that holds the claim.
func (r *MessageRepo) SnapshotRendered(ctx context.Context, orgID, id, claimID string, payload *pb.RenderedPayload) error {
	// A map update bypasses the model's serializer, so encode the jsonb here.
	b, err := protojson.Marshal(payload)
	if err != nil {
		return fmt.Errorf("snapshot rendered: encode: %w", err)
	}
	if err := r.fromSending(ctx, orgID, id, claimID, map[string]any{"rendered_content": string(b)}); err != nil {
		return fmt.Errorf("snapshot rendered: %w", err)
	}
	return nil
}

// MarkBounced records a hard bounce. From any state but a terminal one (DEAD,
// BOUNCED, SUPPRESSED, SKIPPED): a delivered message can still bounce later.
func (r *MessageRepo) MarkBounced(ctx context.Context, orgID, id string) error {
	if err := r.transition(ctx, orgID, id, "status NOT IN ?", []any{terminals}, map[string]any{
		"status": pb.MessageStatus_MESSAGE_STATUS_BOUNCED.String(),
	}); err != nil {
		return fmt.Errorf("mark bounced: %w", err)
	}
	return nil
}

// GetByProviderMessageID resolves a message across orgs by its provider id: the
// lookup that discovers the org for email feedback. Returns nil, nil when no
// dispatched row matches, so feedback for an address this service never mailed
// writes nothing.
func (r *MessageRepo) GetByProviderMessageID(ctx context.Context, providerMessageID string) (*model.CommsMessage, error) {
	var out *model.CommsMessage
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var m model.CommsMessage
		err := tx.Where("provider_message_id = ?", providerMessageID).First(&m).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		out = &m
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("lookup by provider_message_id: %w", err)
	}
	return out, nil
}

// ErrNotFound lets callers distinguish a missing row via errors.Is(err, dao.ErrNotFound).
var ErrNotFound = gorm.ErrRecordNotFound
