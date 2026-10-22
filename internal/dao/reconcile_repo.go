// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package dao

import (
	"context"
	"fmt"
	"sort"
	"time"

	"gorm.io/gorm"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
)

// Ref is a minimal (org, id) pointer the reconciler re-enqueues. Reconcile scans
// run across organizations, so they carry the org alongside the id.
type Ref struct {
	OrganizationID string
	ID             string
	// Channel and Usecase are filled for message rows a bounded update moved,
	// so the caller can count the move. Channel is the enum name.
	Channel string
	Usecase string
}

// Reputation is the sender-reputation sample over a window.
type Reputation struct {
	Sent        int64
	HardBounces int64
	Complaints  int64
}

// IReconcileDAO holds the cross-org maintenance scans behind the reconcile and
// prune jobs and the admin commands.
type IReconcileDAO interface {
	// StuckRequests returns ACCEPTED/FANNING_OUT requests older than cutoff
	// (reconcile arm 1).
	StuckRequests(ctx context.Context, cutoff time.Time, limit int) ([]Ref, error)
	// RequeueStuckSendingGuaranteed counts a crash as an attempt on up to limit
	// SENDING-past-cutoff GUARANTEED rows. A row whose next attempt reaches its
	// usecase's cap goes DEAD(crash_exhausted); the rest go back to QUEUED for
	// re-enqueue, accepting a rare duplicate (arm 2).
	RequeueStuckSendingGuaranteed(ctx context.Context, cutoff time.Time, limit int, attemptCap AttemptCap) (requeued, dead []Ref, err error)
	// UnconfirmStuckSendingBestEffort marks up to limit SENDING-past-cutoff
	// BEST_EFFORT rows UNCONFIRMED(uncertain_send), with no re-send (arm 2).
	// Returns the count.
	UnconfirmStuckSendingBestEffort(ctx context.Context, cutoff time.Time, limit int) ([]Ref, error)
	// MarkMissingConfirmation marks up to limit SENT+CONFIRMED rows with no
	// delivery event since cutoff UNCONFIRMED(no_delivery_feedback) (arm 3).
	// Returns the count.
	MarkMissingConfirmation(ctx context.Context, cutoff time.Time, limit int) ([]Ref, error)
	// CountOverdueFailed counts FAILED rows whose next_retry_at is before cutoff:
	// retries the retry task should already have run.
	CountOverdueFailed(ctx context.Context, cutoff time.Time) (int64, error)
	// ReputationCounts counts, since the given time, email sends (SENT events),
	// complaints (COMPLAINT events) and hard bounces (messages that became
	// BOUNCED).
	ReputationCounts(ctx context.Context, since time.Time) (Reputation, error)
	// RetriableFailed returns FAILED rows whose next_retry_at has passed (arm 4 —
	// a lost retry task).
	RetriableFailed(ctx context.Context, now time.Time, limit int) ([]Ref, error)
	// AgedQueued returns QUEUED rows older than cutoff (arm 4 — a lost dispatch
	// trigger).
	AgedQueued(ctx context.Context, cutoff time.Time, limit int) ([]Ref, error)
	// PruneProviderPayload nulls provider_payload on events older than cutoff (the
	// prune job). No row is deleted. Returns the count.
	PruneProviderPayload(ctx context.Context, cutoff time.Time) (int64, error)
	// PruneTombstones deletes purge tombstones created before cutoff.
	PruneTombstones(ctx context.Context, cutoff time.Time) (int64, error)
	// ListDead returns DEAD rows matching the optional org + dead_reason filter
	// (`admin redrive-dead` without --execute). Empty filters match all.
	ListDead(ctx context.Context, orgID, deadReason string, limit int) ([]*model.CommsMessage, error)
	// RedriveDead resets the given rows that are still DEAD to QUEUED
	// (attempts=0, next_retry_at/last_error/dead_reason cleared) so the
	// reconcile aged-QUEUED arm re-dispatches them. The caller passes the ids
	// ListDead showed, so a row that died after the listing is not reset. Run it
	// only after the underlying cause is fixed.
	RedriveDead(ctx context.Context, ids []string) (int64, error)
	// PurgeOrg hard-deletes an org's comms data in FK order (delivery_events →
	// messages → requests, plus preferences + destinations); comms_suppressions
	// survives. Returns the comms_messages rows deleted.
	PurgeOrg(ctx context.Context, orgID string) (int64, error)
}

// ReconcileRepo is the GORM-backed IReconcileDAO.
type ReconcileRepo struct{ db *gorm.DB }

// NewReconcileRepo constructs a ReconcileRepo.
func NewReconcileRepo(db *gorm.DB) *ReconcileRepo { return &ReconcileRepo{db: db} }

var _ IReconcileDAO = (*ReconcileRepo)(nil)

// StuckRequests returns ACCEPTED or FANNING_OUT requests last updated before
// cutoff, oldest first, at most limit.
func (r *ReconcileRepo) StuckRequests(ctx context.Context, cutoff time.Time, limit int) ([]Ref, error) {
	var refs []Ref
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.Model(&model.CommsRequest{}).
			Where("status IN ? AND updated_at < ?",
				[]string{
					pb.RequestStatus_REQUEST_STATUS_ACCEPTED.String(),
					pb.RequestStatus_REQUEST_STATUS_FANNING_OUT.String(),
				}, cutoff).
			Order("updated_at").Limit(limit).
			Select("organization_id", "id").
			Scan(&refs).Error
	})
	if err != nil {
		return nil, fmt.Errorf("reconcile stuck requests: %w", err)
	}
	return refs, nil
}

// boundedUpdate flips at most limit rows matching where (oldest updated_at first)
// to the set values, in one statement, and returns exactly the rows it flipped.
// The row set is picked once in a MATERIALIZED CTE: as a plain IN (subquery) the
// planner may rescan a LIMIT ... SKIP LOCKED subquery per outer row, and each
// rescan skips the rows just locked, so the limit stops binding. FOR UPDATE locks
// each picked row, so no worker can move it before this statement commits, and
// SKIP LOCKED keeps two reconcilers off the same rows. The outer repeat of where
// adds nothing while that lock clause stands; it is a guard should it change.
func boundedUpdate(tx *gorm.DB, where string, whereArgs []any, set string, setArgs []any, limit int) ([]Ref, error) {
	var refs []Ref
	args := append(append(append([]any{}, whereArgs...), limit), setArgs...)
	args = append(args, whereArgs...)
	err := tx.Raw(
		`WITH picked AS MATERIALIZED (`+
			`SELECT id FROM comms_messages WHERE `+where+` ORDER BY updated_at LIMIT ? FOR UPDATE SKIP LOCKED) `+
			`UPDATE comms_messages SET `+set+` FROM picked WHERE comms_messages.id = picked.id AND `+where+
			` RETURNING comms_messages.organization_id, comms_messages.id, comms_messages.channel,`+
			` (SELECT usecase FROM comms_requests WHERE comms_requests.id = comms_messages.request_id) AS usecase`,
		args...).Scan(&refs).Error
	return refs, err
}

// DeadReasonCrashExhausted is the dead_reason of a GUARANTEED message whose
// worker stopped mid-send on every attempt it was given.
const DeadReasonCrashExhausted = "crash_exhausted"

// AttemptCap is the number of attempts after which a message is DEAD: Default
// for every usecase, or the usecase's own value in ByUsecase.
type AttemptCap struct {
	Default   int
	ByUsecase map[string]int
}

// sql returns an expression for the cap of the comms_messages row in scope,
// read through its request's usecase, and its arguments.
func (c AttemptCap) sql() (string, []any) {
	if len(c.ByUsecase) == 0 {
		return "?", []any{c.Default}
	}
	keys := make([]string, 0, len(c.ByUsecase))
	for k := range c.ByUsecase {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	expr := "(SELECT CASE r.usecase"
	var args []any
	for _, k := range keys {
		expr += " WHEN ? THEN ?::int"
		args = append(args, k, c.ByUsecase[k])
	}
	expr += " ELSE ?::int END FROM comms_requests r WHERE r.id = comms_messages.request_id)"
	return expr, append(args, c.Default)
}

// RequeueStuckSendingGuaranteed implements IReconcileDAO. Both moves count the
// crash in attempts, so a message whose worker keeps dying is not re-sent
// without end: it is capped like any other failure.
func (r *ReconcileRepo) RequeueStuckSendingGuaranteed(ctx context.Context, cutoff time.Time, limit int, attemptCap AttemptCap) ([]Ref, []Ref, error) {
	var requeued, dead []Ref
	stuck := "status = ? AND updated_at < ? AND delivery_guarantee = ?"
	stuckArgs := []any{pb.MessageStatus_MESSAGE_STATUS_SENDING.String(), cutoff, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED.String()}
	capExpr, capArgs := attemptCap.sql()
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		dead, err = boundedUpdate(tx,
			stuck+" AND attempts + 1 >= "+capExpr, append(append([]any{}, stuckArgs...), capArgs...),
			"status = ?, attempts = attempts + 1, dead_reason = ?, claim_id = NULL, updated_at = ?",
			[]any{pb.MessageStatus_MESSAGE_STATUS_DEAD.String(), DeadReasonCrashExhausted, time.Now()},
			limit)
		if err != nil {
			return err
		}
		requeued, err = boundedUpdate(tx, stuck, stuckArgs,
			"status = ?, attempts = attempts + 1, claim_id = NULL, updated_at = ?",
			[]any{pb.MessageStatus_MESSAGE_STATUS_QUEUED.String(), time.Now()},
			limit)
		return err
	})
	if err != nil {
		return nil, nil, fmt.Errorf("reconcile requeue stuck sending: %w", err)
	}
	return requeued, dead, nil
}

// UnconfirmStuckSendingBestEffort implements IReconcileDAO.
func (r *ReconcileRepo) UnconfirmStuckSendingBestEffort(ctx context.Context, cutoff time.Time, limit int) ([]Ref, error) {
	var refs []Ref
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		refs, err = boundedUpdate(tx,
			"status = ? AND updated_at < ? AND delivery_guarantee = ?",
			[]any{pb.MessageStatus_MESSAGE_STATUS_SENDING.String(), cutoff, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT.String()},
			"status = ?, unconfirmed_reason = ?, claim_id = NULL, updated_at = ?",
			[]any{pb.MessageStatus_MESSAGE_STATUS_UNCONFIRMED.String(), "uncertain_send", time.Now()},
			limit)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("reconcile unconfirm stuck sending: %w", err)
	}
	return refs, nil
}

// MarkMissingConfirmation implements IReconcileDAO.
func (r *ReconcileRepo) MarkMissingConfirmation(ctx context.Context, cutoff time.Time, limit int) ([]Ref, error) {
	var refs []Ref
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		refs, err = boundedUpdate(tx,
			"status = ? AND verification_level = ? AND updated_at < ?",
			[]any{pb.MessageStatus_MESSAGE_STATUS_SENT.String(), pb.VerificationLevel_VERIFICATION_LEVEL_CONFIRMED.String(), cutoff},
			"status = ?, unconfirmed_reason = ?, updated_at = ?",
			[]any{pb.MessageStatus_MESSAGE_STATUS_UNCONFIRMED.String(), "no_delivery_feedback", time.Now()},
			limit)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("reconcile mark missing confirmation: %w", err)
	}
	return refs, nil
}

// CountOverdueFailed implements IReconcileDAO.
func (r *ReconcileRepo) CountOverdueFailed(ctx context.Context, cutoff time.Time) (int64, error) {
	var n int64
	err := r.db.WithContext(ctx).Model(&model.CommsMessage{}).
		Where("status = ? AND next_retry_at < ?", pb.MessageStatus_MESSAGE_STATUS_FAILED.String(), cutoff).
		Count(&n).Error
	if err != nil {
		return 0, fmt.Errorf("reconcile count overdue failed: %w", err)
	}
	return n, nil
}

// ReputationCounts implements IReconcileDAO.
func (r *ReconcileRepo) ReputationCounts(ctx context.Context, since time.Time) (Reputation, error) {
	var out Reputation
	db := r.db.WithContext(ctx)
	err := db.Raw(`SELECT
		count(*) FILTER (WHERE event_type = ?) AS sent,
		count(*) FILTER (WHERE event_type = ?) AS complaints
		FROM comms_delivery_events WHERE occurred_at >= ?`,
		pb.DeliveryEventType_DELIVERY_EVENT_TYPE_SENT.String(), pb.DeliveryEventType_DELIVERY_EVENT_TYPE_COMPLAINT.String(), since).
		Scan(&out).Error
	if err != nil {
		return Reputation{}, fmt.Errorf("reconcile reputation events: %w", err)
	}
	if err := db.Model(&model.CommsMessage{}).
		Where("status = ? AND updated_at >= ?", pb.MessageStatus_MESSAGE_STATUS_BOUNCED.String(), since).
		Count(&out.HardBounces).Error; err != nil {
		return Reputation{}, fmt.Errorf("reconcile reputation bounces: %w", err)
	}
	return out, nil
}

// RetriableFailed returns FAILED rows whose next_retry_at is before now, oldest
// first, at most limit.
func (r *ReconcileRepo) RetriableFailed(ctx context.Context, now time.Time, limit int) ([]Ref, error) {
	var refs []Ref
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.Model(&model.CommsMessage{}).
			Where("status = ? AND next_retry_at < ?", pb.MessageStatus_MESSAGE_STATUS_FAILED.String(), now).
			Order("next_retry_at").Limit(limit).
			Select("organization_id", "id").Scan(&refs).Error
	})
	if err != nil {
		return nil, fmt.Errorf("reconcile retriable failed: %w", err)
	}
	return refs, nil
}

// AgedQueued returns QUEUED rows last updated before cutoff, oldest first, at
// most limit.
func (r *ReconcileRepo) AgedQueued(ctx context.Context, cutoff time.Time, limit int) ([]Ref, error) {
	var refs []Ref
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.Model(&model.CommsMessage{}).
			Where("status = ? AND updated_at < ?", pb.MessageStatus_MESSAGE_STATUS_QUEUED.String(), cutoff).
			Order("updated_at").Limit(limit).
			Select("organization_id", "id").Scan(&refs).Error
	})
	if err != nil {
		return nil, fmt.Errorf("reconcile aged queued: %w", err)
	}
	return refs, nil
}

// PruneProviderPayload implements IReconcileDAO.
func (r *ReconcileRepo) PruneProviderPayload(ctx context.Context, cutoff time.Time) (int64, error) {
	var n int64
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		res := tx.Model(&model.CommsDeliveryEvent{}).
			Where("created_at < ? AND provider_payload IS NOT NULL", cutoff).
			Update("provider_payload", gorm.Expr("NULL"))
		n = res.RowsAffected
		return res.Error
	})
	if err != nil {
		return 0, fmt.Errorf("reconcile prune provider payload: %w", err)
	}
	return n, nil
}

// ListDead implements IReconcileDAO.
func (r *ReconcileRepo) ListDead(ctx context.Context, orgID, deadReason string, limit int) ([]*model.CommsMessage, error) {
	var out []*model.CommsMessage
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		q := tx.Where("status = ?", pb.MessageStatus_MESSAGE_STATUS_DEAD.String())
		if orgID != "" {
			q = q.Where("organization_id = ?", orgID)
		}
		if deadReason != "" {
			q = q.Where("dead_reason = ?", deadReason)
		}
		return q.Order("updated_at").Limit(limit).Find(&out).Error
	})
	if err != nil {
		return nil, fmt.Errorf("reconcile list dead: %w", err)
	}
	return out, nil
}

// RedriveDead implements IReconcileDAO.
func (r *ReconcileRepo) RedriveDead(ctx context.Context, ids []string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	var n int64
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Re-assert status = DEAD in the UPDATE: a row that left DEAD since it was
		// listed (a concurrent redrive, say) is skipped, not reset to QUEUED.
		res := tx.Model(&model.CommsMessage{}).
			Where("id IN ? AND status = ?", ids, pb.MessageStatus_MESSAGE_STATUS_DEAD.String()).
			Updates(map[string]any{
				"status":        pb.MessageStatus_MESSAGE_STATUS_QUEUED.String(),
				"attempts":      0,
				"next_retry_at": gorm.Expr("NULL"),
				"last_error":    gorm.Expr("NULL"),
				"dead_reason":   gorm.Expr("NULL"),
				"updated_at":    time.Now(),
			})
		n = res.RowsAffected
		return res.Error
	})
	if err != nil {
		return 0, fmt.Errorf("reconcile redrive dead: %w", err)
	}
	return n, nil
}

// PurgeOrg implements IReconcileDAO, in one transaction.
func (r *ReconcileRepo) PurgeOrg(ctx context.Context, orgID string) (int64, error) {
	var deletedMessages int64
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// A purged email message that reached the provider leaves a tombstone, so
		// a bounce or complaint that arrives later still suppresses the address.
		if err := tx.Exec(`INSERT INTO comms_purge_tombstones (provider_message_id, recipient, created_at)
			SELECT provider_message_id, lower(recipient), now() FROM comms_messages
			WHERE organization_id = ? AND channel = ? AND provider_message_id IS NOT NULL AND provider_message_id <> ''
			ON CONFLICT (provider_message_id) DO NOTHING`, orgID, pb.Channel_CHANNEL_EMAIL.String()).Error; err != nil {
			return fmt.Errorf("keep purge tombstones: %w", err)
		}
		// FK order: events → messages → requests. preferences/destinations have no
		// cross-FKs. comms_suppressions is intentionally not touched.
		if err := tx.Where("organization_id = ?", orgID).Delete(&model.CommsDeliveryEvent{}).Error; err != nil {
			return err
		}
		res := tx.Where("organization_id = ?", orgID).Delete(&model.CommsMessage{})
		if res.Error != nil {
			return res.Error
		}
		deletedMessages = res.RowsAffected
		if err := tx.Where("organization_id = ?", orgID).Delete(&model.CommsRequest{}).Error; err != nil {
			return err
		}
		if err := tx.Where("organization_id = ?", orgID).Delete(&model.CommsPreference{}).Error; err != nil {
			return err
		}
		return tx.Where("organization_id = ?", orgID).Delete(&model.CommsChannelDestination{}).Error
	})
	if err != nil {
		return 0, fmt.Errorf("reconcile purge org: %w", err)
	}
	return deletedMessages, nil
}

// PruneTombstones implements IReconcileDAO.
func (r *ReconcileRepo) PruneTombstones(ctx context.Context, cutoff time.Time) (int64, error) {
	res := r.db.WithContext(ctx).Where("created_at < ?", cutoff).Delete(&model.CommsPurgeTombstone{})
	if res.Error != nil {
		return 0, fmt.Errorf("prune purge tombstones: %w", res.Error)
	}
	return res.RowsAffected, nil
}
