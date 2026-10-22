// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package dao

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
)

// SuppressionRepo is the GORM-backed ISuppressionDAO. comms_suppressions has no
// organization: a suppression applies to every sender.
type SuppressionRepo struct {
	db *gorm.DB
}

// NewSuppressionRepo constructs a SuppressionRepo.
func NewSuppressionRepo(db *gorm.DB) *SuppressionRepo { return &SuppressionRepo{db: db} }

var _ ISuppressionDAO = (*SuppressionRepo)(nil)

// NormalizeEmail is the form suppressions are stored and matched in: trimmed and
// lowercased. Mail providers treat the local part case-insensitively, so a bounce
// for Jane@Example.com must block jane@example.com.
func NormalizeEmail(email string) string { return strings.ToLower(strings.TrimSpace(email)) }

// GetActiveByEmail returns the live suppression for an address, matched
// case-insensitively, or nil if none.
func (r *SuppressionRepo) GetActiveByEmail(ctx context.Context, email string) (*model.CommsSuppression, error) {
	email = NormalizeEmail(email)
	var out *model.CommsSuppression
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var s model.CommsSuppression
		err := tx.Where("lower(email) = ? AND deleted_at IS NULL", email).First(&s).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		out = &s
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("get active suppression: %w", err)
	}
	return out, nil
}

// SuppressIfAbsent inserts a live suppression for an address, stored normalized;
// the unique-live index makes a second insert for an already-suppressed address
// a no-op.
func (r *SuppressionRepo) SuppressIfAbsent(ctx context.Context, s *model.CommsSuppression) error {
	s.Email = NormalizeEmail(s.Email)
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(s).Error
	})
	if err != nil {
		return fmt.Errorf("suppress address: %w", err)
	}
	return nil
}

// Deactivate soft-deletes the live suppression for an address (un-suppress),
// matched case-insensitively; a re-run on an already-deactivated address is a
// no-op.
func (r *SuppressionRepo) Deactivate(ctx context.Context, email string) error {
	email = NormalizeEmail(email)
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		now := time.Now()
		return tx.Model(&model.CommsSuppression{}).
			Where("lower(email) = ? AND deleted_at IS NULL", email).
			Updates(map[string]any{"deleted_at": now, "updated_at": now}).Error
	})
	if err != nil {
		return fmt.Errorf("deactivate suppression: %w", err)
	}
	return nil
}

// TombstoneRecipient implements ISuppressionDAO: the address a purge kept for a
// provider message id, if the tombstone is no older than notBefore. The bound
// matches the prune job's, which deletes tombstones created before its cutoff,
// so a tombstone the prune has not reached yet still counts only while live.
func (r *SuppressionRepo) TombstoneRecipient(ctx context.Context, providerMessageID string, notBefore time.Time) (string, bool, error) {
	var t model.CommsPurgeTombstone
	q := r.db.WithContext(ctx).Where("provider_message_id = ?", providerMessageID)
	if !notBefore.IsZero() {
		q = q.Where("created_at >= ?", notBefore)
	}
	err := q.Take(&t).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("get purge tombstone: %w", err)
	}
	return t.Recipient, true, nil
}
