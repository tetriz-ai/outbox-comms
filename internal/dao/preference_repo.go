// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package dao

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
)

// IPreferenceDAO reads and writes the preference gate (comms_preferences).
type IPreferenceDAO interface {
	// IsEnabled resolves "may we send?" for (category, channel) with the
	// PERSON-over-ORG cascade: a PERSON row wins over an ORG row; with no
	// row at all the default is on (absence = default-enabled). Transactional
	// usecases never call this (the caller bypasses the gate).
	IsEnabled(ctx context.Context, orgID, personID string, category pb.MessageCategory, channel pb.Channel) (bool, error)
	// Upsert sets one preference cell (a person's or admin's edit).
	Upsert(ctx context.Context, p *model.CommsPreference) error
	// ListForEntity returns the preference rows for one entity (a person's or admin's read).
	ListForEntity(ctx context.Context, orgID string, entityType pb.EntityType, entityID string) ([]*model.CommsPreference, error)
}

// PreferenceRepo is the GORM-backed IPreferenceDAO.
type PreferenceRepo struct{ db *gorm.DB }

// NewPreferenceRepo constructs a PreferenceRepo.
func NewPreferenceRepo(db *gorm.DB) *PreferenceRepo { return &PreferenceRepo{db: db} }

var _ IPreferenceDAO = (*PreferenceRepo)(nil)

// IsEnabled implements the most-specific-wins cascade: select the PERSON row if
// present (for a non-empty personID), else the ORG row, else default to enabled.
func (r *PreferenceRepo) IsEnabled(ctx context.Context, orgID, personID string, category pb.MessageCategory, channel pb.Channel) (bool, error) {
	db := r.db.WithContext(ctx)
	q := db.Model(&model.CommsPreference{}).
		Where("organization_id = ? AND category = ? AND channel = ?", orgID, category.String(), channel.String())
	if personID != "" {
		q = q.Where("(entity_type = ? AND entity_id = ?) OR entity_type = ?",
			pb.EntityType_ENTITY_TYPE_PERSON.String(), personID,
			pb.EntityType_ENTITY_TYPE_ORG.String())
	} else {
		q = q.Where("entity_type = ?", pb.EntityType_ENTITY_TYPE_ORG.String())
	}
	// PERSON sorts before ORG so the most-specific row is first. Parameterized
	// (gorm.Expr) rather than string-concatenated so the enum value goes through
	// the prepared-statement path, consistent with every other query here.
	var rows []*model.CommsPreference
	if err := q.Clauses(clause.OrderBy{Expression: gorm.Expr("entity_type = ? DESC", pb.EntityType_ENTITY_TYPE_PERSON.String())}).Limit(1).Find(&rows).Error; err != nil {
		return false, fmt.Errorf("resolve preference: %w", err)
	}
	if len(rows) == 0 {
		return true, nil // absence = default-enabled
	}
	return rows[0].Enabled, nil
}

// Upsert sets one preference cell on the unique (org, entity, category, channel).
// ON CONFLICT DO UPDATE (one atomic statement), not FirstOrCreate (SELECT then
// INSERT): two concurrent profile/admin saves of the same cell would otherwise
// both miss the SELECT and race to a unique-constraint error.
func (r *PreferenceRepo) Upsert(ctx context.Context, p *model.CommsPreference) error {
	db := r.db.WithContext(ctx)
	if err := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "organization_id"}, {Name: "entity_type"}, {Name: "entity_id"}, {Name: "category"}, {Name: "channel"}},
		DoUpdates: clause.AssignmentColumns([]string{"enabled", "updated_at"}),
	}).Create(p).Error; err != nil {
		return fmt.Errorf("upsert preference: %w", err)
	}
	return nil
}

// ListForEntity returns one entity's preference rows.
func (r *PreferenceRepo) ListForEntity(ctx context.Context, orgID string, entityType pb.EntityType, entityID string) ([]*model.CommsPreference, error) {
	db := r.db.WithContext(ctx)
	var out []*model.CommsPreference
	err := db.Where("organization_id = ? AND entity_type = ? AND entity_id = ?", orgID, entityType.String(), entityID).Find(&out).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("list preferences: %w", err)
	}
	return out, nil
}
