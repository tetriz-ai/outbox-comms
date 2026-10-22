// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package dao

import (
	"context"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
)

// IDestinationDAO reads and writes the routing destinations
// (comms_channel_destinations).
type IDestinationDAO interface {
	// Resolve returns the configured destination_ref for (entity, usecase,
	// channel): one ref, or empty when none is configured (the fan-out then
	// writes a SKIPPED(no_destination) row).
	Resolve(ctx context.Context, orgID string, entityType pb.EntityType, entityID string, usecase string, channel pb.Channel) ([]string, error)
	// ReplaceForChannel makes (entity, usecase, channel) route to exactly the
	// given ref: one channel per scope, usecase and channel.
	ReplaceForChannel(ctx context.Context, d *model.CommsChannelDestination) error
	// ListForOrg returns every destination row of the organization, each with
	// its scope (ORG or PERSON and the entity id).
	ListForOrg(ctx context.Context, orgID string) ([]*model.CommsChannelDestination, error)
}

// DestinationRepo is the GORM-backed IDestinationDAO.
type DestinationRepo struct{ db *gorm.DB }

// NewDestinationRepo constructs a DestinationRepo.
func NewDestinationRepo(db *gorm.DB) *DestinationRepo { return &DestinationRepo{db: db} }

var _ IDestinationDAO = (*DestinationRepo)(nil)

// Resolve returns the scope's configured destination_ref, one or none (the
// table holds one row per scope). The caller passes the concrete entity tier
// from the entry's destination_scope, and only that tier is read: there is no
// fallback from PERSON to ORG or back.
func (r *DestinationRepo) Resolve(ctx context.Context, orgID string, entityType pb.EntityType, entityID string, usecase string, channel pb.Channel) ([]string, error) {
	var refs []string
	err := r.db.WithContext(ctx).Model(&model.CommsChannelDestination{}).
		Where("organization_id = ? AND entity_type = ? AND entity_id = ? AND usecase = ? AND channel = ?",
			orgID, entityType.String(), entityID, usecase, channel.String()).
		Order("destination_ref").
		Pluck("destination_ref", &refs).Error
	if err != nil {
		return nil, fmt.Errorf("resolve destinations: %w", err)
	}
	return refs, nil
}

// ReplaceForChannel makes (org, entity, usecase, channel) route to exactly
// d.DestinationRef. The table holds one row per scope, so this is one atomic
// INSERT ... ON CONFLICT DO UPDATE: picking a new channel replaces the old
// rather than stacking onto it, and of two concurrent calls the one that
// commits last wins, with no window in which both refs exist.
func (r *DestinationRepo) ReplaceForChannel(ctx context.Context, d *model.CommsChannelDestination) error {
	if err := r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "organization_id"}, {Name: "entity_type"}, {Name: "entity_id"}, {Name: "usecase"}, {Name: "channel"}},
		DoUpdates: clause.AssignmentColumns([]string{"destination_ref", "destination_kind", "updated_at"}),
	}).Create(d).Error; err != nil {
		return fmt.Errorf("replace destination: %w", err)
	}
	return nil
}

// ListForOrg returns every destination row of the organization, ordered by
// scope, usecase, channel and ref.
func (r *DestinationRepo) ListForOrg(ctx context.Context, orgID string) ([]*model.CommsChannelDestination, error) {
	var out []*model.CommsChannelDestination
	if err := r.db.WithContext(ctx).Where("organization_id = ?", orgID).
		Order("entity_type, entity_id, usecase, channel, destination_ref").Find(&out).Error; err != nil {
		return nil, fmt.Errorf("list destinations: %w", err)
	}
	return out, nil
}
