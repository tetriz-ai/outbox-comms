// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package dao

import (
	"context"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
)

// DeliveryEventRepo is the GORM-backed IDeliveryEventDAO (append-only).
type DeliveryEventRepo struct{ db *gorm.DB }

// NewDeliveryEventRepo constructs a DeliveryEventRepo.
func NewDeliveryEventRepo(db *gorm.DB) *DeliveryEventRepo { return &DeliveryEventRepo{db: db} }

var _ IDeliveryEventDAO = (*DeliveryEventRepo)(nil)

// Append inserts one delivery event. When provider_event_id is set the insert is
// idempotent on it (ON CONFLICT DO NOTHING) so a feedback redelivery never
// double-counts; internal events (nil provider_event_id) always insert.
func (r *DeliveryEventRepo) Append(ctx context.Context, e *model.CommsDeliveryEvent) error {
	db := r.db.WithContext(ctx)
	q := db
	if e.ProviderEventID != nil && *e.ProviderEventID != "" {
		q = q.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "provider_event_id"}},
			// Match the PARTIAL unique index (WHERE provider_event_id IS NOT NULL),
			// else Postgres can't infer the arbiter (SQLSTATE 42P10).
			TargetWhere: clause.Where{Exprs: []clause.Expression{
				clause.Expr{SQL: "provider_event_id IS NOT NULL"},
			}},
			DoNothing: true,
		})
	}
	if err := q.Create(e).Error; err != nil {
		return fmt.Errorf("append delivery event: %w", err)
	}
	return nil
}

// ListByMessage returns the message's delivery events oldest-first, scoped to
// the org.
func (r *DeliveryEventRepo) ListByMessage(ctx context.Context, orgID, messageID string) ([]*model.CommsDeliveryEvent, error) {
	var events []*model.CommsDeliveryEvent
	err := r.db.WithContext(ctx).
		Where("organization_id = ? AND message_id = ?", orgID, messageID).
		Order("occurred_at ASC, id ASC").
		Find(&events).Error
	if err != nil {
		return nil, fmt.Errorf("list delivery events for message %s: %w", messageID, err)
	}
	return events, nil
}
