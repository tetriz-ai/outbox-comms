// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package dao

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
)

// RequestRepo is the GORM-backed IRequestDAO.
type RequestRepo struct {
	db *gorm.DB
}

// NewRequestRepo constructs a RequestRepo.
func NewRequestRepo(db *gorm.DB) *RequestRepo { return &RequestRepo{db: db} }

var _ IRequestDAO = (*RequestRepo)(nil)

// CreateIfAbsent inserts the request idempotently on (org, idempotency_key): a
// re-submit returns the existing row and created=false.
func (r *RequestRepo) CreateIfAbsent(ctx context.Context, req *model.CommsRequest) (bool, *model.CommsRequest, error) {
	db := r.db.WithContext(ctx)
	res := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "organization_id"}, {Name: "idempotency_key"}},
		DoNothing: true,
	}).Create(req)
	if res.Error != nil {
		return false, nil, fmt.Errorf("insert comms_request: %w", res.Error)
	}
	if res.RowsAffected == 1 {
		return true, req, nil
	}
	// Conflict: the row already existed — return the canonical one by its key.
	existing, err := r.getByKey(ctx, req.OrganizationID, req.IdempotencyKey)
	if err != nil {
		return false, nil, fmt.Errorf("load conflicting comms_request: %w", err)
	}
	return false, existing, nil
}

// GetByHandle returns the request by (org, handle). The handle is the producer's
// idempotency_key.
func (r *RequestRepo) GetByHandle(ctx context.Context, orgID, handle string) (*model.CommsRequest, error) {
	out, err := r.getByKey(ctx, orgID, handle)
	if err != nil {
		return nil, fmt.Errorf("get comms_request by handle: %w", err)
	}
	return out, nil
}

// GetByID returns the request by (org, id) — the UUID the fan-out trigger carries.
func (r *RequestRepo) GetByID(ctx context.Context, orgID, id string) (*model.CommsRequest, error) {
	db := r.db.WithContext(ctx)
	var out model.CommsRequest
	if err := db.Where("organization_id = ? AND id = ?", orgID, id).First(&out).Error; err != nil {
		return nil, fmt.Errorf("get comms_request by id: %w", err)
	}
	return &out, nil
}

// GetUsecasesByIDs batch-resolves usecase for a page of request ids (the
// usecase ListMessages shows for each message). Returns an empty map without
// querying when ids is empty; a missing/foreign id is simply absent from the
// returned map.
func (r *RequestRepo) GetUsecasesByIDs(ctx context.Context, orgID string, ids []string) (map[string]string, error) {
	out := make(map[string]string, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	var rows []struct {
		ID      string
		Usecase string
	}
	db := r.db.WithContext(ctx)
	if err := db.Model(&model.CommsRequest{}).
		Select("id, usecase").
		Where("organization_id = ? AND id IN ?", orgID, ids).
		Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("get comms_request usecases by ids: %w", err)
	}
	for _, row := range rows {
		out[row.ID] = row.Usecase
	}
	return out, nil
}

func (r *RequestRepo) getByKey(ctx context.Context, orgID, key string) (*model.CommsRequest, error) {
	db := r.db.WithContext(ctx)
	var out model.CommsRequest
	if err := db.Where("organization_id = ? AND idempotency_key = ?", orgID, key).First(&out).Error; err != nil {
		return nil, fmt.Errorf("get comms_request by key: %w", err)
	}
	return &out, nil
}

// UpdateStatus advances the request's fan-out lifecycle.
func (r *RequestRepo) UpdateStatus(ctx context.Context, orgID, id string, status pb.RequestStatus) error {
	db := r.db.WithContext(ctx)
	res := db.Model(&model.CommsRequest{}).
		Where("organization_id = ? AND id = ?", orgID, id).
		Updates(map[string]any{"status": status.String(), "updated_at": time.Now()})
	if res.Error != nil {
		return fmt.Errorf("update comms_request status: %w", res.Error)
	}
	// A zero-row update is a silent failure, not a success: (org, id) matched
	// nothing (wrong org or a deleted/absent row), so the lifecycle move the
	// fan-out believes happened did not — surface it so the request is re-driven
	// rather than treated as advanced (mirrors MessageRepo.transition).
	if res.RowsAffected == 0 {
		return fmt.Errorf("update comms_request status: no row for (org=%s, id=%s)", orgID, id)
	}
	return nil
}
