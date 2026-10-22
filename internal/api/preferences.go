// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package api

import (
	"context"

	"github.com/google/uuid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
)

// GetPersonPreferences returns the preference rows of the person named by person_id.
func (s *Service) GetPersonPreferences(ctx context.Context, req *pb.GetPersonPreferencesRequest) (*pb.GetPersonPreferencesResponse, error) {
	canonicalOrg(req)
	rows, err := s.deps.PreferenceDAO.ListForEntity(ctx, req.GetOrganizationId(), pb.EntityType_ENTITY_TYPE_PERSON, req.GetPersonId())
	if err != nil {
		s.log().ErrorContext(ctx, "GetPersonPreferences: list person preferences failed", "error", err)
		return nil, status.Error(codes.Internal, "failed to read preferences")
	}
	prefs := make([]*pb.Preference, 0, len(rows))
	for _, r := range rows {
		prefs = append(prefs, r.ToProto())
	}
	return &pb.GetPersonPreferencesResponse{Preferences: prefs}, nil
}

// UpdatePersonPreferences upserts preference cells for the person named by
// person_id. The tier is always PERSON and the entity is always that person.
func (s *Service) UpdatePersonPreferences(ctx context.Context, req *pb.UpdatePersonPreferencesRequest) (*pb.UpdatePersonPreferencesResponse, error) {
	canonicalOrg(req)
	if err := validPreferences(req.GetPreferences()); err != nil {
		return nil, err
	}
	for _, p := range req.GetPreferences() {
		m := &model.CommsPreference{
			OrganizationID: req.GetOrganizationId(),
			EntityType:     pb.EntityType_ENTITY_TYPE_PERSON,
			EntityID:       req.GetPersonId(),
			Category:       p.GetCategory(),
			Channel:        p.GetChannel(),
			Enabled:        p.GetEnabled(),
		}
		if err := s.deps.PreferenceDAO.Upsert(ctx, m); err != nil {
			s.log().ErrorContext(ctx, "UpdatePersonPreferences: upsert person preference failed", "error", err)
			return nil, status.Error(codes.Internal, "failed to update preferences")
		}
	}
	return &pb.UpdatePersonPreferencesResponse{}, nil
}

// GetOrgPreferences returns the org-default (ORG tier) preference rows.
func (s *Service) GetOrgPreferences(ctx context.Context, req *pb.GetOrgPreferencesRequest) (*pb.GetOrgPreferencesResponse, error) {
	canonicalOrg(req)
	rows, err := s.deps.PreferenceDAO.ListForEntity(ctx, req.GetOrganizationId(), pb.EntityType_ENTITY_TYPE_ORG, req.GetOrganizationId())
	if err != nil {
		s.log().ErrorContext(ctx, "GetOrgPreferences: list org preferences failed", "error", err)
		return nil, status.Error(codes.Internal, "failed to read org preferences")
	}
	prefs := make([]*pb.Preference, 0, len(rows))
	for _, r := range rows {
		prefs = append(prefs, r.ToProto())
	}
	return &pb.GetOrgPreferencesResponse{Preferences: prefs}, nil
}

// SetOrgPreferences upserts the org-default preference cells (ORG tier, forced).
func (s *Service) SetOrgPreferences(ctx context.Context, req *pb.SetOrgPreferencesRequest) (*pb.SetOrgPreferencesResponse, error) {
	canonicalOrg(req)
	if err := validPreferences(req.GetPreferences()); err != nil {
		return nil, err
	}
	for _, p := range req.GetPreferences() {
		m := &model.CommsPreference{
			OrganizationID: req.GetOrganizationId(),
			EntityType:     pb.EntityType_ENTITY_TYPE_ORG,
			EntityID:       req.GetOrganizationId(),
			Category:       p.GetCategory(),
			Channel:        p.GetChannel(),
			Enabled:        p.GetEnabled(),
		}
		if err := s.deps.PreferenceDAO.Upsert(ctx, m); err != nil {
			s.log().ErrorContext(ctx, "SetOrgPreferences: upsert org preference failed", "error", err)
			return nil, status.Error(codes.Internal, "failed to set org preferences")
		}
	}
	return &pb.SetOrgPreferencesResponse{}, nil
}

// GetDestinations returns every routing destination of the organization, each
// with its scope: the organization itself (ORG) or one person in it (PERSON).
func (s *Service) GetDestinations(ctx context.Context, req *pb.GetDestinationsRequest) (*pb.GetDestinationsResponse, error) {
	canonicalOrg(req)
	rows, err := s.deps.DestinationDAO.ListForOrg(ctx, req.GetOrganizationId())
	if err != nil {
		s.log().ErrorContext(ctx, "GetDestinations: list org destinations failed", "error", err)
		return nil, status.Error(codes.Internal, "failed to read destinations")
	}
	dests := make([]*pb.Destination, 0, len(rows))
	for _, r := range rows {
		dests = append(dests, r.ToProto())
	}
	return &pb.GetDestinationsResponse{Destinations: dests}, nil
}

// SetDestination sets the routing destination of one scope, usecase and
// channel. The scope is the one given: ORG, whose entity_id must be the
// organization_id, or PERSON, whose entity_id is that person's UUID and not
// the organization_id. Any other type is INVALID_ARGUMENT, as is a
// destination_kind other than CHANNEL (or unspecified, read as CHANNEL). It
// replaces any channel routed for that scope, usecase and channel before,
// rather than adding to it: picking a new channel must not leave the old one
// receiving messages.
func (s *Service) SetDestination(ctx context.Context, req *pb.SetDestinationRequest) (*pb.SetDestinationResponse, error) {
	canonicalOrg(req)
	d := req.GetDestination()
	if d.GetChannel() != pb.Channel_CHANNEL_SLACK {
		return nil, status.Errorf(codes.InvalidArgument, "a destination's channel must be CHANNEL_SLACK, not %v", d.GetChannel())
	}
	// The destination's scope is the one given: the organization itself, or one
	// person in it.
	entityID := d.GetEntityId()
	switch d.GetEntityType() {
	case pb.EntityType_ENTITY_TYPE_ORG:
		if !isUUID(entityID) || uuid.MustParse(entityID).String() != req.GetOrganizationId() {
			return nil, status.Error(codes.InvalidArgument, "an ORG destination's entity_id must be the organization_id")
		}
		entityID = req.GetOrganizationId()
	case pb.EntityType_ENTITY_TYPE_PERSON:
		if !isUUID(entityID) {
			return nil, status.Errorf(codes.InvalidArgument, "a PERSON destination's entity_id %q is not a UUID", entityID)
		}
		entityID = uuid.MustParse(entityID).String()
		// A person is not the organization: a PERSON row on the org's id would
		// share its entity with the ORG row, and no entry could tell them apart.
		if entityID == req.GetOrganizationId() {
			return nil, status.Error(codes.InvalidArgument, "a PERSON destination's entity_id must be a person, not the organization_id")
		}
	default:
		return nil, status.Errorf(codes.InvalidArgument, "a destination's entity_type must be ENTITY_TYPE_ORG or ENTITY_TYPE_PERSON, not %v", d.GetEntityType())
	}
	// Only a channel can be routed to; unspecified means a channel.
	kind := d.GetDestinationKind()
	switch kind {
	case pb.DestinationKind_DESTINATION_KIND_UNSPECIFIED:
		kind = pb.DestinationKind_DESTINATION_KIND_CHANNEL
	case pb.DestinationKind_DESTINATION_KIND_CHANNEL:
	default:
		return nil, status.Errorf(codes.InvalidArgument, "a destination's destination_kind must be DESTINATION_KIND_CHANNEL, not %v", kind)
	}
	m := &model.CommsChannelDestination{
		OrganizationID:  req.GetOrganizationId(),
		EntityType:      d.GetEntityType(),
		EntityID:        entityID,
		Usecase:         d.GetUsecase(),
		Channel:         d.GetChannel(),
		DestinationKind: kind,
		DestinationRef:  d.GetDestinationRef(),
	}
	if err := s.deps.DestinationDAO.ReplaceForChannel(ctx, m); err != nil {
		s.log().ErrorContext(ctx, "SetDestination: replace destination failed", "error", err)
		return nil, status.Error(codes.Internal, "failed to set destination")
	}
	return &pb.SetDestinationResponse{}, nil
}

// PurgeOrgComms deletes an org's requests, messages, delivery events,
// preferences and destinations. Suppressions survive: they are a fact about an
// address, not about the org.
func (s *Service) PurgeOrgComms(ctx context.Context, req *pb.PurgeOrgCommsRequest) (*pb.PurgeOrgCommsResponse, error) {
	canonicalOrg(req)
	n, err := s.deps.ReconcileDAO.PurgeOrg(ctx, req.GetOrganizationId())
	if err != nil {
		s.log().ErrorContext(ctx, "PurgeOrgComms: purge org comms data failed", "error", err)
		return nil, status.Error(codes.Internal, "failed to purge org comms data")
	}
	return &pb.PurgeOrgCommsResponse{DeletedMessages: n}, nil
}

// validPreferences rejects a preference without a real category and channel:
// an unset or unknown value would store a cell no send ever matches.
func validPreferences(prefs []*pb.Preference) error {
	for i, p := range prefs {
		if _, ok := pb.MessageCategory_name[int32(p.GetCategory())]; !ok || p.GetCategory() == pb.MessageCategory_MESSAGE_CATEGORY_UNSPECIFIED {
			return status.Errorf(codes.InvalidArgument, "preferences[%d]: category %v is not a set, known category", i, p.GetCategory())
		}
		if _, ok := pb.Channel_name[int32(p.GetChannel())]; !ok || p.GetChannel() == pb.Channel_CHANNEL_UNSPECIFIED {
			return status.Errorf(codes.InvalidArgument, "preferences[%d]: channel %v is not a set, known channel", i, p.GetChannel())
		}
	}
	return nil
}
