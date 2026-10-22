// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package api implements the CommsService gRPC handlers. SubmitComms stores a
// request before it enqueues anything. The other handlers read status and
// history, check eligibility, manage preferences and destinations, and purge
// an organization.
package api

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"log/slog"

	"github.com/tetriz-ai/outbox-comms/gate"
	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
	"github.com/tetriz-ai/outbox-comms/templates"
)

// ConnectionChecker reports whether the org has a live channel connection. Slack
// eligibility needs it.
type ConnectionChecker interface {
	IsConnected(ctx context.Context, orgID string, channel pb.Channel) (bool, error)
}

// Deps are the front-door dependencies. SubmitComms uses RequestDAO, Publisher and
// Gate; the read RPCs use the rest. Unset deps are tolerated for the RPCs that do
// not need them.
type Deps struct {
	// Logger receives the service's log lines. Nil means slog's default.
	Logger         *slog.Logger
	RequestDAO     dao.IRequestDAO
	MessageDAO     dao.IMessageDAO
	EventDAO       dao.IDeliveryEventDAO
	PreferenceDAO  dao.IPreferenceDAO
	DestinationDAO dao.IDestinationDAO
	SuppressionDAO dao.ISuppressionDAO
	OrgGate        gate.OrgGate
	ConnChecker    ConnectionChecker
	Publisher      RequestPublisher
	// ReconcileDAO backs PurgeOrgComms.
	ReconcileDAO dao.IReconcileDAO
	// Gate is the per-org switch every submit passes. SubmitComms fails closed on
	// it: a nil Gate rejects, so a wiring mistake disables sending rather than
	// silently un-gating it.
	Gate gate.CommsGate
}

// Service is the CommsService gRPC server.
type Service struct {
	pb.UnimplementedCommsServiceServer
	deps Deps
}

// NewService constructs the front-door service.
func NewService(deps Deps) *Service {
	return &Service{deps: deps}
}

var _ pb.CommsServiceServer = (*Service)(nil)

// SubmitComms is the front door: it validates the usecase, params and the whole
// entries batch, persists a comms_requests row idempotently on
// (org, idempotency_key), enqueues the request id, and returns the handle.
// Persist-first: the row is durable the instant the RPC returns, so a lost
// enqueue is re-driven from the DB by reconcile.
func (s *Service) SubmitComms(ctx context.Context, req *pb.SubmitCommsRequest) (*pb.SubmitCommsResponse, error) {
	canonicalOrg(req)
	if err := validateBatch(req.GetUsecase(), req); err != nil {
		s.log().WarnContext(ctx, "SubmitComms rejected", "idempotency_key", req.GetIdempotencyKey(), "error", err)
		return nil, err //nolint:wrapcheck // gRPC status pass-through from validateBatch; %w would corrupt status.Code()
	}

	// Whole-feature gate: the single chokepoint every producer passes, so one
	// per-org switch turns sending on or off. Fails closed: a nil Gate rejects.
	if s.deps.Gate == nil || !s.deps.Gate.Enabled(req.GetOrganizationId()) {
		s.log().WarnContext(ctx, "SubmitComms rejected: comms disabled for org",
			"org_id", req.GetOrganizationId(), "idempotency_key", req.GetIdempotencyKey())
		return nil, status.Error(codes.FailedPrecondition, "comms is not enabled for this organization")
	}

	contract, err := templates.Get(req.GetUsecase())
	if err != nil {
		// validateBatch already proved the usecase is registered; reaching here
		// means the validator and the registry disagree: a code bug.
		s.log().ErrorContext(ctx, "SubmitComms: usecase passed validation but missing from template registry",
			"usecase", req.GetUsecase(), "error", err)
		return nil, status.Errorf(codes.InvalidArgument, "unknown usecase: %v", err)
	}
	guarantee := req.GetDeliveryGuarantee()
	if guarantee == pb.DeliveryGuarantee_DELIVERY_GUARANTEE_UNSPECIFIED {
		guarantee = contract.DefaultGuarantee
	}

	row := &model.CommsRequest{
		OrganizationID:    req.GetOrganizationId(),
		IdempotencyKey:    req.GetIdempotencyKey(),
		Usecase:           req.GetUsecase(),
		Params:            req.GetParams(),
		Content:           req.GetContent(),
		Entries:           &pb.CommsEntries{Entries: req.GetEntries()},
		DeliveryGuarantee: guarantee,
		Status:            pb.RequestStatus_REQUEST_STATUS_ACCEPTED,
	}
	if aid := req.GetAnalyticsId(); aid != "" {
		row.AnalyticsID = &aid
	}

	created, persisted, err := s.deps.RequestDAO.CreateIfAbsent(ctx, row)
	if err != nil {
		s.log().ErrorContext(ctx, "SubmitComms persist failed", "idempotency_key", req.GetIdempotencyKey(), "error", err)
		return nil, status.Error(codes.Internal, "failed to persist comms request")
	}
	// A reused key must name the same request. A different body under it would
	// otherwise be dropped in silence while the caller believes it was sent.
	if !created && !sameRequest(persisted, row) {
		s.log().WarnContext(ctx, "SubmitComms rejected: idempotency key reused with a different request",
			"org_id", req.GetOrganizationId(), "idempotency_key", req.GetIdempotencyKey())
		return nil, status.Error(codes.AlreadyExists,
			"this idempotency key was already used for a different request (usecase, params, content, entries or delivery guarantee); use a new key for new content")
	}

	// Enqueue only on first create: a re-submit returns the same handle with no
	// re-processing. A publish failure is non-fatal: the row is durable (ACCEPTED)
	// and the reconcile stuck-request arm re-drives the fan-out.
	if created {
		if err := s.deps.Publisher.PublishRequest(ctx, persisted.OrganizationID, persisted.ID); err != nil {
			s.log().ErrorContext(ctx, "SubmitComms enqueue failed; row is durable, reconcile will re-drive",
				"request_id", persisted.ID, "error", err)
		}
	}

	// handle == idempotency_key: stable across resubmits and known to the producer.
	return &pb.SubmitCommsResponse{Handle: persisted.IdempotencyKey}, nil
}

// sameRequest reports whether a stored request and a new submission are the
// same request: equal usecase, params, content, entries and effective delivery
// guarantee. Absent params, content or entries equal empty ones, as storage
// does not keep the difference.
func sameRequest(stored, submitted *model.CommsRequest) bool {
	return stored.Usecase == submitted.Usecase &&
		stored.DeliveryGuarantee == submitted.DeliveryGuarantee &&
		proto.Equal(orEmptyStruct(stored.Params), orEmptyStruct(submitted.Params)) &&
		proto.Equal(orEmptyContent(stored.Content), orEmptyContent(submitted.Content)) &&
		proto.Equal(orEmptyEntries(stored.Entries), orEmptyEntries(submitted.Entries))
}

func orEmptyStruct(s *structpb.Struct) *structpb.Struct {
	if s == nil {
		return &structpb.Struct{}
	}
	return s
}

func orEmptyContent(c *pb.MessageContent) *pb.MessageContent {
	if c == nil {
		return &pb.MessageContent{}
	}
	return c
}

func orEmptyEntries(e *pb.CommsEntries) *pb.CommsEntries {
	if e == nil {
		return &pb.CommsEntries{}
	}
	return e
}

// orDefault is l, or slog's default logger when l is nil.
func orDefault(l *slog.Logger) *slog.Logger {
	if l != nil {
		return l
	}
	return slog.Default()
}

func (s *Service) log() *slog.Logger { return orDefault(s.deps.Logger) }
