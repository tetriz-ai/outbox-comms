// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
	"github.com/tetriz-ai/outbox-comms/templates"
)

// GetCommsStatus returns a request plus its per-message rollup by handle. The
// rollup is PARTIAL when any message is a non-success terminal
// (SKIPPED/DEAD/BOUNCED/SUPPRESSED/UNCONFIRMED), else DONE.
func (s *Service) GetCommsStatus(ctx context.Context, req *pb.GetCommsStatusRequest) (*pb.GetCommsStatusResponse, error) {
	canonicalOrg(req)
	r, err := s.deps.RequestDAO.GetByHandle(ctx, req.GetOrganizationId(), req.GetHandle())
	if err != nil {
		if errors.Is(err, dao.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "no comms request for that handle")
		}
		// A DB failure is not a missing row — surface Internal (and log) so the
		// caller can retry rather than treating the request as absent.
		s.log().ErrorContext(ctx, "GetCommsStatus: get request by handle failed", "org_id", req.GetOrganizationId(), "error", err)
		return nil, status.Error(codes.Internal, "failed to read comms request")
	}
	msgs, err := s.deps.MessageDAO.ListByRequest(ctx, req.GetOrganizationId(), r.ID)
	if err != nil {
		s.log().ErrorContext(ctx, "GetCommsStatus: list messages failed", "org_id", req.GetOrganizationId(), "error", err)
		return nil, status.Error(codes.Internal, "failed to read message rollup")
	}
	items := make([]*pb.MessageStatusItem, 0, len(msgs))
	// A failed fan-out wrote no message, so nothing was sent: partial, not a
	// clean empty success.
	partial := r.Status == pb.RequestStatus_REQUEST_STATUS_FANOUT_FAILED
	done := r.Status == pb.RequestStatus_REQUEST_STATUS_FANNED_OUT || r.Status == pb.RequestStatus_REQUEST_STATUS_FANOUT_FAILED
	for _, m := range msgs {
		if isNonSuccessTerminal(m.Status) {
			partial = true
		}
		if !isTerminal(m) {
			done = false
		}
		item := &pb.MessageStatusItem{
			MessageId: m.ID, Channel: m.Channel, Recipient: m.Recipient, Status: m.Status,
			UpdatedAt: timestamppb.New(m.UpdatedAt),
		}
		if m.SkipReason != nil {
			item.SkipReason = *m.SkipReason
		}
		if m.DeadReason != nil {
			item.DeadReason = *m.DeadReason
		}
		items = append(items, item)
	}
	return &pb.GetCommsStatusResponse{
		Handle: r.IdempotencyKey, RequestStatus: r.Status, IsPartial: partial, Done: done, Messages: items,
	}, nil
}

// CheckSendEligibility is the advisory, read-only preflight: per channel,
// would a send proceed now? NOT authoritative — the dispatcher re-checks (TOCTOU).
func (s *Service) CheckSendEligibility(ctx context.Context, req *pb.CheckSendEligibilityRequest) (*pb.CheckSendEligibilityResponse, error) {
	canonicalOrg(req)
	if req.GetPersonId() != "" {
		if err := requireUUID("person_id", req.GetPersonId()); err != nil {
			return nil, err
		}
	}
	// target_kind is optional, but a set one must be a known kind that the
	// requested channels can carry, by the same rule SubmitComms applies to an
	// entry.
	if tk := req.GetTargetKind(); tk != pb.TargetKind_TARGET_KIND_UNSPECIFIED {
		if _, ok := pb.TargetKind_name[int32(tk)]; !ok {
			return nil, status.Errorf(codes.InvalidArgument, "unknown target_kind %d", int32(tk))
		}
		for _, ch := range req.GetChannels() {
			if !validChannelTarget(ch, tk) {
				return nil, status.Errorf(codes.InvalidArgument, "invalid (channel=%s, target_kind=%s) pair", ch, tk)
			}
		}
	}
	contract, err := templates.Get(req.GetUsecase())
	if err != nil {
		s.log().WarnContext(ctx, "CheckSendEligibility: unknown usecase", "usecase", req.GetUsecase(), "error", err)
		return nil, status.Errorf(codes.InvalidArgument, "unknown usecase: %v", err)
	}
	active, frozen := true, false
	if s.deps.OrgGate != nil {
		active, frozen, err = s.deps.OrgGate.CheckOrg(ctx, req.GetOrganizationId())
		if err != nil {
			s.log().ErrorContext(ctx, "CheckSendEligibility: org gate check failed", "org_id", req.GetOrganizationId(), "error", err)
			return nil, status.Error(codes.Internal, "org gate check failed")
		}
	}
	transactional := contract.Category == pb.MessageCategory_MESSAGE_CATEGORY_TRANSACTIONAL

	// The same per-org gate SubmitComms fails closed on. Reflecting it here lets a
	// producer skip expensive work SubmitComms would reject at the end. A nil
	// Gate makes every channel ineligible, matching SubmitComms.
	commsEnabled := s.deps.Gate != nil && s.deps.Gate.Enabled(req.GetOrganizationId())

	out := &pb.CheckSendEligibilityResponse{}
	for _, ch := range req.GetChannels() {
		ce := &pb.ChannelEligibility{Channel: ch, Eligible: true}
		if !commsEnabled {
			ce.Eligible, ce.Reasons = false, append(ce.Reasons, "comms_disabled")
		}
		if !active {
			ce.Eligible, ce.Reasons = false, append(ce.Reasons, "org_inactive")
		}
		if frozen {
			ce.Eligible, ce.Reasons = false, append(ce.Reasons, "org_frozen")
		}
		if !contract.ChannelEnabled(ch) {
			ce.Eligible, ce.Reasons = false, append(ce.Reasons, "channel_not_enabled")
		}
		// Preference gate (non-transactional only; transactional bypasses).
		if !transactional && s.deps.PreferenceDAO != nil {
			enabled, perr := s.deps.PreferenceDAO.IsEnabled(ctx, req.GetOrganizationId(), req.GetPersonId(), contract.Category, ch)
			if perr != nil {
				// Fail closed: a channel the check could not clear is not eligible.
				s.log().WarnContext(ctx, "CheckSendEligibility: preference gate errored", "org_id", req.GetOrganizationId(), "error", perr)
				ce.Eligible, ce.Reasons = false, append(ce.Reasons, "preference_lookup_failed")
			} else if !enabled {
				ce.Eligible, ce.Reasons = false, append(ce.Reasons, "opted_out")
			}
		}
		// Slack needs a live connection and, for a send to a configured
		// destination, that destination: the person's when person_id is set,
		// else the organization's, the scope the fan-out would resolve, with no
		// fallback between them. A send to a channel the caller names needs no
		// configured route. A lookup that fails leaves the channel ineligible,
		// like the preference gate.
		if ch == pb.Channel_CHANNEL_SLACK {
			if s.deps.ConnChecker != nil {
				connected, cerr := s.deps.ConnChecker.IsConnected(ctx, req.GetOrganizationId(), ch)
				if cerr != nil {
					s.log().WarnContext(ctx, "CheckSendEligibility: connection check errored", "org_id", req.GetOrganizationId(), "error", cerr)
					ce.Eligible, ce.Reasons = false, append(ce.Reasons, "connection_lookup_failed")
				} else if !connected {
					ce.Eligible, ce.Reasons = false, append(ce.Reasons, "no_connection")
				}
			}
			if s.deps.DestinationDAO != nil && req.GetTargetKind() == pb.TargetKind_TARGET_KIND_CONFIGURED_DESTINATION {
				scope, entity := pb.EntityType_ENTITY_TYPE_ORG, req.GetOrganizationId()
				if req.GetPersonId() != "" {
					scope, entity = pb.EntityType_ENTITY_TYPE_PERSON, req.GetPersonId()
				}
				refs, derr := s.deps.DestinationDAO.Resolve(ctx, req.GetOrganizationId(), scope, entity, req.GetUsecase(), ch)
				if derr != nil {
					s.log().WarnContext(ctx, "CheckSendEligibility: destination resolve errored", "org_id", req.GetOrganizationId(), "error", derr)
					ce.Eligible, ce.Reasons = false, append(ce.Reasons, "destination_lookup_failed")
				} else if len(refs) == 0 {
					ce.Eligible, ce.Reasons = false, append(ce.Reasons, "no_destination")
				}
			}
		}
		out.ChannelEligibility = append(out.ChannelEligibility, ce)
	}
	return out, nil
}

// historyPageDefault / historyPageMax bound a ListMessages page: an unset or
// over-large page_size is clamped so a client can never force an unbounded scan.
const (
	historyPageDefault = 50
	historyPageMax     = 200
)

// ListMessages returns the org's message history newest-first, keyset-paginated
// on (created_at, id). The first page omits the cursor; the response hands back
// the cursor for the next page and has_more. The DAO fetches
// limit+1 rows so has_more is known without a count query.
func (s *Service) ListMessages(ctx context.Context, req *pb.ListMessagesRequest) (*pb.ListMessagesResponse, error) {
	canonicalOrg(req)
	if err := requireUUID("organization_id", req.GetOrganizationId()); err != nil {
		return nil, err
	}
	if (req.GetBeforeCreatedAt() == nil) != (req.GetBeforeId() == "") {
		return nil, status.Error(codes.InvalidArgument, "the cursor needs both before_created_at and before_id, or neither")
	}
	if req.GetBeforeId() != "" {
		if err := requireUUID("before_id", req.GetBeforeId()); err != nil {
			return nil, err
		}
	}
	limit := int(req.GetPageSize())
	if limit <= 0 {
		limit = historyPageDefault
	}
	if limit > historyPageMax {
		limit = historyPageMax
	}
	var beforeCreatedAt *time.Time
	if req.GetBeforeCreatedAt() != nil {
		t := req.GetBeforeCreatedAt().AsTime()
		beforeCreatedAt = &t
	}
	// Over-fetch one row to detect a further page without a separate count.
	msgs, err := s.deps.MessageDAO.ListByOrg(ctx, req.GetOrganizationId(), beforeCreatedAt, req.GetBeforeId(), limit+1)
	if err != nil {
		s.log().ErrorContext(ctx, "ListMessages: list by org failed", "org_id", req.GetOrganizationId(), "error", err)
		return nil, status.Error(codes.Internal, "failed to list messages")
	}
	hasMore := len(msgs) > limit
	if hasMore {
		msgs = msgs[:limit]
	}
	// Batch-resolve usecase per distinct request id in this page — one query for
	// the whole page, not one per message.
	requestIDs := make([]string, 0, len(msgs))
	seen := make(map[string]struct{}, len(msgs))
	for _, m := range msgs {
		if _, ok := seen[m.RequestID]; !ok {
			seen[m.RequestID] = struct{}{}
			requestIDs = append(requestIDs, m.RequestID)
		}
	}
	// Usecase is display metadata: a lookup failure degrades it to empty instead
	// of failing the page.
	usecaseByRequestID, err := s.deps.RequestDAO.GetUsecasesByIDs(ctx, req.GetOrganizationId(), requestIDs)
	if err != nil {
		s.log().ErrorContext(ctx, "ListMessages: resolve usecases failed", "org_id", req.GetOrganizationId(), "error", err)
		usecaseByRequestID = map[string]string{}
	}
	items := make([]*pb.MessageHistoryItem, 0, len(msgs))
	for _, m := range msgs {
		item := &pb.MessageHistoryItem{
			MessageId: m.ID, RequestId: m.RequestID, Channel: m.Channel, Recipient: m.Recipient,
			Status: m.Status, CreatedAt: timestamppb.New(m.CreatedAt), UpdatedAt: timestamppb.New(m.UpdatedAt),
			Usecase: usecaseByRequestID[m.RequestID],
		}
		if m.SkipReason != nil {
			item.SkipReason = *m.SkipReason
		}
		if m.DeadReason != nil {
			item.DeadReason = *m.DeadReason
		}
		items = append(items, item)
	}
	out := &pb.ListMessagesResponse{Messages: items, HasMore: hasMore}
	if hasMore && len(msgs) > 0 {
		last := msgs[len(msgs)-1]
		out.NextBeforeCreatedAt = timestamppb.New(last.CreatedAt)
		out.NextBeforeId = last.ID
	}
	return out, nil
}

// GetMessageEvents returns one message's delivery-event timeline oldest-first.
// The message_id is validated against the org by ListByMessage's org-scoped read,
// so a foreign id simply yields an empty timeline (no cross-org leak).
func (s *Service) GetMessageEvents(ctx context.Context, req *pb.GetMessageEventsRequest) (*pb.GetMessageEventsResponse, error) {
	canonicalOrg(req)
	if err := requireUUID("organization_id", req.GetOrganizationId()); err != nil {
		return nil, err
	}
	if err := requireUUID("message_id", req.GetMessageId()); err != nil {
		return nil, err
	}
	events, err := s.deps.EventDAO.ListByMessage(ctx, req.GetOrganizationId(), req.GetMessageId())
	if err != nil {
		s.log().ErrorContext(ctx, "GetMessageEvents: list by message failed", "org_id", req.GetOrganizationId(), "error", err)
		return nil, status.Error(codes.Internal, "failed to read message events")
	}
	items := make([]*pb.DeliveryEventItem, 0, len(events))
	for _, e := range events {
		item := &pb.DeliveryEventItem{
			EventType: e.EventType, OccurredAt: timestamppb.New(e.OccurredAt),
		}
		if e.ProviderEventID != nil {
			item.ProviderEventId = *e.ProviderEventID
		}
		items = append(items, item)
	}
	return &pb.GetMessageEventsResponse{Events: items}, nil
}

// isTerminal reports whether a message has reached a terminal state of the
// message lifecycle: no send or retry is left for it. SENT is terminal only
// when its usecase does not wait for a delivery event.
func isTerminal(m *model.CommsMessage) bool {
	switch m.Status {
	case pb.MessageStatus_MESSAGE_STATUS_SENT:
		return m.VerificationLevel != pb.VerificationLevel_VERIFICATION_LEVEL_CONFIRMED
	case pb.MessageStatus_MESSAGE_STATUS_DELIVERED:
		return true
	default:
		return isNonSuccessTerminal(m.Status)
	}
}

// isNonSuccessTerminal reports whether a message ended in a non-delivered state
// that makes the request rollup PARTIAL.
func isNonSuccessTerminal(s pb.MessageStatus) bool {
	switch s {
	case pb.MessageStatus_MESSAGE_STATUS_SKIPPED,
		pb.MessageStatus_MESSAGE_STATUS_DEAD,
		pb.MessageStatus_MESSAGE_STATUS_BOUNCED,
		pb.MessageStatus_MESSAGE_STATUS_SUPPRESSED,
		pb.MessageStatus_MESSAGE_STATUS_UNCONFIRMED:
		return true
	default:
		return false
	}
}

// requireUUID returns INVALID_ARGUMENT unless v is a UUID in the 36-character
// hyphenated form, the form the proto rules accept. The id columns are
// uuid-typed, so Postgres would reject anything else as an internal error.
func requireUUID(field, v string) error {
	if !isUUID(v) {
		return status.Errorf(codes.InvalidArgument, "%s %q is not a UUID", field, v)
	}
	return nil
}

// isUUID reports whether v is a UUID in the 36-character hyphenated form, in
// either case: the form the proto rules accept. uuid.Parse alone also takes
// braced, URN and unhyphenated spellings.
func isUUID(v string) bool {
	_, err := uuid.Parse(v)
	return err == nil && len(v) == 36
}

// canonicalOrg rewrites req's organization_id to the lowercase hyphenated form
// when it is a UUID in the 36-character form. Every RPC calls it first, so the
// gates, the stores and the token lookup all see one spelling of an org. Any
// other value is left as is, for validation to reject.
func canonicalOrg(req proto.Message) {
	m := req.ProtoReflect()
	f := m.Descriptor().Fields().ByName("organization_id")
	if f == nil {
		return
	}
	if v := m.Get(f).String(); isUUID(v) {
		id, _ := uuid.Parse(v)
		m.Set(f, protoreflect.ValueOfString(id.String()))
	}
}
