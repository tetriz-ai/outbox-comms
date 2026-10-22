// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package api

import (
	"fmt"
	"net/mail"
	"strings"

	"github.com/google/uuid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/templates"
)

// maxEntries is the one-pass fan-out cap: a larger send is the producer's to
// chunk. Mirrors the buf.validate max_items on SubmitCommsRequest.entries.
const maxEntries = 1000

// maxRecipientBytes caps a recipient so the (request_id, channel, recipient)
// unique index entry stays under Postgres's 2704-byte btree limit, with room.
// An email address is at most 254 bytes and a Slack channel id about a dozen.
// maxIdempotencyKeyBytes caps the handle, which is indexed the same way.
// Both mirror the buf.validate max_bytes rules.
const (
	maxRecipientBytes      = 1024
	maxIdempotencyKeyBytes = 255
)

// validateBatch runs the submit-time checks the buf.validate layer
// cannot express (cross-field + intra-batch dedup), returning an INVALID_ARGUMENT
// status error on the first violation. usecase + params are validated separately.
func validateBatch(usecase string, params *pb.SubmitCommsRequest) error {
	if n := len(params.GetIdempotencyKey()); n > maxIdempotencyKeyBytes {
		return status.Errorf(codes.InvalidArgument, "idempotency_key is %d bytes, over the %d cap", n, maxIdempotencyKeyBytes)
	}
	if hasNUL(params.ProtoReflect()) {
		return status.Errorf(codes.InvalidArgument, "%s contains a NUL byte", nulString(params.ProtoReflect(), ""))
	}
	entries := params.GetEntries()
	if len(entries) == 0 {
		return status.Error(codes.InvalidArgument, "entries must not be empty")
	}
	if len(entries) > maxEntries {
		return status.Errorf(codes.InvalidArgument, "entries exceeds the %d cap (chunk the send)", maxEntries)
	}
	c, err := templates.Get(usecase)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "unknown usecase: %v", err)
	}
	if err := templates.ValidateParams(usecase, params.GetParams()); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid params: %v", err)
	}

	seen := make(map[string]struct{}, len(entries))
	for i, e := range entries {
		if err := validateEntry(c, params.GetOrganizationId(), e); err != nil {
			return status.Errorf(codes.InvalidArgument, "entry[%d]: %v", i, err)
		}
		key, err := entryDedupKey(e)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "entry[%d]: %v", i, err)
		}
		if _, dup := seen[key]; dup {
			return status.Errorf(codes.InvalidArgument, "entry[%d]: duplicate dedup key within the batch (%s)", i, key)
		}
		seen[key] = struct{}{}
	}
	return nil
}

// validateEntry checks one entry's cross-field rules: a defined channel and
// target_kind, a valid (channel, target_kind) pair (EMAIL_ADDRESS, SLACK_CHANNEL
// and CONFIGURED_DESTINATION), a channel the usecase
// enables, slack_delivery only on SLACK entries, and the CONFIGURED_DESTINATION
// recipient/destination_scope exclusivity, and a destination_scope that can
// resolve: ORG naming the request's organization, or PERSON naming someone other
// than the organization. org is the request's canonical organization_id.
func validateEntry(c templates.Contract, org string, e *pb.CommsEntry) error {
	if e.GetChannel() == pb.Channel_CHANNEL_UNSPECIFIED {
		return fmt.Errorf("channel is unspecified")
	}
	if e.GetTargetKind() == pb.TargetKind_TARGET_KIND_UNSPECIFIED {
		return fmt.Errorf("target_kind is unspecified")
	}
	if !c.ChannelEnabled(e.GetChannel()) {
		return fmt.Errorf("channel %s is not enabled for this usecase", e.GetChannel())
	}
	if id := e.GetSubjectPersonId(); id != "" {
		if !isUUID(id) {
			return fmt.Errorf("subject_person_id %q is not a UUID", id)
		}
	}
	if e.GetChannel() != pb.Channel_CHANNEL_SLACK && e.GetSlackDelivery() != pb.SlackDelivery_SLACK_DELIVERY_UNSPECIFIED {
		return fmt.Errorf("slack_delivery must be unset on a non-SLACK entry")
	}
	if !validChannelTarget(e.GetChannel(), e.GetTargetKind()) {
		return fmt.Errorf("invalid (channel=%s, target_kind=%s) pair", e.GetChannel(), e.GetTargetKind())
	}
	if e.GetTargetKind() == pb.TargetKind_TARGET_KIND_CONFIGURED_DESTINATION {
		if e.GetRecipient() != "" {
			return fmt.Errorf("CONFIGURED_DESTINATION entry must carry an empty recipient (server-resolved)")
		}
		if e.GetDestinationScope() == nil || e.GetDestinationScope().GetEntityId() == "" {
			return fmt.Errorf("CONFIGURED_DESTINATION entry requires a destination_scope")
		}
		ds := e.GetDestinationScope()
		if !isUUID(ds.GetEntityId()) {
			return fmt.Errorf("destination_scope.entity_id %q is not a UUID", ds.GetEntityId())
		}
		// A scope that names no row SetDestination can write would skip every
		// time: reject it here, where the producer can see why.
		sameAsOrg := uuid.MustParse(ds.GetEntityId()).String() == org
		switch ds.GetEntityType() {
		case pb.EntityType_ENTITY_TYPE_ORG:
			if !sameAsOrg {
				return fmt.Errorf("an ORG destination_scope's entity_id must be the organization_id")
			}
		case pb.EntityType_ENTITY_TYPE_PERSON:
			if sameAsOrg {
				return fmt.Errorf("a PERSON destination_scope's entity_id must be a person, not the organization_id")
			}
		default:
			return fmt.Errorf("destination_scope.entity_type must be ENTITY_TYPE_ORG or ENTITY_TYPE_PERSON, not %v", ds.GetEntityType())
		}
		if e.GetSubjectPersonId() != "" {
			return fmt.Errorf("subject_person_id is only valid on a concrete entry (a CONFIGURED_DESTINATION entry is gated on its destination_scope)")
		}
	} else {
		if e.GetDestinationScope() != nil {
			return fmt.Errorf("destination_scope is only valid on a CONFIGURED_DESTINATION entry")
		}
		if e.GetRecipient() == "" {
			return fmt.Errorf("recipient must not be empty")
		}
		if n := len(e.GetRecipient()); n > maxRecipientBytes {
			return fmt.Errorf("recipient is %d bytes, over the %d cap", n, maxRecipientBytes)
		}
		if e.GetChannel() == pb.Channel_CHANNEL_EMAIL {
			if a, err := mail.ParseAddress(e.GetRecipient()); err != nil || a.Address != e.GetRecipient() {
				return fmt.Errorf("recipient is not a bare email address")
			}
		}
	}
	return nil
}

// validChannelTarget is the (channel, target_kind) allowlist.
func validChannelTarget(ch pb.Channel, tk pb.TargetKind) bool {
	switch ch {
	case pb.Channel_CHANNEL_EMAIL:
		return tk == pb.TargetKind_TARGET_KIND_EMAIL_ADDRESS
	case pb.Channel_CHANNEL_SLACK:
		return tk == pb.TargetKind_TARGET_KIND_SLACK_CHANNEL ||
			tk == pb.TargetKind_TARGET_KIND_CONFIGURED_DESTINATION
	default:
		return false
	}
}

// entryDedupKey is (channel, recipient) for a concrete entry, with an email
// address compared as the suppression list compares it (dao.NormalizeEmail), and
// (channel, entity_type, entity_id) for a CONFIGURED_DESTINATION entry — the
// post-resolution rows are also deduped by uq_comms_messages_dedup at fan-out,
// but catching a collision at submit avoids a silently collapsed entry.
func entryDedupKey(e *pb.CommsEntry) (string, error) {
	if e.GetTargetKind() == pb.TargetKind_TARGET_KIND_CONFIGURED_DESTINATION {
		ds := e.GetDestinationScope()
		return fmt.Sprintf("%s|dest:%s:%s", e.GetChannel(), ds.GetEntityType(), ds.GetEntityId()), nil
	}
	recipient := e.GetRecipient()
	if e.GetChannel() == pb.Channel_CHANNEL_EMAIL {
		recipient = dao.NormalizeEmail(recipient)
	}
	return fmt.Sprintf("%s|%s", e.GetChannel(), recipient), nil
}

// hasNUL reports whether any string under m contains a NUL byte. It builds no
// paths, so the common case costs one walk; nulString names the field after a hit.
func hasNUL(m protoreflect.Message) bool {
	found := false
	value := func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch fd.Kind() {
		case protoreflect.StringKind:
			return strings.IndexByte(v.String(), 0) >= 0
		case protoreflect.MessageKind, protoreflect.GroupKind:
			return hasNUL(v.Message())
		}
		return false
	}
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsList():
			l := v.List()
			for i := 0; i < l.Len() && !found; i++ {
				found = value(fd, l.Get(i))
			}
		case fd.IsMap():
			v.Map().Range(func(k protoreflect.MapKey, mv protoreflect.Value) bool {
				found = strings.IndexByte(k.String(), 0) >= 0 || value(fd.MapValue(), mv)
				return !found
			})
		default:
			found = value(fd, v)
		}
		return !found
	})
	return found
}

// nulString returns the path of the first string field, list element, map key
// or map value under m that contains a NUL byte, or "". Postgres text and jsonb
// reject NUL, so such a request could never be stored.
func nulString(m protoreflect.Message, prefix string) string {
	found := ""
	str := func(fd protoreflect.FieldDescriptor, v protoreflect.Value, path string) string {
		switch {
		case fd.Kind() == protoreflect.StringKind && strings.ContainsRune(v.String(), 0):
			return path
		case fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind:
			return nulString(v.Message(), path+".")
		}
		return ""
	}
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		path := prefix + string(fd.Name())
		switch {
		case fd.IsList():
			l := v.List()
			for i := 0; i < l.Len() && found == ""; i++ {
				found = str(fd, l.Get(i), fmt.Sprintf("%s[%d]", path, i))
			}
		case fd.IsMap():
			v.Map().Range(func(k protoreflect.MapKey, mv protoreflect.Value) bool {
				if strings.ContainsRune(k.String(), 0) {
					found = path
				} else {
					found = str(fd.MapValue(), mv, path+"["+k.String()+"]")
				}
				return found == ""
			})
		default:
			found = str(fd, v, path)
		}
		return found == ""
	})
	return found
}
