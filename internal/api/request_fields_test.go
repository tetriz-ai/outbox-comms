// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package api_test

// Tests of the request and response fields fix batch 11 added: target_kind on
// CheckSendEligibility and done on GetCommsStatus, and of the destination scope
// the eligibility check resolves.

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"buf.build/go/protovalidate"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/api"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
	"github.com/tetriz-ai/outbox-comms/templates"
)

// TestEligibilityByTargetKind pins which target kinds check for a configured
// destination: only CONFIGURED_DESTINATION reports no_destination.
func TestEligibilityByTargetKind(t *testing.T) {
	tx := testDB.Begin()
	t.Cleanup(func() { tx.Rollback() })
	svc := api.NewService(api.Deps{OrgGate: fakeOrgGate{active: true}, Gate: allowGate{enabled: true}, DestinationDAO: dao.NewDestinationRepo(tx)})
	for kind, want := range map[pb.TargetKind]string{
		pb.TargetKind_TARGET_KIND_UNSPECIFIED:            "",
		pb.TargetKind_TARGET_KIND_SLACK_CHANNEL:          "",
		pb.TargetKind_TARGET_KIND_CONFIGURED_DESTINATION: "no_destination",
	} {
		resp, err := svc.CheckSendEligibility(context.Background(), &pb.CheckSendEligibilityRequest{
			OrganizationId: testOrg, Usecase: templates.UsecaseNotice, Channels: []pb.Channel{pb.Channel_CHANNEL_SLACK}, TargetKind: kind,
		})
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(resp.GetChannelEligibility()[0].GetReasons(), ","); got != want {
			t.Errorf("%v: reasons %q, want %q", kind, got, want)
		}
	}
}

// TestCommsStatusDone pins done and is_partial. done is false while the request
// has not fanned out, or while any message is QUEUED, SENDING, FAILED waiting
// for a retry, or SENT waiting for a delivery event; it is true once every
// message is terminal. is_partial is true when at least one message ended in a
// non-delivered terminal state, or the fan-out failed. A failed fan-out usually
// sent nothing, but rows a crashed earlier run made can still be there.
func TestCommsStatusDone(t *testing.T) {
	ctx := context.Background()
	ack, confirmed := pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, pb.VerificationLevel_VERIFICATION_LEVEL_CONFIRMED
	type msg struct {
		st pb.MessageStatus
		vl pb.VerificationLevel
	}
	for _, tc := range []struct {
		name          string
		request       pb.RequestStatus
		msgs          []msg
		done, partial bool
	}{
		{"accepted, no messages yet", pb.RequestStatus_REQUEST_STATUS_ACCEPTED, nil, false, false},
		{"queued", pb.RequestStatus_REQUEST_STATUS_FANNED_OUT, []msg{{pb.MessageStatus_MESSAGE_STATUS_QUEUED, ack}}, false, false},
		{"sending", pb.RequestStatus_REQUEST_STATUS_FANNED_OUT, []msg{{pb.MessageStatus_MESSAGE_STATUS_DELIVERED, ack}, {pb.MessageStatus_MESSAGE_STATUS_SENDING, ack}}, false, false},
		{"failed, awaiting a retry", pb.RequestStatus_REQUEST_STATUS_FANNED_OUT, []msg{{pb.MessageStatus_MESSAGE_STATUS_FAILED, ack}}, false, false},
		{"sent, waiting for delivery", pb.RequestStatus_REQUEST_STATUS_FANNED_OUT, []msg{{pb.MessageStatus_MESSAGE_STATUS_SENT, confirmed}}, false, false},
		{"sent and delivered", pb.RequestStatus_REQUEST_STATUS_FANNED_OUT, []msg{{pb.MessageStatus_MESSAGE_STATUS_SENT, ack}, {pb.MessageStatus_MESSAGE_STATUS_DELIVERED, ack}}, true, false},
		{"one undelivered", pb.RequestStatus_REQUEST_STATUS_FANNED_OUT, []msg{{pb.MessageStatus_MESSAGE_STATUS_DELIVERED, ack}, {pb.MessageStatus_MESSAGE_STATUS_UNCONFIRMED, ack}}, true, true},
		{"undelivered while another is queued", pb.RequestStatus_REQUEST_STATUS_FANNED_OUT, []msg{{pb.MessageStatus_MESSAGE_STATUS_SKIPPED, ack}, {pb.MessageStatus_MESSAGE_STATUS_QUEUED, ack}}, false, true},
		{"fan-out failed, nothing sent", pb.RequestStatus_REQUEST_STATUS_FANOUT_FAILED, nil, true, true},
		{"FANOUT_FAILED with rows", pb.RequestStatus_REQUEST_STATUS_FANOUT_FAILED, []msg{{pb.MessageStatus_MESSAGE_STATUS_DELIVERED, ack}}, true, true},
		{"FANOUT_FAILED with a row still queued", pb.RequestStatus_REQUEST_STATUS_FANOUT_FAILED, []msg{{pb.MessageStatus_MESSAGE_STATUS_QUEUED, ack}}, false, true},
		{"fanning out, no messages yet", pb.RequestStatus_REQUEST_STATUS_FANNING_OUT, nil, false, false},
		{"fanning out, every row so far delivered", pb.RequestStatus_REQUEST_STATUS_FANNING_OUT, []msg{{pb.MessageStatus_MESSAGE_STATUS_DELIVERED, ack}}, false, false},
		{"dead, bounced and suppressed", pb.RequestStatus_REQUEST_STATUS_FANNED_OUT, []msg{{pb.MessageStatus_MESSAGE_STATUS_DEAD, ack}, {pb.MessageStatus_MESSAGE_STATUS_BOUNCED, ack}, {pb.MessageStatus_MESSAGE_STATUS_SUPPRESSED, ack}}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := testDB.Begin()
			t.Cleanup(func() { tx.Rollback() })
			reqs, msgs := dao.NewRequestRepo(tx), dao.NewMessageRepo(tx)
			_, req, err := reqs.CreateIfAbsent(ctx, &model.CommsRequest{OrganizationID: testOrg, IdempotencyKey: "done-" + tc.name,
				Usecase: templates.UsecaseNotice, DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, Status: tc.request})
			if err != nil {
				t.Fatal(err)
			}
			tx.Exec("UPDATE comms_requests SET status = ? WHERE id = ?", tc.request.String(), req.ID)
			for i, m := range tc.msgs {
				row := &model.CommsMessage{OrganizationID: testOrg, RequestID: req.ID, Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SES,
					Recipient: fmt.Sprintf("r%d@example.com", i), Status: m.st, VerificationLevel: m.vl, DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED}
				if _, err := msgs.MaterializeBatch(ctx, testOrg, []*model.CommsMessage{row}); err != nil {
					t.Fatal(err)
				}
			}
			svc := api.NewService(api.Deps{RequestDAO: reqs, MessageDAO: msgs})
			got, err := svc.GetCommsStatus(ctx, &pb.GetCommsStatusRequest{OrganizationId: testOrg, Handle: "done-" + tc.name})
			if err != nil {
				t.Fatal(err)
			}
			if got.GetDone() != tc.done || got.GetIsPartial() != tc.partial {
				t.Fatalf("done %v, is_partial %v; want %v, %v", got.GetDone(), got.GetIsPartial(), tc.done, tc.partial)
			}
		})
	}
}

// TestEligibilityDestinationScope pins which destination a
// CONFIGURED_DESTINATION check resolves: the person's when person_id is set,
// else the organization's, with no fallback between them, as the fan-out
// resolves the scope an entry names.
func TestEligibilityDestinationScope(t *testing.T) {
	ctx := context.Background()
	personRouted, personBare := uuid.NewString(), uuid.NewString()
	for _, tc := range []struct {
		name     string
		routes   []pb.EntityType
		personID string
		want     string
	}{
		{"person route, person asked", []pb.EntityType{pb.EntityType_ENTITY_TYPE_PERSON}, personRouted, ""},
		{"person route, person asked in upper case", []pb.EntityType{pb.EntityType_ENTITY_TYPE_PERSON}, strings.ToUpper(personRouted), ""},
		{"person route, no person asked", []pb.EntityType{pb.EntityType_ENTITY_TYPE_PERSON}, "", "no_destination"},
		{"org route, no person asked", []pb.EntityType{pb.EntityType_ENTITY_TYPE_ORG}, "", ""},
		{"org route, a person without a route asked", []pb.EntityType{pb.EntityType_ENTITY_TYPE_ORG}, personBare, "no_destination"},
		{"both routes, person asked", []pb.EntityType{pb.EntityType_ENTITY_TYPE_ORG, pb.EntityType_ENTITY_TYPE_PERSON}, personRouted, ""},
		{"both routes, no person asked", []pb.EntityType{pb.EntityType_ENTITY_TYPE_ORG, pb.EntityType_ENTITY_TYPE_PERSON}, "", ""},
		{"no route, person asked", nil, personRouted, "no_destination"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := testDB.Begin()
			t.Cleanup(func() { tx.Rollback() })
			for _, kind := range tc.routes {
				entity := testOrg
				if kind == pb.EntityType_ENTITY_TYPE_PERSON {
					entity = personRouted
				}
				if err := tx.Create(&model.CommsChannelDestination{OrganizationID: testOrg, EntityType: kind, EntityID: entity,
					Usecase: templates.UsecaseNotice, Channel: pb.Channel_CHANNEL_SLACK,
					DestinationKind: pb.DestinationKind_DESTINATION_KIND_CHANNEL, DestinationRef: "C0" + kind.String()}).Error; err != nil {
					t.Fatal(err)
				}
			}
			svc := api.NewService(api.Deps{OrgGate: fakeOrgGate{active: true}, Gate: allowGate{enabled: true}, DestinationDAO: dao.NewDestinationRepo(tx)})
			resp, err := svc.CheckSendEligibility(ctx, &pb.CheckSendEligibilityRequest{OrganizationId: testOrg, Usecase: templates.UsecaseNotice,
				Channels: []pb.Channel{pb.Channel_CHANNEL_SLACK}, TargetKind: pb.TargetKind_TARGET_KIND_CONFIGURED_DESTINATION, PersonId: tc.personID})
			if err != nil {
				t.Fatal(err)
			}
			if got := strings.Join(resp.GetChannelEligibility()[0].GetReasons(), ","); got != tc.want {
				t.Fatalf("reasons %q, want %q", got, tc.want)
			}
		})
	}
}

// TestEligibilityTargetKindValidation pins that a set target_kind must be a
// known kind that every requested channel can carry, by SubmitComms's rule.
func TestEligibilityTargetKindValidation(t *testing.T) {
	svc := api.NewService(api.Deps{OrgGate: fakeOrgGate{active: true}, Gate: allowGate{enabled: true}})
	email, slack := pb.Channel_CHANNEL_EMAIL, pb.Channel_CHANNEL_SLACK
	for _, tc := range []struct {
		name  string
		kind  pb.TargetKind
		chans []pb.Channel
		ok    bool
	}{
		{"unknown kind", pb.TargetKind(99), []pb.Channel{slack}, false},
		{"unknown kind, no channels", pb.TargetKind(99), nil, false},
		{"email address on slack", pb.TargetKind_TARGET_KIND_EMAIL_ADDRESS, []pb.Channel{slack}, false},
		{"slack channel on email", pb.TargetKind_TARGET_KIND_SLACK_CHANNEL, []pb.Channel{email}, false},
		{"undefined target kind 4", pb.TargetKind(4), []pb.Channel{slack}, false},
		{"email address on email and slack", pb.TargetKind_TARGET_KIND_EMAIL_ADDRESS, []pb.Channel{email, slack}, false},
		{"email address on email", pb.TargetKind_TARGET_KIND_EMAIL_ADDRESS, []pb.Channel{email}, true},
		{"slack channel on slack", pb.TargetKind_TARGET_KIND_SLACK_CHANNEL, []pb.Channel{slack}, true},
		{"unspecified on both", pb.TargetKind_TARGET_KIND_UNSPECIFIED, []pb.Channel{email, slack}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.CheckSendEligibility(context.Background(), &pb.CheckSendEligibilityRequest{OrganizationId: testOrg,
				Usecase: templates.UsecaseNotice, Channels: tc.chans, TargetKind: tc.kind})
			if tc.ok && err != nil {
				t.Fatalf("got %v, want accepted", err)
			}
			if !tc.ok && status.Code(err) != codes.InvalidArgument {
				t.Fatalf("got %v, want InvalidArgument", err)
			}
		})
	}
}

// TestEligibilityPairMatrix holds the eligibility check's (channel,
// target_kind) verdict to a fixed table and to SubmitComms's verdict for an
// entry of the same pair, over every channel and kind plus an undefined one of
// each. Only email to an address and Slack to a named channel or a configured
// destination are valid. An unspecified kind is valid for the check (a send to
// a channel the caller names) and never for an entry, which must say.
func TestEligibilityPairMatrix(t *testing.T) {
	contract, err := templates.Get(templates.UsecaseNotice)
	if err != nil {
		t.Fatal(err)
	}
	svc := api.NewService(api.Deps{OrgGate: fakeOrgGate{active: true}, Gate: allowGate{enabled: true}})
	valid := map[[2]int32]bool{
		{int32(pb.Channel_CHANNEL_EMAIL), int32(pb.TargetKind_TARGET_KIND_EMAIL_ADDRESS)}:          true,
		{int32(pb.Channel_CHANNEL_SLACK), int32(pb.TargetKind_TARGET_KIND_SLACK_CHANNEL)}:          true,
		{int32(pb.Channel_CHANNEL_SLACK), int32(pb.TargetKind_TARGET_KIND_CONFIGURED_DESTINATION)}: true,
	}
	for _, ch := range []pb.Channel{pb.Channel_CHANNEL_UNSPECIFIED, pb.Channel_CHANNEL_EMAIL, pb.Channel_CHANNEL_SLACK, pb.Channel(3), pb.Channel(99)} {
		for _, kind := range []pb.TargetKind{pb.TargetKind_TARGET_KIND_UNSPECIFIED, pb.TargetKind_TARGET_KIND_EMAIL_ADDRESS, pb.TargetKind_TARGET_KIND_SLACK_CHANNEL,
			pb.TargetKind_TARGET_KIND_CONFIGURED_DESTINATION, pb.TargetKind(4), pb.TargetKind(99)} {
			want := valid[[2]int32{int32(ch), int32(kind)}]
			_, err := svc.CheckSendEligibility(context.Background(), &pb.CheckSendEligibilityRequest{OrganizationId: testOrg,
				Usecase: templates.UsecaseNotice, Channels: []pb.Channel{ch}, TargetKind: kind})
			checkOK := err == nil
			if err != nil && status.Code(err) != codes.InvalidArgument {
				t.Fatalf("(%v, %v): %v", ch, kind, err)
			}
			if kind == pb.TargetKind_TARGET_KIND_UNSPECIFIED {
				if !checkOK {
					t.Errorf("(%v, unspecified): check refused it (%v); want accepted", ch, err)
				}
				continue
			}
			if checkOK != want {
				t.Errorf("(%v, %v): check accepted=%v, want %v", ch, kind, checkOK, want)
			}
			entry := &pb.CommsEntry{Channel: ch, TargetKind: kind, Recipient: "dev@example.com"}
			switch kind {
			case pb.TargetKind_TARGET_KIND_SLACK_CHANNEL:
				entry.Recipient = "C0MATRIX"
			case pb.TargetKind_TARGET_KIND_CONFIGURED_DESTINATION:
				entry.Recipient = ""
				entry.DestinationScope = &pb.DestinationScope{EntityType: pb.EntityType_ENTITY_TYPE_ORG, EntityId: testOrg}
			}
			if submitOK := api.ValidateEntry(contract, testOrg, entry) == nil; submitOK != checkOK {
				t.Errorf("(%v, %v): check accepted=%v but SubmitComms's entry check accepted=%v", ch, kind, checkOK, submitOK)
			}
		}
	}
}

// TestUndefinedEnumNumbersAreRejected pins that numbers no enum value defines
// (channels 3, 4 and 5, target kind 4) are INVALID_ARGUMENT on an entry, both
// under the proto rules the server applies first and in SubmitComms itself.
func TestUndefinedEnumNumbersAreRejected(t *testing.T) {
	v, err := protovalidate.New()
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range []*pb.CommsEntry{
		{Channel: pb.Channel(3), TargetKind: pb.TargetKind_TARGET_KIND_EMAIL_ADDRESS, Recipient: "dev@example.com"},
		{Channel: pb.Channel(4), TargetKind: pb.TargetKind_TARGET_KIND_EMAIL_ADDRESS, Recipient: "dev@example.com"},
		{Channel: pb.Channel(5), TargetKind: pb.TargetKind_TARGET_KIND_EMAIL_ADDRESS, Recipient: "dev@example.com"},
		{Channel: pb.Channel_CHANNEL_SLACK, TargetKind: pb.TargetKind(4), Recipient: "C0X"},
	} {
		req := &pb.SubmitCommsRequest{OrganizationId: testOrg, IdempotencyKey: "undefined", Usecase: templates.UsecaseNotice,
			Content: &pb.MessageContent{Base: &pb.RenderableContent{Subject: "x"}}, Entries: []*pb.CommsEntry{e}}
		if v.Validate(req) == nil {
			t.Errorf("(%v, %v) passes the proto rules", e.GetChannel(), e.GetTargetKind())
		}
		svc, _, _ := newServiceTx(t)
		if _, err := svc.SubmitComms(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("(%v, %v): SubmitComms %v, want InvalidArgument", e.GetChannel(), e.GetTargetKind(), err)
		}
	}
}
