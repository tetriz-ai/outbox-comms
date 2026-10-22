// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package api_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/tetriz-ai/outbox-comms/gate"
	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/api"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
	"github.com/tetriz-ai/outbox-comms/internal/sender"
	"github.com/tetriz-ai/outbox-comms/internal/testdb"
	"github.com/tetriz-ai/outbox-comms/templates"
)

var testDB *gorm.DB

func TestMain(m *testing.M) { os.Exit(testdb.Main(m, &testDB)) }

// fakePublisher records PublishRequest calls so a test can assert enqueue count.
type fakePublisher struct{ calls int }

func (f *fakePublisher) PublishRequest(context.Context, string, string) error {
	f.calls++
	return nil
}

const testOrg = "11111111-1111-1111-1111-111111111111"

func newService(t *testing.T) (*api.Service, *fakePublisher) {
	t.Helper()
	svc, pub, _ := newServiceTx(t)
	return svc, pub
}

// newServiceTx is newService plus the transaction it writes through, for tests
// that read back what a call stored.
func newServiceTx(t *testing.T) (*api.Service, *fakePublisher, *gorm.DB) {
	t.Helper()
	tx := testDB.Begin()
	if tx.Error != nil {
		t.Fatalf("begin tx: %v", tx.Error)
	}
	t.Cleanup(func() { tx.Rollback() })
	pub := &fakePublisher{}
	return api.NewService(api.Deps{RequestDAO: dao.NewRequestRepo(tx), Publisher: pub, Gate: allowGate{enabled: true}}), pub, tx
}

type fakeOrgGate struct{ active, frozen bool }

func (g fakeOrgGate) CheckOrg(context.Context, string) (bool, bool, error) {
	return g.active, g.frozen, nil
}

// allowGate is a CommsGate stub. enabled drives every org; SubmitComms fails
// closed, so the pipeline tests pass an enabled one and the gate's reject paths
// (nil gate, disabled gate) are covered by TestSubmitCommsGate.
type allowGate struct{ enabled bool }

func (g allowGate) Enabled(string) bool { return g.enabled }

// TestGetCommsStatusRollup proves the per-message rollup: a request with one
// DELIVERED and one SKIPPED message reports IsPartial=true with the breakdown.
func TestGetCommsStatusRollup(t *testing.T) {
	ctx := context.Background()
	tx := testDB.Begin()
	t.Cleanup(func() { tx.Rollback() })
	reqRepo := dao.NewRequestRepo(tx)
	msgRepo := dao.NewMessageRepo(tx)
	svc := api.NewService(api.Deps{RequestDAO: reqRepo, MessageDAO: msgRepo})

	_, req, err := reqRepo.CreateIfAbsent(ctx, &model.CommsRequest{
		OrganizationID: testOrg, IdempotencyKey: "status:1", Usecase: templates.UsecaseNotice,
		DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, Status: pb.RequestStatus_REQUEST_STATUS_FANNED_OUT,
		Params: &structpb.Struct{},
	})
	if err != nil {
		t.Fatalf("seed req: %v", err)
	}
	mk := func(recipient string, st pb.MessageStatus) *model.CommsMessage {
		return &model.CommsMessage{
			OrganizationID: testOrg, RequestID: req.ID, Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SES,
			Recipient: recipient, Status: st, VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK,
			DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
		}
	}
	if _, err := msgRepo.MaterializeBatch(ctx, testOrg, []*model.CommsMessage{
		mk("a@example.com", pb.MessageStatus_MESSAGE_STATUS_DELIVERED),
		mk("b@example.com", pb.MessageStatus_MESSAGE_STATUS_SKIPPED),
	}); err != nil {
		t.Fatalf("seed msgs: %v", err)
	}
	resp, err := svc.GetCommsStatus(ctx, &pb.GetCommsStatusRequest{OrganizationId: testOrg, Handle: "status:1"})
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if !resp.GetIsPartial() {
		t.Errorf("a SKIPPED + DELIVERED mix must roll up PARTIAL")
	}
	if len(resp.GetMessages()) != 2 {
		t.Errorf("want 2 message items, got %d", len(resp.GetMessages()))
	}
}

// TestCheckSendEligibility proves the advisory preflight: a transactional usecase
// on a healthy org is eligible; a frozen org is not.
func TestCheckSendEligibility(t *testing.T) {
	ctx := context.Background()
	healthy := api.NewService(api.Deps{OrgGate: fakeOrgGate{active: true, frozen: false}, Gate: allowGate{enabled: true}})
	resp, err := healthy.CheckSendEligibility(ctx, &pb.CheckSendEligibilityRequest{
		OrganizationId: testOrg, Usecase: templates.UsecaseNotice,
		Channels: []pb.Channel{pb.Channel_CHANNEL_EMAIL},
	})
	if err != nil {
		t.Fatalf("eligibility: %v", err)
	}
	if !resp.GetChannelEligibility()[0].GetEligible() {
		t.Errorf("transactional notice on a healthy org should be eligible, got %v", resp.GetChannelEligibility()[0].GetReasons())
	}

	frozen := api.NewService(api.Deps{OrgGate: fakeOrgGate{active: true, frozen: true}, Gate: allowGate{enabled: true}})
	resp, err = frozen.CheckSendEligibility(ctx, &pb.CheckSendEligibilityRequest{
		OrganizationId: testOrg, Usecase: templates.UsecaseNotice,
		Channels: []pb.Channel{pb.Channel_CHANNEL_EMAIL},
	})
	if err != nil {
		t.Fatalf("eligibility frozen: %v", err)
	}
	if resp.GetChannelEligibility()[0].GetEligible() {
		t.Errorf("a frozen org must not be eligible")
	}

	// Gate off: a healthy org is still ineligible, tagged comms_disabled, the same
	// gate SubmitComms fails closed on, so a producer can stop before expensive
	// work.
	disabled := api.NewService(api.Deps{OrgGate: fakeOrgGate{active: true, frozen: false}, Gate: allowGate{enabled: false}})
	resp, err = disabled.CheckSendEligibility(ctx, &pb.CheckSendEligibilityRequest{
		OrganizationId: testOrg, Usecase: templates.UsecaseNotice,
		Channels: []pb.Channel{pb.Channel_CHANNEL_EMAIL},
	})
	if err != nil {
		t.Fatalf("eligibility disabled: %v", err)
	}
	ce := resp.GetChannelEligibility()[0]
	if ce.GetEligible() {
		t.Errorf("a comms-disabled org must not be eligible")
	}
	found := false
	for _, r := range ce.GetReasons() {
		if r == "comms_disabled" {
			found = true
		}
	}
	if !found {
		t.Errorf("a comms-disabled org must carry the comms_disabled reason, got %v", ce.GetReasons())
	}
}

// TestPreferenceRPCsRoundTrip proves the profile/admin preference RPCs upsert and
// read back, with the caller-tier forced (a user only sets their own PERSON row).
func TestPreferenceRPCsRoundTrip(t *testing.T) {
	ctx := context.Background()
	tx := testDB.Begin()
	t.Cleanup(func() { tx.Rollback() })
	svc := api.NewService(api.Deps{PreferenceDAO: dao.NewPreferenceRepo(tx), DestinationDAO: dao.NewDestinationRepo(tx)})
	person := "22222222-2222-2222-2222-222222222222"

	if _, err := svc.UpdatePersonPreferences(ctx, &pb.UpdatePersonPreferencesRequest{
		OrganizationId: testOrg, PersonId: person,
		Preferences: []*pb.Preference{{
			Category: pb.MessageCategory_MESSAGE_CATEGORY_PRODUCT,
			Channel:  pb.Channel_CHANNEL_EMAIL, Enabled: false,
		}},
	}); err != nil {
		t.Fatalf("update my prefs: %v", err)
	}
	got, err := svc.GetPersonPreferences(ctx, &pb.GetPersonPreferencesRequest{OrganizationId: testOrg, PersonId: person})
	if err != nil {
		t.Fatalf("get my prefs: %v", err)
	}
	if len(got.GetPreferences()) != 1 || got.GetPreferences()[0].GetEnabled() {
		t.Fatalf("expected one disabled PERSON product/email pref, got %+v", got.GetPreferences())
	}
	if got.GetPreferences()[0].GetEntityType() != pb.EntityType_ENTITY_TYPE_PERSON {
		t.Errorf("UpdatePersonPreferences must force the PERSON tier")
	}
}

// TestDestinationRPCsRoundTrip proves the admin destination RPCs upsert + read.
func TestDestinationRPCsRoundTrip(t *testing.T) {
	ctx := context.Background()
	tx := testDB.Begin()
	t.Cleanup(func() { tx.Rollback() })
	svc := api.NewService(api.Deps{DestinationDAO: dao.NewDestinationRepo(tx)})

	if _, err := svc.SetDestination(ctx, &pb.SetDestinationRequest{
		OrganizationId: testOrg,
		Destination: &pb.Destination{
			EntityType: pb.EntityType_ENTITY_TYPE_ORG, EntityId: testOrg,
			Usecase: templates.UsecaseDigest,
			Channel: pb.Channel_CHANNEL_SLACK, DestinationRef: "C0AB12CD3",
		},
	}); err != nil {
		t.Fatalf("set destination: %v", err)
	}
	got, err := svc.GetDestinations(ctx, &pb.GetDestinationsRequest{OrganizationId: testOrg})
	if err != nil {
		t.Fatalf("get destinations: %v", err)
	}
	if len(got.GetDestinations()) != 1 || got.GetDestinations()[0].GetDestinationRef() != "C0AB12CD3" {
		t.Fatalf("expected one destination C0AB12CD3, got %+v", got.GetDestinations())
	}
}

// TestPurgeOrgComms pins what a purge removes: the organization's requests,
// messages, preferences, and its destinations of both scopes, ORG and PERSON. A
// suppression, which belongs to an address and not the organization, stays.
func TestPurgeOrgComms(t *testing.T) {
	ctx := context.Background()
	// PurgeOrg opens its own transaction, so use the base testDB
	// + explicit cleanup. The purge itself removes most rows; cleanup catches residue.
	tx := testDB
	t.Cleanup(func() {
		testDB.Exec("DELETE FROM comms_delivery_events WHERE organization_id = ?", testOrg)
		testDB.Exec("DELETE FROM comms_messages WHERE organization_id = ?", testOrg)
		testDB.Exec("DELETE FROM comms_requests WHERE organization_id = ?", testOrg)
		testDB.Exec("DELETE FROM comms_preferences WHERE organization_id = ?", testOrg)
		testDB.Exec("DELETE FROM comms_channel_destinations WHERE organization_id = ?", testOrg)
		testDB.Exec("DELETE FROM comms_suppressions WHERE email = ?", "purge-kept@example.com")
	})
	reqRepo := dao.NewRequestRepo(tx)
	msgRepo := dao.NewMessageRepo(tx)
	prefRepo := dao.NewPreferenceRepo(tx)
	svc := api.NewService(api.Deps{ReconcileDAO: dao.NewReconcileRepo(tx)})

	_, req, err := reqRepo.CreateIfAbsent(ctx, &model.CommsRequest{
		OrganizationID: testOrg, IdempotencyKey: "purge:1", Usecase: templates.UsecaseNotice,
		DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, Status: pb.RequestStatus_REQUEST_STATUS_FANNED_OUT,
		Params: &structpb.Struct{},
	})
	if err != nil {
		t.Fatalf("seed request: %v", err)
	}
	if _, err := msgRepo.MaterializeBatch(ctx, testOrg, []*model.CommsMessage{{
		OrganizationID: testOrg, RequestID: req.ID, Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SES,
		Recipient: "dev@example.com", Status: pb.MessageStatus_MESSAGE_STATUS_QUEUED,
		VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
	}}); err != nil {
		t.Fatalf("seed msg: %v", err)
	}
	if err := prefRepo.Upsert(ctx, &model.CommsPreference{
		OrganizationID: testOrg, EntityType: pb.EntityType_ENTITY_TYPE_ORG, EntityID: testOrg,
		Category: pb.MessageCategory_MESSAGE_CATEGORY_PRODUCT, Channel: pb.Channel_CHANNEL_EMAIL, Enabled: false,
	}); err != nil {
		t.Fatalf("seed pref: %v", err)
	}
	destRepo := dao.NewDestinationRepo(tx)
	for kind, entity := range map[pb.EntityType]string{pb.EntityType_ENTITY_TYPE_ORG: testOrg, pb.EntityType_ENTITY_TYPE_PERSON: "22222222-2222-4222-8222-222222222222"} {
		if err := destRepo.ReplaceForChannel(ctx, &model.CommsChannelDestination{OrganizationID: testOrg, EntityType: kind, EntityID: entity,
			Usecase: templates.UsecaseAlert, Channel: pb.Channel_CHANNEL_SLACK, DestinationKind: pb.DestinationKind_DESTINATION_KIND_CHANNEL, DestinationRef: "C0" + kind.String()}); err != nil {
			t.Fatalf("seed destination: %v", err)
		}
	}
	if err := dao.NewSuppressionRepo(tx).SuppressIfAbsent(ctx, &model.CommsSuppression{Email: "purge-kept@example.com", Reason: "hard_bounce"}); err != nil {
		t.Fatalf("seed suppression: %v", err)
	}

	resp, err := svc.PurgeOrgComms(ctx, &pb.PurgeOrgCommsRequest{OrganizationId: testOrg})
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if resp.GetDeletedMessages() != 1 {
		t.Errorf("expected 1 deleted message, got %d", resp.GetDeletedMessages())
	}
	var reqCount, prefCount int64
	tx.Model(&model.CommsRequest{}).Where("organization_id = ?", testOrg).Count(&reqCount)
	tx.Model(&model.CommsPreference{}).Where("organization_id = ?", testOrg).Count(&prefCount)
	if reqCount != 0 || prefCount != 0 {
		t.Errorf("purge must delete requests + preferences, got req=%d pref=%d", reqCount, prefCount)
	}
	var destCount int64
	tx.Model(&model.CommsChannelDestination{}).Where("organization_id = ?", testOrg).Count(&destCount)
	if destCount != 0 {
		t.Errorf("purge left %d destination(s), want 0 (ORG and PERSON)", destCount)
	}
	if sup, err := dao.NewSuppressionRepo(tx).GetActiveByEmail(ctx, "purge-kept@example.com"); err != nil || sup == nil {
		t.Errorf("the suppression did not survive the purge: %v, %v", sup, err)
	}
}

// schemaUsecase is a test usecase whose params must decode into a message with a
// buf.validate rule (pb.Destination's destination_ref is min_len 1), exercising
// the typed-schema checks at submit.
const schemaUsecase = "test.schema"

func init() {
	templates.MustRegister(schemaUsecase, templates.Contract{
		NewParams:           func() proto.Message { return &pb.Destination{} },
		Category:            pb.MessageCategory_MESSAGE_CATEGORY_TRANSACTIONAL,
		EnabledChannels:     []pb.Channel{pb.Channel_CHANNEL_EMAIL},
		DefaultVerification: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK,
		DefaultGuarantee:    pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
	})
}

func sampleParams(t *testing.T) *structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(map[string]any{"title": "Welcome to Example Corp"})
	if err != nil {
		t.Fatalf("params: %v", err)
	}
	return s
}

func emailEntry(addr string) *pb.CommsEntry {
	return &pb.CommsEntry{
		Channel:    pb.Channel_CHANNEL_EMAIL,
		TargetKind: pb.TargetKind_TARGET_KIND_EMAIL_ADDRESS,
		Recipient:  addr,
	}
}

// TestSubmitCommsPersistsAndEnqueues proves the happy path: a request row is
// persisted and one enqueue fires, and the handle is the idempotency_key.
func TestSubmitCommsPersistsAndEnqueues(t *testing.T) {
	ctx := context.Background()
	svc, pub := newService(t)
	resp, err := svc.SubmitComms(ctx, &pb.SubmitCommsRequest{
		OrganizationId: testOrg,
		IdempotencyKey: "notice:p1:1",
		Usecase:        templates.UsecaseNotice,
		Params:         sampleParams(t),
		Entries:        []*pb.CommsEntry{emailEntry("dev@example.com")},
	})
	if err != nil {
		t.Fatalf("SubmitComms: %v", err)
	}
	if resp.GetHandle() != "notice:p1:1" {
		t.Errorf("handle = %q, want the idempotency_key", resp.GetHandle())
	}
	if pub.calls != 1 {
		t.Errorf("enqueue count = %d, want 1", pub.calls)
	}
}

// TestSubmitCommsIdempotent proves a re-submit collapses to one row, the same
// handle, and exactly one enqueue.
func TestSubmitCommsIdempotent(t *testing.T) {
	ctx := context.Background()
	svc, pub := newService(t)
	req := &pb.SubmitCommsRequest{
		OrganizationId: testOrg,
		IdempotencyKey: "notice:p1:2",
		Usecase:        templates.UsecaseNotice,
		Params:         sampleParams(t),
		Entries:        []*pb.CommsEntry{emailEntry("dev@example.com")},
	}
	r1, err := svc.SubmitComms(ctx, req)
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}
	r2, err := svc.SubmitComms(ctx, req)
	if err != nil {
		t.Fatalf("re-submit: %v", err)
	}
	if r1.GetHandle() != r2.GetHandle() {
		t.Errorf("re-submit handle differs: %q vs %q", r1.GetHandle(), r2.GetHandle())
	}
	if pub.calls != 1 {
		t.Errorf("re-submit must enqueue once, got %d", pub.calls)
	}
}

// TestSubmitCommsRejections proves the submit validation surface: bad params,
// empty batch, and cross-field violations all return INVALID_ARGUMENT with no
// row and no enqueue.
func TestSubmitCommsRejections(t *testing.T) {
	cfgDest := func() *pb.CommsEntry {
		return &pb.CommsEntry{
			Channel:    pb.Channel_CHANNEL_SLACK,
			TargetKind: pb.TargetKind_TARGET_KIND_CONFIGURED_DESTINATION,
			Recipient:  "C123", // illegal — must be empty
			DestinationScope: &pb.DestinationScope{
				EntityType: pb.EntityType_ENTITY_TYPE_ORG, EntityId: testOrg,
			},
		}
	}
	tests := []struct {
		name   string
		mutate func(t *testing.T, r *pb.SubmitCommsRequest)
	}{
		{"param breaks the usecase's schema", func(_ *testing.T, r *pb.SubmitCommsRequest) {
			r.Usecase = schemaUsecase
			s, _ := structpb.NewStruct(map[string]any{"destination_ref": ""}) // min_len 1
			r.Params = s
		}},
		{"param unknown to the usecase's schema", func(_ *testing.T, r *pb.SubmitCommsRequest) {
			r.Usecase = schemaUsecase
			s, _ := structpb.NewStruct(map[string]any{"destination_ref": "C1", "bogus": "x"})
			r.Params = s
		}},
		{"unregistered usecase", func(_ *testing.T, r *pb.SubmitCommsRequest) { r.Usecase = "no.such.usecase" }},
		{"empty entries", func(_ *testing.T, r *pb.SubmitCommsRequest) { r.Entries = nil }},
		{"configured_destination with recipient", func(_ *testing.T, r *pb.SubmitCommsRequest) {
			r.Usecase = templates.UsecaseNotice
			r.Entries = []*pb.CommsEntry{cfgDest()}
		}},
		{"intra-batch duplicate", func(_ *testing.T, r *pb.SubmitCommsRequest) {
			r.Entries = []*pb.CommsEntry{emailEntry("dup@example.com"), emailEntry("dup@example.com")}
		}},
		{"malformed email recipient", func(_ *testing.T, r *pb.SubmitCommsRequest) {
			r.Entries = []*pb.CommsEntry{emailEntry("not-an-address")}
		}},
		{"email recipient with a display name", func(_ *testing.T, r *pb.SubmitCommsRequest) {
			r.Entries = []*pb.CommsEntry{emailEntry("Jane Doe <jane@example.com>")}
		}},
		{"email recipient with a header break", func(_ *testing.T, r *pb.SubmitCommsRequest) {
			r.Entries = []*pb.CommsEntry{emailEntry("jane@example.com\r\nBcc: x@example.com")}
		}},
		{"subject_person_id that is not a UUID", func(_ *testing.T, r *pb.SubmitCommsRequest) {
			e := emailEntry("dev@example.com")
			e.SubjectPersonId = "person-7"
			r.Entries = []*pb.CommsEntry{e}
		}},
		{"subject_person_id on a configured destination", func(_ *testing.T, r *pb.SubmitCommsRequest) {
			e := cfgDest()
			e.Recipient = ""
			e.SubjectPersonId = "22222222-2222-2222-2222-222222222222"
			r.Entries = []*pb.CommsEntry{e}
		}},
		{"subject_person_id as a braced UUID", func(_ *testing.T, r *pb.SubmitCommsRequest) {
			e := emailEntry("dev@example.com")
			e.SubjectPersonId = "{22222222-2222-2222-2222-222222222222}"
			r.Entries = []*pb.CommsEntry{e}
		}},
		{"destination scope entity_id as a URN", func(_ *testing.T, r *pb.SubmitCommsRequest) {
			e := cfgDest()
			e.Recipient = ""
			e.DestinationScope.EntityId = "urn:uuid:" + testOrg
			r.Entries = []*pb.CommsEntry{e}
		}},
		{"destination scope entity_id without hyphens", func(_ *testing.T, r *pb.SubmitCommsRequest) {
			e := cfgDest()
			e.Recipient = ""
			e.DestinationScope.EntityId = strings.ReplaceAll(testOrg, "-", "")
			r.Entries = []*pb.CommsEntry{e}
		}},
		{"destination scope type 3, undefined", func(_ *testing.T, r *pb.SubmitCommsRequest) {
			e := cfgDest()
			e.Recipient = ""
			e.DestinationScope.EntityType = pb.EntityType(3)
			r.Entries = []*pb.CommsEntry{e}
		}},
		{"destination scope type unspecified", func(_ *testing.T, r *pb.SubmitCommsRequest) {
			e := cfgDest()
			e.Recipient = ""
			e.DestinationScope.EntityType = pb.EntityType_ENTITY_TYPE_UNSPECIFIED
			r.Entries = []*pb.CommsEntry{e}
		}},
		{"destination scope type 99", func(_ *testing.T, r *pb.SubmitCommsRequest) {
			e := cfgDest()
			e.Recipient = ""
			e.DestinationScope.EntityType = pb.EntityType(99)
			r.Entries = []*pb.CommsEntry{e}
		}},
		{"ORG destination scope of another organization", func(_ *testing.T, r *pb.SubmitCommsRequest) {
			e := cfgDest()
			e.Recipient = ""
			e.DestinationScope.EntityId = "22222222-2222-2222-2222-222222222222"
			r.Entries = []*pb.CommsEntry{e}
		}},
		{"PERSON destination scope on the organization's id", func(_ *testing.T, r *pb.SubmitCommsRequest) {
			e := cfgDest()
			e.Recipient = ""
			e.DestinationScope.EntityType = pb.EntityType_ENTITY_TYPE_PERSON
			r.Entries = []*pb.CommsEntry{e}
		}},
		{"slack_delivery on email entry", func(_ *testing.T, r *pb.SubmitCommsRequest) {
			e := emailEntry("dev@example.com")
			e.SlackDelivery = pb.SlackDelivery_SLACK_DELIVERY_LINK
			r.Entries = []*pb.CommsEntry{e}
		}},
		{"destination scope entity_id that is not a UUID", func(_ *testing.T, r *pb.SubmitCommsRequest) {
			e := cfgDest()
			e.Recipient = ""
			e.DestinationScope.EntityId = "not-a-uuid"
			r.Entries = []*pb.CommsEntry{emailEntry("ok@example.com"), e}
		}},
		{"recipient over 1024 bytes", func(_ *testing.T, r *pb.SubmitCommsRequest) {
			r.Entries = []*pb.CommsEntry{emailEntry("ok@example.com"), {
				Channel: pb.Channel_CHANNEL_SLACK, TargetKind: pb.TargetKind_TARGET_KIND_SLACK_CHANNEL,
				Recipient: strings.Repeat("C", 1025), SlackDelivery: pb.SlackDelivery_SLACK_DELIVERY_LINK,
			}}
		}},
		{"idempotency key over 255 bytes", func(_ *testing.T, r *pb.SubmitCommsRequest) {
			r.IdempotencyKey = strings.Repeat("k", 256)
		}},
		{"the same email address in two cases", func(_ *testing.T, r *pb.SubmitCommsRequest) {
			r.Entries = []*pb.CommsEntry{emailEntry("jane@example.com"), emailEntry("Jane@Example.com")}
		}},
		{"NUL byte in content", func(_ *testing.T, r *pb.SubmitCommsRequest) {
			r.Content = &pb.MessageContent{Base: &pb.RenderableContent{Subject: "a\x00b"}}
		}},
		{"NUL byte in a params key", func(_ *testing.T, r *pb.SubmitCommsRequest) {
			r.Params = &structpb.Struct{Fields: map[string]*structpb.Value{"ti\x00tle": structpb.NewStringValue("x")}}
		}},
		{"NUL byte in params", func(_ *testing.T, r *pb.SubmitCommsRequest) {
			s, _ := structpb.NewStruct(map[string]any{"title": "a\x00b"})
			r.Params = s
		}},
		{"NUL byte in an entry's vars", func(_ *testing.T, r *pb.SubmitCommsRequest) {
			e := emailEntry("dev@example.com")
			e.Vars, _ = structpb.NewStruct(map[string]any{"name": []any{"a\x00b"}})
			r.Entries = []*pb.CommsEntry{e}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			svc, pub, tx := newServiceTx(t)
			req := &pb.SubmitCommsRequest{
				OrganizationId: testOrg,
				IdempotencyKey: "k-" + tt.name,
				Usecase:        templates.UsecaseNotice,
				Params:         sampleParams(t),
				Entries:        []*pb.CommsEntry{emailEntry("dev@example.com")},
			}
			tt.mutate(t, req)
			_, err := svc.SubmitComms(ctx, req)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("want InvalidArgument, got %v", err)
			}
			if pub.calls != 0 {
				t.Errorf("rejected submit must not enqueue, got %d", pub.calls)
			}
			var stored int64
			tx.Model(&model.CommsRequest{}).Where("organization_id = ?", testOrg).Count(&stored)
			if stored != 0 {
				t.Errorf("rejected submit must store nothing, found %d request rows", stored)
			}
		})
	}
}

// TestSubmitCommsGate proves the whole-feature gate fails closed: a disabled gate
// AND a nil gate (a wiring regression) both reject with FailedPrecondition and
// enqueue nothing — the feature must never be silently un-gated.
func TestSubmitCommsGate(t *testing.T) {
	good := func() *pb.SubmitCommsRequest {
		return &pb.SubmitCommsRequest{
			OrganizationId: testOrg,
			IdempotencyKey: "gate:1",
			Usecase:        templates.UsecaseNotice,
			Params:         sampleParams(t),
			Entries:        []*pb.CommsEntry{emailEntry("dev@example.com")},
		}
	}
	tests := []struct {
		name string
		gate gate.CommsGate
	}{
		{"disabled gate", allowGate{enabled: false}},
		{"nil gate (wiring regression)", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			tx := testDB.Begin()
			t.Cleanup(func() { tx.Rollback() })
			pub := &fakePublisher{}
			svc := api.NewService(api.Deps{RequestDAO: dao.NewRequestRepo(tx), Publisher: pub, Gate: tt.gate})
			_, err := svc.SubmitComms(ctx, good())
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("want FailedPrecondition, got %v", err)
			}
			if pub.calls != 0 {
				t.Errorf("gated submit must not enqueue, got %d", pub.calls)
			}
		})
	}
}

// TestListMessagesPagination proves ListMessages keyset-pages newest-first:
// a 3-message org read with page_size=2 returns 2 rows + has_more, and the
// cursor-driven second page returns the remaining row with has_more=false.
func TestListMessagesPagination(t *testing.T) {
	ctx := context.Background()
	tx := testDB.Begin()
	t.Cleanup(func() { tx.Rollback() })
	reqRepo := dao.NewRequestRepo(tx)
	msgRepo := dao.NewMessageRepo(tx)
	svc := api.NewService(api.Deps{MessageDAO: msgRepo, RequestDAO: reqRepo})

	_, req, err := reqRepo.CreateIfAbsent(ctx, &model.CommsRequest{
		OrganizationID: testOrg, IdempotencyKey: "hist:1", Usecase: templates.UsecaseNotice,
		DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, Status: pb.RequestStatus_REQUEST_STATUS_FANNED_OUT,
		Params: &structpb.Struct{},
	})
	if err != nil {
		t.Fatalf("seed req: %v", err)
	}
	mk := func(recipient string) *model.CommsMessage {
		return &model.CommsMessage{
			OrganizationID: testOrg, RequestID: req.ID, Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SES,
			Recipient: recipient, Status: pb.MessageStatus_MESSAGE_STATUS_SENT,
			VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK,
			DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
		}
	}
	if _, err := msgRepo.MaterializeBatch(ctx, testOrg, []*model.CommsMessage{
		mk("a@example.com"), mk("b@example.com"), mk("c@example.com"),
	}); err != nil {
		t.Fatalf("seed msgs: %v", err)
	}

	page1, err := svc.ListMessages(ctx, &pb.ListMessagesRequest{OrganizationId: testOrg, PageSize: 2})
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	if len(page1.GetMessages()) != 2 || !page1.GetHasMore() {
		t.Fatalf("page1 want 2 rows + has_more, got %d rows has_more=%v", len(page1.GetMessages()), page1.GetHasMore())
	}
	for _, m := range page1.GetMessages() {
		if m.GetUsecase() != templates.UsecaseNotice {
			t.Fatalf("message %s: want usecase resolved from its request, got %v", m.GetMessageId(), m.GetUsecase())
		}
	}
	page2, err := svc.ListMessages(ctx, &pb.ListMessagesRequest{
		OrganizationId: testOrg, PageSize: 2,
		BeforeCreatedAt: page1.GetNextBeforeCreatedAt(), BeforeId: page1.GetNextBeforeId(),
	})
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	if len(page2.GetMessages()) != 1 || page2.GetHasMore() {
		t.Fatalf("page2 want 1 row + no has_more, got %d rows has_more=%v", len(page2.GetMessages()), page2.GetHasMore())
	}
}

// TestListMessagesResolvesUsecasePerRequestOnMixedPage proves the dedup/batch
// lookup in ListMessages keys usecase back to the RIGHT request when a single
// page mixes messages from two different requests — TestListMessagesPagination
// only ever seeds one request, which would not catch a wrong-key regression in
// the seen-map / usecaseByRequestID lookup.
func TestListMessagesResolvesUsecasePerRequestOnMixedPage(t *testing.T) {
	ctx := context.Background()
	tx := testDB.Begin()
	t.Cleanup(func() { tx.Rollback() })
	reqRepo := dao.NewRequestRepo(tx)
	msgRepo := dao.NewMessageRepo(tx)
	svc := api.NewService(api.Deps{MessageDAO: msgRepo, RequestDAO: reqRepo})

	newReq := func(key string, usecase string) *model.CommsRequest {
		return &model.CommsRequest{
			OrganizationID: testOrg, IdempotencyKey: key, Usecase: usecase,
			DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
			Status:            pb.RequestStatus_REQUEST_STATUS_FANNED_OUT,
			Params:            &structpb.Struct{},
		}
	}
	_, digestReq, err := reqRepo.CreateIfAbsent(ctx, newReq("hist:mixed:digest", templates.UsecaseDigest))
	if err != nil {
		t.Fatalf("seed digest req: %v", err)
	}
	_, noticeReq, err := reqRepo.CreateIfAbsent(ctx, newReq("hist:mixed:notice", templates.UsecaseNotice))
	if err != nil {
		t.Fatalf("seed notice req: %v", err)
	}
	mk := func(requestID, recipient string) *model.CommsMessage {
		return &model.CommsMessage{
			OrganizationID: testOrg, RequestID: requestID, Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SES,
			Recipient: recipient, Status: pb.MessageStatus_MESSAGE_STATUS_SENT,
			VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK,
			DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
		}
	}
	if _, err := msgRepo.MaterializeBatch(ctx, testOrg, []*model.CommsMessage{
		mk(digestReq.ID, "a@example.com"), mk(noticeReq.ID, "b@example.com"),
	}); err != nil {
		t.Fatalf("seed msgs: %v", err)
	}

	resp, err := svc.ListMessages(ctx, &pb.ListMessagesRequest{OrganizationId: testOrg})
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if len(resp.GetMessages()) != 2 {
		t.Fatalf("want 2 rows, got %d", len(resp.GetMessages()))
	}
	for _, m := range resp.GetMessages() {
		switch m.GetRequestId() {
		case digestReq.ID:
			if m.GetUsecase() != templates.UsecaseDigest {
				t.Fatalf("digest request's message resolved to the wrong usecase: %v", m.GetUsecase())
			}
		case noticeReq.ID:
			if m.GetUsecase() != templates.UsecaseNotice {
				t.Fatalf("notice request's message resolved to the wrong usecase: %v", m.GetUsecase())
			}
		default:
			t.Fatalf("unexpected request id %s", m.GetRequestId())
		}
	}
}

// TestListMessagesDegradesWhenUsecaseResolveFails proves a RequestDAO failure
// must not fail the history page: messages still come back, with an empty
// usecase.
func TestListMessagesDegradesWhenUsecaseResolveFails(t *testing.T) {
	ctx := context.Background()
	tx := testDB.Begin()
	t.Cleanup(func() { tx.Rollback() })
	reqRepo := dao.NewRequestRepo(tx)
	msgRepo := dao.NewMessageRepo(tx)

	failingRequestDAO := failingUsecases{IRequestDAO: reqRepo}
	svc := api.NewService(api.Deps{MessageDAO: msgRepo, RequestDAO: failingRequestDAO})

	_, req, err := reqRepo.CreateIfAbsent(ctx, &model.CommsRequest{
		OrganizationID: testOrg, IdempotencyKey: "hist:degrade", Usecase: templates.UsecaseNotice,
		DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, Status: pb.RequestStatus_REQUEST_STATUS_FANNED_OUT,
		Params: &structpb.Struct{},
	})
	if err != nil {
		t.Fatalf("seed req: %v", err)
	}
	if _, err := msgRepo.MaterializeBatch(ctx, testOrg, []*model.CommsMessage{{
		OrganizationID: testOrg, RequestID: req.ID, Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SES,
		Recipient: "a@example.com", Status: pb.MessageStatus_MESSAGE_STATUS_SENT,
		VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK,
		DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
	}}); err != nil {
		t.Fatalf("seed msg: %v", err)
	}

	resp, err := svc.ListMessages(ctx, &pb.ListMessagesRequest{OrganizationId: testOrg})
	if err != nil {
		t.Fatalf("ListMessages must not fail when only usecase resolution errors, got: %v", err)
	}
	if len(resp.GetMessages()) != 1 {
		t.Fatalf("want the message to still be returned, got %d", len(resp.GetMessages()))
	}
	if got := resp.GetMessages()[0].GetUsecase(); got != "" {
		t.Fatalf("want unresolved (UNSPECIFIED) usecase on the degrade path, got %v", got)
	}
}

// TestGetMessageEventsTimeline proves the per-message event drill-down returns
// the appended provider events oldest-first.
func TestGetMessageEventsTimeline(t *testing.T) {
	ctx := context.Background()
	tx := testDB.Begin()
	t.Cleanup(func() { tx.Rollback() })
	reqRepo := dao.NewRequestRepo(tx)
	msgRepo := dao.NewMessageRepo(tx)
	evRepo := dao.NewDeliveryEventRepo(tx)
	svc := api.NewService(api.Deps{EventDAO: evRepo})

	_, req, err := reqRepo.CreateIfAbsent(ctx, &model.CommsRequest{
		OrganizationID: testOrg, IdempotencyKey: "ev:1", Usecase: templates.UsecaseNotice,
		DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, Status: pb.RequestStatus_REQUEST_STATUS_FANNED_OUT,
		Params: &structpb.Struct{},
	})
	if err != nil {
		t.Fatalf("seed req: %v", err)
	}
	msg := &model.CommsMessage{
		OrganizationID: testOrg, RequestID: req.ID, Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SES,
		Recipient: "a@example.com", Status: pb.MessageStatus_MESSAGE_STATUS_SENT,
		VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK,
		DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
	}
	if _, err := msgRepo.MaterializeBatch(ctx, testOrg, []*model.CommsMessage{msg}); err != nil {
		t.Fatalf("seed msg: %v", err)
	}
	stored, err := msgRepo.ListByRequest(ctx, testOrg, req.ID)
	if err != nil || len(stored) != 1 {
		t.Fatalf("read back msg: %v (n=%d)", err, len(stored))
	}
	now := time.Now().UTC()
	for _, et := range []pb.DeliveryEventType{
		pb.DeliveryEventType_DELIVERY_EVENT_TYPE_SENT,
		pb.DeliveryEventType_DELIVERY_EVENT_TYPE_DELIVERED,
	} {
		if err := evRepo.Append(ctx, &model.CommsDeliveryEvent{
			OrganizationID: testOrg, MessageID: stored[0].ID, EventType: et, OccurredAt: now,
		}); err != nil {
			t.Fatalf("append event %v: %v", et, err)
		}
		now = now.Add(time.Second)
	}
	resp, err := svc.GetMessageEvents(ctx, &pb.GetMessageEventsRequest{OrganizationId: testOrg, MessageId: stored[0].ID})
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	if len(resp.GetEvents()) != 2 {
		t.Fatalf("want 2 events, got %d", len(resp.GetEvents()))
	}
	if resp.GetEvents()[0].GetEventType() != pb.DeliveryEventType_DELIVERY_EVENT_TYPE_SENT {
		t.Errorf("timeline must be oldest-first: want SENT first, got %v", resp.GetEvents()[0].GetEventType())
	}
}

// failingUsecases is a request DAO whose usecase lookup always fails.
type failingUsecases struct{ dao.IRequestDAO }

func (failingUsecases) GetUsecasesByIDs(context.Context, string, []string) (map[string]string, error) {
	return nil, errors.New("db unavailable")
}

// TestGetCommsStatusPartialStatuses pins which terminal statuses make a request
// partial: every non-success end, UNCONFIRMED included, next to a DELIVERED row.
func TestGetCommsStatusPartialStatuses(t *testing.T) {
	for _, st := range []pb.MessageStatus{
		pb.MessageStatus_MESSAGE_STATUS_UNCONFIRMED, pb.MessageStatus_MESSAGE_STATUS_DEAD,
		pb.MessageStatus_MESSAGE_STATUS_BOUNCED, pb.MessageStatus_MESSAGE_STATUS_SUPPRESSED,
		pb.MessageStatus_MESSAGE_STATUS_SKIPPED, pb.MessageStatus_MESSAGE_STATUS_SENT,
	} {
		t.Run(st.String(), func(t *testing.T) {
			ctx := context.Background()
			tx := testDB.Begin()
			t.Cleanup(func() { tx.Rollback() })
			reqRepo, msgRepo := dao.NewRequestRepo(tx), dao.NewMessageRepo(tx)
			svc := api.NewService(api.Deps{RequestDAO: reqRepo, MessageDAO: msgRepo})
			_, req, err := reqRepo.CreateIfAbsent(ctx, &model.CommsRequest{
				OrganizationID: testOrg, IdempotencyKey: "partial:1", Usecase: templates.UsecaseNotice,
				DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, Status: pb.RequestStatus_REQUEST_STATUS_FANNED_OUT,
				Params: &structpb.Struct{},
			})
			if err != nil {
				t.Fatal(err)
			}
			mk := func(recipient string, s pb.MessageStatus) *model.CommsMessage {
				return &model.CommsMessage{
					OrganizationID: testOrg, RequestID: req.ID, Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SES,
					Recipient: recipient, Status: s, VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK,
					DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
				}
			}
			if _, err := msgRepo.MaterializeBatch(ctx, testOrg, []*model.CommsMessage{
				mk("a@example.com", pb.MessageStatus_MESSAGE_STATUS_DELIVERED), mk("b@example.com", st),
			}); err != nil {
				t.Fatal(err)
			}
			resp, err := svc.GetCommsStatus(ctx, &pb.GetCommsStatusRequest{OrganizationId: testOrg, Handle: "partial:1"})
			if err != nil {
				t.Fatal(err)
			}
			want := st != pb.MessageStatus_MESSAGE_STATUS_SENT
			if resp.GetIsPartial() != want {
				t.Fatalf("is_partial = %v, want %v", resp.GetIsPartial(), want)
			}
		})
	}
}

// TestListMessagesLastPageAtTheLimit pins has_more at the boundary: a history of
// exactly page_size rows is one page with has_more false.
func TestListMessagesLastPageAtTheLimit(t *testing.T) {
	ctx := context.Background()
	tx := testDB.Begin()
	t.Cleanup(func() { tx.Rollback() })
	reqRepo, msgRepo := dao.NewRequestRepo(tx), dao.NewMessageRepo(tx)
	svc := api.NewService(api.Deps{MessageDAO: msgRepo, RequestDAO: reqRepo})
	_, req, err := reqRepo.CreateIfAbsent(ctx, &model.CommsRequest{
		OrganizationID: testOrg, IdempotencyKey: "hist:exact", Usecase: templates.UsecaseNotice,
		DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, Status: pb.RequestStatus_REQUEST_STATUS_FANNED_OUT,
		Params: &structpb.Struct{},
	})
	if err != nil {
		t.Fatal(err)
	}
	var rows []*model.CommsMessage
	for _, r := range []string{"a@example.com", "b@example.com"} {
		rows = append(rows, &model.CommsMessage{
			OrganizationID: testOrg, RequestID: req.ID, Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SES,
			Recipient: r, Status: pb.MessageStatus_MESSAGE_STATUS_SENT,
			VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
		})
	}
	if _, err := msgRepo.MaterializeBatch(ctx, testOrg, rows); err != nil {
		t.Fatal(err)
	}
	page, err := svc.ListMessages(ctx, &pb.ListMessagesRequest{OrganizationId: testOrg, PageSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.GetMessages()) != 2 || page.GetHasMore() {
		t.Fatalf("want 2 rows and has_more=false, got %d rows has_more=%v", len(page.GetMessages()), page.GetHasMore())
	}
}

// TestSubmitCommsSizeCapsAccept pins the accepting side of the size caps: a
// 1024-byte recipient and a 255-byte idempotency key are fine.
func TestSubmitCommsSizeCapsAccept(t *testing.T) {
	svc, _ := newService(t)
	_, err := svc.SubmitComms(context.Background(), &pb.SubmitCommsRequest{
		OrganizationId: testOrg, IdempotencyKey: strings.Repeat("k", 255), Usecase: templates.UsecaseNotice,
		Entries: []*pb.CommsEntry{{
			Channel: pb.Channel_CHANNEL_SLACK, TargetKind: pb.TargetKind_TARGET_KIND_SLACK_CHANNEL,
			Recipient: strings.Repeat("C", 1024), SlackDelivery: pb.SlackDelivery_SLACK_DELIVERY_LINK,
		}},
	})
	if err != nil {
		t.Fatalf("at-cap submit rejected: %v", err)
	}
}

// TestHistoryRejectsBadInput pins that malformed history requests are the
// caller's error, INVALID_ARGUMENT, never INTERNAL.
func TestHistoryRejectsBadInput(t *testing.T) {
	tx := testDB.Begin()
	t.Cleanup(func() { tx.Rollback() })
	svc := api.NewService(api.Deps{MessageDAO: dao.NewMessageRepo(tx), RequestDAO: dao.NewRequestRepo(tx), EventDAO: dao.NewDeliveryEventRepo(tx)})
	ctx := context.Background()
	tests := []struct {
		name string
		call func() error
	}{
		{"ListMessages without an organization", func() error {
			_, err := svc.ListMessages(ctx, &pb.ListMessagesRequest{})
			return err
		}},
		{"ListMessages with a non-UUID organization", func() error {
			_, err := svc.ListMessages(ctx, &pb.ListMessagesRequest{OrganizationId: "nope"})
			return err
		}},
		{"ListMessages with a cursor time but no id", func() error {
			_, err := svc.ListMessages(ctx, &pb.ListMessagesRequest{OrganizationId: testOrg, BeforeCreatedAt: timestamppb.Now()})
			return err
		}},
		{"ListMessages with a cursor id but no time", func() error {
			_, err := svc.ListMessages(ctx, &pb.ListMessagesRequest{OrganizationId: testOrg, BeforeId: "99999999-9999-9999-9999-999999999999"})
			return err
		}},
		{"ListMessages with a non-UUID cursor id", func() error {
			_, err := svc.ListMessages(ctx, &pb.ListMessagesRequest{OrganizationId: testOrg, BeforeCreatedAt: timestamppb.Now(), BeforeId: "nope"})
			return err
		}},
		{"GetMessageEvents with a non-UUID message id", func() error {
			_, err := svc.GetMessageEvents(ctx, &pb.GetMessageEventsRequest{OrganizationId: testOrg, MessageId: "nope"})
			return err
		}},
		{"GetMessageEvents with a non-UUID organization", func() error {
			_, err := svc.GetMessageEvents(ctx, &pb.GetMessageEventsRequest{OrganizationId: "nope", MessageId: "99999999-9999-9999-9999-999999999999"})
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := status.Code(tt.call()); got != codes.InvalidArgument {
				t.Fatalf("code = %v, want InvalidArgument", got)
			}
		})
	}
}

// TestProtoRulesRejectBadInput pins the buf.validate rules the gRPC
// interceptor applies before any handler runs, for the same inputs the
// handlers reject.
func TestProtoRulesRejectBadInput(t *testing.T) {
	v, err := protovalidate.New()
	if err != nil {
		t.Fatal(err)
	}
	ok := func() *pb.SubmitCommsRequest {
		return &pb.SubmitCommsRequest{OrganizationId: testOrg, IdempotencyKey: "k", Usecase: templates.UsecaseNotice,
			Entries: []*pb.CommsEntry{emailEntry("dev@example.com")}}
	}
	if err := v.Validate(ok()); err != nil {
		t.Fatalf("a good request fails the proto rules: %v", err)
	}
	longKey := ok()
	longKey.IdempotencyKey = strings.Repeat("k", 256)
	longRecipient := ok()
	longRecipient.Entries[0].Recipient = strings.Repeat("a", 1025)
	badScope := ok()
	badScope.Entries = []*pb.CommsEntry{{Channel: pb.Channel_CHANNEL_SLACK, TargetKind: pb.TargetKind_TARGET_KIND_CONFIGURED_DESTINATION,
		DestinationScope: &pb.DestinationScope{EntityType: pb.EntityType_ENTITY_TYPE_ORG, EntityId: "not-a-uuid"}}}
	for name, m := range map[string]proto.Message{
		"idempotency key over 255 bytes":                longKey,
		"recipient over 1024 bytes":                     longRecipient,
		"destination scope entity_id not a UUID":        badScope,
		"ListMessages without an organization":          &pb.ListMessagesRequest{},
		"ListMessages with a non-UUID cursor id":        &pb.ListMessagesRequest{OrganizationId: testOrg, BeforeId: "nope"},
		"GetMessageEvents with a non-UUID message":      &pb.GetMessageEventsRequest{OrganizationId: testOrg, MessageId: "nope"},
		"GetMessageEvents with a non-UUID organization": &pb.GetMessageEventsRequest{OrganizationId: "nope", MessageId: "99999999-9999-9999-9999-999999999999"},
	} {
		if err := v.Validate(m); err == nil {
			t.Errorf("%s: passes the proto rules", name)
		}
	}
}

// TestPurgeKeepsSuppression pins design choice 4: a suppression names an
// address, not an organization, so purging the organization that bounced it
// leaves the address suppressed.
func TestPurgeKeepsSuppression(t *testing.T) {
	ctx := context.Background()
	email := fmt.Sprintf("purge-sup-%d@example.com", time.Now().UnixNano())
	t.Cleanup(func() { testDB.Exec("DELETE FROM comms_suppressions WHERE email = ?", email) })
	sup := dao.NewSuppressionRepo(testDB)
	if err := sup.SuppressIfAbsent(ctx, &model.CommsSuppression{Email: email, Reason: "hard_bounce"}); err != nil {
		t.Fatal(err)
	}
	svc := api.NewService(api.Deps{ReconcileDAO: dao.NewReconcileRepo(testDB)})
	if _, err := svc.PurgeOrgComms(ctx, &pb.PurgeOrgCommsRequest{OrganizationId: testOrg}); err != nil {
		t.Fatal(err)
	}
	if got, err := sup.GetActiveByEmail(ctx, email); err != nil || got == nil {
		t.Fatalf("after the purge the address is not suppressed (%v, %v)", got, err)
	}
}

// TestListMessagesPageCap pins the page cap: asking for 500 rows returns 200,
// with has_more set.
func TestListMessagesPageCap(t *testing.T) {
	ctx := context.Background()
	tx := testDB.Begin()
	t.Cleanup(func() { tx.Rollback() })
	reqRepo, msgRepo := dao.NewRequestRepo(tx), dao.NewMessageRepo(tx)
	svc := api.NewService(api.Deps{MessageDAO: msgRepo, RequestDAO: reqRepo})
	_, req, err := reqRepo.CreateIfAbsent(ctx, &model.CommsRequest{
		OrganizationID: testOrg, IdempotencyKey: "hist:cap", Usecase: templates.UsecaseNotice,
		DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, Status: pb.RequestStatus_REQUEST_STATUS_FANNED_OUT,
		Params: &structpb.Struct{},
	})
	if err != nil {
		t.Fatal(err)
	}
	var rows []*model.CommsMessage
	for i := range 201 {
		rows = append(rows, &model.CommsMessage{
			OrganizationID: testOrg, RequestID: req.ID, Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SES,
			Recipient: fmt.Sprintf("cap%d@example.com", i), Status: pb.MessageStatus_MESSAGE_STATUS_SENT,
			VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
		})
	}
	if _, err := msgRepo.MaterializeBatch(ctx, testOrg, rows); err != nil {
		t.Fatal(err)
	}
	page, err := svc.ListMessages(ctx, &pb.ListMessagesRequest{OrganizationId: testOrg, PageSize: 500})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.GetMessages()) != 200 || !page.GetHasMore() {
		t.Fatalf("want 200 rows and has_more, got %d rows has_more=%v", len(page.GetMessages()), page.GetHasMore())
	}
}

// failingPreferences is an IPreferenceDAO whose lookup always fails.
type failingPreferences struct{ dao.IPreferenceDAO }

func (failingPreferences) IsEnabled(context.Context, string, string, pb.MessageCategory, pb.Channel) (bool, error) {
	return false, errors.New("connection reset by peer")
}

// TestCheckSendEligibilityFailsClosed pins that the preflight never reports a
// send eligible it could not check: a failed preference lookup marks the
// channel ineligible with preference_lookup_failed, and a person_id that is not
// a UUID is INVALID_ARGUMENT, in the handler and in the proto rules.
func TestCheckSendEligibilityFailsClosed(t *testing.T) {
	ctx := context.Background()
	svc := api.NewService(api.Deps{OrgGate: fakeOrgGate{active: true}, Gate: allowGate{enabled: true}, PreferenceDAO: failingPreferences{}})
	resp, err := svc.CheckSendEligibility(ctx, &pb.CheckSendEligibilityRequest{
		OrganizationId: testOrg, Usecase: templates.UsecaseDigest, PersonId: "22222222-2222-2222-2222-222222222222",
		Channels: []pb.Channel{pb.Channel_CHANNEL_EMAIL},
	})
	if err != nil {
		t.Fatal(err)
	}
	ce := resp.GetChannelEligibility()[0]
	if ce.GetEligible() || strings.Join(ce.GetReasons(), ",") != "preference_lookup_failed" {
		t.Fatalf("eligible=%v reasons=%v, want false [preference_lookup_failed]", ce.GetEligible(), ce.GetReasons())
	}

	bad := &pb.CheckSendEligibilityRequest{OrganizationId: testOrg, Usecase: templates.UsecaseDigest, PersonId: "jane", Channels: []pb.Channel{pb.Channel_CHANNEL_EMAIL}}
	if _, err := svc.CheckSendEligibility(ctx, bad); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("person_id \"jane\": %v, want InvalidArgument", err)
	}
	v, err := protovalidate.New()
	if err != nil {
		t.Fatal(err)
	}
	if err := v.Validate(bad); err == nil {
		t.Fatal("person_id \"jane\" passes the proto rules")
	}
	if err := v.Validate(&pb.CheckSendEligibilityRequest{OrganizationId: testOrg, Usecase: "digest"}); err != nil {
		t.Fatalf("an empty person_id fails the proto rules: %v", err)
	}
}

// failingConnections is a ConnectionChecker whose lookup always fails.
type failingConnections struct{}

func (failingConnections) IsConnected(context.Context, string, pb.Channel) (bool, error) {
	return false, errors.New("token store unavailable")
}

// failingDestinations is an IDestinationDAO whose resolve always fails.
type failingDestinations struct{ dao.IDestinationDAO }

func (failingDestinations) Resolve(context.Context, string, pb.EntityType, string, string, pb.Channel) ([]string, error) {
	return nil, errors.New("connection reset by peer")
}

// TestCheckSendEligibilitySlackLookupsFailClosed pins that a Slack channel
// whose connection or destination lookup fails is ineligible, with a reason
// naming the lookup.
func TestCheckSendEligibilitySlackLookupsFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name string
		deps api.Deps
		want string
	}{
		{"connection", api.Deps{ConnChecker: failingConnections{}}, "connection_lookup_failed"},
		{"destination", api.Deps{DestinationDAO: failingDestinations{}}, "destination_lookup_failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.deps.OrgGate, tc.deps.Gate = fakeOrgGate{active: true}, allowGate{enabled: true}
			resp, err := api.NewService(tc.deps).CheckSendEligibility(context.Background(), &pb.CheckSendEligibilityRequest{
				OrganizationId: testOrg, Usecase: templates.UsecaseDigest, Channels: []pb.Channel{pb.Channel_CHANNEL_SLACK}, TargetKind: pb.TargetKind_TARGET_KIND_CONFIGURED_DESTINATION,
			})
			if err != nil {
				t.Fatal(err)
			}
			ce := resp.GetChannelEligibility()[0]
			if ce.GetEligible() || strings.Join(ce.GetReasons(), ",") != tc.want {
				t.Fatalf("eligible=%v reasons=%v, want false [%s]", ce.GetEligible(), ce.GetReasons(), tc.want)
			}
		})
	}
}

// TestPreferenceAndDestinationRejections pins that a preference needs a real
// category and channel, and a destination is Slack only: anything else is
// INVALID_ARGUMENT from the handler and from the proto rules, and nothing is
// stored.
func TestPreferenceAndDestinationRejections(t *testing.T) {
	ctx := context.Background()
	tx := testDB.Begin()
	t.Cleanup(func() { tx.Rollback() })
	svc := api.NewService(api.Deps{PreferenceDAO: dao.NewPreferenceRepo(tx), DestinationDAO: dao.NewDestinationRepo(tx)})
	v, err := protovalidate.New()
	if err != nil {
		t.Fatal(err)
	}
	const person = "22222222-2222-2222-2222-222222222222"
	pref := func(c pb.MessageCategory, ch pb.Channel) []*pb.Preference {
		return []*pb.Preference{{Category: c, Channel: ch, Enabled: false}}
	}
	bad := map[string][]*pb.Preference{
		"unspecified category": pref(pb.MessageCategory_MESSAGE_CATEGORY_UNSPECIFIED, pb.Channel_CHANNEL_EMAIL),
		"unspecified channel":  pref(pb.MessageCategory_MESSAGE_CATEGORY_PRODUCT, pb.Channel_CHANNEL_UNSPECIFIED),
		"undefined category":   pref(pb.MessageCategory(99), pb.Channel_CHANNEL_EMAIL),
		"undefined channel":    pref(pb.MessageCategory_MESSAGE_CATEGORY_PRODUCT, pb.Channel(99)),
	}
	for name, prefs := range bad {
		person := &pb.UpdatePersonPreferencesRequest{OrganizationId: testOrg, PersonId: person, Preferences: prefs}
		if _, err := svc.UpdatePersonPreferences(ctx, person); status.Code(err) != codes.InvalidArgument {
			t.Errorf("person, %s: %v, want InvalidArgument", name, err)
		}
		if v.Validate(person) == nil {
			t.Errorf("person, %s: passes the proto rules", name)
		}
		org := &pb.SetOrgPreferencesRequest{OrganizationId: testOrg, Preferences: prefs}
		if _, err := svc.SetOrgPreferences(ctx, org); status.Code(err) != codes.InvalidArgument {
			t.Errorf("org, %s: %v, want InvalidArgument", name, err)
		}
		if v.Validate(org) == nil {
			t.Errorf("org, %s: passes the proto rules", name)
		}
	}
	for _, ch := range []pb.Channel{pb.Channel_CHANNEL_UNSPECIFIED, pb.Channel_CHANNEL_EMAIL, pb.Channel(3), pb.Channel(99)} {
		req := &pb.SetDestinationRequest{OrganizationId: testOrg, Destination: &pb.Destination{Usecase: "alert", Channel: ch, DestinationRef: "C0X"}}
		if _, err := svc.SetDestination(ctx, req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("destination on %v: %v, want InvalidArgument", ch, err)
		}
		if v.Validate(req) == nil {
			t.Errorf("destination on %v passes the proto rules", ch)
		}
	}
	var stored int64
	tx.Model(&model.CommsPreference{}).Where("organization_id = ?", testOrg).Count(&stored)
	var dests int64
	tx.Model(&model.CommsChannelDestination{}).Where("organization_id = ?", testOrg).Count(&dests)
	if stored != 0 || dests != 0 {
		t.Errorf("stored %d preferences and %d destinations, want none", stored, dests)
	}
}

// TestRequireUUIDForm pins that the handler takes a UUID only in the
// 36-character hyphenated form, in either case, as the proto rules do: braced,
// URN and unhyphenated spellings are INVALID_ARGUMENT from both.
func TestRequireUUIDForm(t *testing.T) {
	svc := api.NewService(api.Deps{OrgGate: fakeOrgGate{active: true}, Gate: allowGate{enabled: true}})
	v, err := protovalidate.New()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		id string
		ok bool
	}{
		{"22222222-2222-2222-2222-222222222222", true},
		{"ABCDEFAB-2222-2222-2222-222222222222", true},
		{"{22222222-2222-2222-2222-222222222222}", false},
		{"urn:uuid:22222222-2222-2222-2222-222222222222", false},
		{"22222222222222222222222222222222", false},
	} {
		t.Run(tc.id, func(t *testing.T) {
			req := &pb.CheckSendEligibilityRequest{OrganizationId: testOrg, Usecase: templates.UsecaseDigest, PersonId: tc.id, Channels: []pb.Channel{pb.Channel_CHANNEL_EMAIL}}
			_, err := svc.CheckSendEligibility(context.Background(), req)
			if got := status.Code(err) != codes.InvalidArgument; got != tc.ok {
				t.Fatalf("handler: %v, want accepted=%v", err, tc.ok)
			}
			if got := v.Validate(req) == nil; got != tc.ok {
				t.Fatalf("proto rules accepted=%v, want %v", got, tc.ok)
			}
		})
	}
}

// TestEligibilityReasons pins each reason the preflight gives, alone, and the
// transactional bypass: a transactional usecase stays eligible over a stored
// opt-out and over a preference store that fails.
func TestEligibilityReasons(t *testing.T) {
	ctx := context.Background()
	tx := testDB.Begin()
	t.Cleanup(func() { tx.Rollback() })
	prefs := dao.NewPreferenceRepo(tx)
	for _, cat := range []pb.MessageCategory{pb.MessageCategory_MESSAGE_CATEGORY_PRODUCT, pb.MessageCategory_MESSAGE_CATEGORY_TRANSACTIONAL} {
		if err := prefs.Upsert(ctx, &model.CommsPreference{
			OrganizationID: testOrg, EntityType: pb.EntityType_ENTITY_TYPE_ORG, EntityID: testOrg,
			Category: cat, Channel: pb.Channel_CHANNEL_EMAIL, Enabled: false,
		}); err != nil {
			t.Fatal(err)
		}
	}
	healthy := fakeOrgGate{active: true}
	for _, tc := range []struct {
		name    string
		deps    api.Deps
		usecase string
		ch      pb.Channel
		want    string
	}{
		{"no_connection", api.Deps{ConnChecker: sender.Connections{Tokens: sender.StaticTokens{}}}, templates.UsecaseNotice, pb.Channel_CHANNEL_SLACK, "no_connection"},
		{"no_destination", api.Deps{DestinationDAO: dao.NewDestinationRepo(tx)}, templates.UsecaseNotice, pb.Channel_CHANNEL_SLACK, "no_destination"},
		{"opted_out", api.Deps{PreferenceDAO: prefs}, templates.UsecaseDigest, pb.Channel_CHANNEL_EMAIL, "opted_out"},
		{"transactional over a stored opt-out", api.Deps{PreferenceDAO: prefs}, templates.UsecaseNotice, pb.Channel_CHANNEL_EMAIL, ""},
		{"transactional over a failing store", api.Deps{PreferenceDAO: failingPreferences{}}, templates.UsecaseNotice, pb.Channel_CHANNEL_EMAIL, ""},
		{"org_inactive", api.Deps{OrgGate: fakeOrgGate{active: false}}, templates.UsecaseNotice, pb.Channel_CHANNEL_EMAIL, "org_inactive"},
		{"org_frozen", api.Deps{OrgGate: fakeOrgGate{active: true, frozen: true}}, templates.UsecaseNotice, pb.Channel_CHANNEL_EMAIL, "org_frozen"},
		{"channel_not_enabled", api.Deps{}, templates.UsecaseNotice, pb.Channel(3), "channel_not_enabled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.deps.OrgGate == nil {
				tc.deps.OrgGate = healthy
			}
			tc.deps.Gate = allowGate{enabled: true}
			// A Slack check asks about the configured destination; the other
			// channels name their target, which a configured destination cannot be.
			kind := pb.TargetKind_TARGET_KIND_UNSPECIFIED
			if tc.ch == pb.Channel_CHANNEL_SLACK {
				kind = pb.TargetKind_TARGET_KIND_CONFIGURED_DESTINATION
			}
			resp, err := api.NewService(tc.deps).CheckSendEligibility(ctx, &pb.CheckSendEligibilityRequest{
				OrganizationId: testOrg, Usecase: tc.usecase, Channels: []pb.Channel{tc.ch}, TargetKind: kind,
			})
			if err != nil {
				t.Fatal(err)
			}
			ce := resp.GetChannelEligibility()[0]
			if got := strings.Join(ce.GetReasons(), ","); got != tc.want || ce.GetEligible() != (tc.want == "") {
				t.Fatalf("eligible=%v reasons=%q, want eligible=%v reasons=%q", ce.GetEligible(), got, tc.want == "", tc.want)
			}
		})
	}
}

// TestProtoRulesRejectNonUUIDOrg pins the organization_id rule on every request
// beyond the two history RPCs: each request passes the rules with a UUID and
// fails them with "nope", so the failure is the organization's.
func TestProtoRulesRejectNonUUIDOrg(t *testing.T) {
	v, err := protovalidate.New()
	if err != nil {
		t.Fatal(err)
	}
	person := "22222222-2222-2222-2222-222222222222"
	for name, build := range map[string]func(org string) proto.Message{
		"CheckSendEligibility": func(o string) proto.Message {
			return &pb.CheckSendEligibilityRequest{OrganizationId: o, Usecase: templates.UsecaseNotice}
		},
		"GetCommsStatus": func(o string) proto.Message { return &pb.GetCommsStatusRequest{OrganizationId: o, Handle: "h"} },
		"GetPersonPreferences": func(o string) proto.Message {
			return &pb.GetPersonPreferencesRequest{OrganizationId: o, PersonId: person}
		},
		"UpdatePersonPreferences": func(o string) proto.Message {
			return &pb.UpdatePersonPreferencesRequest{OrganizationId: o, PersonId: person}
		},
		"GetOrgPreferences": func(o string) proto.Message { return &pb.GetOrgPreferencesRequest{OrganizationId: o} },
		"SetOrgPreferences": func(o string) proto.Message { return &pb.SetOrgPreferencesRequest{OrganizationId: o} },
		"GetDestinations":   func(o string) proto.Message { return &pb.GetDestinationsRequest{OrganizationId: o} },
		"SetDestination": func(o string) proto.Message {
			return &pb.SetDestinationRequest{OrganizationId: o, Destination: &pb.Destination{
				Usecase: templates.UsecaseDigest, Channel: pb.Channel_CHANNEL_SLACK, EntityType: pb.EntityType_ENTITY_TYPE_ORG,
				EntityId: testOrg, DestinationKind: pb.DestinationKind_DESTINATION_KIND_CHANNEL, DestinationRef: "C0AB12CD3",
			}}
		},
		"PurgeOrgComms": func(o string) proto.Message { return &pb.PurgeOrgCommsRequest{OrganizationId: o} },
	} {
		t.Run(name, func(t *testing.T) {
			if err := v.Validate(build(testOrg)); err != nil {
				t.Fatalf("with a UUID organization: %v", err)
			}
			if err := v.Validate(build("nope")); err == nil {
				t.Fatal(`organization_id "nope" passes the proto rules`)
			}
		})
	}
}

// mixedCaseOrg has letters, so its uppercase spelling differs from the
// canonical lowercase one.
const mixedCaseOrg = "abcdef01-2345-4678-9abc-def012345678"

// lowerKeyedGates answers for the lowercase id only: comms off and the org
// frozen. Any other spelling looks like a healthy org.
type lowerKeyedGates struct{}

func (lowerKeyedGates) Enabled(org string) bool { return org != mixedCaseOrg }

func (lowerKeyedGates) CheckOrg(_ context.Context, org string) (bool, bool, error) {
	return true, org == mixedCaseOrg, nil
}

// TestOrgIDIsCanonicalAtEntry pins that an uppercase organization id reaches
// the gates in the canonical lowercase form, so a per-org switch keyed by it
// cannot be stepped round by spelling the id differently.
func TestOrgIDIsCanonicalAtEntry(t *testing.T) {
	ctx := context.Background()
	upper := strings.ToUpper(mixedCaseOrg)
	tx := testDB.Begin()
	t.Cleanup(func() { tx.Rollback() })
	svc := api.NewService(api.Deps{Gate: lowerKeyedGates{}, OrgGate: lowerKeyedGates{}, RequestDAO: dao.NewRequestRepo(tx), Publisher: &fakePublisher{}})
	_, err := svc.SubmitComms(ctx, &pb.SubmitCommsRequest{OrganizationId: upper, IdempotencyKey: "k-upper", Usecase: templates.UsecaseNotice,
		Entries: []*pb.CommsEntry{emailEntry("dev@example.com")}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("SubmitComms with %s: %v, want FailedPrecondition", upper, err)
	}
	resp, err := svc.CheckSendEligibility(ctx, &pb.CheckSendEligibilityRequest{OrganizationId: upper, Usecase: templates.UsecaseNotice,
		Channels: []pb.Channel{pb.Channel_CHANNEL_EMAIL}})
	if err != nil {
		t.Fatal(err)
	}
	ce := resp.GetChannelEligibility()[0]
	if ce.GetEligible() || strings.Join(ce.GetReasons(), ",") != "comms_disabled,org_frozen" {
		t.Fatalf("CheckSendEligibility with %s: eligible=%v reasons=%v, want false [comms_disabled org_frozen]", upper, ce.GetEligible(), ce.GetReasons())
	}
}

// orgSeen records every spelling of one organization id that reaches a
// collaborator: the gates, the connection check, the publisher and the store's
// SQL bind values.
type orgSeen struct {
	lower string
	seen  map[string]int
}

func (o *orgSeen) note(v string) {
	if strings.EqualFold(v, o.lower) {
		o.seen[v]++
	}
}
func (o *orgSeen) Enabled(org string) bool { o.note(org); return true }
func (o *orgSeen) CheckOrg(_ context.Context, org string) (bool, bool, error) {
	o.note(org)
	return true, false, nil
}
func (o *orgSeen) IsConnected(_ context.Context, org string, _ pb.Channel) (bool, error) {
	o.note(org)
	return true, nil
}
func (o *orgSeen) PublishRequest(_ context.Context, org, _ string) error { o.note(org); return nil }

// TestEveryRPCCanonicalisesTheOrg pins item 9 across the API: for an uppercase
// and a mixed-case spelling of one organization id, all twelve RPCs that take
// one pass only the lowercase form to the gates, the connection check, the
// publisher and the store, and a row written under one spelling reads back
// under the other.
func TestEveryRPCCanonicalisesTheOrg(t *testing.T) {
	ctx := context.Background()
	const lower = "abcdef02-2345-4678-9abc-def012345678"
	db, err := gorm.Open(postgres.Open(testdb.SchemaURL()), &gorm.Config{Logger: gormlogger.Discard})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Exec("DELETE FROM comms_requests WHERE organization_id = ?", lower)
		db.Exec("DELETE FROM comms_preferences WHERE organization_id = ?", lower)
		db.Exec("DELETE FROM comms_channel_destinations WHERE organization_id = ?", lower)
		if s, err := db.DB(); err == nil {
			_ = s.Close()
		}
	})
	rec := &orgSeen{lower: lower, seen: map[string]int{}}
	sqlVars := func(tx *gorm.DB) {
		for _, v := range tx.Statement.Vars {
			if s, ok := v.(string); ok {
				rec.note(s)
			}
		}
	}
	cb := db.Callback()
	for name, err := range map[string]error{
		"create": cb.Create().After("gorm:create").Register("test:org_create", sqlVars),
		"query":  cb.Query().After("gorm:query").Register("test:org_query", sqlVars),
		"update": cb.Update().After("gorm:update").Register("test:org_update", sqlVars),
		"delete": cb.Delete().After("gorm:delete").Register("test:org_delete", sqlVars),
		"row":    cb.Row().After("gorm:row").Register("test:org_row", sqlVars),
		"raw":    cb.Raw().After("gorm:raw").Register("test:org_raw", sqlVars),
	} {
		if err != nil {
			t.Fatalf("register %s callback: %v", name, err)
		}
	}
	svc := api.NewService(api.Deps{
		RequestDAO: dao.NewRequestRepo(db), MessageDAO: dao.NewMessageRepo(db), EventDAO: dao.NewDeliveryEventRepo(db),
		PreferenceDAO: dao.NewPreferenceRepo(db), DestinationDAO: dao.NewDestinationRepo(db), SuppressionDAO: dao.NewSuppressionRepo(db),
		ReconcileDAO: dao.NewReconcileRepo(db), OrgGate: rec, Gate: rec, ConnChecker: rec, Publisher: rec,
	})
	person := "22222222-2222-2222-2222-222222222222"
	pref := []*pb.Preference{{Category: pb.MessageCategory_MESSAGE_CATEGORY_PRODUCT, Channel: pb.Channel_CHANNEL_EMAIL, Enabled: false}}
	for _, spelling := range []string{strings.ToUpper(lower), "AbCdEf02-2345-4678-9AbC-dEf012345678"} {
		other := strings.ToUpper(lower)
		if spelling == other {
			other = "aBcDeF02-2345-4678-9aBc-DeF012345678"
		}
		key := "k-canon-" + spelling
		calls := []struct {
			rpc  string
			call func(org string) error
		}{
			{"SubmitComms", func(o string) error {
				_, err := svc.SubmitComms(ctx, &pb.SubmitCommsRequest{OrganizationId: o, IdempotencyKey: key, Usecase: templates.UsecaseNotice, Entries: []*pb.CommsEntry{emailEntry("dev@example.com")}})
				return err
			}},
			{"GetCommsStatus", func(o string) error {
				_, err := svc.GetCommsStatus(ctx, &pb.GetCommsStatusRequest{OrganizationId: o, Handle: key})
				return err
			}},
			{"CheckSendEligibility", func(o string) error {
				_, err := svc.CheckSendEligibility(ctx, &pb.CheckSendEligibilityRequest{OrganizationId: o, Usecase: templates.UsecaseDigest, PersonId: person, Channels: []pb.Channel{pb.Channel_CHANNEL_EMAIL, pb.Channel_CHANNEL_SLACK}})
				return err
			}},
			{"ListMessages", func(o string) error {
				_, err := svc.ListMessages(ctx, &pb.ListMessagesRequest{OrganizationId: o})
				return err
			}},
			{"GetMessageEvents", func(o string) error {
				_, err := svc.GetMessageEvents(ctx, &pb.GetMessageEventsRequest{OrganizationId: o, MessageId: "99999999-9999-9999-9999-999999999999"})
				if status.Code(err) == codes.NotFound {
					return nil
				}
				return err
			}},
			{"UpdatePersonPreferences", func(o string) error {
				_, err := svc.UpdatePersonPreferences(ctx, &pb.UpdatePersonPreferencesRequest{OrganizationId: o, PersonId: person, Preferences: pref})
				return err
			}},
			{"GetPersonPreferences", func(o string) error {
				_, err := svc.GetPersonPreferences(ctx, &pb.GetPersonPreferencesRequest{OrganizationId: o, PersonId: person})
				return err
			}},
			{"SetOrgPreferences", func(o string) error {
				_, err := svc.SetOrgPreferences(ctx, &pb.SetOrgPreferencesRequest{OrganizationId: o, Preferences: pref})
				return err
			}},
			{"GetOrgPreferences", func(o string) error {
				_, err := svc.GetOrgPreferences(ctx, &pb.GetOrgPreferencesRequest{OrganizationId: o})
				return err
			}},
			{"SetDestination", func(o string) error {
				_, err := svc.SetDestination(ctx, &pb.SetDestinationRequest{OrganizationId: o, Destination: &pb.Destination{
					Usecase: templates.UsecaseDigest, Channel: pb.Channel_CHANNEL_SLACK, EntityType: pb.EntityType_ENTITY_TYPE_ORG,
					EntityId: lower, DestinationKind: pb.DestinationKind_DESTINATION_KIND_CHANNEL, DestinationRef: "C0AB12CD3",
				}})
				return err
			}},
			{"GetDestinations", func(o string) error {
				_, err := svc.GetDestinations(ctx, &pb.GetDestinationsRequest{OrganizationId: o})
				return err
			}},
		}
		readBack := func(string) error {
			st, err := svc.GetCommsStatus(ctx, &pb.GetCommsStatusRequest{OrganizationId: other, Handle: key})
			if err != nil || st.GetHandle() != key {
				return fmt.Errorf("GetCommsStatus under %s: %v, handle %q", other, err, st.GetHandle())
			}
			prefs, err := svc.GetOrgPreferences(ctx, &pb.GetOrgPreferencesRequest{OrganizationId: other})
			if err != nil || len(prefs.GetPreferences()) != 1 {
				return fmt.Errorf("GetOrgPreferences under %s: %v, %d preferences", other, err, len(prefs.GetPreferences()))
			}
			var stored []string
			db.Raw("SELECT organization_id::text FROM comms_requests WHERE idempotency_key = ?", key).Scan(&stored)
			if len(stored) != 1 || stored[0] != lower {
				return fmt.Errorf("request stored under %v, want [%s]", stored, lower)
			}
			return nil
		}
		calls = append(calls,
			struct {
				rpc  string
				call func(org string) error
			}{"read back under " + other, readBack},
			struct {
				rpc  string
				call func(org string) error
			}{"PurgeOrgComms", func(o string) error {
				_, err := svc.PurgeOrgComms(ctx, &pb.PurgeOrgCommsRequest{OrganizationId: o})
				return err
			}})
		for _, c := range calls {
			t.Run(spelling+"/"+c.rpc, func(t *testing.T) {
				rec.seen = map[string]int{}
				if err := c.call(spelling); err != nil {
					t.Fatalf("%s: %v", c.rpc, err)
				}
				for v, n := range rec.seen {
					if v != lower {
						t.Errorf("%s passed %q to a collaborator %d time(s), want %q", c.rpc, v, n, lower)
					}
				}
				if rec.seen[lower] == 0 {
					t.Errorf("%s passed the organization to no collaborator; the check saw nothing", c.rpc)
				}
			})
		}
	}
}

// TestSubmitCommsKeyReuse pins what a reused idempotency key does. The same
// request again returns the same handle and stores and enqueues nothing new. A
// request that differs in usecase, params, content, entries or delivery
// guarantee is ALREADY_EXISTS, and nothing about it is stored or enqueued.
func TestSubmitCommsKeyReuse(t *testing.T) {
	ctx := context.Background()
	svc, pub, tx := newServiceTx(t)
	base := func() *pb.SubmitCommsRequest {
		return &pb.SubmitCommsRequest{
			OrganizationId: testOrg, IdempotencyKey: "notice:reuse", Usecase: templates.UsecaseNotice,
			Content: &pb.MessageContent{Base: &pb.RenderableContent{Subject: "Export ready"}},
			Entries: []*pb.CommsEntry{emailEntry("dev@example.com")},
		}
	}
	first, err := svc.SubmitComms(ctx, base())
	if err != nil {
		t.Fatal(err)
	}
	same := base()
	same.DeliveryGuarantee = pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED // the usecase's default, spelled out
	again, err := svc.SubmitComms(ctx, same)
	if err != nil || again.GetHandle() != first.GetHandle() {
		t.Fatalf("identical resubmit: %v, handle %q; want the same handle %q", err, again.GetHandle(), first.GetHandle())
	}
	for name, change := range map[string]func(r *pb.SubmitCommsRequest){
		"usecase": func(r *pb.SubmitCommsRequest) { r.Usecase = templates.UsecaseAlert },
		"params": func(r *pb.SubmitCommsRequest) {
			r.Params, _ = structpb.NewStruct(map[string]any{"title": "other"})
		},
		"content": func(r *pb.SubmitCommsRequest) { r.Content.Base.Subject = "Export failed" },
		"entries": func(r *pb.SubmitCommsRequest) { r.Entries = []*pb.CommsEntry{emailEntry("other@example.com")} },
		"guarantee": func(r *pb.SubmitCommsRequest) {
			r.DeliveryGuarantee = pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := base()
			change(r)
			_, err := svc.SubmitComms(ctx, r)
			if status.Code(err) != codes.AlreadyExists || !strings.Contains(status.Convert(err).Message(), "idempotency key") {
				t.Fatalf("same key, different %s: %v; want AlreadyExists naming the idempotency key", name, err)
			}
		})
	}
	var rows int64
	tx.Model(&model.CommsRequest{}).Where("organization_id = ? AND idempotency_key = ?", testOrg, "notice:reuse").Count(&rows)
	var stored model.CommsRequest
	tx.Where("organization_id = ? AND idempotency_key = ?", testOrg, "notice:reuse").First(&stored)
	if rows != 1 || pub.calls != 1 || stored.Content.GetBase().GetSubject() != "Export ready" {
		t.Fatalf("%d rows, %d enqueues, stored subject %q; want 1, 1, the first request's", rows, pub.calls, stored.Content.GetBase().GetSubject())
	}
}

// TestDestinationScopes pins that SetDestination writes the scope it is given.
// An ORG destination and a PERSON destination for the same usecase and channel
// live side by side, GetDestinations returns both with their scopes, and each
// resolves for its own scope. Any other entity type (an undefined 3 included), an
// ORG id that is not the organization, and a PERSON id that is not a UUID are
// INVALID_ARGUMENT.
func TestDestinationScopes(t *testing.T) {
	ctx := context.Background()
	tx := testDB.Begin()
	t.Cleanup(func() { tx.Rollback() })
	dests := dao.NewDestinationRepo(tx)
	svc := api.NewService(api.Deps{DestinationDAO: dests})
	person := "22222222-2222-2222-2222-222222222222"
	set := func(et pb.EntityType, id, ref string) error {
		_, err := svc.SetDestination(ctx, &pb.SetDestinationRequest{OrganizationId: testOrg, Destination: &pb.Destination{
			EntityType: et, EntityId: id, Usecase: templates.UsecaseDigest, Channel: pb.Channel_CHANNEL_SLACK,
			DestinationKind: pb.DestinationKind_DESTINATION_KIND_CHANNEL, DestinationRef: ref,
		}})
		return err
	}
	setKind := func(kind pb.DestinationKind, ref string) error {
		_, err := svc.SetDestination(ctx, &pb.SetDestinationRequest{OrganizationId: testOrg, Destination: &pb.Destination{
			EntityType: pb.EntityType_ENTITY_TYPE_ORG, EntityId: testOrg, Usecase: templates.UsecaseDigest, Channel: pb.Channel_CHANNEL_SLACK,
			DestinationKind: kind, DestinationRef: ref,
		}})
		return err
	}
	if err := set(pb.EntityType_ENTITY_TYPE_ORG, testOrg, "C0ORG"); err != nil {
		t.Fatal(err)
	}
	if err := set(pb.EntityType_ENTITY_TYPE_PERSON, person, "C0PERSON"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		et   pb.EntityType
		id   string
		want string
	}{
		{pb.EntityType_ENTITY_TYPE_ORG, testOrg, "C0ORG"},
		{pb.EntityType_ENTITY_TYPE_PERSON, person, "C0PERSON"},
		// Each scope reads only its own tier: the other tier's id finds nothing.
		{pb.EntityType_ENTITY_TYPE_PERSON, testOrg, ""},
		{pb.EntityType_ENTITY_TYPE_ORG, person, ""},
	} {
		refs, err := dests.Resolve(ctx, testOrg, tc.et, tc.id, templates.UsecaseDigest, pb.Channel_CHANNEL_SLACK)
		if err != nil || strings.Join(refs, ",") != tc.want {
			t.Fatalf("%v destination resolves to %v, %v; want [%s]", tc.et, refs, err, tc.want)
		}
	}
	got, err := svc.GetDestinations(ctx, &pb.GetDestinationsRequest{OrganizationId: testOrg})
	if err != nil {
		t.Fatal(err)
	}
	var scopes []string
	for _, d := range got.GetDestinations() {
		scopes = append(scopes, d.GetEntityType().String()+"/"+d.GetDestinationRef())
	}
	if want := "ENTITY_TYPE_ORG/C0ORG,ENTITY_TYPE_PERSON/C0PERSON"; strings.Join(scopes, ",") != want {
		t.Fatalf("GetDestinations returned %v, want %s", scopes, want)
	}
	for name, err := range map[string]error{
		"entity type 3, undefined":        set(pb.EntityType(3), testOrg, "C0X"),
		"unspecified":                     set(pb.EntityType_ENTITY_TYPE_UNSPECIFIED, testOrg, "C0X"),
		"ORG of another org":              set(pb.EntityType_ENTITY_TYPE_ORG, person, "C0X"),
		"PERSON not a UUID":               set(pb.EntityType_ENTITY_TYPE_PERSON, "jane", "C0X"),
		"PERSON on the organization's id": set(pb.EntityType_ENTITY_TYPE_PERSON, testOrg, "C0X"),
		"destination kind 2, undefined":   setKind(pb.DestinationKind(2), "C0X"),
		"kind 99":                         setKind(pb.DestinationKind(99), "C0X"),
	} {
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: %v, want InvalidArgument", name, err)
		}
	}
	// The rejected PERSON row on the organization's id was never written.
	if refs, err := dests.Resolve(ctx, testOrg, pb.EntityType_ENTITY_TYPE_PERSON, testOrg, templates.UsecaseDigest, pb.Channel_CHANNEL_SLACK); err != nil || len(refs) != 0 {
		t.Fatalf("PERSON on the organization's id resolves to %v, %v; want none", refs, err)
	}
	// An unspecified kind is stored as CHANNEL. Setting the same ref again, or a
	// new one, leaves one row for the scope, which GetDestinations shows.
	for _, step := range []struct {
		kind pb.DestinationKind
		ref  string
	}{
		{pb.DestinationKind_DESTINATION_KIND_UNSPECIFIED, "C0ORG"},
		{pb.DestinationKind_DESTINATION_KIND_CHANNEL, "C0ORG"},
		{pb.DestinationKind_DESTINATION_KIND_CHANNEL, "C0NEW"},
	} {
		if err := setKind(step.kind, step.ref); err != nil {
			t.Fatalf("set %v %s: %v", step.kind, step.ref, err)
		}
		got, err := svc.GetDestinations(ctx, &pb.GetDestinationsRequest{OrganizationId: testOrg})
		if err != nil {
			t.Fatal(err)
		}
		var rows []string
		for _, d := range got.GetDestinations() {
			rows = append(rows, d.GetEntityType().String()+"/"+d.GetDestinationKind().String()+"/"+d.GetDestinationRef())
		}
		want := "ENTITY_TYPE_ORG/DESTINATION_KIND_CHANNEL/" + step.ref + ",ENTITY_TYPE_PERSON/DESTINATION_KIND_CHANNEL/C0PERSON"
		if strings.Join(rows, ",") != want {
			t.Fatalf("after setting %v %s: GetDestinations %v, want %s", step.kind, step.ref, rows, want)
		}
	}
}

// TestEligibilityForANamedSlackChannel pins that a send addressed straight to a
// Slack channel needs no configured destination: with none set, the Slack
// channel is eligible.
func TestEligibilityForANamedSlackChannel(t *testing.T) {
	tx := testDB.Begin()
	t.Cleanup(func() { tx.Rollback() })
	svc := api.NewService(api.Deps{OrgGate: fakeOrgGate{active: true}, Gate: allowGate{enabled: true}, DestinationDAO: dao.NewDestinationRepo(tx)})
	resp, err := svc.CheckSendEligibility(context.Background(), &pb.CheckSendEligibilityRequest{
		OrganizationId: testOrg, Usecase: templates.UsecaseNotice, Channels: []pb.Channel{pb.Channel_CHANNEL_SLACK},
	})
	if err != nil {
		t.Fatal(err)
	}
	if ce := resp.GetChannelEligibility()[0]; !ce.GetEligible() {
		t.Fatalf("a named Slack channel with no configured destination: reasons %v, want eligible", ce.GetReasons())
	}
}

// TestSubmitAcceptsResolvableScopes is the other side of the scope rejections:
// an ORG scope naming the request's organization and a PERSON scope naming
// someone else are accepted.
func TestSubmitAcceptsResolvableScopes(t *testing.T) {
	for kind, entity := range map[pb.EntityType]string{
		pb.EntityType_ENTITY_TYPE_ORG:    testOrg,
		pb.EntityType_ENTITY_TYPE_PERSON: "22222222-2222-2222-2222-222222222222",
	} {
		t.Run(kind.String(), func(t *testing.T) {
			svc, _, _ := newServiceTx(t)
			_, err := svc.SubmitComms(context.Background(), &pb.SubmitCommsRequest{
				OrganizationId: testOrg, IdempotencyKey: "scope-" + kind.String(), Usecase: templates.UsecaseNotice, Params: sampleParams(t),
				Entries: []*pb.CommsEntry{{Channel: pb.Channel_CHANNEL_SLACK, TargetKind: pb.TargetKind_TARGET_KIND_CONFIGURED_DESTINATION,
					DestinationScope: &pb.DestinationScope{EntityType: kind, EntityId: entity}}},
			})
			if err != nil {
				t.Fatalf("%v scope on %s: %v, want accepted", kind, entity, err)
			}
		})
	}
}
