// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package fanout_test

import (
	"context"
	"errors"
	"testing"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
	"github.com/tetriz-ai/outbox-comms/internal/fanout"
	"github.com/tetriz-ai/outbox-comms/internal/queue"
	"github.com/tetriz-ai/outbox-comms/templates"
)

const personX = "44444444-4444-4444-4444-444444444444"

// TestSubjectPersonGate proves a concrete non-transactional entry naming its
// subject person is gated on that person's preference (PERSON over ORG), and an
// entry naming nobody falls back to the ORG tier.
func TestSubjectPersonGate(t *testing.T) {
	tests := []struct {
		name       string
		personPref *bool
		orgPref    *bool
		subject    string
		usecase    string
		want       pb.MessageStatus
	}{
		{"person opted out", ptr(false), nil, personX, templates.UsecaseDigest, pb.MessageStatus_MESSAGE_STATUS_SKIPPED},
		{"person opt-in beats org opt-out", ptr(true), ptr(false), personX, templates.UsecaseDigest, pb.MessageStatus_MESSAGE_STATUS_QUEUED},
		{"no subject: person row is not consulted", ptr(false), nil, "", templates.UsecaseDigest, pb.MessageStatus_MESSAGE_STATUS_QUEUED},
		{"transactional bypasses the person gate", ptr(false), nil, personX, templates.UsecaseNotice, pb.MessageStatus_MESSAGE_STATUS_QUEUED},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			h := newHarness(t, false)
			if tt.personPref != nil {
				_ = h.prefRepo.Upsert(ctx, &model.CommsPreference{OrganizationID: testOrg, EntityType: pb.EntityType_ENTITY_TYPE_PERSON, EntityID: personX, Category: pb.MessageCategory_MESSAGE_CATEGORY_PRODUCT, Channel: pb.Channel_CHANNEL_EMAIL, Enabled: *tt.personPref})
			}
			if tt.orgPref != nil {
				_ = h.prefRepo.Upsert(ctx, &model.CommsPreference{OrganizationID: testOrg, EntityType: pb.EntityType_ENTITY_TYPE_ORG, EntityID: testOrg, Category: pb.MessageCategory_MESSAGE_CATEGORY_PRODUCT, Channel: pb.Channel_CHANNEL_EMAIL, Enabled: *tt.orgPref})
			}
			e := emailEntry("subject@example.com")
			e.SubjectPersonId = tt.subject
			id := h.seed(t, "subject-"+string(rune('a'+i)), tt.usecase, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT, e)
			if err := h.fo.Process(ctx, testOrg, id); err != nil {
				t.Fatal(err)
			}
			rows, _ := h.msgRepo.ListByRequest(ctx, testOrg, id)
			if len(rows) != 1 || rows[0].Status != tt.want {
				t.Fatalf("status = %v, want %v", rows[0].Status, tt.want)
			}
		})
	}
}

func ptr(b bool) *bool { return &b }

type failingPrefs struct{ dao.IPreferenceDAO }

func (failingPrefs) IsEnabled(context.Context, string, string, pb.MessageCategory, pb.Channel) (bool, error) {
	return false, errors.New("connection reset")
}

// TestPreferenceErrorRetries proves a failed preference lookup fails the pass
// (so the trigger retries) and writes no row, instead of a permanent opted_out.
func TestPreferenceErrorRetries(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, false)
	fo := fanout.NewFanOut(h.reqRepo, h.msgRepo, failingPrefs{h.prefRepo}, h.destRepo, fakeOrgGate{active: true}, h.dispatcher, fanout.StaticVendorResolver{Email: pb.Vendor_VENDOR_SES})
	id := h.seed(t, "pref-err", templates.UsecaseDigest, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT, emailEntry("blip@example.com"))
	err := fo.Process(ctx, testOrg, id)
	if err == nil || queue.IsPermanent(err) {
		t.Fatalf("want a retryable error, got %v", err)
	}
	if rows, _ := h.msgRepo.ListByRequest(ctx, testOrg, id); len(rows) != 0 {
		t.Fatalf("no row may be written on a failed gate, got %d", len(rows))
	}
	if r, _ := h.reqRepo.GetByID(ctx, testOrg, id); r.Status == pb.RequestStatus_REQUEST_STATUS_FANNED_OUT {
		t.Fatal("the request must not be marked FANNED_OUT")
	}
}

// TestFrozenOrgSaysWhy proves a frozen org's request materializes one
// SKIPPED(org_frozen) row per entry and enqueues nothing.
func TestFrozenOrgSaysWhy(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, true)
	id := h.seed(t, "frozen", templates.UsecaseNotice, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, emailEntry("frozen@example.com"), orgDestEntry())
	if err := h.fo.Process(ctx, testOrg, id); err != nil {
		t.Fatal(err)
	}
	rows, _ := h.msgRepo.ListByRequest(ctx, testOrg, id)
	if len(rows) != 2 {
		t.Fatalf("want one row per entry, got %d", len(rows))
	}
	for _, r := range rows {
		if r.Status != pb.MessageStatus_MESSAGE_STATUS_SKIPPED || skipReason(r) != "org_frozen" {
			t.Fatalf("row %s = %v/%s, want SKIPPED(org_frozen)", r.Recipient, r.Status, skipReason(r))
		}
	}
	if len(h.dispatcher.ids) != 0 {
		t.Fatalf("a frozen org must enqueue nothing, got %d", len(h.dispatcher.ids))
	}
}

// TestMissingRequestIsPermanent proves a trigger for a request that does not
// exist is dropped rather than retried.
func TestMissingRequestIsPermanent(t *testing.T) {
	h := newHarness(t, false)
	err := h.fo.Process(context.Background(), testOrg, "55555555-5555-5555-5555-555555555555")
	if !queue.IsPermanent(err) {
		t.Fatalf("want a permanent error, got %v", err)
	}
}
