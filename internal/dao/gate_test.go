// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package dao_test

import (
	"context"
	"testing"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
	"github.com/tetriz-ai/outbox-comms/templates"
)

const personA = "22222222-2222-2222-2222-222222222222"

// TestPreferenceCascade proves the PERSON-over-ORG resolution with default-on:
// no row → enabled; an ORG opt-out → disabled; a PERSON opt-in overriding the
// ORG opt-out → enabled.
func TestPreferenceCascade(t *testing.T) {
	ctx := context.Background()
	repo := dao.NewPreferenceRepo(newTestTx(t))
	cat := pb.MessageCategory_MESSAGE_CATEGORY_PRODUCT
	ch := pb.Channel_CHANNEL_EMAIL

	// No row → default enabled.
	got, err := repo.IsEnabled(ctx, testOrg, personA, cat, ch)
	if err != nil || !got {
		t.Fatalf("default should be enabled, got %v (err=%v)", got, err)
	}

	// ORG opt-out → disabled (for a person with no own row).
	if err := repo.Upsert(ctx, &model.CommsPreference{
		OrganizationID: testOrg, EntityType: pb.EntityType_ENTITY_TYPE_ORG, EntityID: testOrg,
		Category: cat, Channel: ch, Enabled: false,
	}); err != nil {
		t.Fatalf("org upsert: %v", err)
	}
	got, err = repo.IsEnabled(ctx, testOrg, personA, cat, ch)
	if err != nil || got {
		t.Fatalf("org opt-out should disable, got %v (err=%v)", got, err)
	}

	// PERSON opt-in overrides the ORG opt-out → enabled.
	if err := repo.Upsert(ctx, &model.CommsPreference{
		OrganizationID: testOrg, EntityType: pb.EntityType_ENTITY_TYPE_PERSON, EntityID: personA,
		Category: cat, Channel: ch, Enabled: true,
	}); err != nil {
		t.Fatalf("person upsert: %v", err)
	}
	got, err = repo.IsEnabled(ctx, testOrg, personA, cat, ch)
	if err != nil || !got {
		t.Fatalf("person opt-in should override org, got %v (err=%v)", got, err)
	}
}

// TestDestinationResolve proves the destination lookup: empty when none configured, all
// configured refs when present.
func TestDestinationResolve(t *testing.T) {
	ctx := context.Background()
	repoDB := newTestTx(t)
	repo := dao.NewDestinationRepo(repoDB)
	usecase := templates.UsecaseDigest
	ch := pb.Channel_CHANNEL_SLACK

	refs, err := repo.Resolve(ctx, testOrg, pb.EntityType_ENTITY_TYPE_ORG, testOrg, usecase, ch)
	if err != nil || len(refs) != 0 {
		t.Fatalf("unconfigured scope should resolve to none, got %v (err=%v)", refs, err)
	}

	dest := func(ref string) *model.CommsChannelDestination {
		return &model.CommsChannelDestination{
			OrganizationID: testOrg, EntityType: pb.EntityType_ENTITY_TYPE_ORG, EntityID: testOrg,
			Usecase: usecase, Channel: ch,
			DestinationKind: pb.DestinationKind_DESTINATION_KIND_CHANNEL, DestinationRef: ref,
		}
	}
	if err := repoDB.WithContext(ctx).Create(dest("C0AB12CD3")).Error; err != nil {
		t.Fatalf("seed: %v", err)
	}
	// One row per scope: the schema refuses a second ref beside the first.
	if err := repoDB.WithContext(ctx).Exec("SAVEPOINT second").Error; err != nil {
		t.Fatal(err)
	}
	if err := repoDB.WithContext(ctx).Create(dest("C0EF45GH6")).Error; err == nil {
		t.Fatal("a second ref for one scope was stored; want a unique violation")
	}
	if err := repoDB.WithContext(ctx).Exec("ROLLBACK TO SAVEPOINT second").Error; err != nil {
		t.Fatal(err)
	}
	refs, err = repo.Resolve(ctx, testOrg, pb.EntityType_ENTITY_TYPE_ORG, testOrg, usecase, ch)
	if err != nil || len(refs) != 1 || refs[0] != "C0AB12CD3" {
		t.Fatalf("configured scope should resolve its one ref, got %v (err=%v)", refs, err)
	}
}
