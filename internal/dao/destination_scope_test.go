// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package dao_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/google/uuid"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
)

// TestReplaceForChannelOneRowPerScope pins that a scope routes to exactly one
// ref, for an ORG and a PERSON scope. Set one after the other, the second
// replaces the first. Set concurrently from separate connections, exactly one
// survives, because the schema allows one row per scope and each set is an
// upsert on it.
func TestReplaceForChannelOneRowPerScope(t *testing.T) {
	ctx := context.Background()
	org := uuid.NewString()
	person := uuid.NewString()
	t.Cleanup(func() { testDB.Exec("DELETE FROM comms_channel_destinations WHERE organization_id = ?", org) })
	repo := dao.NewDestinationRepo(testDB)
	for _, scope := range []struct {
		name   string
		kind   pb.EntityType
		entity string
	}{
		{"org", pb.EntityType_ENTITY_TYPE_ORG, org},
		{"person", pb.EntityType_ENTITY_TYPE_PERSON, person},
	} {
		dest := func(usecase, ref string) *model.CommsChannelDestination {
			return &model.CommsChannelDestination{OrganizationID: org, EntityType: scope.kind, EntityID: scope.entity,
				Usecase: usecase, Channel: pb.Channel_CHANNEL_SLACK,
				DestinationKind: pb.DestinationKind_DESTINATION_KIND_CHANNEL, DestinationRef: ref}
		}
		resolve := func(usecase string) []string {
			refs, err := repo.Resolve(ctx, org, scope.kind, scope.entity, usecase, pb.Channel_CHANNEL_SLACK)
			if err != nil {
				t.Fatal(err)
			}
			return refs
		}
		t.Run(scope.name+"/sequential", func(t *testing.T) {
			for _, ref := range []string{"C0", "C1"} {
				if err := repo.ReplaceForChannel(ctx, dest("seq", ref)); err != nil {
					t.Fatal(err)
				}
			}
			if refs := resolve("seq"); len(refs) != 1 || refs[0] != "C1" {
				t.Fatalf("after C0 then C1: %v, want [C1]", refs)
			}
		})
		t.Run(scope.name+"/concurrent", func(t *testing.T) {
			// Repeated, since one round can serialise by chance.
			for round := 0; round < 10; round++ {
				usecase := fmt.Sprintf("conc-%d", round)
				var wg sync.WaitGroup
				errs := make(chan error, 4)
				start := make(chan struct{})
				for i := 0; i < 4; i++ {
					wg.Add(1)
					go func(ref string) {
						defer wg.Done()
						<-start
						errs <- repo.ReplaceForChannel(ctx, dest(usecase, ref))
					}(fmt.Sprintf("C%d", i))
				}
				close(start)
				wg.Wait()
				close(errs)
				for err := range errs {
					if err != nil {
						t.Fatal(err)
					}
				}
				if refs := resolve(usecase); len(refs) != 1 {
					t.Fatalf("round %d: four concurrent sets left %v; want exactly one ref", round, refs)
				}
			}
		})
	}
}

// TestResolveReportsAFailedLookup pins that Resolve returns the error of a
// lookup that failed, never an empty result the fan-out would read as "no
// destination".
func TestResolveReportsAFailedLookup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	refs, err := dao.NewDestinationRepo(testDB).Resolve(ctx, uuid.NewString(), pb.EntityType_ENTITY_TYPE_ORG, uuid.NewString(), "alert", pb.Channel_CHANNEL_SLACK)
	if err == nil {
		t.Fatalf("Resolve on a failed lookup returned %v and no error", refs)
	}
}
