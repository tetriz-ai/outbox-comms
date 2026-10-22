// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package api_test

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/templates"
)

// TestEntriesCap pins the per-request cap: 1000 entries are accepted, 1001 are
// rejected; a larger send is the producer's to split.
func TestEntriesCap(t *testing.T) {
	for _, n := range []int{1000, 1001} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			svc, _ := newService(t)
			entries := make([]*pb.CommsEntry, n)
			for i := range entries {
				entries[i] = emailEntry(fmt.Sprintf("u%d@example.com", i))
			}
			_, err := svc.SubmitComms(context.Background(), &pb.SubmitCommsRequest{OrganizationId: testOrg, IdempotencyKey: fmt.Sprint("cap-", n), Usecase: templates.UsecaseNotice, Entries: entries})
			if n == 1000 && err != nil {
				t.Fatalf("1000 entries must be accepted: %v", err)
			}
			if n == 1001 && status.Code(err) != codes.InvalidArgument {
				t.Fatalf("1001 entries must be rejected, got %v", err)
			}
		})
	}
}
