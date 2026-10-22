// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package dao_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
)

// TestPurgeIsOneTransaction pins that a purge removes all of an organization's
// rows or none: when the last delete fails, the requests, messages and events
// are still there, and no purge tombstone was written.
func TestPurgeIsOneTransaction(t *testing.T) {
	ctx := context.Background()
	org := uuid.NewString()
	pmid := "purge-atomic-" + org
	t.Cleanup(func() {
		testDB.Exec("DROP TRIGGER IF EXISTS refuse_purge ON comms_channel_destinations")
		testDB.Exec("DROP FUNCTION IF EXISTS refuse_purge()")
		testDB.Exec("DELETE FROM comms_channel_destinations WHERE organization_id = ?", org)
		testDB.Exec("DELETE FROM comms_delivery_events WHERE organization_id = ?", org)
		testDB.Exec("DELETE FROM comms_messages WHERE organization_id = ?", org)
		testDB.Exec("DELETE FROM comms_requests WHERE organization_id = ?", org)
		testDB.Exec("DELETE FROM comms_purge_tombstones WHERE provider_message_id = ?", pmid)
	})
	_, req, err := dao.NewRequestRepo(testDB).CreateIfAbsent(ctx, &model.CommsRequest{OrganizationID: org, IdempotencyKey: "purge-atomic",
		Usecase: "notice", DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
		Status: pb.RequestStatus_REQUEST_STATUS_FANNED_OUT, Params: &structpb.Struct{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dao.NewMessageRepo(testDB).MaterializeBatch(ctx, org, []*model.CommsMessage{{OrganizationID: org, RequestID: req.ID,
		Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SES, Recipient: "jane@example.com", Status: pb.MessageStatus_MESSAGE_STATUS_SENT,
		VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED}}); err != nil {
		t.Fatal(err)
	}
	testDB.Exec("UPDATE comms_messages SET provider_message_id = ? WHERE organization_id = ?", pmid, org)
	if err := dao.NewDestinationRepo(testDB).ReplaceForChannel(ctx, &model.CommsChannelDestination{OrganizationID: org,
		EntityType: pb.EntityType_ENTITY_TYPE_ORG, EntityID: org, Usecase: "alert", Channel: pb.Channel_CHANNEL_SLACK,
		DestinationKind: pb.DestinationKind_DESTINATION_KIND_CHANNEL, DestinationRef: "C0ATOMIC"}); err != nil {
		t.Fatal(err)
	}
	// The destinations go last in the purge; refuse that delete.
	for _, q := range []string{
		`CREATE FUNCTION refuse_purge() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'refused'; END $$`,
		`CREATE TRIGGER refuse_purge BEFORE DELETE ON comms_channel_destinations FOR EACH ROW EXECUTE FUNCTION refuse_purge()`,
	} {
		if err := testDB.Exec(q).Error; err != nil {
			t.Fatal(err)
		}
	}
	if _, err := dao.NewReconcileRepo(testDB).PurgeOrg(ctx, org); err == nil {
		t.Fatal("PurgeOrg returned nil with its last delete refused")
	}
	var requests, messages, tombstones int64
	testDB.Raw("SELECT count(*) FROM comms_requests WHERE organization_id = ?", org).Scan(&requests)
	testDB.Raw("SELECT count(*) FROM comms_messages WHERE organization_id = ?", org).Scan(&messages)
	testDB.Raw("SELECT count(*) FROM comms_purge_tombstones WHERE provider_message_id = ?", pmid).Scan(&tombstones)
	if requests != 1 || messages != 1 || tombstones != 0 {
		t.Fatalf("after a failed purge: %d request(s), %d message(s), %d tombstone(s); want 1, 1, 0", requests, messages, tombstones)
	}
}
