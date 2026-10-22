// Command route-to-a-configured-channel sets the Slack channel that receives an
// organization's alerts, then submits an alert addressed to "the org's
// configured channel" rather than to a channel id.
package main

import (
	"cmp"
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
)

const org = "8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f"

func main() {
	// OUTBOX_COMMS_GRPC_PORT moves the port, as it moves compose's published port.
	addr := "127.0.0.1:" + cmp.Or(os.Getenv("OUTBOX_COMMS_GRPC_PORT"), "9090")
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	comms := pb.NewCommsServiceClient(conn)
	ctx := context.Background()

	// An admin picks the channel once. Setting it again replaces it.
	if _, err := comms.SetDestination(ctx, &pb.SetDestinationRequest{OrganizationId: org, Destination: &pb.Destination{
		EntityType: pb.EntityType_ENTITY_TYPE_ORG, EntityId: org,
		Usecase: "alert", Channel: pb.Channel_CHANNEL_SLACK, DestinationRef: "C0ONCALL",
	}}); err != nil {
		log.Fatal(err)
	}

	// The producer names the org, not the channel; the fan-out resolves it.
	resp, err := comms.SubmitComms(ctx, &pb.SubmitCommsRequest{
		OrganizationId: org,
		IdempotencyKey: "disk-alert:db-1:2026-10-01T09",
		Usecase:        "alert",
		Content: &pb.MessageContent{Base: &pb.RenderableContent{
			Subject: "Disk 91% full on db-1",
			Blocks: []*pb.ContentBlock{{Block: &pb.ContentBlock_StatList{StatList: &pb.StatList{Stats: []*pb.Stat{
				{Label: "Used", Value: "91%", Delta: "+6%", Direction: pb.Direction_DIRECTION_NEGATIVE},
			}}}}},
		}},
		Entries: []*pb.CommsEntry{{
			Channel:          pb.Channel_CHANNEL_SLACK,
			TargetKind:       pb.TargetKind_TARGET_KIND_CONFIGURED_DESTINATION,
			DestinationScope: &pb.DestinationScope{EntityType: pb.EntityType_ENTITY_TYPE_ORG, EntityId: org},
			SlackDelivery:    pb.SlackDelivery_SLACK_DELIVERY_LINK,
		}},
	})
	if err != nil {
		log.Fatal(err)
	}
	for range 50 {
		st, err := comms.GetCommsStatus(ctx, &pb.GetCommsStatusRequest{OrganizationId: org, Handle: resp.GetHandle()})
		if err != nil {
			log.Fatal(err)
		}
		if ms := st.GetMessages(); len(ms) > 0 && ms[0].GetStatus() == pb.MessageStatus_MESSAGE_STATUS_DELIVERED {
			fmt.Println("routed to", ms[0].GetRecipient(), ms[0].GetStatus())
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	log.Fatal("not delivered within 10s")
}
