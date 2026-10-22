// Command check-before-you-send asks, before any message is built, whether an
// alert to one person would go out on email and on the organization's
// configured Slack channel, and why not where it would not. It checks a fresh organization, then the same organization after an
// admin picks the alert channel and the person turns alert email off, then a
// transactional notice.
package main

import (
	"cmp"
	"context"
	"fmt"
	"log"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
)

// This organization belongs to this example alone; it is purged on every run.
const (
	org  = "3f9a6c2e-7b1d-4e8f-a5c3-2d4b6e8f0a1c"
	jane = "2b0c7a3e-1f4d-4e5a-8b9c-0d1e2f3a4b5c"
)

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

	if _, err := comms.PurgeOrgComms(ctx, &pb.PurgeOrgCommsRequest{OrganizationId: org}); err != nil {
		log.Fatal(err)
	}
	both := []pb.Channel{pb.Channel_CHANNEL_EMAIL, pb.Channel_CHANNEL_SLACK}
	// Each channel is checked as the entry the send would carry: the email
	// goes to Jane's address and is gated on her preferences; the Slack
	// message goes to the organization's configured channel, gated on the
	// organization's.
	entry := map[pb.Channel]*pb.CheckSendEligibilityRequest{
		pb.Channel_CHANNEL_EMAIL: {PersonId: jane, TargetKind: pb.TargetKind_TARGET_KIND_EMAIL_ADDRESS},
		pb.Channel_CHANNEL_SLACK: {TargetKind: pb.TargetKind_TARGET_KIND_CONFIGURED_DESTINATION},
	}
	check := func(label, usecase string, channels []pb.Channel) {
		fmt.Println(label)
		for _, ch := range channels {
			req := entry[ch]
			resp, err := comms.CheckSendEligibility(ctx, &pb.CheckSendEligibilityRequest{
				OrganizationId: org, Usecase: usecase, Channels: []pb.Channel{ch},
				PersonId: req.GetPersonId(), TargetKind: req.GetTargetKind(),
			})
			if err != nil {
				log.Fatal(err)
			}
			for _, e := range resp.GetChannelEligibility() {
				if e.GetEligible() {
					fmt.Println(" ", e.GetChannel(), "eligible")
				} else {
					fmt.Println(" ", e.GetChannel(), "blocked", e.GetReasons())
				}
			}
		}
	}

	check("alert, fresh organization", "alert", both)

	if _, err := comms.SetDestination(ctx, &pb.SetDestinationRequest{OrganizationId: org, Destination: &pb.Destination{
		EntityType: pb.EntityType_ENTITY_TYPE_ORG, EntityId: org,
		Usecase: "alert", Channel: pb.Channel_CHANNEL_SLACK, DestinationRef: "C0ALERTS",
	}}); err != nil {
		log.Fatal(err)
	}
	if _, err := comms.UpdatePersonPreferences(ctx, &pb.UpdatePersonPreferencesRequest{OrganizationId: org, PersonId: jane, Preferences: []*pb.Preference{
		{Category: pb.MessageCategory_MESSAGE_CATEGORY_ALERT, Channel: pb.Channel_CHANNEL_EMAIL, Enabled: false},
	}}); err != nil {
		log.Fatal(err)
	}
	check("alert, after the channel is set and Jane opts out of alert email", "alert", both)
	check("notice, which is transactional", "notice", []pb.Channel{pb.Channel_CHANNEL_EMAIL})
}
