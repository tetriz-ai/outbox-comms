package main

import (
	"cmp"
	"context"
	"fmt"
	"log"
	"os"
	"sort"
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

	resp, err := comms.SubmitComms(ctx, &pb.SubmitCommsRequest{
		OrganizationId: org,
		IdempotencyKey: "export-ready:2026-10-01",
		Usecase:        "notice",
		Content: &pb.MessageContent{Base: &pb.RenderableContent{
			Subject:      "Your export is ready",
			GreetingName: "Jane",
			Blocks: []*pb.ContentBlock{{Block: &pb.ContentBlock_Paragraph{
				Paragraph: &pb.Paragraph{Text: "The October export finished."},
			}}},
		}},
		Entries: []*pb.CommsEntry{
			{Channel: pb.Channel_CHANNEL_EMAIL, TargetKind: pb.TargetKind_TARGET_KIND_EMAIL_ADDRESS, Recipient: "jane@example.com"},
			{Channel: pb.Channel_CHANNEL_SLACK, TargetKind: pb.TargetKind_TARGET_KIND_SLACK_CHANNEL, Recipient: "C0EXPORTS", SlackDelivery: pb.SlackDelivery_SLACK_DELIVERY_LINK},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("handle:", resp.GetHandle())

	for range 50 {
		st, err := comms.GetCommsStatus(ctx, &pb.GetCommsStatusRequest{OrganizationId: org, Handle: resp.GetHandle()})
		if err != nil {
			log.Fatal(err)
		}
		if done(st) {
			ms := st.GetMessages()
			sort.Slice(ms, func(i, j int) bool { return ms[i].GetChannel() < ms[j].GetChannel() })
			for _, m := range ms {
				fmt.Println(m.GetChannel(), m.GetRecipient(), m.GetStatus())
			}
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	log.Fatal("not delivered within 10s")
}

// done reports whether every message has left QUEUED and SENDING.
func done(st *pb.GetCommsStatusResponse) bool {
	if len(st.GetMessages()) == 0 {
		return false
	}
	for _, m := range st.GetMessages() {
		if m.GetStatus() == pb.MessageStatus_MESSAGE_STATUS_QUEUED || m.GetStatus() == pb.MessageStatus_MESSAGE_STATUS_SENDING {
			return false
		}
	}
	return true
}
