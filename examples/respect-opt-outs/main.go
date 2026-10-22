// Command respect-opt-outs shows the preference gate. One person opts out of
// product email; a digest to them and to a colleague sends to the colleague only,
// while a transactional notice to the person still goes through.
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

const (
	org  = "8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f"
	jane = "2b0c7a3e-1f4d-4e5a-8b9c-0d1e2f3a4b5c"
	john = "5d6e7f80-91a2-4b3c-8d4e-5f6a7b8c9d0e"
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

	// Jane turns off product email for herself.
	if _, err := comms.UpdatePersonPreferences(ctx, &pb.UpdatePersonPreferencesRequest{OrganizationId: org, PersonId: jane, Preferences: []*pb.Preference{
		{Category: pb.MessageCategory_MESSAGE_CATEGORY_PRODUCT, Channel: pb.Channel_CHANNEL_EMAIL, Enabled: false},
	}}); err != nil {
		log.Fatal(err)
	}

	email := func(addr, person string) *pb.CommsEntry {
		return &pb.CommsEntry{Channel: pb.Channel_CHANNEL_EMAIL, TargetKind: pb.TargetKind_TARGET_KIND_EMAIL_ADDRESS, Recipient: addr, SubjectPersonId: person}
	}
	body := &pb.MessageContent{Base: &pb.RenderableContent{Subject: "Your monthly summary"}}
	submit := func(key, usecase string, entries ...*pb.CommsEntry) {
		resp, err := comms.SubmitComms(ctx, &pb.SubmitCommsRequest{OrganizationId: org, IdempotencyKey: key, Usecase: usecase, Content: body, Entries: entries})
		if err != nil {
			log.Fatal(err)
		}
		for range 50 {
			st, err := comms.GetCommsStatus(ctx, &pb.GetCommsStatusRequest{OrganizationId: org, Handle: resp.GetHandle()})
			if err != nil {
				log.Fatal(err)
			}
			if settled(st) {
				fmt.Printf("%s (partial=%v)\n", usecase, st.GetIsPartial())
				ms := st.GetMessages()
				sort.Slice(ms, func(i, j int) bool { return ms[i].GetRecipient() < ms[j].GetRecipient() })
				for _, m := range ms {
					line := "  " + m.GetRecipient() + " " + m.GetStatus().String()
					if m.GetSkipReason() != "" {
						line += " (" + m.GetSkipReason() + ")"
					}
					fmt.Println(line)
				}
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
		log.Fatal("not settled within 10s")
	}
	submit("summary:2026-09", "digest", email("jane@example.com", jane), email("john@example.com", john))
	submit("password-changed:jane:1", "notice", email("jane@example.com", jane))
}

// settled reports whether every message has left QUEUED and SENDING.
func settled(st *pb.GetCommsStatusResponse) bool {
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
