// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Command acceptance is the client half of scripts/acceptance.sh: "submit" sends
// one Slack notice and prints its handle; "status HANDLE" prints the message's
// status and attempts.
package main

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
)

const org = "8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: acceptance submit KEY | status HANDLE")
	}
	// OUTBOX_COMMS_GRPC_PORT moves the port, as it moves compose's published port.
	addr := "127.0.0.1:" + cmp.Or(os.Getenv("OUTBOX_COMMS_GRPC_PORT"), "9090")
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	c := pb.NewCommsServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	switch {
	case args[0] == "submit" && len(args) == 2:
		resp, err := c.SubmitComms(ctx, &pb.SubmitCommsRequest{
			OrganizationId: org, IdempotencyKey: args[1], Usecase: "notice",
			Content: &pb.MessageContent{Base: &pb.RenderableContent{Subject: "Deploy finished",
				Blocks: []*pb.ContentBlock{{Block: &pb.ContentBlock_Paragraph{Paragraph: &pb.Paragraph{Text: "Release 42 is live."}}}}}},
			Entries: []*pb.CommsEntry{{Channel: pb.Channel_CHANNEL_SLACK, TargetKind: pb.TargetKind_TARGET_KIND_SLACK_CHANNEL,
				Recipient: "C0DEPLOYS", SlackDelivery: pb.SlackDelivery_SLACK_DELIVERY_LINK}},
		})
		if err != nil {
			return err
		}
		fmt.Println(resp.GetHandle())
	case args[0] == "status" && len(args) == 2:
		resp, err := c.GetCommsStatus(ctx, &pb.GetCommsStatusRequest{OrganizationId: org, Handle: args[1]})
		if err != nil {
			return err
		}
		for _, m := range resp.GetMessages() {
			fmt.Println(m.GetStatus())
		}
	default:
		return fmt.Errorf("usage: acceptance submit KEY | status HANDLE")
	}
	return nil
}
