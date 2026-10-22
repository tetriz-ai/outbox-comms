// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Command server is the service binary scripts/e2e.sh runs: the stock command
// line plus one usecase, e2e.receipt, whose email waits for a delivery event
// (CONFIRMED), which no built-in usecase does.
package main

import (
	"context"
	"os"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/tetriz-ai/outbox-comms/cli"
	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/templates"
)

func main() {
	templates.MustRegister("e2e.receipt", templates.Contract{
		NewParams:           func() proto.Message { return &structpb.Struct{} },
		Category:            pb.MessageCategory_MESSAGE_CATEGORY_TRANSACTIONAL,
		EnabledChannels:     []pb.Channel{pb.Channel_CHANNEL_EMAIL},
		DefaultVerification: pb.VerificationLevel_VERIFICATION_LEVEL_CONFIRMED,
		DefaultGuarantee:    pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
	})
	os.Exit(cli.Main(context.Background(), os.Args[1:]))
}
