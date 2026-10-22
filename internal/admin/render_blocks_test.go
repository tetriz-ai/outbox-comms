// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package admin

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/tetriz-ai/outbox-comms/config"
	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/templates"
)

// TestRenderFailsOnInvalidSlackBlocks pins that admin render exits 1, saying
// why, when a renderer's Slack blocks are not valid JSON or not an array.
func TestRenderFailsOnInvalidSlackBlocks(t *testing.T) {
	for key, tc := range map[string]struct{ blocks, why string }{
		"test.admin.badblocks":    {`[{"text":"a"b"}]`, "not valid JSON"},
		"test.admin.objectblocks": {`{"type":"divider"}`, "not a JSON array"},
	} {
		blocks := tc.blocks
		templates.MustRegister(key, templates.Contract{
			NewParams:           func() proto.Message { return &structpb.Struct{} },
			Category:            pb.MessageCategory_MESSAGE_CATEGORY_TRANSACTIONAL,
			EnabledChannels:     []pb.Channel{pb.Channel_CHANNEL_SLACK},
			DefaultVerification: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK,
			DefaultGuarantee:    pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
			Renderers: map[templates.ChannelVendor]templates.Renderer{
				{Channel: pb.Channel_CHANNEL_SLACK, Vendor: pb.Vendor_VENDOR_SLACK}: func(proto.Message) (*pb.RenderedPayload, error) {
					return &pb.RenderedPayload{SlackText: "x", SlackBlocksJson: blocks}, nil
				},
			},
		})
		e, _, errOut := newEnv(t, config.Config{})
		if code := e.run(context.Background(), []string{"render", "--usecase", key, "--channel", "slack"}); code != 1 {
			t.Fatalf("%s: exit %d, want 1; stderr %s", key, code, errOut.String())
		}
		if !strings.Contains(errOut.String(), tc.why) {
			t.Fatalf("%s: stderr %q does not say %q", key, errOut.String(), tc.why)
		}
	}
}
