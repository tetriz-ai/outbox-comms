// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package templates_test

import (
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/templates"
)

// TestRenderChecksSlackBlocks pins the shape Render accepts from a renderer:
// no blocks, or a JSON array of at most 50 objects. Anything else is a render
// error.
func TestRenderChecksSlackBlocks(t *testing.T) {
	block := `{"type":"section","text":{"type":"mrkdwn","text":"a"}}`
	array := func(n int) string { return "[" + strings.TrimSuffix(strings.Repeat(block+",", n), ",") + "]" }
	for i, tc := range []struct {
		name, blocks, wantErr string
	}{
		{"no blocks", "", ""},
		{"an empty array", "[]", ""},
		{"one block", array(1), ""},
		{"50 blocks, the limit", array(50), ""},
		{"51 blocks, over the limit", array(51), "over Slack's limit of 50"},
		{"invalid JSON", `[{"type":"section","text":"a"b"}]`, "not valid JSON"},
		{"an object, not an array", block, "not a JSON array"},
		{"a string", `"blocks"`, "not a JSON array"},
		{"null", "null", "not a JSON array"},
		{"a number", "1", "not a JSON array"},
		{"whitespace around an empty array", "  [ ]  ", ""},
		{"an empty object", "[{}]", ""},
		{"a nested array", "[[1]]", "not a JSON object"},
		{"a number element", "[1]", "not a JSON object"},
		{"a null element", "[null]", "not a JSON object"},
		{"5000 blocks", array(5000), "over Slack's limit of 50"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := fmt.Sprintf("test.blocks.%d", i)
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
			_, err := templates.Render(key, pb.Channel_CHANNEL_SLACK, pb.Vendor_VENDOR_SLACK, nil)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("Render: %v, want no error", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("Render: %v, want an error containing %q", err, tc.wantErr)
			}
		})
	}
}

// TestCTALinkEscapesPipe pins that a | in the CTA URL is percent-encoded in the
// Slack link, so it cannot end the URL early and become the link's label.
func TestCTALinkEscapesPipe(t *testing.T) {
	out, err := templates.RenderContent(pb.Channel_CHANNEL_SLACK, &pb.RenderableContent{
		Subject: "s",
		Cta:     &pb.CallToAction{Label: "Open the report", Url: "https://example.com/r?a=1|Click here"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.GetSlackBlocksJson(), "https://example.com/r?a=1%7CClick here|Open the report") {
		t.Fatalf("blocks %s, want the | in the URL encoded as %%7C", out.GetSlackBlocksJson())
	}
}
