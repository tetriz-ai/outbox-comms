// Command register-a-usecase is an outbox-comms binary of your own. It registers
// a usecase with a typed params schema and a Go renderer, then hands the command
// line to cli.Main, so serve, admin render and every other command know it.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/tetriz-ai/outbox-comms/cli"
	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/templates"
)

func main() {
	templates.MustRegister("invoice.paid", templates.Contract{
		NewParams:           func() proto.Message { return &structpb.Struct{} },
		Category:            pb.MessageCategory_MESSAGE_CATEGORY_TRANSACTIONAL,
		EnabledChannels:     []pb.Channel{pb.Channel_CHANNEL_SLACK},
		DefaultVerification: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK,
		DefaultGuarantee:    pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
		Renderers: map[templates.ChannelVendor]templates.Renderer{
			{Channel: pb.Channel_CHANNEL_SLACK, Vendor: pb.Vendor_VENDOR_SLACK}: renderInvoicePaid,
		},
	})
	os.Exit(cli.Main(context.Background(), os.Args[1:]))
}

// renderInvoicePaid turns the params into a Slack message. A renderer error
// ends the message DEAD with render_bug, without a send.
func renderInvoicePaid(p proto.Message) (*pb.RenderedPayload, error) {
	f := p.(*structpb.Struct).GetFields()
	number := mrkdwn(f["number"].GetStringValue())
	if number == "" {
		return nil, fmt.Errorf("number is required")
	}
	text := fmt.Sprintf("Invoice *%s* is paid: %s", number, mrkdwn(f["amount"].GetStringValue()))
	blocks, err := slackSection(text)
	if err != nil {
		return nil, err
	}
	return &pb.RenderedPayload{SlackText: "Invoice " + number + " is paid", SlackBlocksJson: blocks}, nil
}

// slackSection is one Block Kit section holding mrkdwn text, encoded as JSON
// so any quote or backslash in the text stays inside the string.
func slackSection(text string) (string, error) {
	type textObject struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	type section struct {
		Type string     `json:"type"`
		Text textObject `json:"text"`
	}
	var b strings.Builder
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode([]section{{Type: "section", Text: textObject{Type: "mrkdwn", Text: text}}}); err != nil {
		return "", err
	}
	return strings.TrimSuffix(b.String(), "\n"), nil
}

// mrkdwn escapes the three characters Slack reads as markup, so a param cannot
// add a link or a mention such as <!channel>.
func mrkdwn(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(s)
}
