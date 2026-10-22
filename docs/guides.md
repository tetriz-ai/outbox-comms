# Guides

One guide per common job, each with a complete Go program and the output it prints. Every program runs against the [quick-start](../README.md#try-it-locally) stack. Save it as `main.go` in a new directory inside your clone but outside `examples/`, such as `scratch/mine/`, and run `go run ./scratch/mine`.

## Send a message

When: you know the recipients and want the message sent once, now.

```go
// Command send-a-message submits one notice to an email address and prints its handle.
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

func main() {
	addr := "127.0.0.1:" + cmp.Or(os.Getenv("OUTBOX_COMMS_GRPC_PORT"), "9090")
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	resp, err := pb.NewCommsServiceClient(conn).SubmitComms(context.Background(), &pb.SubmitCommsRequest{
		OrganizationId: "8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f",
		IdempotencyKey: "invoice-paid:INV-1042",
		Usecase:        "notice",
		Content:        &pb.MessageContent{Base: &pb.RenderableContent{Subject: "Invoice INV-1042 is paid"}},
		Entries: []*pb.CommsEntry{{
			Channel:    pb.Channel_CHANNEL_EMAIL,
			TargetKind: pb.TargetKind_TARGET_KIND_EMAIL_ADDRESS,
			Recipient:  "jane@example.com",
		}},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("handle:", resp.GetHandle())
}
```

```text
handle: invoice-paid:INV-1042
```

A request names a usecase, a content body, and one entry per recipient. A usecase is a registered message type. Three ship built in: `notice`, `alert` and `digest`.

The quick start's program, [send-a-notice](../examples/send-a-notice/), also waits for each message's status.

Remember:

- **Idempotency keys** are per organization and permanent, so use one that names the event, such as `invoice-paid:INV-1042`. The same request under a used key returns the same handle and sends nothing, and a different request is `ALREADY_EXISTS`. A `GUARANTEED` send whose answer was lost can still arrive twice, as [Delivery](production.md#delivery) explains.
- **Status** is per message. A request with one bounced address and one delivered post reports `is_partial: true` and both rows.

## Route to a configured channel

When: the producer knows which organization a message is for, and the organization's admin chooses the channel.

```go
// Command route-to-a-configured-channel sets an organization's alert channel, then sends an
// alert to whatever channel is set.
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

const org = "8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f"

func main() {
	addr := "127.0.0.1:" + cmp.Or(os.Getenv("OUTBOX_COMMS_GRPC_PORT"), "9090")
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	comms := pb.NewCommsServiceClient(conn)
	ctx := context.Background()
	if _, err := comms.SetDestination(ctx, &pb.SetDestinationRequest{OrganizationId: org, Destination: &pb.Destination{
		EntityType: pb.EntityType_ENTITY_TYPE_ORG, EntityId: org,
		Usecase: "alert", Channel: pb.Channel_CHANNEL_SLACK, DestinationRef: "C0ONCALL",
	}}); err != nil {
		log.Fatal(err)
	}
	resp, err := comms.SubmitComms(ctx, &pb.SubmitCommsRequest{
		OrganizationId: org,
		IdempotencyKey: "disk-alert:db-2:2026-10-01T10",
		Usecase:        "alert",
		Content:        &pb.MessageContent{Base: &pb.RenderableContent{Subject: "Disk 92% full on db-2"}},
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
	fmt.Println("handle:", resp.GetHandle())
}
```

```text
handle: disk-alert:db-2:2026-10-01T10
```

The fan-out looks up the channel and sends there. With no channel set, the row is `SKIPPED` with `skip_reason: no_destination`. [route-to-a-configured-channel](../examples/route-to-a-configured-channel/) also waits for the post.

Remember:

- **A second `SetDestination`** for the same scope, usecase and channel replaces the first. A destination belongs to the organization or to one person, and a send scoped to a person uses that person's destination, not the organization's.
- **Email** has no configured destination. Email entries always name the address.

## Respect opt-outs

When: some of your messages are not transactional, and people can turn them off.

```go
// Command respect-opt-outs turns off product email for one person, then asks
// whether a digest to them would go out.
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

const (
	org  = "8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f"
	jane = "2b0c7a3e-1f4d-4e5a-8b9c-0d1e2f3a4b5c"
)

func main() {
	addr := "127.0.0.1:" + cmp.Or(os.Getenv("OUTBOX_COMMS_GRPC_PORT"), "9090")
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	comms := pb.NewCommsServiceClient(conn)
	ctx := context.Background()
	if _, err := comms.UpdatePersonPreferences(ctx, &pb.UpdatePersonPreferencesRequest{OrganizationId: org, PersonId: jane, Preferences: []*pb.Preference{
		{Category: pb.MessageCategory_MESSAGE_CATEGORY_PRODUCT, Channel: pb.Channel_CHANNEL_EMAIL, Enabled: false},
	}}); err != nil {
		log.Fatal(err)
	}
	for _, usecase := range []string{"digest", "notice"} {
		resp, err := comms.CheckSendEligibility(ctx, &pb.CheckSendEligibilityRequest{
			OrganizationId: org, Usecase: usecase, PersonId: jane, Channels: []pb.Channel{pb.Channel_CHANNEL_EMAIL},
		})
		if err != nil {
			log.Fatal(err)
		}
		e := resp.GetChannelEligibility()[0]
		fmt.Println(usecase, e.GetChannel(), "eligible:", e.GetEligible(), e.GetReasons())
	}
}
```

```text
digest CHANNEL_EMAIL eligible: false [opted_out]
notice CHANNEL_EMAIL eligible: true []
```

A person's preference, set with `UpdatePersonPreferences`, beats the organization's, set with `SetOrgPreferences`. No preference at all means on. A `notice` is transactional and skips preferences, while `alert` and `digest` respect them.

[respect-opt-outs](../examples/respect-opt-outs/) submits the digest and the notice and shows the skipped row. [check-before-you-send](../examples/check-before-you-send/) checks email and Slack before and after a destination is set and a person opts out.

Remember:

- **Name the person** on each entry with `subject_person_id`, or only the organization's preference applies. A skipped row says why, with `skip_reason: opted_out`.
- **Suppression** is separate from preference. A hard-bounced address is never mailed again, transactional or not.

## Register a usecase

When: a producer sends params instead of a ready body, and Go code should render them.

```go
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
```

```sh
go run ./examples/register-a-usecase admin render --usecase invoice.paid --channel slack --params '{"number":"INV-1042","amount":"€120.00"}'
```

```text
[{"type":"section","text":{"type":"mrkdwn","text":"Invoice *INV-1042* is paid: €120.00"}}]
```

The contract's two defaults mean:

| Setting | Value | Meaning |
|---|---|---|
| `DefaultVerification` | `PROVIDER_ACK` | Done once the provider accepts the message |
| `DefaultVerification` | `CONFIRMED` | Waits for a delivery event, and ends `UNCONFIRMED` without one |
| `DefaultGuarantee` | `GUARANTEED` | An uncertain send is retried |
| `DefaultGuarantee` | `BEST_EFFORT` | An uncertain send is not retried |

The program is [register-a-usecase](../examples/register-a-usecase/), and [Usecases](reference.md#usecases) has the rules for your own binary.

Remember:

- **Params schema:** `NewParams` returns the message Submit decodes params into. The example's `structpb.Struct` accepts any object, so return your own proto message to make Submit reject params that do not fit.
- **The stock binary** knows only `notice`, `alert` and `digest`. Its `admin render` refuses any other key, so render and serve your usecases from your own binary.

## Operate it

When: a message is stuck, dead or wrongly suppressed, or you want to see a usecase's output.

```sh
docker compose exec outbox-comms outbox-comms admin redrive-dead --reason transient_exhausted
```

```text
0 DEAD message(s) match
read only: pass --execute to reset them to QUEUED (after fixing the cause)
```

The built-in usecases render from a content file:

```sh
go run ./cmd/outbox-comms admin render --usecase notice --channel slack --content examples/notice.json
```

```text
[{"text":{"text":"Your export is ready","type":"plain_text"},"type":"header"},{"text":{"text":"The October export finished and is ready to download.","type":"mrkdwn"},"type":"section"}]
```

Every admin command only reads until it gets its write flag, which [Operator commands](reference.md#operator-commands) lists. `/healthz` reports liveness and `/metrics` reports counts and gauges, and [check-from-the-shell](../examples/check-from-the-shell/) reads them with `curl`.

Remember:

- **Re-driving** only resets a message to `QUEUED`, and the reconcile job dispatches it once it is 5 minutes old, so within about 15 minutes. A message that died of a render bug dies again unless you fix the cause first.
- **Addresses** in admin output are masked (`j***@example.com`).

The [proto files](../proto/outboxcomms/v1/) are the full API reference, [Operator commands](reference.md#operator-commands) is the command reference, and [examples/README.md](../examples/README.md) lists every example.
