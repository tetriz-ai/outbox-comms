// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package admin

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
	"github.com/tetriz-ai/outbox-comms/internal/dispatch"
	"github.com/tetriz-ai/outbox-comms/internal/sender"
	"github.com/tetriz-ai/outbox-comms/internal/slackclient"
	"github.com/tetriz-ai/outbox-comms/server"
	"github.com/tetriz-ai/outbox-comms/templates"
)

// isProd reports whether OUTBOX_COMMS_ENV names production, in any casing, as
// "prod" or "production", with optional delimited suffixes: render --send never
// reaches a production recipient by accident.
func isProd(env string) bool {
	e := strings.ToLower(strings.TrimSpace(env))
	name := strings.FieldsFunc(e, func(r rune) bool { return r == '-' || r == '_' || r == '.' || r == '/' })
	return len(name) > 0 && (name[0] == "prod" || name[0] == "production")
}

// render runs the real renderer for one usecase and channel and prints the
// result. With --send it also delivers it to the single --to target through the
// configured sender: never a list, never a database row. The params are
// validated exactly as a live send would, so a bad sample fails here the same
// way.
func (e *env) render(ctx context.Context, args []string) error {
	fs := e.flags("render", "render --usecase KEY --channel email|slack [--params JSON] [--content FILE] [--send --to TARGET [--org UUID]]")
	usecase := fs.String("usecase", "", "registered usecase key (required)")
	channelFlag := fs.String("channel", "", "email or slack (required)")
	paramsJSON := fs.String("params", "", "sample params as a JSON object")
	contentPath := fs.String("content", "", "a MessageContent JSON file to render instead of the usecase's renderer")
	send := fs.Bool("send", false, "deliver the rendered message to --to")
	to := fs.String("to", "", "an email address (email) or channel id (slack); required with --send")
	org := fs.String("org", "", "organization whose Slack token to use (slack --send)")
	if err := e.parse(fs, args); err != nil {
		return err
	}
	ch, err := parseChannel(*channelFlag)
	if err != nil {
		return err
	}
	if _, err := templates.Get(*usecase); err != nil {
		return usageErr("--usecase %q is not registered (known: %s)", *usecase, strings.Join(templates.Keys(), ", "))
	}
	params, err := parseParams(*paramsJSON)
	if err != nil {
		return err
	}
	if *send && strings.TrimSpace(*to) == "" {
		return usageErr("--to is required with --send")
	}
	if err := e.ready(); err != nil {
		return err
	}

	var rendered *pb.RenderedPayload
	if *contentPath != "" {
		content, err := loadContent(*contentPath)
		if err != nil {
			return err
		}
		if err := templates.ValidateParams(*usecase, params); err != nil {
			return fmt.Errorf("params: %w", err)
		}
		rc := (&model.CommsRequest{Content: content}).RenderableContentForEntry(&pb.CommsEntry{Channel: ch})
		rendered, err = templates.RenderContent(ch, rc)
		if err != nil {
			return fmt.Errorf("render content: %w", err)
		}
	} else {
		rendered, err = templates.Render(*usecase, ch, vendorFor(ch, e.cfg.Email), params)
		if err != nil {
			return fmt.Errorf("render %s on %s: %w", *usecase, ch, err)
		}
	}

	// Email is wrapped in the same chrome, for the usecase's category, as the
	// dispatcher wraps every send, so the preview is what recipients get.
	if ch == pb.Channel_CHANNEL_EMAIL {
		c, err := templates.Get(*usecase)
		if err != nil {
			return err
		}
		chrome := server.EmailChrome(e.cfg)
		chrome.Now = time.Now()
		rendered = templates.WrapEmail(rendered, c.Category, chrome)
	}

	if !*send {
		switch ch {
		case pb.Channel_CHANNEL_EMAIL:
			fmt.Fprintf(e.stdout, "subject: %s\n\n%s\n", rendered.GetEmailSubject(), rendered.GetEmailHtml())
			if rendered.GetEmailText() != "" {
				fmt.Fprintf(e.stdout, "\n--- text part ---\n%s", rendered.GetEmailText())
			}
		default:
			fmt.Fprintln(e.stdout, rendered.GetSlackBlocksJson())
		}
		return nil
	}

	if isProd(e.cfg.Env) {
		return fmt.Errorf("render --send is refused when OUTBOX_COMMS_ENV=%q", e.cfg.Env)
	}
	if key, ok := params.GetFields()[sender.AttachmentKeyParam]; ok && key.GetStringValue() != "" {
		return usageErr("render --send does not attach files; drop %s from --params", sender.AttachmentKeyParam)
	}
	s, err := e.senderFor(ctx, ch)
	if err != nil {
		return err
	}
	res := s.Send(ctx, dispatch.SendInput{OrgID: *org, Channel: ch, Vendor: vendorFor(ch, e.cfg.Email), Recipient: strings.TrimSpace(*to), Rendered: rendered, Params: params})
	if res.Outcome != dispatch.OutcomeSuccess {
		return fmt.Errorf("send to %s failed (outcome=%s): %w", maskEmail(*to), res.Outcome, res.Err)
	}
	fmt.Fprintf(e.stdout, "sent to %s, provider id %s\n", maskEmail(*to), res.ProviderMessageID)
	return nil
}

// senderFor builds the configured sender for a channel, as the service would.
func (e *env) senderFor(ctx context.Context, ch pb.Channel) (dispatch.Sender, error) {
	switch ch {
	case pb.Channel_CHANNEL_EMAIL:
		switch e.cfg.Email {
		case "smtp":
			return sender.NewSMTPSender(sender.SMTPConfig{Addr: e.cfg.SMTPAddr, Username: e.cfg.SMTPUsername, Password: e.cfg.SMTPPassword, From: e.cfg.EmailFrom}, nil), nil
		case "ses":
			ac, err := server.LoadAWSConfig(ctx)
			if err != nil {
				return nil, fmt.Errorf("load AWS config: %w", err)
			}
			return sender.NewEmailSender(sender.NewSESClient(ac), nil, e.cfg.EmailFrom, e.cfg.SESConfigurationSet), nil
		default:
			return nil, fmt.Errorf("email is disabled (OUTBOX_COMMS_EMAIL=%s)", e.cfg.Email)
		}
	default:
		client, err := slackclient.New(nil, e.cfg.SlackAPIURL)
		if err != nil {
			return nil, err
		}
		return sender.NewSlackSender(sender.StaticTokens{Default: e.cfg.SlackBotToken, PerOrg: e.cfg.SlackOrgTokens}, client, nil), nil
	}
}

// vendorFor is the vendor the service resolves for a channel under this config.
func vendorFor(ch pb.Channel, email string) pb.Vendor {
	if ch == pb.Channel_CHANNEL_SLACK {
		return pb.Vendor_VENDOR_SLACK
	}
	if email == "ses" {
		return pb.Vendor_VENDOR_SES
	}
	return pb.Vendor_VENDOR_SMTP
}

func parseChannel(s string) (pb.Channel, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "email":
		return pb.Channel_CHANNEL_EMAIL, nil
	case "slack":
		return pb.Channel_CHANNEL_SLACK, nil
	case "":
		return 0, usageErr("--channel is required (email or slack)")
	default:
		return 0, usageErr("--channel %q must be email or slack", s)
	}
}

func parseParams(s string) (*structpb.Struct, error) {
	st := &structpb.Struct{Fields: map[string]*structpb.Value{}}
	if strings.TrimSpace(s) == "" {
		return st, nil
	}
	if err := st.UnmarshalJSON([]byte(s)); err != nil {
		return nil, usageErr("--params is not a JSON object: %v", err)
	}
	return st, nil
}

// loadContent reads a MessageContent JSON file. Unknown fields are an error, so
// a typo cannot silently drop a block.
func loadContent(path string) (*pb.MessageContent, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // an operator-supplied path is the point of the flag
	if err != nil {
		return nil, fmt.Errorf("read content: %w", err)
	}
	c := &pb.MessageContent{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(raw, c); err != nil {
		return nil, fmt.Errorf("content %s is not valid MessageContent JSON: %w", path, err)
	}
	if c.GetBase() == nil {
		return nil, fmt.Errorf("content %s has no base", path)
	}
	return c, nil
}
