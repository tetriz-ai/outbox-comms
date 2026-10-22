// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package admin

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/mail"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
	"github.com/tetriz-ai/outbox-comms/internal/dispatch"
	"github.com/tetriz-ai/outbox-comms/templates"
)

const chromeContent = `{"base":{"subject":"Your export is ready","blocks":[{"paragraph":{"text":"The October export finished."}}]}}`

// capturingSender records what the dispatcher hands the provider.
type capturingSender struct{ in dispatch.SendInput }

func (c *capturingSender) Send(_ context.Context, in dispatch.SendInput) dispatch.SendResult {
	c.in = in
	return dispatch.SendResult{Outcome: dispatch.OutcomeSuccess, ProviderMessageID: "pmid-chrome"}
}

// TestRenderMatchesDispatch pins that admin render previews the email
// recipients get: its subject, HTML and text parts equal, byte for byte, what
// the dispatcher hands the provider for the same usecase, content and
// configuration, brand chrome included.
func TestRenderMatchesDispatch(t *testing.T) {
	ctx := context.Background()
	cfg := defaults(t)
	cfg.BrandName, cfg.SiteURL = "Example Corp", "https://example.com"
	file := write(t, "c.json", chromeContent)
	e, out, errOut := newEnv(t, cfg)
	if code := e.run(ctx, []string{"render", "--usecase", "notice", "--channel", "email", "--content", file}); code != 0 {
		t.Fatalf("render: exit %d, %s", code, errOut.String())
	}
	got := out.String()

	if testDB == nil {
		t.Skip("needs the test database")
	}
	tx := testDB.Begin()
	t.Cleanup(func() { tx.Rollback() })
	var content pb.MessageContent
	if err := protojson.Unmarshal([]byte(chromeContent), &content); err != nil {
		t.Fatal(err)
	}
	entry := &pb.CommsEntry{Channel: pb.Channel_CHANNEL_EMAIL, TargetKind: pb.TargetKind_TARGET_KIND_EMAIL_ADDRESS, Recipient: "x@example.com"}
	reqs, msgs := dao.NewRequestRepo(tx), dao.NewMessageRepo(tx)
	_, req, err := reqs.CreateIfAbsent(ctx, &model.CommsRequest{
		OrganizationID: org, IdempotencyKey: "render-equals-dispatch", Usecase: templates.UsecaseNotice, Content: &content,
		Entries: &pb.CommsEntries{Entries: []*pb.CommsEntry{entry}}, DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
		Status: pb.RequestStatus_REQUEST_STATUS_FANNED_OUT,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := msgs.MaterializeBatch(ctx, org, []*model.CommsMessage{{
		OrganizationID: org, RequestID: req.ID, Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SMTP, Recipient: "x@example.com",
		Status: pb.MessageStatus_MESSAGE_STATUS_QUEUED, VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK,
		DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, Content: req.RenderableContentForEntry(entry),
	}}); err != nil {
		t.Fatal(err)
	}
	rows, err := msgs.ListByRequest(ctx, org, req.ID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("message rows: %d, %v", len(rows), err)
	}
	sender := &capturingSender{}
	chrome := templates.EmailChromeOptions{
		BrandName: cfg.BrandName, SiteURL: cfg.SiteURL, LogoURL: cfg.LogoURL,
		PreferencesURL: cfg.PreferencesURL, PrivacyURL: cfg.PrivacyURL, TermsURL: cfg.TermsURL,
	}
	d := dispatch.NewDispatcher(msgs, reqs, dao.NewSuppressionRepo(tx), dao.NewDeliveryEventRepo(tx),
		map[pb.Channel]dispatch.Sender{pb.Channel_CHANNEL_EMAIL: sender}, dispatch.DefaultRetryPolicy(), nil, nil, time.Now, chrome)
	if err := d.Dispatch(ctx, org, rows[0].ID); err != nil {
		t.Fatal(err)
	}
	r := sender.in.Rendered
	want := fmt.Sprintf("subject: %s\n\n%s\n", r.GetEmailSubject(), r.GetEmailHtml())
	if r.GetEmailText() != "" {
		want += fmt.Sprintf("\n--- text part ---\n%s", r.GetEmailText())
	}
	if got != want {
		t.Fatalf("admin render differs from what dispatch sends.\nrender:\n%s\ndispatch:\n%s", got, want)
	}
	if !strings.Contains(r.GetEmailHtml(), "Example Corp") {
		t.Fatal("the dispatched email carries no brand chrome")
	}
}

// smtpCatcher is an SMTP server that keeps the DATA of every message.
func smtpCatcher(t *testing.T) (addr string, bodies func() []string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	var mu sync.Mutex
	var got []string
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				r := bufio.NewReader(c)
				say := func(s string) { _, _ = fmt.Fprintf(c, "%s\r\n", s) }
				say("220 catcher.example.com")
				var data *strings.Builder
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					switch cmd := strings.ToUpper(strings.TrimSpace(line)); {
					case data != nil && line == ".\r\n":
						mu.Lock()
						got = append(got, data.String())
						mu.Unlock()
						data = nil
						say("250 queued")
					case data != nil:
						data.WriteString(line)
					case cmd == "DATA":
						data = &strings.Builder{}
						say("354 go ahead")
					case cmd == "QUIT":
						say("221 bye")
						return
					default:
						say("250 ok")
					}
				}
			}(conn)
		}
	}()
	return ln.Addr().String(), func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), got...) }
}

// TestRenderSendCarriesTheChrome pins that render --send delivers the wrapped
// email: the body the SMTP server receives carries the brand name, as a
// dispatched email does.
func TestRenderSendCarriesTheChrome(t *testing.T) {
	addr, bodies := smtpCatcher(t)
	cfg := defaults(t)
	cfg.Env, cfg.SMTPAddr, cfg.BrandName = "staging", addr, "Example Corp"
	file := write(t, "c.json", chromeContent)
	e, _, errOut := newEnv(t, cfg)
	if code := e.run(context.Background(), []string{"render", "--usecase", "notice", "--channel", "email", "--content", file, "--send", "--to", "x@example.com"}); code != 0 {
		t.Fatalf("render --send: exit %d, %s", code, errOut.String())
	}
	b := bodies()
	if len(b) != 1 || !strings.Contains(decodeBody(b[0]), "Example Corp") {
		t.Fatalf("SMTP received %d message(s); want one carrying the brand chrome", len(b))
	}
}

// decodeBody returns the decoded text of every part of a raw MIME message.
func decodeBody(raw string) string {
	msg, err := mail.ReadMessage(strings.NewReader(raw))
	if err != nil {
		return ""
	}
	return decodePart(msg.Header.Get("Content-Type"), msg.Header.Get("Content-Transfer-Encoding"), msg.Body)
}

func decodePart(contentType, encoding string, body io.Reader) string {
	media, params, err := mime.ParseMediaType(contentType)
	if err == nil && strings.HasPrefix(media, "multipart/") {
		var b strings.Builder
		mr := multipart.NewReader(body, params["boundary"])
		for {
			p, err := mr.NextPart()
			if err != nil {
				return b.String()
			}
			b.WriteString(decodePart(p.Header.Get("Content-Type"), p.Header.Get("Content-Transfer-Encoding"), p))
		}
	}
	data, _ := io.ReadAll(body)
	if strings.EqualFold(encoding, "base64") {
		dec, _ := base64.StdEncoding.DecodeString(strings.NewReplacer("\r", "", "\n", "").Replace(string(data)))
		return string(dec)
	}
	return string(data)
}
