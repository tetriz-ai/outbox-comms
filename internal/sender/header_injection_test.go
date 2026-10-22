// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sender_test

import (
	"context"
	"net/mail"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/tetriz-ai/outbox-comms/internal/dispatch"
	"github.com/tetriz-ai/outbox-comms/internal/sender"
)

// TestHeaderInjection pins that a CR or LF in the recipient, the subject or the
// attachment's file name cannot add a header to the message the relay gets.
func TestHeaderInjection(t *testing.T) {
	relay := newScriptedSMTP(t, "250 ok", "250 queued")
	s := sender.NewSMTPSender(sender.SMTPConfig{Addr: relay.addr, From: "Example <noreply@example.com>", Timeout: 5 * time.Second}, fakeFetcher{data: []byte("%PDF-1.4")})
	in := smtpInput()
	in.Recipient = "jane@example.com\r\nBcc: to@example.net"
	in.Rendered.EmailSubject = "Hello\r\nBcc: subject@example.net"
	in.Params, _ = structpb.NewStruct(map[string]any{
		sender.AttachmentKeyParam:      "reports/r.pdf",
		sender.AttachmentFilenameParam: "r.pdf\r\nX-Injected: filename",
	})
	if res := s.Send(context.Background(), in); res.Outcome != dispatch.OutcomeSuccess {
		t.Fatalf("send: %v %v", res.Outcome, res.Err)
	}
	body := <-relay.body
	msg, err := mail.ReadMessage(strings.NewReader(body))
	if err != nil {
		t.Fatalf("parse the relayed message: %v", err)
	}
	for _, h := range []string{"Bcc", "X-Injected"} {
		if v := msg.Header.Get(h); v != "" {
			t.Errorf("the message gained a %s header: %q", h, v)
		}
	}
	if strings.Contains(body, "\r\nX-Injected:") || strings.Contains(body, "\r\nBcc:") {
		t.Errorf("an injected header line reached the relay:\n%s", body)
	}
}
