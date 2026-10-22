// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sender_test

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
	"testing"
	"time"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dispatch"
	"github.com/tetriz-ai/outbox-comms/internal/sender"
)

// scriptedSMTP is a one-connection-at-a-time SMTP relay that answers RCPT TO
// with rcptReply and the end of DATA with dataReply. An empty dataReply drops
// the connection after the body instead of answering; "stall" never answers and
// waits for the client to go.
type scriptedSMTP struct {
	addr      string
	rcptReply string
	dataReply string
	body      chan string
}

func newScriptedSMTP(t *testing.T, rcptReply, dataReply string) *scriptedSMTP {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	s := &scriptedSMTP{addr: ln.Addr().String(), rcptReply: rcptReply, dataReply: dataReply, body: make(chan string, 1)}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.serve(conn)
		}
	}()
	return s
}

func (s *scriptedSMTP) serve(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	r := bufio.NewReader(conn)
	say := func(line string) { _, _ = fmt.Fprintf(conn, "%s\r\n", line) }
	say("220 relay.example.com ready")
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		cmd := strings.ToUpper(strings.TrimSpace(line))
		switch {
		case strings.HasPrefix(cmd, "EHLO"), strings.HasPrefix(cmd, "HELO"):
			say("250 relay.example.com")
		case strings.HasPrefix(cmd, "MAIL FROM"):
			say("250 ok")
		case strings.HasPrefix(cmd, "RCPT TO"):
			say(s.rcptReply)
		case cmd == "DATA":
			say("354 go ahead")
			var b strings.Builder
			for {
				l, err := r.ReadString('\n')
				if err != nil {
					return
				}
				if l == ".\r\n" {
					break
				}
				b.WriteString(l)
			}
			select {
			case s.body <- b.String():
			default:
			}
			if s.dataReply == "" {
				return
			}
			if s.dataReply == "stall" {
				_, _ = r.ReadString('\n')
				return
			}
			say(s.dataReply)
		case cmd == "QUIT":
			say("221 bye")
			return
		default:
			say("250 ok")
		}
	}
}

func smtpInput() dispatch.SendInput {
	return dispatch.SendInput{
		Channel:   pb.Channel_CHANNEL_EMAIL,
		Recipient: "dev@example.com",
		Rendered: &pb.RenderedPayload{
			EmailSubject: "Hello",
			EmailHtml:    "<p>Hi there</p>",
			EmailText:    "Hi there",
		},
	}
}

// TestSMTPErrorClassification pins how the SMTP sender reads a relay's answer:
// a 4xx is transient and a 5xx permanent, before or after the body; a dropped
// connection after the body is uncertain, because the relay may have queued
// the message; and a relay that cannot be reached is transient.
func TestSMTPErrorClassification(t *testing.T) {
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closedAddr := closed.Addr().String()
	_ = closed.Close()

	tests := []struct {
		name     string
		addr     func(t *testing.T) string
		want     dispatch.Outcome
		wantCode string
	}{
		{"accepted", func(t *testing.T) string { return newScriptedSMTP(t, "250 ok", "250 queued").addr }, dispatch.OutcomeSuccess, ""},
		{"4xx at RCPT is transient", func(t *testing.T) string { return newScriptedSMTP(t, "450 mailbox busy", "250 queued").addr }, dispatch.OutcomeTransient, "smtp_450"},
		{"5xx at RCPT is permanent", func(t *testing.T) string { return newScriptedSMTP(t, "550 no such user", "250 queued").addr }, dispatch.OutcomePermanent, "smtp_550"},
		{"4xx after DATA is transient", func(t *testing.T) string { return newScriptedSMTP(t, "250 ok", "451 try later").addr }, dispatch.OutcomeTransient, "smtp_451"},
		{"5xx after DATA is permanent", func(t *testing.T) string { return newScriptedSMTP(t, "250 ok", "554 rejected").addr }, dispatch.OutcomePermanent, "smtp_554"},
		{"dropped after DATA is uncertain", func(t *testing.T) string { return newScriptedSMTP(t, "250 ok", "").addr }, dispatch.OutcomeUncertain, "smtp_uncertain"},
		{"dial failure is transient", func(*testing.T) string { return closedAddr }, dispatch.OutcomeTransient, "smtp_transport"},
		{"an address without a port is permanent", func(*testing.T) string { return "mailhost" }, dispatch.OutcomePermanent, "smtp_config"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := sender.NewSMTPSender(sender.SMTPConfig{Addr: tt.addr(t), From: "Example <noreply@example.com>", Timeout: 5 * time.Second}, nil)
			res := s.Send(context.Background(), smtpInput())
			if res.Outcome != tt.want {
				t.Fatalf("outcome = %v, want %v (err %v)", res.Outcome, tt.want, res.Err)
			}
			if res.ErrCode != tt.wantCode {
				t.Fatalf("error code = %q, want %q", res.ErrCode, tt.wantCode)
			}
			if tt.want == dispatch.OutcomeSuccess && !strings.HasPrefix(res.ProviderMessageID, "<") {
				t.Fatalf("provider id = %q, want the Message-ID", res.ProviderMessageID)
			}
		})
	}
}

// TestSMTPAlternativeMIME pins the body of a message without an attachment: a
// multipart/alternative with the plain text first and the HTML second.
func TestSMTPAlternativeMIME(t *testing.T) {
	relay := newScriptedSMTP(t, "250 ok", "250 queued")
	s := sender.NewSMTPSender(sender.SMTPConfig{Addr: relay.addr, From: "Example <noreply@example.com>", Timeout: 5 * time.Second}, nil)
	if res := s.Send(context.Background(), smtpInput()); res.Outcome != dispatch.OutcomeSuccess {
		t.Fatalf("send: %+v", res)
	}
	msg, err := mail.ReadMessage(strings.NewReader(<-relay.body))
	if err != nil {
		t.Fatal(err)
	}
	if got := msg.Header.Get("Subject"); got != "Hello" {
		t.Errorf("subject = %q", got)
	}
	mediaType, params, err := mime.ParseMediaType(msg.Header.Get("Content-Type"))
	if err != nil || mediaType != "multipart/alternative" {
		t.Fatalf("content type = %q (%v), want multipart/alternative", mediaType, err)
	}
	mr := multipart.NewReader(msg.Body, params["boundary"])
	var types, bodies []string
	for {
		p, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		mt, _, _ := mime.ParseMediaType(p.Header.Get("Content-Type"))
		var r io.Reader = p
		if strings.EqualFold(p.Header.Get("Content-Transfer-Encoding"), "base64") {
			r = base64.NewDecoder(base64.StdEncoding, p)
		}
		b, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		types = append(types, mt)
		bodies = append(bodies, strings.TrimSpace(string(b)))
	}
	if strings.Join(types, ",") != "text/plain,text/html" {
		t.Fatalf("parts = %v, want text/plain then text/html", types)
	}
	if bodies[0] != "Hi there" || !strings.Contains(bodies[1], "<p>Hi there</p>") {
		t.Fatalf("bodies = %q", bodies)
	}
}

// TestSMTPMessageSizeCap pins that SMTP applies the same 10 MiB cap on the
// assembled message as SES: an 8 MiB file ends permanent and the relay
// receives nothing.
func TestSMTPMessageSizeCap(t *testing.T) {
	relay := newScriptedSMTP(t, "250 ok", "250 queued")
	s := sender.NewSMTPSender(sender.SMTPConfig{Addr: relay.addr, From: "Example <noreply@example.com>", Timeout: 5 * time.Second}, fakeFetcher{data: make([]byte, 8*1024*1024)})
	in := smtpInput()
	in.Params = paramsWith(t, "comms/org/big.pdf")
	res := s.Send(context.Background(), in)
	if res.Outcome != dispatch.OutcomePermanent || res.ErrCode != "message_too_large" {
		t.Fatalf("outcome = %v code %q, want permanent message_too_large", res.Outcome, res.ErrCode)
	}
	select {
	case <-relay.body:
		t.Fatal("the relay received an over-cap message")
	default:
	}
}
