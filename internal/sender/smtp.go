// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sender

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/mail"
	"net/smtp"
	"net/textproto"
	"strings"
	"time"

	"github.com/tetriz-ai/outbox-comms/internal/dispatch"
)

// SMTPConfig is the relay an SMTPSender talks to.
type SMTPConfig struct {
	// Addr is host:port of the relay.
	Addr string
	// Username and Password enable PLAIN auth when Username is set. net/smtp
	// refuses PLAIN auth over an unencrypted connection to anything but
	// localhost, so a remote relay needs STARTTLS.
	Username string
	Password string
	// From is the envelope and header sender address.
	From string
	// Timeout bounds the whole SMTP exchange; zero means 30 seconds.
	Timeout time.Duration
}

// SMTPSender sends email through an SMTP relay. It has no delivery feedback: a
// relay's 250 reply is the strongest signal it gets, so a usecase that asks for
// CONFIRMED verification ends UNCONFIRMED(no_delivery_feedback) on SMTP.
type SMTPSender struct {
	cfg   SMTPConfig
	store ObjectFetcher
	now   func() time.Time
}

// NewSMTPSender constructs the SMTP egress. store may be nil when no producer
// attaches files.
func NewSMTPSender(cfg SMTPConfig, store ObjectFetcher) *SMTPSender {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	return &SMTPSender{cfg: cfg, store: store, now: time.Now}
}

var _ dispatch.Sender = (*SMTPSender)(nil)

// Send builds the MIME message (with the attachment when attachment_key is set)
// and hands it to the relay. The provider id is the Message-ID header it sets.
func (s *SMTPSender) Send(ctx context.Context, in dispatch.SendInput) dispatch.SendResult {
	var attachment []byte
	filename := ""
	if key := paramString(in, AttachmentKeyParam); key != "" {
		var fail *dispatch.SendResult
		attachment, fail = loadAttachment(ctx, s.store, in.OrgID, key)
		if fail != nil {
			return *fail
		}
		filename = attachmentDisplayName(in)
	}
	msgID, err := newMessageID(s.cfg.From)
	if err != nil {
		return dispatch.SendResult{Outcome: dispatch.OutcomeTransient, Err: err}
	}
	var body []byte
	if attachment != nil {
		body = buildMIME(s.cfg.From, in.Recipient, in.Rendered.GetEmailSubject(), in.Rendered.GetEmailHtml(), in.Rendered.GetEmailText(), attachment, filename)
	} else {
		body = buildAlternativeMIME(s.cfg.From, in.Recipient, in.Rendered.GetEmailSubject(), in.Rendered.GetEmailHtml(), in.Rendered.GetEmailText())
	}
	raw := fmt.Sprintf("Date: %s\r\nMessage-ID: %s\r\n", s.now().UTC().Format(time.RFC1123Z), msgID) + string(body)
	if fail := checkRawSize([]byte(raw)); fail != nil {
		return *fail
	}

	if err := s.deliver(ctx, in.Recipient, []byte(raw)); err != nil {
		return classifySMTPErr(err)
	}
	return dispatch.SendResult{ProviderMessageID: msgID, Outcome: dispatch.OutcomeSuccess}
}

// deliver runs one SMTP transaction under ctx and the configured timeout.
func (s *SMTPSender) deliver(ctx context.Context, to string, msg []byte) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()
	host, _, err := net.SplitHostPort(s.cfg.Addr)
	if err != nil {
		// A relay address without host:port fails the same way every time.
		return permanentSMTP(fmt.Errorf("smtp addr %q: %w", s.cfg.Addr, err))
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("smtp dial: %w", err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	c, err := smtp.NewClient(conn, host)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("smtp greeting: %w", err)
	}
	defer func() { _ = c.Close() }()
	if ok, _ := c.Extension("STARTTLS"); ok {
		if err := c.StartTLS(tlsConfig(host)); err != nil {
			return fmt.Errorf("smtp starttls: %w", err)
		}
	}
	if s.cfg.Username != "" {
		if err := c.Auth(smtp.PlainAuth("", s.cfg.Username, s.cfg.Password, host)); err != nil {
			return fmt.Errorf("smtp auth: %w", err)
		}
	}
	from, err := mail.ParseAddress(s.cfg.From)
	if err != nil {
		return permanentSMTP(fmt.Errorf("smtp from %q: %w", s.cfg.From, err))
	}
	if err := c.Mail(from.Address); err != nil {
		return fmt.Errorf("smtp MAIL FROM: %w", err)
	}
	if err := c.Rcpt(headerSafe(to)); err != nil {
		return fmt.Errorf("smtp RCPT TO: %w", err)
	}
	w, err := c.Data()
	if err != nil {
		return fmt.Errorf("smtp DATA: %w", err)
	}
	if _, err := w.Write(msg); err != nil {
		return uncertainSMTP(fmt.Errorf("smtp write body: %w", err))
	}
	if err := w.Close(); err != nil {
		// The relay may have queued the message before the error reached us.
		return uncertainSMTP(fmt.Errorf("smtp end of data: %w", err))
	}
	// The relay accepted the message at end of DATA; a failed QUIT changes nothing.
	_ = c.Quit()
	return nil
}

func tlsConfig(host string) *tls.Config {
	return &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}
}

type smtpPermanentError struct{ error }

func (e smtpPermanentError) Unwrap() error { return e.error }

func permanentSMTP(err error) error { return smtpPermanentError{err} }

type smtpUncertainError struct{ error }

func (e smtpUncertainError) Unwrap() error { return e.error }

func uncertainSMTP(err error) error { return smtpUncertainError{err} }

// classifySMTPErr maps an SMTP failure to a dispatch Outcome: a 5xx reply is
// permanent, a 4xx reply or a connection failure before DATA is transient, and a
// failure after the body was sent is uncertain (the relay may have queued it).
func classifySMTPErr(err error) dispatch.SendResult {
	var perm smtpPermanentError
	if errors.As(err, &perm) {
		return dispatch.SendResult{Outcome: dispatch.OutcomePermanent, Err: err, ErrCode: "smtp_config"}
	}
	var tp *textproto.Error
	if errors.As(err, &tp) {
		code := fmt.Sprintf("smtp_%d", tp.Code)
		switch {
		case tp.Code >= 500:
			return dispatch.SendResult{Outcome: dispatch.OutcomePermanent, Err: err, ErrCode: code}
		case tp.Code >= 400:
			return dispatch.SendResult{Outcome: dispatch.OutcomeTransient, Err: err, ErrCode: code}
		}
	}
	var unc smtpUncertainError
	if errors.As(err, &unc) {
		return dispatch.SendResult{Outcome: dispatch.OutcomeUncertain, Err: err, ErrCode: "smtp_uncertain"}
	}
	return dispatch.SendResult{Outcome: dispatch.OutcomeTransient, Err: err, ErrCode: "smtp_transport"}
}

// newMessageID returns a unique RFC 5322 Message-ID on the sender's domain.
func newMessageID(from string) (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("message id: %w", err)
	}
	domain := "localhost"
	if a, err := mail.ParseAddress(from); err == nil {
		if i := strings.LastIndex(a.Address, "@"); i >= 0 && i+1 < len(a.Address) {
			domain = a.Address[i+1:]
		}
	}
	return "<" + hex.EncodeToString(b) + "@" + domain + ">", nil
}
